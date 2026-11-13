#!/usr/bin/env bash
# Sets up the recording server, or updates it after a "git pull": every
# step of docs/deployment.md sections 3, 4 and 8 that can be done from the
# server itself.
#
#   deploy/install.sh [--bucket NAME] [--restart]
#
#   --bucket NAME  write /etc/polyrecorder/upload-hours.env with this
#                  bucket name (without it, an existing file is kept)
#   --restart      restart recorders that are already running, so they use
#                  a newly installed version (costs a couple of seconds of
#                  data); without it, running recorders are left alone
#
# Safe to run again: every step checks whether it's already done. If any
# test fails, it stops before installing anything.
#
# Run as your normal login, not as root: it builds as you, and uses sudo
# only for the steps that need it. Ubuntu only.
#
# Not done here, by design (see docs/deployment.md): the AWS side
# (instance, security group, IAM role), the Google side (bucket, identity
# pool), and putting the secrets in /etc/polyrecorder - those are only
# checked for, and services that need a missing one aren't started.

set -euo pipefail

cpp_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)  # trading_engine_cpp
build_dir="$cpp_dir/build"
bin_dir=/opt/polyrecorder/bin
data_dir=/var/lib/polyrecorder/data
etc_dir=/etc/polyrecorder
unit_dir=/etc/systemd/system

recorders=(binance clob polybolt)
programs=(binance_ws clob_ws polybolt_ws compress_hours)
tests=(recorder_test backoff_test hourly_file_sink_test compress_file_test compress_hours_test)
units=(binance-recorder.service clob-recorder.service polybolt-recorder.service
       compress-hours.service compress-hours.timer
       upload-hours.service upload-hours.timer)

bucket=""
restart=0

usage() {
    cat <<EOF
Usage: $0 [--bucket NAME] [--restart]
  --bucket NAME  write $etc_dir/upload-hours.env with this bucket name
  --restart      restart already-running recorders to use a new version
EOF
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

while (($#)); do
    case "$1" in
        --bucket)
            (($# >= 2)) || die "--bucket needs a value"
            bucket=$2; shift 2 ;;
        --restart)
            restart=1; shift ;;
        -h | --help)
            usage; exit 0 ;;
        *)
            usage >&2; die "unknown option: $1" ;;
    esac
done
if [[ -n $bucket && ! $bucket =~ ^[a-z0-9][a-z0-9._-]+[a-z0-9]$ ]]; then
    die "not a valid bucket name: $bucket"
fi

# Output: a heading per step, then one line per thing checked.
step() { printf '\n== %s\n' "$*"; }
ok()   { echo "   ok       $*"; }
did()  { echo "   done     $*"; }
note() { echo "   NOTE     $*"; }

# Things a person still has to do, repeated at the end.
todo=()

# --- 0. Where and as whom ---
step "Checks"
((EUID != 0)) || die "run as your normal login, not with sudo: it builds as you and asks for sudo itself"
# shellcheck source=/dev/null
[[ -r /etc/os-release ]] && . /etc/os-release
[[ ${ID:-} == ubuntu ]] || die "this script is for Ubuntu (found: ${ID:-unknown})"
ok "Ubuntu ${VERSION_ID:-}, running as $(id -un), repo at $cpp_dir"
# Ask for the sudo password once, up front.
sudo -v

# --- 1. Packages ---
step "Packages"
installed() { dpkg-query -W -f='${Status}' "$1" 2>/dev/null | grep -q "install ok installed"; }

packages=(build-essential cmake git pkg-config libssl-dev zlib1g-dev jq curl ca-certificates gnupg)
missing=()
for p in "${packages[@]}"; do
    installed "$p" || missing+=("$p")
done
if ((${#missing[@]})); then
    sudo apt-get update -q
    sudo apt-get install -y -q "${missing[@]}"
    did "installed ${missing[*]}"
else
    ok "build tools, jq, curl"
fi

# The Google Cloud CLI comes from Google's own package source.
if installed google-cloud-cli; then
    ok "google-cloud-cli"
else
    keyring=/usr/share/keyrings/cloud.google.gpg
    source_list=/etc/apt/sources.list.d/google-cloud-sdk.list
    [[ -f $keyring ]] || curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg |
        sudo gpg --dearmor -o "$keyring"
    [[ -f $source_list ]] ||
        echo "deb [signed-by=$keyring] https://packages.cloud.google.com/apt cloud-sdk main" |
        sudo tee "$source_list" >/dev/null
    sudo apt-get update -q
    sudo apt-get install -y -q google-cloud-cli
    did "installed google-cloud-cli"
fi

# --- 2. Build and test (as you, not root) ---
step "Build and test"
cmake -S "$cpp_dir" -B "$build_dir" -DCMAKE_BUILD_TYPE=Release >/dev/null
cmake --build "$build_dir" -j"$(nproc)"
for t in "${tests[@]}"; do
    (cd "$build_dir" && "./$t" >/dev/null) || die "$t failed - nothing installed. Run build/$t to see why."
    ok "$t"
done
"$cpp_dir/deploy/upload_hours_test.sh" >/dev/null ||
    die "upload_hours_test.sh failed - nothing installed. Run deploy/upload_hours_test.sh to see why."
ok "upload_hours_test.sh"

# --- 3. The service account ---
step "Service account"
if id polyrecorder &>/dev/null; then
    ok "account polyrecorder"
else
    sudo useradd --system --no-create-home --shell /usr/sbin/nologin polyrecorder
    did "created account polyrecorder"
fi
me=$(id -un)
if id -nG "$me" | grep -qw polyrecorder; then
    ok "$me is in group polyrecorder"
else
    sudo usermod -aG polyrecorder "$me"
    did "added $me to group polyrecorder (log out and back in to read the data)"
fi

# --- 4. Folders ---
# "install -d" creates a folder, or corrects an existing one's owner and
# mode, so running it every time keeps them right.
step "Folders"
sudo install -d -o root -g root -m 755 "$bin_dir"
sudo install -d -o polyrecorder -g polyrecorder -m 750 /var/lib/polyrecorder "$data_dir"
sudo install -d -o root -g root -m 700 "$etc_dir"
ok "$bin_dir (root, 755)"
ok "$data_dir (polyrecorder, 750)"
ok "$etc_dir (root, 700)"

# Copy a file into place (root-owned) only if it differs from what's
# there. Returns 0 if it was installed, 1 if it was already identical.
put() {  # put <source> <destination> <mode>
    if sudo cmp -s "$1" "$2"; then
        return 1
    fi
    sudo install -o root -g root -m "$3" "$1" "$2"
}

# --- 5. Programs ---
step "Programs"
new_recorders=()  # recorders whose program changed
for p in "${programs[@]}"; do
    if put "$build_dir/$p" "$bin_dir/$p" 755; then
        did "installed $p"
        if [[ $p == *_ws ]]; then
            new_recorders+=("${p%_ws}")
        fi
    else
        ok "$p (unchanged)"
    fi
done
if put "$cpp_dir/deploy/upload_hours.sh" "$bin_dir/upload_hours" 755; then
    did "installed upload_hours"
else
    ok "upload_hours (unchanged)"
fi

# --- 6. Clock ---
step "Clock"
if systemctl is-enabled --quiet chrony-wait.service; then
    ok "chrony-wait enabled"
else
    sudo systemctl enable --quiet chrony-wait.service
    did "enabled chrony-wait (recorders wait for a synced clock at boot)"
fi

# --- 7. Unit files ---
step "systemd units"
new_units=()
for u in "${units[@]}"; do
    if put "$cpp_dir/deploy/systemd/$u" "$unit_dir/$u" 644; then
        did "installed $u"
        new_units+=("$u")
    else
        ok "$u (unchanged)"
    fi
done
if ((${#new_units[@]})); then
    sudo systemctl daemon-reload
    did "systemd reloaded"
fi

# --- 8. Secrets and settings: checked, never created (except the bucket name) ---
step "Secrets and settings"

polybolt_ready=1
env_file="$etc_dir/polybolt.env"
if sudo test -f "$env_file"; then
    sudo chown root:root "$env_file"
    sudo chmod 600 "$env_file"
    for name in POLY_API_KEY POLY_SECRET POLY_PASSPHRASE; do
        if ! sudo grep -q "^$name=" "$env_file"; then
            note "$env_file has no $name"
            polybolt_ready=0
        fi
    done
    # The recording server never trades, so it must never hold these.
    for name in PRIVATE_KEY WALLET_ADDRESS; do
        if sudo grep -q "^$name=" "$env_file"; then
            note "$env_file contains $name - remove it: the recording server must not hold it"
            todo+=("remove $name from $env_file (deployment.md 3.5)")
        fi
    done
    ((polybolt_ready)) && ok "$env_file (root, 600)"
else
    polybolt_ready=0
fi
((polybolt_ready)) ||
    todo+=("put POLY_API_KEY, POLY_SECRET and POLY_PASSPHRASE in $env_file, then run again (deployment.md 3.5)")

upload_ready=1
if sudo test -f "$etc_dir/gcs-wif.json"; then
    ok "$etc_dir/gcs-wif.json"
else
    upload_ready=0
    todo+=("put the Google settings file in $etc_dir/gcs-wif.json, then run again (deployment.md 6.4-6.5)")
fi

bucket_file="$etc_dir/upload-hours.env"
if [[ -n $bucket ]]; then
    if sudo test -f "$bucket_file" && [[ $(sudo cat "$bucket_file") == "BUCKET=$bucket" ]]; then
        ok "$bucket_file (BUCKET=$bucket)"
    else
        printf 'BUCKET=%s\n' "$bucket" | sudo tee "$bucket_file" >/dev/null
        did "wrote $bucket_file (BUCKET=$bucket)"
    fi
fi
if sudo test -f "$bucket_file"; then
    sudo chown root:root "$bucket_file"
    sudo chmod 600 "$bucket_file"
    [[ -n $bucket ]] || ok "$bucket_file (kept)"
else
    upload_ready=0
    todo+=("run again with --bucket <name>, or create $bucket_file (deployment.md 8.2)")
fi

# The IAM role that lets the server into Google (deployment.md 6.3).
token=$(curl -s -m 2 -X PUT http://169.254.169.254/latest/api/token \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 60" || true)
if [[ -n $token ]] &&
   curl -s -m 2 -H "X-aws-ec2-metadata-token: $token" \
       http://169.254.169.254/latest/meta-data/iam/info | grep -q polyrecorder-gcs-uploader; then
    ok "IAM role polyrecorder-gcs-uploader attached"
else
    upload_ready=0
    todo+=("attach the IAM role polyrecorder-gcs-uploader to this instance (deployment.md 6.3)")
fi

# --- 9. Start ---
step "Start"

# Enable a recorder and start it if it isn't running. A running one is
# restarted only with --restart; otherwise, if its program or unit
# changed, say so.
start_recorder() {  # start_recorder <feed>
    local unit="$1-recorder.service"
    local changed=0
    if [[ " ${new_recorders[*]} " == *" $1 "* || " ${new_units[*]} " == *" $unit "* ]]; then
        changed=1
    fi
    sudo systemctl enable --quiet "$unit"
    if ! systemctl is-active --quiet "$unit"; then
        sudo systemctl start "$unit"
        did "started $unit"
    elif ((restart)); then
        sudo systemctl restart "$unit"
        did "restarted $unit"
    elif ((changed)); then
        note "$unit runs the old version: restart when convenient (or run again with --restart)"
    else
        ok "$unit running"
    fi
}

start_timer() {  # start_timer <name>
    sudo systemctl enable --quiet --now "$1.timer"
    ok "$1.timer enabled"
}

start_recorder binance
start_recorder clob
if ((polybolt_ready)); then
    start_recorder polybolt
else
    note "polybolt-recorder not started: its credentials are missing"
fi
start_timer compress-hours
if ((upload_ready)); then
    start_timer upload-hours
else
    note "upload-hours not started: the Google link isn't complete"
fi

# --- Summary ---
step "Summary"
for u in "${units[@]}"; do
    printf '   %-28s %-10s %s\n' "$u" \
        "$(systemctl is-enabled "$u" 2>/dev/null || true)" \
        "$(systemctl is-active "$u" 2>/dev/null || true)"
done

if ((${#todo[@]})); then
    echo
    echo "Still to do by hand:"
    for t in "${todo[@]}"; do
        echo " - $t"
    done
    exit 1
fi
echo
echo "All set."
