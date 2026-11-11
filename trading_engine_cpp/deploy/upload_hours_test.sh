#!/usr/bin/env bash
# Tests for upload_hours.sh, against a fake Google: stand-in "gcloud" and
# "curl" commands that keep the "bucket" in a local folder. Nothing
# touches the network or the real bucket.
#
#   ./upload_hours_test.sh
#
# Linux only, like the script itself. Needs jq and flock.
# Prints "all checks passed" and exits 0, or names each failed check and
# exits 1.

set -uo pipefail

script="$(cd "$(dirname "$0")" && pwd)/upload_hours.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
data="$tmp/data"
export FAKE_BUCKET="$tmp/bucket"

failures=0
check() {  # check "<what should be true>" <command...>
    local what=$1
    shift
    if ! "$@"; then
        echo "FAILED: $what"
        failures=$((failures + 1))
    fi
}

# --- The fake Google ---
mkdir -p "$tmp/bin"

# Fake gcloud: hands out a token, copies uploads into the bucket folder,
# and fingerprints files (with sha256 standing in for CRC32C - the script
# only ever compares fingerprints, never decodes them).
cat >"$tmp/bin/gcloud" <<'EOF'
#!/usr/bin/env bash
fingerprint() { sha256sum "$1" | cut -c1-16; }
case "$1 $2" in
    "auth print-access-token")
        echo "fake-token" ;;
    "storage cp")
        # Uploading in parts would need delete permission on the real bucket.
        [[ ${CLOUDSDK_STORAGE_PARALLEL_COMPOSITE_UPLOAD_ENABLED:-} == False ]] || exit 1
        [[ -z ${FAKE_UPLOAD_FAIL:-} ]] || exit 1
        source=${@: -2:1}
        object=${@: -1}
        object=${object#gs://test-bucket/}
        [[ -e $FAKE_BUCKET/$object ]] && exit 0   # --no-clobber: skip
        mkdir -p "$(dirname "$FAKE_BUCKET/$object")"
        cp "$source" "$FAKE_BUCKET/$object" ;;
    "storage hash")
        fingerprint "${@: -1}" ;;
    *)
        exit 1 ;;
esac
EOF

# Fake curl: answers a Cloud Storage listing from the bucket folder, in the
# real API's shape ({} for an empty day), FAKE_PAGE_SIZE objects per page.
cat >"$tmp/bin/curl" <<'EOF'
#!/usr/bin/env bash
fingerprint() { sha256sum "$1" | cut -c1-16; }
prefix="" page=0 header="" url=""
while (($#)); do
    case "$1" in
        --data-urlencode)
            case "$2" in
                prefix=*)    prefix=${2#prefix=} ;;
                pageToken=*) page=${2#pageToken=} ;;
            esac
            shift 2 ;;
        -H)
            header=$(cat "${2#@}"); shift 2 ;;
        -*)
            shift ;;
        *)
            url=$1; shift ;;
    esac
done
if [[ -n ${FAKE_LIST_FAIL:-} ]]; then
    echo '{"error": {"code": 503, "message": "fake outage"}}'
    exit 22
fi
[[ $header == "Authorization: Bearer fake-token" ]] || exit 22
[[ $url == "https://storage.googleapis.com/storage/v1/b/test-bucket/o" ]] || exit 22

lines=()
if [[ -d $FAKE_BUCKET/$prefix ]]; then
    while IFS= read -r path; do
        lines+=("${path#"$FAKE_BUCKET"/}"$'\t'"$(stat -c %s "$path")"$'\t'"$(fingerprint "$path")")
    done < <(find "$FAKE_BUCKET/$prefix" -type f | sort)
fi
size=${FAKE_PAGE_SIZE:-1000}
next=""
((page + size < ${#lines[@]})) && next=$((page + size))
printf '%s\n' "${lines[@]:page:size}" | jq -R -s --arg next "$next" '
    split("\n") | map(select(length > 0) | split("\t")
                      | {name: .[0], size: .[1], crc32c: .[2]})
    | (if length == 0 then {} else {items: .} end)
    + (if $next == "" then {} else {nextPageToken: $next} end)'
EOF
chmod +x "$tmp/bin/gcloud" "$tmp/bin/curl"
export PATH="$tmp/bin:$PATH"

# --- Helpers ---
reset() {
    rm -rf "$data" "$FAKE_BUCKET"
    mkdir -p "$data" "$FAKE_BUCKET"
    unset FAKE_LIST_FAIL FAKE_UPLOAD_FAIL FAKE_PAGE_SIZE
}

# Run the script; its output goes to $out, its exit code to $code.
run() {
    code=0
    out=$("$script" --data-dir "$data" --bucket test-bucket "$@" 2>&1) || code=$?
}

# hour_name <feed> <date expression> -> e.g. clob_2026-11-10T17.jsonl.zst
hour_name() {
    echo "$1_$(date -u -d "$2" +%Y-%m-%dT%H).jsonl.zst"
}

# A local recording: data/<feed>/<name>, with some content.
make_local() {  # make_local <feed> <name> [content]
    mkdir -p "$data/$1"
    printf '%s' "${3:-content of $2}" >"$data/$1/$2"
}

# The bucket path the script should use for a local file.
bucket_path() {  # bucket_path <feed> <name>
    local day=${2#"$1"_}
    echo "$FAKE_BUCKET/$1/date=${day%%T*}/$2"
}

# Put a copy of a local file into the bucket (optionally with other content).
make_remote() {  # make_remote <feed> <name> [content]
    local path
    path=$(bucket_path "$1" "$2")
    mkdir -p "$(dirname "$path")"
    if (($# >= 3)); then
        printf '%s' "$3" >"$path"
    else
        cp "$data/$1/$2" "$path"
    fi
}

contains() { [[ $1 == *"$2"* ]]; }

young=$(hour_name clob "yesterday")
# Two hours of the same day, 30 days ago (same bucket folder).
old_day=$(date -u -d "30 days ago" +%Y-%m-%d)
old="clob_${old_day}T05.jsonl.zst"
old2="clob_${old_day}T06.jsonl.zst"

# --- Bad usage: exit 2 ---
reset
code=0; "$script" >/dev/null 2>&1 || code=$?
check "no options -> exit 2" test "$code" -eq 2
run --keep-days 0;          check "--keep-days 0 -> exit 2" test "$code" -eq 2
run --keep-days 08;         check "--keep-days 08 -> exit 2" test "$code" -eq 2
run --keep-days 7x;         check "--keep-days 7x -> exit 2" test "$code" -eq 2
run --verbos;               check "unknown option -> exit 2" test "$code" -eq 2
code=0; "$script" --data-dir "$data" --bucket Bad_Name >/dev/null 2>&1 || code=$?
check "invalid bucket name -> exit 2" test "$code" -eq 2
code=0; "$script" --data-dir "$tmp/missing" --bucket test-bucket >/dev/null 2>&1 || code=$?
check "missing data folder -> exit 2" test "$code" -eq 2

# --- Nothing there ---
reset
run
check "empty data folder -> exit 0" test "$code" -eq 0
check "empty data folder -> 'nothing to do'" contains "$out" "nothing to do"

# --- A new file is uploaded, and kept locally ---
reset
make_local clob "$young"
run
check "new file -> exit 0" test "$code" -eq 0
check "new file -> uploaded to <feed>/date=<day>/" cmp -s "$data/clob/$young" "$(bucket_path clob "$young")"
check "new file -> still on disk" test -e "$data/clob/$young"
check "new file -> '1 uploaded'" contains "$out" "1 uploaded, 0 deleted, 0 failed"

# A second pass finds it in the bucket and leaves it alone (too young).
run
check "second pass -> exit 0" test "$code" -eq 0
check "second pass -> 'nothing to do'" contains "$out" "nothing to do"
check "second pass -> young file kept" test -e "$data/clob/$young"

# --- An old file the bucket holds intact is deleted ---
reset
make_local clob "$old"
make_remote clob "$old"
run
check "old matching file -> exit 0" test "$code" -eq 0
check "old matching file -> deleted locally" test ! -e "$data/clob/$old"
check "old matching file -> still in bucket" test -e "$(bucket_path clob "$old")"
check "old matching file -> '1 deleted'" contains "$out" "0 uploaded, 1 deleted, 0 failed"

# --- An old file the bucket lacks: uploaded, deleted only on a later pass ---
reset
make_local clob "$old"
run
check "old missing file -> uploaded" cmp -s "$data/clob/$old" "$(bucket_path clob "$old")"
check "old missing file -> not deleted in the same pass" test -e "$data/clob/$old"
run
check "old missing file -> deleted on the next pass" test ! -e "$data/clob/$old"

# --- --keep-days: an hour 2 days old is kept with 7, deleted with 1 ---
reset
two_days=$(hour_name clob "2 days ago")
make_local clob "$two_days"
make_remote clob "$two_days"
run
check "2-day-old file, default 7 days -> kept" test -e "$data/clob/$two_days"
run --keep-days 1
check "2-day-old file, --keep-days 1 -> deleted" test ! -e "$data/clob/$two_days"

# --- Size differs: kept, exit 1 ---
reset
make_local clob "$old" "local content"
make_remote clob "$old" "different and longer content"
run
check "size mismatch -> exit 1" test "$code" -eq 1
check "size mismatch -> kept" test -e "$data/clob/$old"
check "size mismatch -> says why" contains "$out" "NOT deleting $old: bucket has"

# --- Same size, different content (fingerprint differs): kept, exit 1 ---
reset
make_local clob "$old" "aaaa"
make_remote clob "$old" "bbbb"
run
check "fingerprint mismatch -> exit 1" test "$code" -eq 1
check "fingerprint mismatch -> kept" test -e "$data/clob/$old"
check "fingerprint mismatch -> says why" contains "$out" "NOT deleting $old: CRC32C differs"

# --- Listing fails: nothing uploaded, nothing deleted, exit 1 ---
reset
make_local clob "$old"
make_remote clob "$old"
make_local clob "$young"
export FAKE_LIST_FAIL=1
run
check "listing fails -> exit 1" test "$code" -eq 1
check "listing fails -> old file kept" test -e "$data/clob/$old"
check "listing fails -> nothing uploaded" test ! -e "$(bucket_path clob "$young")"
check "listing fails -> says nothing deleted" contains "$out" "nothing deleted"

# --- Upload fails: exit 1, and the rest of the pass still runs ---
reset
make_local clob "$young"
make_local clob "$old"
make_remote clob "$old"
export FAKE_UPLOAD_FAIL=1
run
check "upload fails -> exit 1" test "$code" -eq 1
check "upload fails -> says so" contains "$out" "FAILED to upload $young"
check "upload fails -> other files still handled" test ! -e "$data/clob/$old"

# --- Files that aren't finished recordings are ignored ---
reset
mkdir -p "$data/clob" "$data/binance"
touch "$data/clob/${young%.zst}"                  # .jsonl, not compressed yet
touch "$data/clob/$young.tmp"                     # compression in progress
touch "$data/binance/$young"                      # clob file in the binance folder
touch "$data/clob/clob_2026-11-10T1.jsonl.zst"    # malformed hour
touch "$data/clob/notes.txt"
run
check "other files -> exit 0" test "$code" -eq 0
check "other files -> 'nothing to do'" contains "$out" "nothing to do"
check "other files -> bucket still empty" test -z "$(find "$FAKE_BUCKET" -type f)"

# --- Dry run: reports, changes nothing ---
reset
make_local clob "$young"
make_local clob "$old"
make_remote clob "$old"
run --dry-run
check "dry run -> exit 0" test "$code" -eq 0
check "dry run -> reports the upload" contains "$out" "would upload $young"
check "dry run -> reports the delete" contains "$out" "would delete $old"
check "dry run -> nothing uploaded" test ! -e "$(bucket_path clob "$young")"
check "dry run -> nothing deleted" test -e "$data/clob/$old"

# --- A listing over several pages is read to the end ---
reset
make_local clob "$old"
make_local clob "$old2"
make_remote clob "$old"
make_remote clob "$old2"
export FAKE_PAGE_SIZE=1
run
check "paged listing -> exit 0" test "$code" -eq 0
check "paged listing -> first page's file deleted" test ! -e "$data/clob/$old"
check "paged listing -> second page's file deleted" test ! -e "$data/clob/$old2"

# --- Several feeds, each to its own folder ---
reset
binance_young=$(hour_name binance "yesterday")
make_local clob "$young"
make_local binance "$binance_young"
run
check "two feeds -> clob uploaded" test -e "$(bucket_path clob "$young")"
check "two feeds -> binance uploaded" test -e "$(bucket_path binance "$binance_young")"

# --- A pass already running: skip, exit 0 ---
reset
make_local clob "$young"
: >"$data/.upload_hours.lock"
exec 8<"$data/.upload_hours.lock"
flock -n 8
run
exec 8<&-
check "locked -> exit 0" test "$code" -eq 0
check "locked -> says it skipped" contains "$out" "skipping"
check "locked -> nothing uploaded" test ! -e "$(bucket_path clob "$young")"

if ((failures == 0)); then
    echo "all checks passed"
    exit 0
fi
echo "$failures checks failed"
exit 1
