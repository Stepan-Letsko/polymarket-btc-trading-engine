#!/usr/bin/env bash
# One pass of uploading compressed hourly recordings to Google Cloud
# Storage, and of deleting the local copies the bucket is proven to hold.
#
#   upload_hours.sh --data-dir DIR --bucket NAME [--keep-days N] [--dry-run]
#
# 1. Finds every <feed>/<feed>_YYYY-MM-DDTHH.jsonl.zst under --data-dir.
#    Nothing else is touched: .jsonl files are still being recorded or
#    compressed, .tmp files are a compression in progress.
# 2. Lists, once, what the bucket holds for those days, with each file's
#    size and CRC32C fingerprint. If that fails, the pass stops before
#    deleting anything.
# 3. Uploads every file the bucket lacks, to
#    gs://<bucket>/<feed>/date=YYYY-MM-DD/<file>, never overwriting.
# 4. Deletes a local file only when its hour ended more than --keep-days
#    ago AND the bucket holds it with the same size AND the same CRC32C.
#
# Credentials are whatever gcloud is set up with. On the server,
# CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE points it at the Workload
# Identity Federation settings file (docs/deployment.md, section 6).
#
# Linux only (GNU date and stat). Needs gcloud, curl, jq and flock.
#
# Exit code: 0 if everything succeeded, 1 if anything failed (the next
# pass retries it), 2 for bad usage.

# Stop at the first failed command, unset variable, or failure inside a
# pipe - so "couldn't list the bucket" can never lead on to deleting.
set -euo pipefail

# Upload every file in one piece. Above 150 MB gcloud would otherwise
# split it into parts and join them in the bucket, which needs delete
# permission - deliberately not granted to the server.
export CLOUDSDK_STORAGE_PARALLEL_COMPOSITE_UPLOAD_ENABLED=False

data_dir=""
bucket=""
keep_days=7
dry_run=0

usage() {
    cat <<EOF
Usage: $0 --data-dir DIR --bucket NAME [--keep-days N] [--dry-run]
  --data-dir   folder holding <feed>/<feed>_YYYY-MM-DDTHH.jsonl.zst (required)
  --bucket     Cloud Storage bucket name, without gs:// (required)
  --keep-days  keep local copies this many days after their hour (default 7)
  --dry-run    print what would be uploaded and deleted; change nothing
EOF
}

# Bad usage: say why, exit 2.
die() {
    echo "$*" >&2
    exit 2
}

while (($#)); do
    case "$1" in
        --data-dir)
            (($# >= 2)) || die "--data-dir needs a value"
            data_dir=$2; shift 2 ;;
        --bucket)
            (($# >= 2)) || die "--bucket needs a value"
            bucket=$2; shift 2 ;;
        --keep-days)
            (($# >= 2)) || die "--keep-days needs a value"
            keep_days=$2; shift 2 ;;
        --dry-run)
            dry_run=1; shift ;;
        -h | --help)
            usage; exit 0 ;;
        *)
            usage >&2; die "unknown option: $1" ;;
    esac
done

[[ -n $data_dir ]] || { usage >&2; die "--data-dir is required"; }
[[ -d $data_dir ]] || die "not a folder: $data_dir"
[[ -n $bucket ]] || { usage >&2; die "--bucket is required"; }
# Google's naming rules, roughly; also keeps anything odd out of the URLs.
[[ $bucket =~ ^[a-z0-9][a-z0-9._-]+[a-z0-9]$ ]] || die "not a valid bucket name: $bucket"
# Whole days, at least 1, no leading zero (bash would read "08" as octal).
[[ $keep_days =~ ^[1-9][0-9]*$ ]] || die "--keep-days must be a whole number of days, at least 1"

# One pass at a time, even if one is started by hand during a timer pass.
# Opened read-only, which is enough for a lock, so it works whichever
# account created the file.
lock="$data_dir/.upload_hours.lock"
[[ -e $lock ]] || : >"$lock"
exec 9<"$lock"
if ! flock -n 9; then
    echo "another pass is still running; skipping"
    exit 0
fi

# Bytes -> "120.45" (MB), for the log.
mb() {
    awk -v b="$1" 'BEGIN { printf "%.2f", b / 1e6 }'
}

# --- 1. Local files ---
# The same strict name rule as compress_hours, and the feed in the name
# must match the folder it's in.
name_re='^([a-z0-9]+)_([0-9]{4}-[0-9]{2}-[0-9]{2})T([0-9]{2})\.jsonl\.zst$'
files=()
declare -A prefixes=()  # "<feed>/date=YYYY-MM-DD/" for every local day
shopt -s nullglob
for path in "$data_dir"/*/*.jsonl.zst; do
    name=${path##*/}
    folder=${path%/*}
    folder=${folder##*/}
    [[ $name =~ $name_re ]] || continue
    [[ ${BASH_REMATCH[1]} == "$folder" ]] || continue
    files+=("$path")
    prefixes["${BASH_REMATCH[1]}/date=${BASH_REMATCH[2]}/"]=1
done

if ((${#files[@]} == 0)); then
    echo "nothing to do"
    exit 0
fi

# --- 2. What the bucket holds, for the local days only ---
# Through Cloud Storage's JSON API: an empty day is an empty answer, where
# "gcloud storage ls" would report an error. The 1-hour access token
# comes from gcloud, and reaches curl through a pipe rather than its
# command line, where any account on the machine could read it.
token=$(gcloud auth print-access-token)
declare -A remote_size=() remote_crc=()
for prefix in "${!prefixes[@]}"; do
    page=""
    while :; do
        query=(--data-urlencode "prefix=$prefix"
               --data-urlencode "fields=items(name,size,crc32c),nextPageToken")
        [[ -z $page ]] || query+=(--data-urlencode "pageToken=$page")
        if ! response=$(curl -sS --fail-with-body --get "${query[@]}" \
                -H @<(printf 'Authorization: Bearer %s' "$token") \
                "https://storage.googleapis.com/storage/v1/b/$bucket/o"); then
            echo "cannot list gs://$bucket/$prefix, nothing deleted: $response" >&2
            exit 1
        fi
        # A reply that isn't valid JSON stops the pass, like a failed request.
        if ! items=$(jq -r '.items[]? | [.name, .size, .crc32c] | @tsv' <<<"$response") ||
           ! page=$(jq -r '.nextPageToken // empty' <<<"$response"); then
            echo "unreadable listing for gs://$bucket/$prefix, nothing deleted" >&2
            exit 1
        fi
        while IFS=$'\t' read -r object size crc; do
            [[ -n $object ]] || continue
            remote_size[$object]=$size
            remote_crc[$object]=$crc
        done <<<"$items"
        [[ -n $page ]] || break
    done
done

# --- 3 and 4. Upload what's missing, delete what's proven safe ---
uploaded=0
deleted=0
failed=0
now=$(date +%s)
for path in "${files[@]}"; do
    name=${path##*/}
    [[ $name =~ $name_re ]]
    feed=${BASH_REMATCH[1]}
    day=${BASH_REMATCH[2]}
    hour=${BASH_REMATCH[3]}
    object="$feed/date=$day/$name"
    local_size=$(stat -c %s "$path")

    if [[ -z ${remote_size[$object]+set} ]]; then
        if ((dry_run)); then
            echo "would upload $name ($(mb "$local_size") MB) to gs://$bucket/$object"
            uploaded=$((uploaded + 1))
            continue
        fi
        # --no-clobber: never replace something already there. gcloud
        # checks the CRC32C of what arrived against the local file.
        start=$(date +%s.%N)
        if gcloud storage cp --no-clobber --no-user-output-enabled "$path" "gs://$bucket/$object"; then
            seconds=$(awk -v a="$start" -v b="$(date +%s.%N)" 'BEGIN { printf "%.1f", b - a }')
            echo "uploaded $name: $(mb "$local_size") MB in ${seconds}s"
            uploaded=$((uploaded + 1))
        else
            echo "FAILED to upload $name" >&2
            failed=$((failed + 1))
        fi
        # Deleting waits for a later pass, which lists the bucket afresh.
        continue
    fi

    # In the bucket. Old enough to delete? Judged by the hour in the
    # name (when it was recorded), not by when the file was last touched.
    hour_end=$(($(date -u -d "$day $hour:00:00" +%s) + 3600))
    ((now - hour_end >= keep_days * 86400)) || continue

    if [[ ${remote_size[$object]} != "$local_size" ]]; then
        echo "NOT deleting $name: bucket has ${remote_size[$object]} bytes, local file $local_size" >&2
        failed=$((failed + 1))
        continue
    fi
    # Read the whole file once, only now that it's due for deletion.
    if ! local_crc=$(gcloud storage hash --skip-md5 --format='value(crc32c_hash)' "$path"); then
        echo "NOT deleting $name: cannot compute its CRC32C" >&2
        failed=$((failed + 1))
        continue
    fi
    if [[ $local_crc != "${remote_crc[$object]}" ]]; then
        echo "NOT deleting $name: CRC32C differs (bucket ${remote_crc[$object]}, local $local_crc)" >&2
        failed=$((failed + 1))
        continue
    fi

    if ((dry_run)); then
        echo "would delete $name (in bucket, CRC32C matches)"
    else
        rm -- "$path"
        echo "deleted $name (in bucket, CRC32C matches)"
    fi
    deleted=$((deleted + 1))
done

if ((dry_run)); then
    echo "dry run: $uploaded to upload, $deleted to delete, $failed problems"
elif ((uploaded + deleted + failed == 0)); then
    echo "nothing to do"
else
    echo "$uploaded uploaded, $deleted deleted, $failed failed"
fi
((failed == 0)) || exit 1
