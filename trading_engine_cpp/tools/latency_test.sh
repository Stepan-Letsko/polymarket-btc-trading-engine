#!/usr/bin/env bash
# Measures the round trip from this machine to a server: how long a request
# takes to get its first byte of answer back, over one reused connection.
#
#   ./tools/latency_test.sh [url] [count]
#
#   url    default https://clob.polymarket.com/time - an answer the front
#          door (Cloudflare) can't serve from memory, so every request
#          travels all the way to Polymarket's own server and back
#   count  default 1000 requests, sent 5 per second (~3.5 minutes) - gentle
#          enough to stay far below rate limits
#
# Run it on each machine being compared AT THE SAME TIME, so both see the
# same conditions on the server's side. Needs only bash and curl 7.84+.

set -euo pipefail

URL="${1:-https://clob.polymarket.com/time}"
COUNT="${2:-1000}"
RATE="5/s"

if ! [[ "$COUNT" =~ ^[0-9]+$ ]] || [ "$COUNT" -lt 10 ]; then
    echo "count must be a whole number, at least 10" >&2
    exit 1
fi

echo "machine:  $(hostname)"
echo "started:  $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "target:   $URL"

# Which front-door location answers us (Cloudflare's cf-ray ends in a
# location code, CloudFront names its "pop") - useful context, as each
# machine may be served from a different one.
curl -sI --max-time 5 "$URL" 2>/dev/null | grep -iE '^(server|cf-ray|x-amz-cf-pop):' | sed 's/^/          /' || true

# One request = "-o /dev/null <url>", repeated. curl sends them one after
# another over the SAME connection (as a trading client would), paced by
# --rate. Request 0 opens the connection and is not counted as a sample.
args=()
for ((i = 0; i <= COUNT; i++)); do
    args+=(-o /dev/null "$URL")
done

# Per request, curl prints (all in seconds, from the start of that request):
#   num_connects    1 if this request had to open a connection, else 0
#   http_code       the answer's status, e.g. 200
#   time_namelookup DNS done (the name turned into an IP address)
#   time_connect    TCP handshake done. Minus the DNS time, that is one
#                   round trip to the front door
#   time_pretransfer  ready to send (connection + encryption set up)
#   time_starttransfer  first byte of the answer arrived
# For a reused connection, starttransfer - pretransfer is exactly
# "question out -> first byte of answer back".
#
# --http1.1: every machine uses the same protocol, so a difference in the
# results comes from the network, not from curl choosing differently.
echo "sending:  $((COUNT + 1)) requests at $RATE (~$(( (COUNT + 1) / 5 / 60 + 1 )) min)..."
results=$(curl -s --http1.1 --rate "$RATE" --max-time 600 \
    -w '%{num_connects} %{http_code} %{time_namelookup} %{time_connect} %{time_pretransfer} %{time_starttransfer}\n' \
    "${args[@]}")

# Hand everything to awk: check each sample, then sort and summarise.
echo "$results" | awk -v count="$COUNT" '
    NR == 1 {
        connect_ms = ($4 - $3) * 1000   # the opening request: TCP handshake only, DNS excluded
        next
    }
    {
        if ($1 != 0) { reconnects++; next }   # connection was reopened: not comparable, skip
        if ($2 != 200) { bad[$2]++; next }    # e.g. 429 = rate limited
        samples[++n] = ($6 - $5) * 1000
    }
    END {
        if (n == 0) { print "no usable samples"; exit 1 }

        # Sort the samples (insertion sort: awk has no built-in sort).
        for (i = 2; i <= n; i++) {
            v = samples[i]
            for (j = i - 1; j >= 1 && samples[j] > v; j--) samples[j + 1] = samples[j]
            samples[j + 1] = v
        }
        # Percentile by rank: p90 = the sample 90% of the way up the sorted list.
        split("50 90 99", ps, " ")
        for (k = 1; k <= 3; k++) {
            r = int(ps[k] / 100 * n + 0.999999); if (r < 1) r = 1
            pct[ps[k]] = samples[r]
        }

        printf "\nfront door only (TCP connect): %8.2f ms\n", connect_ms
        printf "full round trip, %d samples:\n", n
        printf "  min %.2f   median %.2f   p90 %.2f   p99 %.2f   max %.2f ms\n",
               samples[1], pct[50], pct[90], pct[99], samples[n]

        if (reconnects > 0) printf "note: %d requests had to reopen the connection (skipped)\n", reconnects
        for (code in bad)   printf "note: %d answers had status %s (skipped)\n", bad[code], code
        if (n < count * 0.95) { print "WARNING: fewer than 95% of requests usable - treat with care"; exit 1 }
    }'
