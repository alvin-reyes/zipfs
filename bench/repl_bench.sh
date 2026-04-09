#!/usr/bin/env bash
# Replication Benchmark for zipfs cluster
# Uploads files to one node, measures time until peers can serve them.
#
# Usage: ./bench/repl_bench.sh [upload_node_url] [peer1_url] [peer2_url]
#
# Defaults to the 3 Railway nodes.

set -euo pipefail

UPLOAD_NODE="${1:-https://zipfs-node3-production.up.railway.app}"
PEER1="${2:-https://zipfs-production.up.railway.app}"
PEER2="${3:-https://zipfs-node2-production.up.railway.app}"

POLL_INTERVAL=0.5   # seconds between polls
MAX_WAIT=120        # max seconds to wait for replication
SIZES=(1024 10240 102400 1048576 10485760)  # 1KB 10KB 100KB 1MB 10MB
SIZE_LABELS=("1KB" "10KB" "100KB" "1MB" "10MB")
RESULTS_FILE="/tmp/zipfs_repl_bench_$(date +%s).csv"

echo "=============================================="
echo "  zipfs Replication Benchmark"
echo "=============================================="
echo "Upload node:  $UPLOAD_NODE"
echo "Peer 1:       $PEER1"
echo "Peer 2:       $PEER2"
echo "Poll interval: ${POLL_INTERVAL}s"
echo "Max wait:     ${MAX_WAIT}s"
echo "File sizes:   ${SIZE_LABELS[*]}"
echo "Results:      $RESULTS_FILE"
echo "=============================================="
echo ""

# CSV header
echo "size_label,size_bytes,upload_ms,peer1_repl_ms,peer2_repl_ms,peer1_first_byte_ms,peer2_first_byte_ms" > "$RESULTS_FILE"

# Helper: current time in milliseconds
now_ms() {
    python3 -c "import time; print(int(time.time() * 1000))"
}

# Helper: poll a peer until CID is available, return time in ms
poll_peer() {
    local peer_url="$1"
    local cid="$2"
    local start_ms="$3"
    local deadline_ms=$(( start_ms + MAX_WAIT * 1000 ))

    while true; do
        local current_ms
        current_ms=$(now_ms)
        if (( current_ms > deadline_ms )); then
            echo "TIMEOUT"
            return
        fi

        local http_code
        http_code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 "${peer_url}/ipfs/${cid}" 2>/dev/null || echo "000")

        if [[ "$http_code" == "200" ]]; then
            local done_ms
            done_ms=$(now_ms)
            echo $(( done_ms - start_ms ))
            return
        fi

        sleep "$POLL_INTERVAL"
    done
}

# Helper: poll peer and also measure first-byte time on success
poll_peer_with_ttfb() {
    local peer_url="$1"
    local cid="$2"
    local start_ms="$3"
    local deadline_ms=$(( start_ms + MAX_WAIT * 1000 ))

    while true; do
        local current_ms
        current_ms=$(now_ms)
        if (( current_ms > deadline_ms )); then
            echo "TIMEOUT,TIMEOUT"
            return
        fi

        local result
        result=$(curl -s -o /dev/null -w "%{http_code},%{time_starttransfer}" --max-time 10 "${peer_url}/ipfs/${cid}" 2>/dev/null || echo "000,0")

        local http_code ttfb
        http_code=$(echo "$result" | cut -d, -f1)
        ttfb=$(echo "$result" | cut -d, -f2)

        if [[ "$http_code" == "200" ]]; then
            local done_ms
            done_ms=$(now_ms)
            local repl_ms=$(( done_ms - start_ms ))
            local ttfb_ms
            ttfb_ms=$(python3 -c "print(int(float('${ttfb}') * 1000))")
            echo "${repl_ms},${ttfb_ms}"
            return
        fi

        sleep "$POLL_INTERVAL"
    done
}

# Run benchmark for each file size
for i in "${!SIZES[@]}"; do
    size="${SIZES[$i]}"
    label="${SIZE_LABELS[$i]}"

    echo "--- Test: ${label} file ---"

    # Generate random test data
    dd if=/dev/urandom of="/tmp/zipfs_bench_${label}.bin" bs="$size" count=1 2>/dev/null

    # Upload and measure
    echo -n "  Uploading to ${UPLOAD_NODE}... "
    upload_start=$(now_ms)

    upload_resp=$(curl -s -X POST \
        -F "file=@/tmp/zipfs_bench_${label}.bin;filename=bench_${label}.bin" \
        --max-time 300 \
        "${UPLOAD_NODE}/api/v0/add" 2>/dev/null)

    upload_end=$(now_ms)
    upload_ms=$(( upload_end - upload_start ))

    # Extract CID from NDJSON response (last line has the root CID)
    cid=$(echo "$upload_resp" | tail -1 | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['Hash'])" 2>/dev/null || echo "")

    if [[ -z "$cid" ]]; then
        echo "FAILED (no CID returned)"
        echo "  Response: ${upload_resp}"
        echo "${label},${size},${upload_ms},FAILED,FAILED,FAILED,FAILED" >> "$RESULTS_FILE"
        continue
    fi

    echo "done (${upload_ms}ms, CID: ${cid:0:20}...)"

    # Poll both peers in parallel
    echo -n "  Waiting for replication to peers... "
    repl_start=$(now_ms)

    # Run polls in background
    peer1_result_file="/tmp/zipfs_bench_p1_$$"
    peer2_result_file="/tmp/zipfs_bench_p2_$$"

    poll_peer_with_ttfb "$PEER1" "$cid" "$repl_start" > "$peer1_result_file" &
    pid1=$!
    poll_peer_with_ttfb "$PEER2" "$cid" "$repl_start" > "$peer2_result_file" &
    pid2=$!

    wait $pid1 $pid2

    peer1_result=$(cat "$peer1_result_file")
    peer2_result=$(cat "$peer2_result_file")
    rm -f "$peer1_result_file" "$peer2_result_file"

    peer1_repl_ms=$(echo "$peer1_result" | cut -d, -f1)
    peer1_ttfb_ms=$(echo "$peer1_result" | cut -d, -f2)
    peer2_repl_ms=$(echo "$peer2_result" | cut -d, -f1)
    peer2_ttfb_ms=$(echo "$peer2_result" | cut -d, -f2)

    echo "done"
    echo "  Upload:          ${upload_ms}ms"
    echo "  Peer 1 repl:     ${peer1_repl_ms}ms (TTFB: ${peer1_ttfb_ms}ms)"
    echo "  Peer 2 repl:     ${peer2_repl_ms}ms (TTFB: ${peer2_ttfb_ms}ms)"
    echo ""

    echo "${label},${size},${upload_ms},${peer1_repl_ms},${peer2_repl_ms},${peer1_ttfb_ms},${peer2_ttfb_ms}" >> "$RESULTS_FILE"

    # Cleanup
    rm -f "/tmp/zipfs_bench_${label}.bin"
done

echo "=============================================="
echo "  Results Summary"
echo "=============================================="
echo ""

# Pretty-print CSV
printf "%-8s %12s %12s %12s %12s %12s\n" "Size" "Upload(ms)" "Peer1(ms)" "Peer2(ms)" "P1-TTFB(ms)" "P2-TTFB(ms)"
printf "%-8s %12s %12s %12s %12s %12s\n" "--------" "------------" "------------" "------------" "------------" "------------"
tail -n +2 "$RESULTS_FILE" | while IFS=, read -r label size upload p1 p2 p1t p2t; do
    printf "%-8s %12s %12s %12s %12s %12s\n" "$label" "$upload" "$p1" "$p2" "$p1t" "$p2t"
done

echo ""
echo "Full CSV: $RESULTS_FILE"
echo ""

# Compute averages (excluding TIMEOUT/FAILED)
echo "--- Averages (excluding timeouts) ---"
python3 << 'PYEOF'
import csv, sys

with open(sys.argv[1] if len(sys.argv) > 1 else "/dev/stdin") as f:
    reader = csv.DictReader(f)
    rows = list(reader)

metrics = ["upload_ms", "peer1_repl_ms", "peer2_repl_ms"]
labels = ["Upload", "Peer 1 Repl", "Peer 2 Repl"]

for metric, label in zip(metrics, labels):
    vals = []
    for r in rows:
        try:
            vals.append(int(r[metric]))
        except (ValueError, KeyError):
            pass
    if vals:
        avg = sum(vals) / len(vals)
        mn, mx = min(vals), max(vals)
        print(f"  {label:20s}: avg={avg:8.0f}ms  min={mn:8d}ms  max={mx:8d}ms  (n={len(vals)})")
    else:
        print(f"  {label:20s}: no data")
PYEOF "$RESULTS_FILE"
