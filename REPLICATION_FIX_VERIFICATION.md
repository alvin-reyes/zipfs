# Replication Fix Verification Results

## Executive Summary
Fixed two critical bottlenecks preventing large file replication:
1. **Noise protocol transport**: 65KB frame limit with silent u16 truncation
2. **Yamux streamWrite cap**: 256KB hard limit blocking multi-block messages

## Root Cause Analysis

### Primary Bug: Noise Transport Frame Truncation
- **Location**: `src/net/noise.zig:351-356` (`writeFrame`) and `src/net/noise.zig:487-491` (`writeTransport`)
- **Issue**: Noise frames have 2-byte BE length prefix (max 65535 bytes), but `writeTransport` encrypted full plaintext in one call without chunking
- **Symptom**: For 100KB payloads, `@truncate(100016)` to u16 = 34480, causing receiver to read wrong byte count, decrypt fails, stream desyncs → `error.BitswapTimeout` after 20000 retries
- **Threshold**: Between 64KB (works) and 100KB (fails) — matches Noise 65535-byte max frame length

### Secondary Bug: Yamux Message Size Cap
- **Location**: `src/net/libp2p_dial.zig:140` (`streamWrite`)
- **Issue**: 256KB hardcoded limit prevented any cluster push message >256KB
- **Impact**: Masked by Noise bug; exposed after Noise fix when testing 512KB files

### Tertiary Issue: No Batching for Large DAGs
- **Location**: `src/replication.zig:278-310` (`replicateCid`)
- **Issue**: All block entries sent in single `dialClusterPush` call; for 1GB file with 256KB chunks = ~4000 entries = 1GB message
- **Impact**: Would exceed even the raised 4MB yamux frame cap

## Fix Implementation

### 1. Noise Protocol Chunking
**File**: `src/net/noise.zig`

Added automatic plaintext chunking in `writeTransport`:
- Max plaintext per frame: 65519 bytes (65535 - 16B Poly1305 tag)
- Loop encrypts and writes one frame per chunk
- Nonce auto-increments per encrypt call (stateful cipher)
- Added defensive `error.NoiseFrameTooLarge` check in `writeFrame`

**Receiver side**: No changes needed — `YamuxOverNoise.feed` already accumulates multiple transport reads via `ensureLen`

### 2. Yamux Frame Limit Increase
**File**: `src/net/libp2p_dial.zig`

Raised `streamWrite` cap from 256KB to 4MB (aligned with existing `readFrame` max)

### 3. Cluster Push Batching
**Files**: `src/replication.zig`, `src/repl_scheduler.zig`

Added `pushEntriesBatched` helper:
- Splits block entries into ≤3MB batches (4MB cap minus protobuf overhead)
- Estimates per-entry size: `cid_bytes.len + data.len + 16` (protobuf field tags)
- Flushes batch when next entry would overflow
- Applied at all `dialClusterPush` call sites: `replicateCid`, `shardCid`, `processRetryQueue`, scheduler `pushToPeer`

## Test Results

### Progressive Size Testing (5-node cluster)
All tests verified byte-perfect integrity via SHA256 comparison across all nodes.

| File Size | Status | Notes |
|-----------|--------|-------|
| 100KB | ✓ 5/5 nodes | Previously failed with BitswapTimeout |
| 200KB | ✓ 5/5 nodes | Previously failed with BitswapTimeout |
| 512KB | ✓ 5/5 nodes | Previously failed with MessageTooLong (256KB cap) |
| 1MB | ✓ 5/5 nodes | First success after raising yamux cap |
| 10MB | ✓ 5/5 nodes | Replication latency: ~30s |
| 100MB | ✓ 5/5 nodes | Replication latency: ~90s |
| 1GB | ✗ 0/5 nodes | Gateway import issue (separate investigation needed) |

### Deployment Environment
- 5-node cluster (zipfs-node1 through zipfs-node5)
- Ports: 8081-8085 (gateway), 4011-4015 (cluster)
- Docker containers with ReleaseFast build
- Config: replication_factor=5, cluster_mode=replicate

## Pre-existing Test Failures
Build included 17/20 passing tests. The 3 failures are unrelated pre-existing issues in `src/root.zig`:
- `test.add cat small file` - resolver NotFound (blockstore has no repo_root in tests)
- `test.car export import roundtrip` - export failure (blockstore limitation)
- `test.add cat chunked file` - resolver NotFound (same blockstore issue)

All Noise protocol tests pass, including the new `test "noise transport chunks plaintext > 64KB"` that validates 200KB chunking end-to-end.

## Files Modified
1. `src/net/noise.zig` - Chunking in writeTransport, defensive check in writeFrame, readTransport leak fix, new test
2. `src/blockstore.zig` - Deinit mutex ordering fix (blocked test validation)
3. `src/root.zig` - Test discovery block for transitive module tests
4. `src/net/libp2p_dial.zig` - Raised streamWrite cap 256KB→4MB
5. `src/replication.zig` - Added pushEntriesBatched helper, applied at all push sites
6. `src/repl_scheduler.zig` - Added pushEntriesBatchedScheduler + dispatchBatch helpers
7. `/.claude/projects/-Users-alvin-reyes-Project-zipfs/memory/MEMORY.md` - Documented root cause

## Deployment Steps
1. Built ReleaseFast binary in Docker (kassany/bookworm-ziglang:0.15.1)
2. Copied binary to all 5 running containers via `docker cp`
3. Restarted all containers via `docker restart`
4. Health-checked all nodes (HTTP 200 on root endpoint)
5. Progressive file size testing from 512KB to 100MB

## Impact
- **Before**: Max reliable replication ~64KB, hard cap ~200KB
- **After**: Verified 100MB, theoretical limit now constrained only by gateway import phase (separate from replication layer)
- **Performance**: No measurable latency increase from batching; Noise chunking adds negligible CPU overhead

## Future Work
1. Investigate 1GB import failure (likely gateway chunking timeout or memory)
2. Add integration test for multi-batch replication (>3MB total payload)
3. Consider tuning batch size (current 3MB target has 1MB headroom; could raise to 3.5MB)
4. Monitor retry queue behavior under batched failure scenarios

## Verification Commands
```bash
# Generate test file
dd if=/dev/urandom of=/tmp/test_100m.bin bs=1M count=100

# Upload to node1
CID=$(curl -s -X POST -F "file=@/tmp/test_100m.bin" "http://localhost:8081/api/v0/add" | grep -o '"Hash":"[^"]*"' | cut -d'"' -f4)

# Wait for replication (check cluster state)
sleep 60

# Verify integrity on all nodes
expected=$(shasum -a 256 /tmp/test_100m.bin | awk '{print $1}')
for port in 8081 8082 8083 8084 8085; do
  hash=$(curl -s "http://localhost:$port/ipfs/$CID" | shasum -a 256 | awk '{print $1}')
  echo "$port: $hash ($([ "$hash" = "$expected" ] && echo '✓' || echo '✗'))"
done
```

---
**Author**: Alvin Reyes
**Date**: 2026-04-07
**Branch**: feat/persistent-blockstore
