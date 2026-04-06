//! Proactive replication and sharding: consistent hash ring, DAG push, retry queue, self-healing loop.

const std = @import("std");
const cluster_mod = @import("cluster.zig");
const blockstore_mod = @import("blockstore.zig");
const resolver = @import("resolver.zig");
const cid_mod = @import("cid.zig");
const pin_mod = @import("pin.zig");
const config_mod = @import("config.zig");
const cluster_push = @import("net/cluster_push.zig");
const multiaddr = @import("net/multiaddr.zig");
const repl_queue_mod = @import("repl_queue.zig");

const ClusterState = cluster_mod.ClusterState;
const ClusterPeer = cluster_mod.ClusterPeer;
const ClusterMode = cluster_mod.ClusterMode;
const Blockstore = blockstore_mod.Blockstore;
const Cid = cid_mod.Cid;

/// Consistent hash ring position for a key.
fn ringPosition(key: []const u8) u256 {
    var out: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(key, &out, .{});
    var result: u256 = 0;
    for (out, 0..) |b, i| {
        result |= @as(u256, b) << @intCast((31 - i) * 8);
    }
    return result;
}

/// Select N peers from alive peers using consistent hashing on the CID.
/// Skips peers already in `exclude`.
pub fn selectPeersConsistentHash(
    allocator: std.mem.Allocator,
    alive: []const ClusterPeer,
    cid_str: []const u8,
    n: usize,
    exclude: []const []const u8,
) ![]ClusterPeer {
    if (alive.len == 0) return try allocator.alloc(ClusterPeer, 0);

    const target = ringPosition(cid_str);

    // Score each peer by distance from target on the ring
    const ScoredPeer = struct {
        peer: ClusterPeer,
        distance: u256,
    };
    var scored = std.ArrayList(ScoredPeer).empty;
    defer scored.deinit(allocator);

    for (alive) |p| {
        var excluded = false;
        for (exclude) |ex| {
            if (std.mem.eql(u8, p.addr, ex)) {
                excluded = true;
                break;
            }
        }
        if (excluded) continue;

        const pos = ringPosition(p.addr);
        const raw_dist = if (pos >= target) pos - target else target - pos;
        const dist = if (raw_dist == 0) @as(u256, 0) else blk: {
            const max_u256: u256 = std.math.maxInt(u256);
            break :blk @min(raw_dist, max_u256 - raw_dist + 1);
        };
        try scored.append(allocator, .{ .peer = p, .distance = dist });
    }

    // Sort by distance (closest first)
    std.mem.sortUnstable(ScoredPeer, scored.items, {}, struct {
        fn lessThan(_: void, a: ScoredPeer, b: ScoredPeer) bool {
            return a.distance < b.distance;
        }
    }.lessThan);

    const count = @min(n, scored.items.len);
    var result = try allocator.alloc(ClusterPeer, count);
    for (scored.items[0..count], 0..) |sp, i| {
        result[i] = sp.peer;
    }
    return result;
}

/// Collect all block CID strings in a DAG recursively (including root).
pub fn collectDagBlocks(
    allocator: std.mem.Allocator,
    store: *const Blockstore,
    root_key: []const u8,
) ![][]u8 {
    var visited = std.StringHashMapUnmanaged(void).empty;
    defer visited.deinit(allocator);

    var result = std.ArrayList([]u8).empty;
    errdefer {
        for (result.items) |r| allocator.free(r);
        result.deinit(allocator);
    }

    var work = std.ArrayList([]u8).empty;
    defer {
        for (work.items) |w| allocator.free(w);
        work.deinit(allocator);
    }

    const initial = try allocator.dupe(u8, root_key);
    try work.append(allocator, initial);

    while (work.items.len > 0) {
        const key = work.pop() orelse break;
        defer allocator.free(key);

        if (visited.contains(key)) continue;
        const owned_key = try allocator.dupe(u8, key);
        errdefer allocator.free(owned_key);
        try visited.put(allocator, owned_key, {});

        const result_key = try allocator.dupe(u8, key);
        errdefer allocator.free(result_key);
        try result.append(allocator, result_key);

        const children = resolver.dagChildKeys(allocator, store, key) catch continue;
        defer {
            for (children) |c| allocator.free(c);
            allocator.free(children);
        }
        for (children) |child| {
            if (!visited.contains(child)) {
                try work.append(allocator, try allocator.dupe(u8, child));
            }
        }
    }

    // Free visited keys
    var vit = visited.keyIterator();
    while (vit.next()) |k| allocator.free(k.*);

    return try result.toOwnedSlice(allocator);
}

/// Classify blocks into structure (dag-pb intermediate) and data (raw leaf) nodes.
pub fn classifyBlocks(
    allocator: std.mem.Allocator,
    store: *const Blockstore,
    block_keys: []const []const u8,
) !struct { structure: [][]const u8, data_blocks: [][]const u8 } {
    var structure = std.ArrayList([]const u8).empty;
    errdefer structure.deinit(allocator);
    var data_blocks = std.ArrayList([]const u8).empty;
    errdefer data_blocks.deinit(allocator);

    for (block_keys) |key| {
        var c = Cid.parse(allocator, key) catch {
            try data_blocks.append(allocator, key);
            continue;
        };
        defer c.deinit(allocator);

        if (c.codec == cid_mod.codec_dag_pb) {
            // Check if it has children (structure node) or is a leaf dag-pb
            const children = resolver.dagChildKeys(allocator, store, key) catch {
                try data_blocks.append(allocator, key);
                continue;
            };
            defer {
                for (children) |ch| allocator.free(ch);
                allocator.free(children);
            }
            if (children.len > 0) {
                try structure.append(allocator, key);
            } else {
                try data_blocks.append(allocator, key);
            }
        } else {
            try data_blocks.append(allocator, key);
        }
    }

    return .{
        .structure = try structure.toOwnedSlice(allocator),
        .data_blocks = try data_blocks.toOwnedSlice(allocator),
    };
}

/// Assign a block to a shard index deterministically.
pub fn shardForBlock(block_cid: []const u8, shard_count: u16) u16 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(block_cid, &hash, .{});
    const val: u16 = @as(u16, hash[0]) | (@as(u16, hash[1]) << 8);
    return val % shard_count;
}

/// Build block entries from blockstore for a set of CID keys.
pub fn buildBlockEntries(
    allocator: std.mem.Allocator,
    store: *const Blockstore,
    keys: []const []const u8,
) ![]cluster_push.BlockEntry {
    var entries = std.ArrayList(cluster_push.BlockEntry).empty;
    errdefer {
        for (entries.items) |e| {
            allocator.free(e.cid_bytes);
            allocator.free(e.data);
        }
        entries.deinit(allocator);
    }

    for (keys) |key| {
        const data = store.get(key) orelse continue;
        var c = Cid.parse(allocator, key) catch continue;
        defer c.deinit(allocator);
        const cid_bytes = c.toBytes(allocator) catch continue;
        errdefer allocator.free(cid_bytes);
        const data_owned = try allocator.dupe(u8, data);
        errdefer allocator.free(data_owned);
        try entries.append(allocator, .{ .cid_bytes = cid_bytes, .data = data_owned });
    }

    return try entries.toOwnedSlice(allocator);
}

/// Free block entries allocated by buildBlockEntries.
pub fn freeBlockEntries(allocator: std.mem.Allocator, entries: []cluster_push.BlockEntry) void {
    for (entries) |e| {
        allocator.free(e.cid_bytes);
        allocator.free(e.data);
    }
    allocator.free(entries);
}

/// Replicate a CID's full DAG to N cluster peers.
pub fn replicateCid(
    allocator: std.mem.Allocator,
    state: *ClusterState,
    store: *const Blockstore,
    cid_str: []const u8,
    target_n: u8,
    cluster_secret: ?[]const u8,
    ed25519_secret64: [64]u8,
) !void {
    // Collect all blocks in DAG
    const all_blocks = try collectDagBlocks(allocator, store, cid_str);
    defer {
        for (all_blocks) |b| allocator.free(b);
        allocator.free(all_blocks);
    }

    // Ensure replica record exists
    _ = try state.upsertReplica(cid_str, .replicate, target_n);

    // Get alive peers
    const alive = try state.alivePeers(allocator);
    defer allocator.free(alive);

    // Get current confirmed peers for this CID
    const rec = state.replicas.getPtr(cid_str) orelse return;
    const already_confirmed = rec.confirmed_peers;

    // Select new peers
    const needed: usize = if (target_n > already_confirmed.len) target_n - already_confirmed.len else 0;
    if (needed == 0) return;

    const selected = try selectPeersConsistentHash(allocator, alive, cid_str, needed, already_confirmed);
    defer allocator.free(selected);

    // Build block entries
    const entries = try buildBlockEntries(allocator, store, all_blocks);
    defer freeBlockEntries(allocator, entries);

    // Push to each selected peer and track confirmations
    var confirmed_count: usize = already_confirmed.len;
    for (selected) |peer| {
        const hp = cluster_push.parseHostPort(allocator, peer.addr) catch {
            try state.addRetry(.push_blocks, cid_str, peer.addr, null);
            continue;
        };
        defer allocator.free(hp.host);

        _ = cluster_push.dialClusterPush(
            allocator,
            hp.host,
            hp.port,
            cid_str,
            entries,
            cluster_secret,
            ed25519_secret64,
        ) catch {
            try state.addRetry(.push_blocks, cid_str, peer.addr, null);
            continue;
        };

        try state.addConfirmedPeer(cid_str, peer.addr);
        confirmed_count += 1;
    }

    // Write quorum check: require at least 1 confirmed peer for data safety.
    // If no peers confirmed at all, return error so caller can retry.
    if (confirmed_count == 0 and target_n > 0) {
        return error.NoQuorum;
    }
}

/// Shard a CID's DAG across cluster peers.
pub fn shardCid(
    allocator: std.mem.Allocator,
    state: *ClusterState,
    store: *const Blockstore,
    cid_str: []const u8,
    shard_count: u16,
    cluster_secret: ?[]const u8,
    ed25519_secret64: [64]u8,
) !void {
    const all_blocks = try collectDagBlocks(allocator, store, cid_str);
    defer {
        for (all_blocks) |b| allocator.free(b);
        allocator.free(all_blocks);
    }

    const classified = try classifyBlocks(allocator, store, all_blocks);
    defer {
        allocator.free(classified.structure);
        allocator.free(classified.data_blocks);
    }

    _ = try state.upsertReplica(cid_str, .shard, @intCast(@min(shard_count, 255)));

    const alive = try state.alivePeers(allocator);
    defer allocator.free(alive);

    if (alive.len == 0) return;

    // Push structure nodes to ALL peers
    if (classified.structure.len > 0) {
        const struct_entries = try buildBlockEntries(allocator, store, classified.structure);
        defer freeBlockEntries(allocator, struct_entries);

        for (alive) |peer| {
            const hp = cluster_push.parseHostPort(allocator, peer.addr) catch continue;
            defer allocator.free(hp.host);
            _ = cluster_push.dialClusterPush(
                allocator,
                hp.host,
                hp.port,
                cid_str,
                struct_entries,
                cluster_secret,
                ed25519_secret64,
            ) catch {
                state.addRetry(.push_shard, cid_str, peer.addr, null) catch {};
                continue;
            };
        }
    }

    // Assign data blocks to shards and push
    // Group blocks by shard index
    var shard_groups = std.AutoHashMapUnmanaged(u16, std.ArrayList([]const u8)).empty;
    defer {
        var git = shard_groups.valueIterator();
        while (git.next()) |v| v.deinit(allocator);
        shard_groups.deinit(allocator);
    }

    for (classified.data_blocks) |key| {
        const si = shardForBlock(key, shard_count);
        const gop = try shard_groups.getOrPut(allocator, si);
        if (!gop.found_existing) gop.value_ptr.* = std.ArrayList([]const u8).empty;
        try gop.value_ptr.append(allocator, key);
    }

    // Push each shard group to its assigned peer
    var sit = shard_groups.iterator();
    while (sit.next()) |entry| {
        const si = entry.key_ptr.*;
        const block_keys = entry.value_ptr.items;
        const peer_idx = si % @as(u16, @intCast(alive.len));
        const peer = alive[peer_idx];

        const shard_entries = buildBlockEntries(allocator, store, block_keys) catch continue;
        defer freeBlockEntries(allocator, shard_entries);

        const hp = cluster_push.parseHostPort(allocator, peer.addr) catch continue;
        defer allocator.free(hp.host);

        _ = cluster_push.dialClusterPush(
            allocator,
            hp.host,
            hp.port,
            cid_str,
            shard_entries,
            cluster_secret,
            ed25519_secret64,
        ) catch {
            state.addRetry(.push_shard, cid_str, peer.addr, si) catch {};
            continue;
        };
    }
}

/// Process the retry queue: attempt overdue retries with exponential backoff.
pub fn processRetryQueue(
    allocator: std.mem.Allocator,
    state: *ClusterState,
    store: *const Blockstore,
    cluster_secret: ?[]const u8,
    ed25519_secret64: [64]u8,
) void {
    const now = std.time.nanoTimestamp();
    var i: usize = 0;
    while (i < state.retry_queue.items.len) {
        var entry = &state.retry_queue.items[i];
        if (entry.next_retry_ns > now) {
            i += 1;
            continue;
        }

        if (entry.attempts >= cluster_mod.max_retry_attempts) {
            // Max retries exceeded — mark peer as suspect and remove entry
            if (state.findPeerByAddr(entry.target_peer)) |peer| {
                peer.is_alive = false;
            }
            var removed = state.retry_queue.orderedRemove(i);
            removed.deinit(state.allocator);
            continue;
        }

        // Attempt the operation
        var success = false;
        switch (entry.op) {
            .push_blocks => {
                const all_blocks = collectDagBlocks(allocator, store, entry.cid) catch {
                    entry.next_retry_ns = now + entry.nextBackoffNs();
                    entry.attempts += 1;
                    i += 1;
                    continue;
                };
                defer {
                    for (all_blocks) |b| allocator.free(b);
                    allocator.free(all_blocks);
                }
                const entries = buildBlockEntries(allocator, store, all_blocks) catch {
                    entry.next_retry_ns = now + entry.nextBackoffNs();
                    entry.attempts += 1;
                    i += 1;
                    continue;
                };
                defer freeBlockEntries(allocator, entries);

                const hp = cluster_push.parseHostPort(allocator, entry.target_peer) catch {
                    entry.next_retry_ns = now + entry.nextBackoffNs();
                    entry.attempts += 1;
                    i += 1;
                    continue;
                };
                defer allocator.free(hp.host);

                if (cluster_push.dialClusterPush(
                    allocator,
                    hp.host,
                    hp.port,
                    entry.cid,
                    entries,
                    cluster_secret,
                    ed25519_secret64,
                )) |_| {
                    state.addConfirmedPeer(entry.cid, entry.target_peer) catch {};
                    success = true;
                } else |_| {}
            },
            .push_shard => {
                // Re-push blocks to the target peer (same as push_blocks)
                const all_blocks = collectDagBlocks(allocator, store, entry.cid) catch {
                    entry.next_retry_ns = now + entry.nextBackoffNs();
                    entry.attempts += 1;
                    i += 1;
                    continue;
                };
                defer {
                    for (all_blocks) |b| allocator.free(b);
                    allocator.free(all_blocks);
                }
                const entries = buildBlockEntries(allocator, store, all_blocks) catch {
                    entry.next_retry_ns = now + entry.nextBackoffNs();
                    entry.attempts += 1;
                    i += 1;
                    continue;
                };
                defer freeBlockEntries(allocator, entries);

                const hp = cluster_push.parseHostPort(allocator, entry.target_peer) catch {
                    entry.next_retry_ns = now + entry.nextBackoffNs();
                    entry.attempts += 1;
                    i += 1;
                    continue;
                };
                defer allocator.free(hp.host);

                if (cluster_push.dialClusterPush(
                    allocator,
                    hp.host,
                    hp.port,
                    entry.cid,
                    entries,
                    cluster_secret,
                    ed25519_secret64,
                )) |_| {
                    success = true;
                } else |_| {}
            },
            .have_check => {
                // Verification retry — just ping to confirm peer is reachable
                const hp = cluster_push.parseHostPort(allocator, entry.target_peer) catch {
                    entry.next_retry_ns = now + entry.nextBackoffNs();
                    entry.attempts += 1;
                    i += 1;
                    continue;
                };
                defer allocator.free(hp.host);

                if (cluster_push.dialClusterPing(allocator, hp.host, hp.port, cluster_secret, ed25519_secret64) catch false) {
                    success = true;
                } else {
                    // Peer still down
                }
            },
        }

        if (success) {
            var removed = state.retry_queue.orderedRemove(i);
            removed.deinit(state.allocator);
        } else {
            entry.next_retry_ns = now + entry.nextBackoffNs();
            entry.attempts += 1;
            i += 1;
        }
    }
}

/// Context for the self-healing background thread.
pub const SelfHealCtx = struct {
    store: *Blockstore,
    mu: *std.Thread.Mutex,
    repo_root: []const u8,
    ed25519_secret64: [64]u8,
    interval_ns: u64,
    cluster_secret: ?[]const u8,
    cluster_mode: ClusterMode,
    replication_factor: u8,
    shard_count: u16,
    /// Optional replication queue for delegating work to the scheduler (daemon mode).
    /// When null, repairs are performed directly (CLI `cluster sync` mode).
    queue: ?*repl_queue_mod.ReplQueue = null,
    /// Mutex guarding ClusterState load/save to prevent concurrent file access races.
    state_mu: ?*std.Thread.Mutex = null,
};

/// Background self-healing loop. Runs as a daemon thread.
pub fn selfHealingLoop(ctx: *SelfHealCtx) void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const allocator = gpa_state.allocator();

    while (true) {
        selfHealOnce(allocator, ctx) catch {};
        std.Thread.sleep(ctx.interval_ns);
    }
}

/// Run a single self-healing cycle (public for `cluster sync`).
pub fn selfHealOnce(allocator: std.mem.Allocator, ctx: *SelfHealCtx) !void {
    // Lock state_mu to prevent concurrent ClusterState load/save races with the scheduler
    if (ctx.state_mu) |smu| smu.lock();
    defer if (ctx.state_mu) |smu| smu.unlock();

    var state = ClusterState.load(allocator, ctx.repo_root) catch return;
    defer state.deinit();

    // 1. Liveness check: ping each peer
    for (state.peers.items) |*peer| {
        const hp = cluster_push.parseHostPort(allocator, peer.addr) catch continue;
        defer allocator.free(hp.host);

        const alive = cluster_push.dialClusterPing(allocator, hp.host, hp.port, ctx.cluster_secret, ctx.ed25519_secret64) catch false;
        if (alive) {
            peer.is_alive = true;
            peer.consecutive_failures = 0;
            peer.last_seen_ns = std.time.nanoTimestamp();
        } else {
            peer.consecutive_failures +|= 1;
            if (peer.consecutive_failures >= 3) {
                peer.is_alive = false;
            }
        }
    }

    // 2. Pin sync: compare pins with cluster replicas
    var pins = pin_mod.PinSet.load(allocator, ctx.repo_root) catch pin_mod.PinSet{};
    defer pins.deinit(allocator);

    // Guard: if PinSet loaded as empty but replicas exist, the pin file may be
    // corrupted or missing. Skip replica cleanup to prevent mass deletion (C7 DR fix).
    const pins_loaded_empty = (pins.direct.count() == 0 and pins.recursive.count() == 0);
    const replicas_exist = (state.replicas.count() > 0);

    // New pins -> create replica records (always safe to add)
    // replication_factor = total copies; subtract 1 for origin to get peer target
    const peer_target: u8 = if (ctx.replication_factor > 1) ctx.replication_factor - 1 else 1;
    var pin_it = pins.recursive.keyIterator();
    while (pin_it.next()) |k| {
        if (!state.replicas.contains(k.*)) {
            _ = state.upsertReplica(k.*, ctx.cluster_mode, peer_target) catch continue;
        }
    }

    // Removed pins -> remove replica records
    // ONLY remove replicas if we have a non-empty pin set (or no replicas to protect)
    if (!pins_loaded_empty or !replicas_exist) {
        var to_remove = std.ArrayList([]const u8).empty;
        defer to_remove.deinit(allocator);
        var rit = state.replicas.keyIterator();
        while (rit.next()) |k| {
            if (!pins.recursive.contains(k.*) and !pins.direct.contains(k.*)) {
                to_remove.append(allocator, k.*) catch continue;
            }
        }
        for (to_remove.items) |k| {
            state.removeReplica(k);
        }
    }

    // 3. Replica repair
    // Note: blockstore reads below are lock-free; store.put is protected in libp2p_serve.

    // Collect CIDs that need repair (can't modify replicas while iterating)
    var repair_list = std.ArrayList(struct { cid: []const u8, mode: ClusterMode, target_n: u8 }).empty;
    defer repair_list.deinit(allocator);

    var rep_it = state.replicas.iterator();
    while (rep_it.next()) |entry| {
        const rec = entry.value_ptr;
        switch (rec.mode) {
            .replicate => {
                var alive_confirmed: u8 = 0;
                for (rec.confirmed_peers) |cp| {
                    if (state.findPeerByAddr(cp)) |p| {
                        if (p.is_alive) alive_confirmed += 1;
                    }
                }
                if (alive_confirmed < rec.target_n) {
                    repair_list.append(allocator, .{
                        .cid = rec.cid,
                        .mode = .replicate,
                        .target_n = rec.target_n,
                    }) catch {};
                }
            },
            .shard => {
                var needs_repair = false;
                for (rec.shard_assignments) |sa| {
                    const peer_dead = if (state.findPeerByAddr(sa.peer_id)) |p| !p.is_alive else true;
                    if (peer_dead) {
                        needs_repair = true;
                        break;
                    }
                }
                if (needs_repair) {
                    repair_list.append(allocator, .{
                        .cid = rec.cid,
                        .mode = .shard,
                        .target_n = rec.target_n,
                    }) catch {};
                }
            },
        }
    }

    // Now perform repairs outside of iterator
    for (repair_list.items) |item| {
        if (ctx.queue) |q| {
            // Daemon mode: delegate to scheduler queue for batched, rate-limited dispatch
            const cid_dup = q.allocator.dupe(u8, item.cid) catch continue;
            q.push(.{
                .cid = cid_dup,
                .mode = item.mode,
                .priority = .heal,
                .enqueued_ns = std.time.nanoTimestamp(),
                .target_peer = null,
                .shard_index = null,
            });
        } else {
            // CLI mode: perform repair directly, holding sync_mu for blockstore safety
            ctx.mu.lock();
            switch (item.mode) {
                .replicate => {
                    replicateCid(
                        allocator,
                        &state,
                        ctx.store,
                        item.cid,
                        item.target_n,
                        ctx.cluster_secret,
                        ctx.ed25519_secret64,
                    ) catch {};
                },
                .shard => {
                    shardCid(
                        allocator,
                        &state,
                        ctx.store,
                        item.cid,
                        ctx.shard_count,
                        ctx.cluster_secret,
                        ctx.ed25519_secret64,
                    ) catch {};
                },
            }
            ctx.mu.unlock();
        }
    }

    // 4. Process retry queue (holds sync_mu for blockstore safety)
    ctx.mu.lock();
    processRetryQueue(allocator, &state, ctx.store, ctx.cluster_secret, ctx.ed25519_secret64);
    ctx.mu.unlock();

    // 5. Save state atomically
    state.save(ctx.repo_root) catch |e| {
        std.log.err("cluster state save failed: {}", .{e});
    };
}
