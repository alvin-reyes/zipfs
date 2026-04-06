//! Replication scheduler: inbox poller, priority-dispatched scheduler loop, and batched per-peer workers.

const std = @import("std");
const repl_queue = @import("repl_queue.zig");
const cluster_mod = @import("cluster.zig");
const replication = @import("replication.zig");
const blockstore_mod = @import("blockstore.zig");
const cluster_push = @import("net/cluster_push.zig");
const conn_pool = @import("net/conn_pool.zig");
const manifest_mod = @import("manifest.zig");
const pull_engine = @import("pull_engine.zig");

const ReplQueue = repl_queue.ReplQueue;
const ReplItem = repl_queue.ReplItem;
const RateLimiter = repl_queue.RateLimiter;
const PeerConcurrency = repl_queue.PeerConcurrency;
const ClusterState = cluster_mod.ClusterState;
const ClusterMode = cluster_mod.ClusterMode;
const Blockstore = blockstore_mod.Blockstore;

/// Shared context for the replication scheduler threads.
pub const SchedulerCtx = struct {
    queue: *ReplQueue,
    store: *Blockstore,
    mu: *std.Thread.Mutex,
    repo_root: []const u8,
    ed25519_secret64: [64]u8,
    cluster_secret: ?[]const u8,
    cluster_mode: ClusterMode,
    replication_factor: u8,
    shard_count: u16,
    inbox_poll_ns: u64,
    rate_limiter: *RateLimiter,
    peer_concurrency: *PeerConcurrency,
    max_per_peer: u8,
    batch_size: u16,
    /// Cached cluster peer addresses from config (avoids re-reading config.json per item).
    cluster_peers: []const []const u8 = &.{},
    /// Mutex guarding ClusterState load/save to prevent concurrent file access races.
    state_mu: ?*std.Thread.Mutex = null,
    /// Connection pool for reusing Yamux connections to cluster peers.
    pool: ?*conn_pool.ConnPool = null,
    /// Size threshold in bytes for pull replication (0 = disabled, use push for everything).
    pull_replication_threshold: u64 = 0,
    /// Gateway port for HTTP block pull endpoint.
    gateway_port: u16 = 8080,
};

/// Inbox poller loop: reads CIDs from the repl_inbox file and enqueues them.
pub fn inboxPollLoop(ctx: *SchedulerCtx) void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const allocator = gpa_state.allocator();

    // Recover stranded processing file from prior crash.
    // Append its contents to the inbox (don't rename, which would overwrite new CIDs).
    recoverStrandedInbox(allocator, ctx.repo_root);

    while (true) {
        drainInbox(allocator, ctx);
        std.Thread.sleep(ctx.inbox_poll_ns);
    }
}

/// Recover a stranded .processing file from a prior crash.
/// Appends its contents to the inbox (instead of rename, which would overwrite new CIDs).
fn recoverStrandedInbox(allocator: std.mem.Allocator, repo_root: []const u8) void {
    const pp = std.fs.path.join(allocator, &.{ repo_root, "repl_inbox.processing" }) catch return;
    defer allocator.free(pp);
    const ip = std.fs.path.join(allocator, &.{ repo_root, "repl_inbox" }) catch return;
    defer allocator.free(ip);

    // Read the stranded processing file
    const data = std.fs.cwd().readFileAlloc(allocator, pp, 8 << 20) catch {
        // No processing file or can't read — nothing to recover
        return;
    };
    defer allocator.free(data);

    if (data.len == 0) {
        std.fs.cwd().deleteFile(pp) catch {};
        return;
    }

    // Append to existing inbox (preserves any new CIDs written since crash)
    const inbox_file = std.fs.cwd().openFile(ip, .{ .mode = .write_only }) catch |err| switch (err) {
        error.FileNotFound => {
            // No inbox exists — safe to just rename
            std.fs.cwd().rename(pp, ip) catch {};
            return;
        },
        else => return,
    };
    defer inbox_file.close();
    inbox_file.seekFromEnd(0) catch return;
    inbox_file.writeAll(data) catch return;
    inbox_file.sync() catch return;

    // Clean up the processing file after successful append
    std.fs.cwd().deleteFile(pp) catch {};
}

/// Read and process the inbox file using atomic rename to prevent TOCTOU races.
/// Uses the queue's allocator for CID strings to avoid cross-allocator free issues.
fn drainInbox(allocator: std.mem.Allocator, ctx: *SchedulerCtx) void {
    const path = std.fs.path.join(allocator, &.{ ctx.repo_root, "repl_inbox" }) catch return;
    defer allocator.free(path);
    const proc_path = std.fs.path.join(allocator, &.{ ctx.repo_root, "repl_inbox.processing" }) catch return;
    defer allocator.free(proc_path);

    // Atomically rename inbox -> processing file (no TOCTOU window for data loss)
    std.fs.cwd().rename(path, proc_path) catch |err| switch (err) {
        error.FileNotFound => return, // No inbox file — nothing to do
        else => return,
    };

    // Read from the renamed (now-exclusive) file
    const data = std.fs.cwd().readFileAlloc(allocator, proc_path, 8 << 20) catch {
        // Rename back on read failure so data isn't lost
        std.fs.cwd().rename(proc_path, path) catch {};
        return;
    };
    defer allocator.free(data);

    if (data.len == 0) {
        std.fs.cwd().deleteFile(proc_path) catch {};
        return;
    }

    // Use the queue's allocator for CID strings to ensure consistent allocator on free
    const q_alloc = ctx.queue.allocator;

    // Parse lines and enqueue. Format: "CID" or "CID:N" where N is replication factor.
    var line_it = std.mem.splitScalar(u8, data, '\n');
    while (line_it.next()) |line| {
        if (line.len == 0) continue;
        var repl_factor: u8 = 0; // 0 = use default
        var cid_part = line;
        if (std.mem.lastIndexOfScalar(u8, line, ':')) |colon| {
            const factor_str = line[colon + 1 ..];
            if (std.fmt.parseInt(u8, factor_str, 10)) |f| {
                repl_factor = f;
                cid_part = line[0..colon];
            } else |_| {}
        }
        const cid_dup = q_alloc.dupe(u8, cid_part) catch continue;
        ctx.queue.push(.{
            .cid = cid_dup,
            .mode = ctx.cluster_mode,
            .priority = .immediate,
            .enqueued_ns = std.time.nanoTimestamp(),
            .target_peer = null,
            .shard_index = null,
            .replication_factor = repl_factor,
        });
    }

    // Only delete processing file after all items are enqueued
    std.fs.cwd().deleteFile(proc_path) catch {};
}

/// Main scheduler loop: pops batches from the queue and dispatches per-peer workers.
pub fn schedulerLoop(ctx: *SchedulerCtx) void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const allocator = gpa_state.allocator();

    var last_evict_ns: i128 = std.time.nanoTimestamp();

    while (true) {
        const batch = ctx.queue.popBatch(ctx.batch_size, 5 * std.time.ns_per_s);
        if (batch.len == 0) {
            // Evict stale pooled connections periodically (every 60s)
            evictStaleConnections(ctx, &last_evict_ns);
            continue;
        }

        // Process each item in the batch
        for (batch) |*item| {
            // Rate limit
            ctx.rate_limiter.waitAcquire();

            processItem(allocator, ctx, item);
        }

        ctx.queue.allocator.free(batch);

        // Evict stale pooled connections periodically (every 60s)
        evictStaleConnections(ctx, &last_evict_ns);
    }
}

/// Evict stale pooled connections if 60 seconds have passed since last eviction.
fn evictStaleConnections(ctx: *SchedulerCtx, last_evict_ns: *i128) void {
    const now = std.time.nanoTimestamp();
    const evict_interval_ns: i128 = 60 * std.time.ns_per_s;
    if (now - last_evict_ns.* >= evict_interval_ns) {
        if (ctx.pool) |pool| {
            pool.evictStale(evict_interval_ns);
        }
        last_evict_ns.* = now;
    }
}

/// Process a single replication item: load state, resolve peers, push blocks.
/// Uses the queue's allocator for item deallocation to match allocation source.
/// No external blockstore lock needed — Blockstore is internally thread-safe via cache_mu.
/// state_mu is narrowed to file I/O only (load/save) to prevent blocking during network pushes.
fn processItem(allocator: std.mem.Allocator, ctx: *SchedulerCtx, item: *ReplItem) void {
    defer item.deinit(ctx.queue.allocator);

    // Load cluster state under state_mu (protects file access only)
    var state = blk: {
        if (ctx.state_mu) |smu| smu.lock();
        defer if (ctx.state_mu) |smu| smu.unlock();
        break :blk ClusterState.load(allocator, ctx.repo_root) catch return;
    };
    defer state.deinit();

    // Add configured peers if needed
    addConfiguredPeers(&state, ctx);

    if (item.target_peer) |target| {
        // Targeted retry: push directly to the specified peer
        pushToPeer(allocator, ctx, &state, item.cid, target, item.mode, item.shard_index, item.retry_count);
    } else {
        // No external blockstore lock — Blockstore is internally thread-safe via cache_mu

        // Check if this DAG is large enough for pull-based replication
        if (ctx.pull_replication_threshold > 0 and item.mode == .replicate) {
            const block_estimate = manifest_mod.estimateDagBlocks(allocator, ctx.store, item.cid, 1000);
            const chunk_size: u64 = 262144; // 256KB default
            if (block_estimate * chunk_size > ctx.pull_replication_threshold) {
                // Large DAG: use manifest-based pull replication
                createAndDistributeManifest(allocator, ctx, &state, item);
                {
                    if (ctx.state_mu) |smu| smu.lock();
                    defer if (ctx.state_mu) |smu| smu.unlock();
                    state.save(ctx.repo_root) catch |e| {
                        std.log.err("cluster state save failed: {}", .{e});
                    };
                }
                return;
            }
        }

        // Auto-select peers and replicate/shard (existing push path)
        switch (item.mode) {
            .replicate => {
                const factor = if (item.replication_factor > 0) item.replication_factor else ctx.replication_factor;
                // factor = total copies; subtract 1 for the origin node to get peer count
                const peer_count: u8 = if (factor > 1) factor - 1 else 1;
                replication.replicateCid(
                    allocator,
                    &state,
                    ctx.store,
                    item.cid,
                    peer_count,
                    ctx.cluster_secret,
                    ctx.ed25519_secret64,
                ) catch {
                    // Re-enqueue with higher priority on failure, preserving per-item factor
                    reEnqueueWithRetryAndFactor(ctx, item.cid, item.mode, .retry_high, null, null, item.retry_count, item.replication_factor);
                    {
                        if (ctx.state_mu) |smu| smu.lock();
                        defer if (ctx.state_mu) |smu| smu.unlock();
                        state.save(ctx.repo_root) catch |e| {
                            std.log.err("cluster state save failed: {}", .{e});
                        };
                    }
                    return;
                };
            },
            .shard => {
                replication.shardCid(
                    allocator,
                    &state,
                    ctx.store,
                    item.cid,
                    ctx.shard_count,
                    ctx.cluster_secret,
                    ctx.ed25519_secret64,
                ) catch {
                    reEnqueueWithRetryAndFactor(ctx, item.cid, item.mode, .retry_high, null, null, item.retry_count, item.replication_factor);
                    {
                        if (ctx.state_mu) |smu| smu.lock();
                        defer if (ctx.state_mu) |smu| smu.unlock();
                        state.save(ctx.repo_root) catch |e| {
                            std.log.err("cluster state save failed: {}", .{e});
                        };
                    }
                    return;
                };
            },
        }
    }

    {
        if (ctx.state_mu) |smu| smu.lock();
        defer if (ctx.state_mu) |smu| smu.unlock();
        state.save(ctx.repo_root) catch |e| {
            std.log.err("cluster state save failed: {}", .{e});
        };
    }
}

/// Push blocks for a CID directly to a specific peer.
fn pushToPeer(
    allocator: std.mem.Allocator,
    ctx: *SchedulerCtx,
    state: *ClusterState,
    cid_str: []const u8,
    peer_addr: []const u8,
    mode: ClusterMode,
    shard_index: ?u16,
    retry_count: u8,
) void {
    // Check peer concurrency
    if (!ctx.peer_concurrency.acquire(peer_addr)) {
        // Re-enqueue with normal priority (concurrency limit, not a failure — don't increment retry)
        reEnqueueWithRetry(ctx, cid_str, mode, .normal, peer_addr, shard_index, retry_count);
        return;
    }
    defer ctx.peer_concurrency.release(peer_addr);

    // Read blocks (no external lock — Blockstore is internally thread-safe via cache_mu)
    const all_blocks = replication.collectDagBlocks(allocator, ctx.store, cid_str) catch {
        reEnqueueWithRetry(ctx, cid_str, mode, .retry_high, peer_addr, shard_index, retry_count);
        return;
    };
    const entries = replication.buildBlockEntries(allocator, ctx.store, all_blocks) catch {
        for (all_blocks) |b| allocator.free(b);
        allocator.free(all_blocks);
        reEnqueueWithRetry(ctx, cid_str, mode, .retry_high, peer_addr, shard_index, retry_count);
        return;
    };

    defer {
        for (all_blocks) |b| allocator.free(b);
        allocator.free(all_blocks);
    }
    defer replication.freeBlockEntries(allocator, entries);

    const hp = cluster_push.parseHostPort(allocator, peer_addr) catch {
        reEnqueueWithRetry(ctx, cid_str, mode, .retry_high, peer_addr, shard_index, retry_count);
        return;
    };
    defer allocator.free(hp.host);

    const push_fn = if (ctx.pool) |pool|
        cluster_push.dialClusterPushPooled(
            allocator,
            pool,
            hp.host,
            hp.port,
            cid_str,
            entries,
            ctx.cluster_secret,
            ctx.ed25519_secret64,
        )
    else
        cluster_push.dialClusterPush(
            allocator,
            hp.host,
            hp.port,
            cid_str,
            entries,
            ctx.cluster_secret,
            ctx.ed25519_secret64,
        );
    _ = push_fn catch {
        // Re-enqueue as retry
        reEnqueueWithRetry(ctx, cid_str, mode, .retry_low, peer_addr, shard_index, retry_count);
        return;
    };

    // Success — record confirmed peer
    state.addConfirmedPeer(cid_str, peer_addr) catch {
        reEnqueueWithRetry(ctx, cid_str, mode, .retry_high, peer_addr, shard_index, retry_count);
        return;
    };
}

/// Re-enqueue a failed item with a new priority.
/// Uses the queue's allocator to ensure consistent allocation/deallocation.
/// Drops items that exceed the maximum retry count to prevent infinite loops.
fn reEnqueueWithRetry(
    ctx: *SchedulerCtx,
    cid_str: []const u8,
    mode: ClusterMode,
    priority: repl_queue.Priority,
    target_peer: ?[]const u8,
    shard_index: ?u16,
    retry_count: u8,
) void {
    reEnqueueWithRetryAndFactor(ctx, cid_str, mode, priority, target_peer, shard_index, retry_count, 0);
}

fn reEnqueueWithRetryAndFactor(
    ctx: *SchedulerCtx,
    cid_str: []const u8,
    mode: ClusterMode,
    priority: repl_queue.Priority,
    target_peer: ?[]const u8,
    shard_index: ?u16,
    retry_count: u8,
    repl_factor: u8,
) void {
    // Drop items that exceed max retries — self-healing will catch them
    if (retry_count >= repl_queue.max_repl_retries) return;

    const q_alloc = ctx.queue.allocator;
    const cid_dup = q_alloc.dupe(u8, cid_str) catch return;
    const peer_dup = if (target_peer) |tp| q_alloc.dupe(u8, tp) catch {
        q_alloc.free(cid_dup);
        return;
    } else null;

    ctx.queue.push(.{
        .cid = cid_dup,
        .mode = mode,
        .priority = priority,
        .enqueued_ns = std.time.nanoTimestamp(),
        .target_peer = peer_dup,
        .shard_index = shard_index,
        .retry_count = retry_count + 1,
        .replication_factor = repl_factor,
    });
}

/// Add cluster peers from the cached config to cluster state.
fn addConfiguredPeers(state: *ClusterState, ctx: *SchedulerCtx) void {
    for (ctx.cluster_peers) |addr| {
        state.addPeer("unknown", addr) catch {};
    }
}

/// Create a manifest for a large DAG and notify target peers to pull.
fn createAndDistributeManifest(
    allocator: std.mem.Allocator,
    ctx: *SchedulerCtx,
    state: *ClusterState,
    item: *ReplItem,
) void {
    const factor = if (item.replication_factor > 0) item.replication_factor else ctx.replication_factor;
    const peer_count: u8 = if (factor > 1) factor - 1 else 1;

    // Get alive peers for manifest assignment
    const alive = state.alivePeers(allocator) catch return;
    defer allocator.free(alive);

    const selected = replication.selectPeersConsistentHash(
        allocator,
        alive,
        item.cid,
        peer_count,
        &.{},
    ) catch return;
    defer allocator.free(selected);

    if (selected.len == 0) return;

    // Build peer address list
    var peer_addrs = allocator.alloc([]const u8, selected.len) catch return;
    defer allocator.free(peer_addrs);
    for (selected, 0..) |s, i| {
        peer_addrs[i] = s.addr;
    }

    // Create manifest (streaming BFS walk, writes .cids file)
    var m = manifest_mod.createManifest(
        allocator,
        ctx.store,
        null, // no external lock needed — Blockstore is internally thread-safe
        ctx.repo_root,
        item.cid,
        peer_addrs,
        factor,
    ) catch |err| {
        std.log.err("manifest creation failed for {s}: {}", .{ item.cid, err });
        return;
    };
    defer m.deinit(allocator);

    // Ensure replica record exists
    _ = state.upsertReplica(item.cid, .replicate, peer_count) catch {};

    // Notify each peer via HTTP to start pulling
    for (selected) |peer| {
        const hp = cluster_push.parseHostPort(allocator, peer.addr) catch continue;
        defer allocator.free(hp.host);

        // Notify peer to pull from our gateway
        pull_engine.notifyPeerHttp(
            allocator,
            hp.host,
            hp.port,
            item.cid,
            hp.host,
            ctx.gateway_port,
            ctx.cluster_secret,
        ) catch {
            std.log.warn("manifest notify failed for peer {s}", .{peer.addr});
            continue;
        };
    }

    std.log.info("manifest created for {s}: {d} blocks, notified {d} peers", .{
        item.cid, m.total_blocks, selected.len,
    });
}
