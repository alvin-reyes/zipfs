//! Replication scheduler: inbox poller, priority-dispatched scheduler loop, and batched per-peer workers.

const std = @import("std");
const repl_queue = @import("repl_queue.zig");
const cluster_mod = @import("cluster.zig");
const replication = @import("replication.zig");
const blockstore_mod = @import("blockstore.zig");
const cluster_push = @import("net/cluster_push.zig");


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
    const data = std.fs.cwd().readFileAlloc(allocator, pp, 1 << 20) catch {
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
    const data = std.fs.cwd().readFileAlloc(allocator, proc_path, 1 << 20) catch {
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

    while (true) {
        const batch = ctx.queue.popBatch(ctx.batch_size, 5 * std.time.ns_per_s);
        if (batch.len == 0) continue;

        // Process each item in the batch
        for (batch) |*item| {
            // Rate limit
            ctx.rate_limiter.waitAcquire();

            processItem(allocator, ctx, item);
        }

        ctx.queue.allocator.free(batch);
    }
}

/// Process a single replication item: load state, resolve peers, push blocks.
/// Uses the queue's allocator for item deallocation to match allocation source.
fn processItem(allocator: std.mem.Allocator, ctx: *SchedulerCtx, item: *ReplItem) void {
    defer item.deinit(ctx.queue.allocator);

    // Lock state_mu to prevent concurrent ClusterState load/save races with self-healing
    if (ctx.state_mu) |smu| smu.lock();
    defer if (ctx.state_mu) |smu| smu.unlock();

    // Load cluster state
    var state = ClusterState.load(allocator, ctx.repo_root) catch return;
    defer state.deinit();

    // Add configured peers if needed
    addConfiguredPeers(&state, ctx);

    if (item.target_peer) |target| {
        // Targeted retry: push directly to the specified peer
        pushToPeer(allocator, ctx, &state, item.cid, target, item.mode, item.shard_index, item.retry_count);
    } else {
        // Hold blockstore mutex during replication to prevent data races with swarm
        ctx.mu.lock();
        defer ctx.mu.unlock();

        // Auto-select peers and replicate/shard
        switch (item.mode) {
            .replicate => {
                const factor = if (item.replication_factor > 0) item.replication_factor else ctx.replication_factor;
                replication.replicateCid(
                    allocator,
                    &state,
                    ctx.store,
                    item.cid,
                    factor,
                    ctx.cluster_secret,
                    ctx.ed25519_secret64,
                ) catch {
                    // Re-enqueue with higher priority on failure, preserving per-item factor
                    reEnqueueWithRetryAndFactor(ctx, item.cid, item.mode, .retry_high, null, null, item.retry_count, item.replication_factor);
                    state.save(ctx.repo_root) catch |e| {
                        std.log.err("cluster state save failed: {}", .{e});
                    };
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
                    state.save(ctx.repo_root) catch |e| {
                        std.log.err("cluster state save failed: {}", .{e});
                    };
                    return;
                };
            },
        }
    }

    state.save(ctx.repo_root) catch |e| {
        std.log.err("cluster state save failed: {}", .{e});
    };
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

    // Hold blockstore mutex while reading blocks
    ctx.mu.lock();
    const all_blocks = replication.collectDagBlocks(allocator, ctx.store, cid_str) catch {
        ctx.mu.unlock();
        reEnqueueWithRetry(ctx, cid_str, mode, .retry_high, peer_addr, shard_index, retry_count);
        return;
    };
    const entries = replication.buildBlockEntries(allocator, ctx.store, all_blocks) catch {
        ctx.mu.unlock();
        for (all_blocks) |b| allocator.free(b);
        allocator.free(all_blocks);
        reEnqueueWithRetry(ctx, cid_str, mode, .retry_high, peer_addr, shard_index, retry_count);
        return;
    };
    ctx.mu.unlock();

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

    _ = cluster_push.dialClusterPush(
        allocator,
        hp.host,
        hp.port,
        cid_str,
        entries,
        ctx.cluster_secret,
        ctx.ed25519_secret64,
    ) catch {
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
