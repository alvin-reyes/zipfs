//! Pull engine: HTTP-based block fetching with checkpointing and retry.
//!
//! Peers pull blocks one-at-a-time from the coordinator's HTTP API,
//! checkpointing progress every batch so transfers are resumable.

const std = @import("std");
const blockstore_mod = @import("blockstore.zig");
const cid_mod = @import("cid.zig");
const manifest_mod = @import("manifest.zig");

const Blockstore = blockstore_mod.Blockstore;
const Manifest = manifest_mod.Manifest;

/// Shared context for pull operations, carrying the blockstore, synchronization
/// mutex, and configuration needed by all pull workers.
pub const PullCtx = struct {
    store: *Blockstore,
    mu: *std.Thread.Mutex,
    repo_root: []const u8,
    cluster_secret: ?[]const u8,
    max_concurrent_pulls: u8 = 4,
    pull_batch_size: u16 = 32,
};

/// Describes a single pull job: which manifest to pull, from which origin, and where to resume.
pub const PullJob = struct {
    root_cid: []const u8,
    origin_host: []const u8,
    origin_port: u16,
    checkpoint_idx: u64,
    total_blocks: u64,
};

/// Pull blocks for a manifest from the origin via HTTP.
/// Reads .cids file in batches, skips blocks we already have,
/// fetches missing blocks via HTTP, checkpoints every batch.
/// Returns the number of blocks pulled.
pub fn pullManifest(
    allocator: std.mem.Allocator,
    ctx: *const PullCtx,
    job: *const PullJob,
) !u64 {
    var checkpoint_idx = job.checkpoint_idx;
    var total_pulled: u64 = 0;
    const batch_size = ctx.pull_batch_size;

    while (true) {
        // Read next batch of CIDs from the .cids file
        const cids = manifest_mod.readCidBatch(
            allocator,
            ctx.repo_root,
            job.root_cid,
            checkpoint_idx,
            batch_size,
        ) catch |err| {
            std.log.err("pull: readCidBatch failed: {}", .{err});
            return err;
        };
        defer {
            for (cids) |c| allocator.free(c);
            allocator.free(cids);
        }

        if (cids.len == 0) break; // EOF — all CIDs processed

        for (cids) |cid_str| {
            // Skip blocks we already have (no lock — Blockstore is internally thread-safe)
            const already_has = ctx.store.has(cid_str);
            if (already_has) {
                checkpoint_idx += 1;
                continue;
            }

            // Fetch block via HTTP
            const block_data = httpFetchBlock(
                allocator,
                job.origin_host,
                job.origin_port,
                cid_str,
                ctx.cluster_secret,
            ) catch |err| {
                switch (err) {
                    error.NotFound => {
                        // 404 — origin may have GC'd it, skip
                        std.log.warn("pull: block {s} not found on origin, skipping", .{cid_str});
                        checkpoint_idx += 1;
                        continue;
                    },
                    else => {
                        // Connection failure — save progress and return for retry
                        manifest_mod.saveProgress(
                            allocator,
                            ctx.repo_root,
                            job.root_cid,
                            job.origin_host,
                            checkpoint_idx,
                            total_pulled,
                        ) catch {};
                        return err;
                    },
                }
            };
            defer allocator.free(block_data);

            // Store the block (no lock — Blockstore is internally thread-safe)
            {
                var c = cid_mod.Cid.parse(allocator, cid_str) catch {
                    checkpoint_idx += 1;
                    continue;
                };
                defer c.deinit(allocator);
                ctx.store.put(allocator, c, block_data) catch {
                    checkpoint_idx += 1;
                    continue;
                };
            }

            total_pulled += 1;
            checkpoint_idx += 1;
        }

        // Checkpoint progress after each batch
        manifest_mod.saveProgress(
            allocator,
            ctx.repo_root,
            job.root_cid,
            job.origin_host,
            checkpoint_idx,
            total_pulled,
        ) catch {};
    }

    return total_pulled;
}

/// Fetch a single block from the origin node via HTTP/1.1.
/// Returns owned block data on success. Caller must free the returned slice.
/// Returns error.NotFound for 404, error.Forbidden for 403.
fn httpFetchBlock(
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
    cid_str: []const u8,
    cluster_secret: ?[]const u8,
) ![]u8 {
    // Connect
    const addr = try std.net.Address.parseIp(host, port);
    const stream = try std.net.tcpConnectToAddress(addr);
    defer stream.close();

    // Build request
    var req_buf: [2048]u8 = undefined;
    const req = if (cluster_secret) |secret|
        std.fmt.bufPrint(&req_buf, "GET /api/v0/block/{s} HTTP/1.1\r\nHost: {s}:{d}\r\nX-Cluster-Secret: {s}\r\nConnection: close\r\n\r\n", .{ cid_str, host, port, secret }) catch return error.RequestTooLong
    else
        std.fmt.bufPrint(&req_buf, "GET /api/v0/block/{s} HTTP/1.1\r\nHost: {s}:{d}\r\nConnection: close\r\n\r\n", .{ cid_str, host, port }) catch return error.RequestTooLong;

    try stream.writeAll(req);

    // Read response header
    var hdr_buf: [4096]u8 = undefined;
    var hdr_len: usize = 0;
    while (hdr_len < hdr_buf.len) {
        const n = stream.read(hdr_buf[hdr_len..]) catch return error.ConnectionError;
        if (n == 0) return error.ConnectionClosed;
        hdr_len += n;
        // Check for end of headers
        if (std.mem.indexOf(u8, hdr_buf[0..hdr_len], "\r\n\r\n") != null) break;
    }

    const hdr = hdr_buf[0..hdr_len];

    // Parse status line: "HTTP/1.1 NNN reason\r\n"
    const status_end = std.mem.indexOf(u8, hdr, "\r\n") orelse return error.BadResponse;
    const status_line = hdr[0..status_end];

    // Extract status code at fixed position (RFC 7230: "HTTP/1.1 NNN")
    if (status_line.len < 12) return error.BadResponse;
    const status_code = status_line[9..12];
    if (std.mem.eql(u8, status_code, "404")) return error.NotFound;
    if (std.mem.eql(u8, status_code, "403")) return error.Forbidden;
    if (!std.mem.eql(u8, status_code, "200")) return error.BadStatus;

    // Parse Content-Length
    const cl_marker = "Content-Length: ";
    const cl_idx = std.mem.indexOf(u8, hdr, cl_marker) orelse
        (std.mem.indexOf(u8, hdr, "content-length: ") orelse return error.NoContentLength);
    const cl_start = cl_idx + cl_marker.len;
    const cl_end_offset = std.mem.indexOf(u8, hdr[cl_start..], "\r\n") orelse return error.BadResponse;
    const cl_str = hdr[cl_start .. cl_start + cl_end_offset];
    const content_length = std.fmt.parseInt(usize, cl_str, 10) catch return error.BadContentLength;

    // Limit block size to 64MB
    if (content_length > 64 * 1024 * 1024) return error.BlockTooLarge;

    // Read body
    const body_start_idx = (std.mem.indexOf(u8, hdr, "\r\n\r\n") orelse return error.BadResponse) + 4;
    const initial_body = hdr[body_start_idx..hdr_len];

    const body = try allocator.alloc(u8, content_length);
    errdefer allocator.free(body);

    const have = @min(initial_body.len, content_length);
    @memcpy(body[0..have], initial_body[0..have]);

    var total = have;
    while (total < content_length) {
        const n = stream.read(body[total..]) catch return error.ConnectionError;
        if (n == 0) return error.ConnectionClosed;
        total += n;
    }

    return body;
}

/// Pull worker loop: watches for manifests with status=ready or status=active,
/// and pulls blocks for each assigned peer.
pub fn pullWorkerLoop(ctx: *const PullCtx) void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const allocator = gpa_state.allocator();

    while (true) {
        pullOnce(allocator, ctx);
        std.Thread.sleep(10 * std.time.ns_per_s); // Check every 10 seconds
    }
}

/// Run one pull cycle: scan for .notify files, pull blocks from origin.
fn pullOnce(allocator: std.mem.Allocator, ctx: *const PullCtx) void {
    const dir_path = std.fs.path.join(allocator, &.{ ctx.repo_root, "manifests" }) catch return;
    defer allocator.free(dir_path);

    var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true }) catch return;
    defer dir.close();

    // Collect .notify filenames first (can't modify dir while iterating)
    var notify_files = std.ArrayList([]u8).empty;
    defer {
        for (notify_files.items) |f| allocator.free(f);
        notify_files.deinit(allocator);
    }

    var it = dir.iterate();
    while (it.next() catch null) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".notify")) continue;
        notify_files.append(allocator, allocator.dupe(u8, entry.name) catch continue) catch continue;
    }

    for (notify_files.items) |filename| {
        // Extract root_cid from filename: "<root_cid>.notify"
        const root_cid = filename[0 .. filename.len - ".notify".len];
        if (root_cid.len == 0) continue;

        const notify_path = std.fs.path.join(allocator, &.{ dir_path, filename }) catch continue;
        defer allocator.free(notify_path);

        // Read notify content: "origin_host:origin_port"
        const content = std.fs.cwd().readFileAlloc(allocator, notify_path, 4096) catch continue;
        defer allocator.free(content);

        // Trim whitespace
        var trimmed = content;
        while (trimmed.len > 0 and (trimmed[trimmed.len - 1] == '\n' or trimmed[trimmed.len - 1] == '\r' or trimmed[trimmed.len - 1] == ' '))
            trimmed = trimmed[0 .. trimmed.len - 1];

        if (trimmed.len == 0) {
            std.fs.cwd().deleteFile(notify_path) catch {};
            continue;
        }

        // Parse "host:port"
        const colon = std.mem.lastIndexOfScalar(u8, trimmed, ':') orelse {
            std.log.warn("pull: invalid notify content for {s}", .{root_cid});
            continue;
        };
        const origin_host = trimmed[0..colon];
        const origin_port = std.fmt.parseInt(u16, trimmed[colon + 1 ..], 10) catch {
            std.log.warn("pull: invalid port in notify for {s}", .{root_cid});
            continue;
        };

        // Load checkpoint progress if resuming
        const progress = manifest_mod.loadProgress(allocator, ctx.repo_root, root_cid, origin_host);

        // Load manifest to get total_blocks (may not exist yet if origin hasn't sent it)
        var total_blocks: u64 = 0;
        if (Manifest.load(allocator, ctx.repo_root, root_cid)) |m| {
            total_blocks = m.total_blocks;
            var mm = m;
            mm.deinit(allocator);
        } else |_| {}

        const job = PullJob{
            .root_cid = root_cid,
            .origin_host = origin_host,
            .origin_port = origin_port,
            .checkpoint_idx = progress.last_idx,
            .total_blocks = total_blocks,
        };

        std.log.info("pull: starting pull for {s} from {s}:{d} (checkpoint={d})", .{
            root_cid, origin_host, origin_port, progress.last_idx,
        });

        const pulled = pullManifest(allocator, ctx, &job) catch |err| {
            std.log.err("pull: pullManifest failed for {s}: {}", .{ root_cid, err });
            continue; // Keep .notify file for retry on next cycle
        };

        std.log.info("pull: completed {s}: pulled {d} blocks", .{ root_cid, pulled });

        // Update manifest status to complete
        if (Manifest.load(allocator, ctx.repo_root, root_cid)) |loaded| {
            var m = loaded;
            defer m.deinit(allocator);
            m.status = .complete;
            m.updated_ns = std.time.nanoTimestamp();
            m.save(allocator, ctx.repo_root) catch {};
        } else |_| {}

        // Remove .notify file on success
        std.fs.cwd().deleteFile(notify_path) catch {};
    }
}

/// Notify a peer to start pulling a manifest via HTTP POST (fire-and-forget).
/// The response status is not checked; the peer will independently verify
/// the manifest and begin pulling.
pub fn notifyPeerHttp(
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
    root_cid: []const u8,
    origin_host: []const u8,
    origin_port: u16,
    cluster_secret: ?[]const u8,
) !void {
    const addr = try std.net.Address.parseIp(host, port);
    const stream = try std.net.tcpConnectToAddress(addr);
    defer stream.close();

    const body = try std.fmt.allocPrint(allocator,
        "{{\"root_cid\":\"{s}\",\"origin_host\":\"{s}\",\"origin_port\":{d}}}",
        .{ root_cid, origin_host, origin_port },
    );
    defer allocator.free(body);

    var req_buf: [2048]u8 = undefined;
    const req = if (cluster_secret) |secret|
        std.fmt.bufPrint(&req_buf,
            "POST /api/v0/cluster/manifest/notify HTTP/1.1\r\nHost: {s}:{d}\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nX-Cluster-Secret: {s}\r\nConnection: close\r\n\r\n",
            .{ host, port, body.len, secret },
        ) catch return error.RequestTooLong
    else
        std.fmt.bufPrint(&req_buf,
            "POST /api/v0/cluster/manifest/notify HTTP/1.1\r\nHost: {s}:{d}\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
            .{ host, port, body.len },
        ) catch return error.RequestTooLong;

    try stream.writeAll(req);
    try stream.writeAll(body);

    // Drain response so the peer sees a clean close.
    var resp_buf: [1024]u8 = undefined;
    _ = stream.read(&resp_buf) catch {};
}
