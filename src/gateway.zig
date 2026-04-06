//! HTTP gateway: `GET /ipfs/<cid>/<path>` + `POST /api/v0/add` (Kubo-compatible).

const std = @import("std");
const Blockstore = @import("blockstore.zig").Blockstore;
const resolver = @import("resolver.zig");
const importer = @import("importer.zig");
const pin = @import("pin.zig");
const repl_queue = @import("repl_queue.zig");
const cluster_mod = @import("cluster.zig");
const replication = @import("replication.zig");

/// Context for the gateway server, carrying mutable state needed for writes.
pub const GatewayCtx = struct {
    store: *Blockstore,
    store_sync: ?*std.Thread.Mutex = null,
    repo_root: []const u8,
    chunk_size: u32 = 262144,
    port: u16 = 8080,
    /// Node identity: PeerID string (e.g. "QmXyz..."), set by caller.
    peer_id: ?[]const u8 = null,
    /// Addresses this node listens on (multiaddr strings).
    listen_addrs: []const []const u8 = &.{},
    /// Addresses this node announces to the network.
    announce_addrs: []const []const u8 = &.{},
    /// Cluster peer addresses.
    cluster_peers: []const []const u8 = &.{},
    /// Replication factor (0 = not configured).
    replication_factor: u8 = 0,
    /// Max concurrent connections (default 64).
    max_conns: u32 = 64,
    /// Direct queue injection: bypass file inbox for replication (daemon mode).
    queue: ?*repl_queue.ReplQueue = null,
    /// Cluster mode for replication items.
    cluster_mode: cluster_mod.ClusterMode = .replicate,
    /// Synchronous replication: block response until peers confirm.
    sync_repl: bool = false,
    /// Ed25519 secret for cluster push authentication.
    ed25519_secret64: ?[64]u8 = null,
    /// Cluster shared secret for authentication.
    cluster_secret: ?[]const u8 = null,
    /// Mutex guarding ClusterState load/save.
    state_mu: ?*std.Thread.Mutex = null,
};

fn sendResp(stream: std.net.Stream, status: []const u8, content_type: []const u8, body: []const u8) void {
    var hdr: [512]u8 = undefined;
    const h = std.fmt.bufPrint(&hdr,
        "{s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
        .{ status, content_type, body.len },
    ) catch return;
    stream.writeAll(h) catch return;
    stream.writeAll(body) catch return;
}

fn sendRespWithHeaders(stream: std.net.Stream, status: []const u8, extra_headers: []const u8, body: []const u8) void {
    var hdr: [1024]u8 = undefined;
    const h = std.fmt.bufPrint(&hdr,
        "{s}\r\n{s}Content-Length: {d}\r\nConnection: close\r\n\r\n",
        .{ status, extra_headers, body.len },
    ) catch return;
    stream.writeAll(h) catch return;
    stream.writeAll(body) catch return;
}

/// Extract a header value from raw HTTP headers. Case-insensitive field name match.
fn getHeader(headers: []const u8, name: []const u8) ?[]const u8 {
    var rest = headers;
    while (std.mem.indexOf(u8, rest, "\r\n")) |nl| {
        const line = rest[0..nl];
        rest = rest[nl + 2 ..];
        if (line.len == 0) break;
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const field = line[0..colon];
        if (field.len != name.len) continue;
        var match = true;
        for (field, name) |a, b| {
            if (std.ascii.toLower(a) != std.ascii.toLower(b)) {
                match = false;
                break;
            }
        }
        if (!match) continue;
        // Skip ": " prefix
        var val = line[colon + 1 ..];
        while (val.len > 0 and val[0] == ' ') val = val[1..];
        return val;
    }
    return null;
}

/// Extract boundary string from Content-Type header value.
fn parseBoundary(content_type: []const u8) ?[]const u8 {
    const marker = "boundary=";
    const idx = std.mem.indexOf(u8, content_type, marker) orelse return null;
    var bnd = content_type[idx + marker.len ..];
    // Strip optional quotes
    if (bnd.len > 0 and bnd[0] == '"') {
        bnd = bnd[1..];
        if (std.mem.indexOfScalar(u8, bnd, '"')) |end| {
            bnd = bnd[0..end];
        }
    }
    // Trim at semicolon/space if present
    if (std.mem.indexOfAny(u8, bnd, "; \r\n")) |end| {
        bnd = bnd[0..end];
    }
    return if (bnd.len > 0) bnd else null;
}

/// Extract filename from a Content-Disposition header line.
fn parseFilename(disposition: []const u8) []const u8 {
    const marker = "filename=\"";
    const idx = std.mem.indexOf(u8, disposition, marker) orelse return "file";
    const start = idx + marker.len;
    const rest = disposition[start..];
    const end = std.mem.indexOfScalar(u8, rest, '"') orelse rest.len;
    return if (end > 0) rest[0..end] else "file";
}

/// Escape a string for safe embedding in a JSON string value.
/// Escapes backslash, double-quote, and control characters.
fn jsonEscapeString(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    for (input) |c| {
        switch (c) {
            '"' => try out.appendSlice(allocator, "\\\""),
            '\\' => try out.appendSlice(allocator, "\\\\"),
            '\n' => try out.appendSlice(allocator, "\\n"),
            '\r' => try out.appendSlice(allocator, "\\r"),
            '\t' => try out.appendSlice(allocator, "\\t"),
            else => {
                if (c < 0x20) {
                    var esc: [6]u8 = undefined;
                    _ = std.fmt.bufPrint(&esc, "\\u{x:0>4}", .{c}) catch continue;
                    try out.appendSlice(allocator, &esc);
                } else {
                    try out.append(allocator, c);
                }
            },
        }
    }
    return out.toOwnedSlice(allocator);
}

/// Extract file data and filename from multipart/form-data body.
/// Returns (file_data, filename) or null if parsing fails.
fn parseMultipartFile(body: []const u8, boundary: []const u8) ?struct { data: []const u8, filename: []const u8 } {
    // Find first boundary
    var search_bnd: [74]u8 = undefined;
    const full_bnd = std.fmt.bufPrint(&search_bnd, "--{s}", .{boundary}) catch return null;

    const bnd_start = std.mem.indexOf(u8, body, full_bnd) orelse return null;
    const after_bnd = body[bnd_start + full_bnd.len ..];

    // Find headers end (blank line)
    const hdrs_end = std.mem.indexOf(u8, after_bnd, "\r\n\r\n") orelse return null;
    const part_headers = after_bnd[0..hdrs_end];
    const data_start = after_bnd[hdrs_end + 4 ..];

    // Find closing boundary
    var close_bnd: [78]u8 = undefined;
    const close_full = std.fmt.bufPrint(&close_bnd, "\r\n--{s}", .{boundary}) catch return null;
    const data_end = std.mem.indexOf(u8, data_start, close_full) orelse data_start.len;

    const filename = parseFilename(part_headers);

    return .{ .data = data_start[0..data_end], .filename = filename };
}

/// Read the full HTTP request body based on Content-Length.
/// `initial_body` is the portion already read after headers.
/// Returns allocated body buffer that caller must free.
fn readFullBody(allocator: std.mem.Allocator, stream: std.net.Stream, content_length: usize, initial_body: []const u8) ![]u8 {
    const max_body: usize = 10 * 1024 * 1024 * 1024; // 10GB
    if (content_length > max_body) return error.PayloadTooLarge;

    const body_buf = try allocator.alloc(u8, content_length);
    errdefer allocator.free(body_buf);

    // Copy what we already have
    const have = @min(initial_body.len, content_length);
    @memcpy(body_buf[0..have], initial_body[0..have]);

    // Read the rest
    var total = have;
    while (total < content_length) {
        const n = stream.read(body_buf[total..content_length]) catch return error.ReadError;
        if (n == 0) return error.UnexpectedEof;
        total += n;
    }

    return body_buf;
}

/// Parse query string parameter value. Returns null if not found.
fn getQueryParam(query: []const u8, name: []const u8) ?[]const u8 {
    var rest = query;
    while (rest.len > 0) {
        const amp = std.mem.indexOfScalar(u8, rest, '&') orelse rest.len;
        const pair = rest[0..amp];
        rest = if (amp < rest.len) rest[amp + 1 ..] else "";
        const eq = std.mem.indexOfScalar(u8, pair, '=') orelse continue;
        if (std.mem.eql(u8, pair[0..eq], name)) {
            return pair[eq + 1 ..];
        }
    }
    return null;
}

/// Synchronous replication: load cluster state, push blocks to peers, return confirmed count.
/// No external blockstore lock needed — Blockstore is internally thread-safe via cache_mu.
/// state_mu is narrowed to file I/O only (load/save) to prevent blocking gateway threads during network pushes.
fn doSyncReplication(allocator: std.mem.Allocator, ctx: *const GatewayCtx, cid_str: []const u8, repl_factor: u8) usize {
    // Load cluster state under state_mu (protects file access only)
    var cstate = blk: {
        if (ctx.state_mu) |smu| smu.lock();
        defer if (ctx.state_mu) |smu| smu.unlock();
        break :blk cluster_mod.ClusterState.load(allocator, ctx.repo_root) catch |err| {
            std.log.err("sync repl: ClusterState.load failed: {}", .{err});
            return 0;
        };
    };
    defer cstate.deinit();

    for (ctx.cluster_peers) |addr| {
        cstate.addPeer("unknown", addr) catch {};
    }

    const factor = if (repl_factor > 0) repl_factor else ctx.replication_factor;
    const peer_target: u8 = if (factor > 1) factor - 1 else 1;

    // Replicate without holding any lock — blockstore reads and network I/O are lock-free
    replication.replicateCid(
        allocator,
        &cstate,
        ctx.store,
        cid_str,
        peer_target,
        ctx.cluster_secret,
        ctx.ed25519_secret64 orelse [_]u8{0} ** 64,
    ) catch |err| {
        std.log.err("sync repl: replicateCid failed: {}", .{err});
    };

    // Save state under state_mu
    {
        if (ctx.state_mu) |smu| smu.lock();
        defer if (ctx.state_mu) |smu| smu.unlock();
        cstate.save(ctx.repo_root) catch |err| {
            std.log.err("sync repl: state save failed: {}", .{err});
        };
    }

    // Count confirmed peers from replica record
    if (cstate.replicas.getPtr(cid_str)) |rec| {
        return rec.confirmed_peers.len;
    }
    return 0;
}

/// Handle POST /api/v0/add — import file, pin, notify replication inbox.
fn handleAdd(allocator: std.mem.Allocator, ctx: *const GatewayCtx, stream: std.net.Stream, headers: []const u8, initial_body: []const u8, target: []const u8) void {
    // Parse Content-Length
    const cl_str = getHeader(headers, "content-length") orelse {
        sendResp(stream, "HTTP/1.1 411 Length Required", "text/plain", "Content-Length required\n");
        return;
    };
    const content_length = std.fmt.parseInt(usize, cl_str, 10) catch {
        sendResp(stream, "HTTP/1.1 400 Bad Request", "text/plain", "invalid Content-Length\n");
        return;
    };

    // Parse Content-Type for boundary
    const ct = getHeader(headers, "content-type") orelse {
        sendResp(stream, "HTTP/1.1 400 Bad Request", "text/plain", "Content-Type required\n");
        return;
    };
    const boundary = parseBoundary(ct) orelse {
        sendResp(stream, "HTTP/1.1 400 Bad Request", "text/plain", "multipart boundary required\n");
        return;
    };

    // Read full body
    const body = readFullBody(allocator, stream, content_length, initial_body) catch |err| switch (err) {
        error.PayloadTooLarge => {
            sendResp(stream, "HTTP/1.1 413 Payload Too Large", "text/plain", "max upload size is 10GB\n");
            return;
        },
        else => {
            sendResp(stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "failed to read body\n");
            return;
        },
    };
    defer allocator.free(body);

    // Extract file from multipart
    const parsed = parseMultipartFile(body, boundary) orelse {
        sendResp(stream, "HTTP/1.1 400 Bad Request", "text/plain", "could not parse multipart file\n");
        return;
    };

    // Import file into blockstore and get CID string
    // No external lock needed — Blockstore is internally thread-safe via cache_mu
    var cid_buf: [128]u8 = undefined;
    var cid_len: usize = 0;
    {
        const root_cid = importer.addFileWithChunk(allocator, ctx.store, parsed.data, ctx.chunk_size) catch {
            sendResp(stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "import failed\n");
            return;
        };
        defer root_cid.deinit(allocator);

        // Copy CID string to stack buffer
        const cid_str_tmp = root_cid.toString(allocator) catch {
            sendResp(stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "cid encoding failed\n");
            return;
        };
        defer allocator.free(cid_str_tmp);
        if (cid_str_tmp.len > cid_buf.len) {
            sendResp(stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "cid too long\n");
            return;
        }
        @memcpy(cid_buf[0..cid_str_tmp.len], cid_str_tmp);
        cid_len = cid_str_tmp.len;
    }
    const cid_str = cid_buf[0..cid_len];

    // Parse query params for ?pin= (default true) and ?replication=N
    var do_pin = true;
    var repl_factor: u8 = 0; // 0 = use cluster default
    if (std.mem.indexOf(u8, target, "?")) |qmark| {
        const query = target[qmark + 1 ..];
        if (getQueryParam(query, "pin")) |val| {
            if (std.mem.eql(u8, val, "false") or std.mem.eql(u8, val, "0")) {
                do_pin = false;
            }
        }
        if (getQueryParam(query, "replication")) |val| {
            repl_factor = std.fmt.parseInt(u8, val, 10) catch 0;
        }
    }

    // Pin + replication
    var replicated_count: usize = 0;
    if (do_pin) {
        var pins = pin.PinSet.load(allocator, ctx.repo_root) catch |err| {
            std.log.err("PinSet.load failed: {}", .{err});
            return;
        };
        defer pins.deinit(allocator);
        pins.pinDirect(allocator, cid_str) catch |err| {
            std.log.err("pinDirect failed: {}", .{err});
        };
        pins.save(allocator, ctx.repo_root) catch |err| {
            std.log.err("pin save failed: {}", .{err});
        };

        if (ctx.sync_repl) {
            // Change C: synchronous replication — block until peers confirm
            replicated_count = doSyncReplication(allocator, ctx, cid_str, repl_factor);
        } else if (ctx.queue) |q| {
            // Change B: direct queue injection — bypass file inbox
            const duped_cid = q.allocator.dupe(u8, cid_str) catch null;
            if (duped_cid) |cid_owned| {
                q.push(.{
                    .cid = cid_owned,
                    .mode = ctx.cluster_mode,
                    .priority = .immediate,
                    .enqueued_ns = std.time.nanoTimestamp(),
                    .target_peer = null,
                    .shard_index = null,
                    .replication_factor = repl_factor,
                });
            } else {
                // OOM fallback: use file-based inbox
                pin.notifyInboxWithFactor(allocator, ctx.repo_root, cid_str, repl_factor) catch {};
            }
        } else {
            // Fallback: file-based inbox (gateway-only mode or no cluster)
            pin.notifyInboxWithFactor(allocator, ctx.repo_root, cid_str, repl_factor) catch {};
        }
    }

    // Kubo-compatible NDJSON response (with JSON-escaped filename)
    const escaped_name = jsonEscapeString(allocator, parsed.filename) catch {
        sendResp(stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "format error\n");
        return;
    };
    defer allocator.free(escaped_name);

    const json_resp = if (ctx.sync_repl)
        std.fmt.allocPrint(
            allocator,
            "{{\"Name\":\"{s}\",\"Hash\":\"{s}\",\"Size\":\"{d}\",\"Replicated\":{d}}}\n",
            .{ escaped_name, cid_str, parsed.data.len, replicated_count },
        )
    else
        std.fmt.allocPrint(
            allocator,
            "{{\"Name\":\"{s}\",\"Hash\":\"{s}\",\"Size\":\"{d}\"}}\n",
            .{ escaped_name, cid_str, parsed.data.len },
        );
    const json_body = json_resp catch {
        sendResp(stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "json format error\n");
        return;
    };
    defer allocator.free(json_body);

    sendRespWithHeaders(
        stream,
        "HTTP/1.1 200 OK",
        "Content-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\n",
        json_body,
    );
}

/// Constant-time comparison for cluster secret validation.
fn validateClusterSecret(ctx: *const GatewayCtx, headers: []const u8) bool {
    const expected = ctx.cluster_secret orelse return true; // No secret = open cluster
    const provided = getHeader(headers, "x-cluster-secret") orelse return false;
    if (expected.len != provided.len) return false;
    // XOR-accumulate for constant-time comparison
    var acc: u8 = 0;
    for (expected, provided) |a, b| {
        acc |= a ^ b;
    }
    return acc == 0;
}

/// Handle GET /api/v0/block/<cid> — return raw block data for pull replication.
fn handleBlockGet(allocator: std.mem.Allocator, ctx: *const GatewayCtx, stream: std.net.Stream, target: []const u8, headers: []const u8) void {
    if (!validateClusterSecret(ctx, headers)) {
        sendResp(stream, "HTTP/1.1 403 Forbidden", "text/plain", "invalid cluster secret\n");
        return;
    }

    // Extract CID from path: /api/v0/block/<cid>
    const prefix = "/api/v0/block/";
    if (!std.mem.startsWith(u8, target, prefix)) {
        sendResp(stream, "HTTP/1.1 400 Bad Request", "text/plain", "invalid path\n");
        return;
    }
    const cid_str = target[prefix.len..];
    if (cid_str.len == 0) {
        sendResp(stream, "HTTP/1.1 400 Bad Request", "text/plain", "missing CID\n");
        return;
    }

    // Look up block in store (no external lock — Blockstore is internally thread-safe)
    const block_data = ctx.store.get(allocator, cid_str);

    if (block_data) |data| {
        defer allocator.free(data);
        sendResp(stream, "HTTP/1.1 200 OK", "application/octet-stream", data);
    } else {
        sendResp(stream, "HTTP/1.1 404 Not Found", "text/plain", "block not found\n");
    }
}

/// Handle POST /api/v0/cluster/manifest/notify — receive manifest pull notification.
fn handleManifestNotify(allocator: std.mem.Allocator, ctx: *const GatewayCtx, stream: std.net.Stream, headers: []const u8, initial_body: []const u8) void {
    if (!validateClusterSecret(ctx, headers)) {
        sendResp(stream, "HTTP/1.1 403 Forbidden", "text/plain", "invalid cluster secret\n");
        return;
    }

    // Read body
    const cl_str = getHeader(headers, "content-length") orelse {
        sendResp(stream, "HTTP/1.1 411 Length Required", "text/plain", "Content-Length required\n");
        return;
    };
    const content_length = std.fmt.parseInt(usize, cl_str, 10) catch {
        sendResp(stream, "HTTP/1.1 400 Bad Request", "text/plain", "invalid Content-Length\n");
        return;
    };
    if (content_length > 4096) {
        sendResp(stream, "HTTP/1.1 413 Payload Too Large", "text/plain", "body too large\n");
        return;
    }

    const body = readFullBody(allocator, stream, content_length, initial_body) catch {
        sendResp(stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "failed to read body\n");
        return;
    };
    defer allocator.free(body);

    // Parse JSON: {"root_cid":"...","origin_host":"...","origin_port":N}
    const Json = struct {
        root_cid: ?[]const u8 = null,
        origin_host: ?[]const u8 = null,
        origin_port: ?u16 = null,
    };
    var parsed = std.json.parseFromSlice(Json, allocator, body, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    }) catch {
        sendResp(stream, "HTTP/1.1 400 Bad Request", "text/plain", "invalid JSON\n");
        return;
    };
    defer parsed.deinit();

    const root_cid = parsed.value.root_cid orelse {
        sendResp(stream, "HTTP/1.1 400 Bad Request", "text/plain", "missing root_cid\n");
        return;
    };
    // Validate root_cid: reject path separators to prevent directory traversal
    for (root_cid) |c| {
        if (c == '/' or c == '\\' or c == 0) {
            sendResp(stream, "HTTP/1.1 400 Bad Request", "text/plain", "invalid root_cid\n");
            return;
        }
    }
    if (std.mem.indexOf(u8, root_cid, "..") != null) {
        sendResp(stream, "HTTP/1.1 400 Bad Request", "text/plain", "invalid root_cid\n");
        return;
    }
    const origin_host = parsed.value.origin_host orelse {
        sendResp(stream, "HTTP/1.1 400 Bad Request", "text/plain", "missing origin_host\n");
        return;
    };
    const origin_port = parsed.value.origin_port orelse {
        sendResp(stream, "HTTP/1.1 400 Bad Request", "text/plain", "missing origin_port\n");
        return;
    };

    // Write a manifest notification file for the pull worker to pick up
    const dir_path = std.fs.path.join(allocator, &.{ ctx.repo_root, "manifests" }) catch {
        sendResp(stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "path error\n");
        return;
    };
    defer allocator.free(dir_path);
    std.fs.cwd().makePath(dir_path) catch {};

    const notify_filename = std.fmt.allocPrint(allocator, "{s}.notify", .{root_cid}) catch {
        sendResp(stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "format error\n");
        return;
    };
    defer allocator.free(notify_filename);
    const notify_path = std.fs.path.join(allocator, &.{ dir_path, notify_filename }) catch {
        sendResp(stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "path error\n");
        return;
    };
    defer allocator.free(notify_path);

    const notify_content = std.fmt.allocPrint(allocator, "{s}:{d}", .{ origin_host, origin_port }) catch {
        sendResp(stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "format error\n");
        return;
    };
    defer allocator.free(notify_content);

    const file = std.fs.cwd().createFile(notify_path, .{ .truncate = true }) catch {
        sendResp(stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "write error\n");
        return;
    };
    file.writeAll(notify_content) catch {
        file.close();
        sendResp(stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "write error\n");
        return;
    };
    file.sync() catch |e| {
        std.log.warn("manifest notify fsync failed: {}", .{e});
    };
    file.close();

    sendRespWithHeaders(
        stream,
        "HTTP/1.1 200 OK",
        "Content-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\n",
        "{\"status\":\"accepted\"}\n",
    );
}

/// Handle GET/POST /api/v0/id -- return node identity, addresses, and cluster info as JSON.
fn handleId(allocator: std.mem.Allocator, ctx: *const GatewayCtx, stream: std.net.Stream) void {
    // Build addresses JSON array
    var addrs_buf: std.ArrayList(u8) = .empty;
    defer addrs_buf.deinit(allocator);
    addrs_buf.appendSlice(allocator, "[") catch return;
    const all_addrs = if (ctx.announce_addrs.len > 0) ctx.announce_addrs else ctx.listen_addrs;
    for (all_addrs, 0..) |addr, i| {
        if (i > 0) addrs_buf.appendSlice(allocator, ",") catch return;
        addrs_buf.appendSlice(allocator, "\"") catch return;
        addrs_buf.appendSlice(allocator, addr) catch return;
        addrs_buf.appendSlice(allocator, "\"") catch return;
    }
    addrs_buf.appendSlice(allocator, "]") catch return;

    // Build cluster peers JSON array
    var peers_buf: std.ArrayList(u8) = .empty;
    defer peers_buf.deinit(allocator);
    peers_buf.appendSlice(allocator, "[") catch return;
    for (ctx.cluster_peers, 0..) |peer, i| {
        if (i > 0) peers_buf.appendSlice(allocator, ",") catch return;
        peers_buf.appendSlice(allocator, "\"") catch return;
        peers_buf.appendSlice(allocator, peer) catch return;
        peers_buf.appendSlice(allocator, "\"") catch return;
    }
    peers_buf.appendSlice(allocator, "]") catch return;

    const pid = ctx.peer_id orelse "unknown";
    const resp = std.fmt.allocPrint(
        allocator,
        "{{\"ID\":\"{s}\",\"Addresses\":{s},\"ClusterPeers\":{s},\"ReplicationFactor\":{d}}}\n",
        .{ pid, addrs_buf.items, peers_buf.items, ctx.replication_factor },
    ) catch return;
    defer allocator.free(resp);

    sendRespWithHeaders(
        stream,
        "HTTP/1.1 200 OK",
        "Content-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\n",
        resp,
    );
}

/// Handle a single connection in its own thread.
fn handleConnection(ctx: *const GatewayCtx, stream: std.net.Stream, active: *std.atomic.Value(u32)) void {
    defer stream.close();
    defer _ = active.fetchSub(1, .release);

    // Per-thread allocator (page_allocator avoids GPA contention)
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var buf: [65536]u8 = undefined;
    const n = stream.read(&buf) catch return;
    if (n == 0) return;
    const req = buf[0..n];

    // Find end of request line
    const line_end = std.mem.indexOf(u8, req, "\r\n") orelse return;
    const line = req[0..line_end];
    if (line.len < 10) return;

    // Determine method
    const is_get = std.mem.startsWith(u8, line, "GET ");
    const is_post = std.mem.startsWith(u8, line, "POST ");
    if (!is_get and !is_post) {
        sendResp(stream, "HTTP/1.1 405 Method Not Allowed", "text/plain", "method not allowed\n");
        return;
    }

    // Parse target path
    const method_len: usize = if (is_get) 4 else 5;
    const path_end = std.mem.indexOfScalar(u8, line[method_len..], ' ') orelse return;
    const target = line[method_len .. method_len + path_end];

    // Find headers section (everything after request line)
    const headers = req[line_end + 2 ..];

    // Route: POST /api/v0/add or POST /api/v0/id
    if (is_post) {
        const target_path = if (std.mem.indexOf(u8, target, "?")) |q| target[0..q] else target;
        if (std.mem.eql(u8, target_path, "/api/v0/add")) {
            // Find where body starts (after \r\n\r\n)
            const hdrs_end = std.mem.indexOf(u8, req, "\r\n\r\n") orelse return;
            const body_start = hdrs_end + 4;
            const initial_body = if (body_start < n) req[body_start..n] else "";
            handleAdd(allocator, ctx, stream, headers, initial_body, target);
            return;
        }
        if (std.mem.eql(u8, target_path, "/api/v0/id")) {
            handleId(allocator, ctx, stream);
            return;
        }
        if (std.mem.eql(u8, target_path, "/api/v0/cluster/manifest/notify")) {
            const hdrs_end_post = std.mem.indexOf(u8, req, "\r\n\r\n") orelse return;
            const body_start_post = hdrs_end_post + 4;
            const initial_body_post = if (body_start_post < n) req[body_start_post..n] else "";
            handleManifestNotify(allocator, ctx, stream, headers, initial_body_post);
            return;
        }
        sendResp(stream, "HTTP/1.1 404 Not Found", "text/plain", "not found\n");
        return;
    }

    // Route: GET /api/v0/id
    {
        const get_path = if (std.mem.indexOf(u8, target, "?")) |q| target[0..q] else target;
        if (std.mem.eql(u8, get_path, "/api/v0/id")) {
            handleId(allocator, ctx, stream);
            return;
        }
        // Route: GET /api/v0/block/<cid> — raw block data for pull replication
        if (std.mem.startsWith(u8, get_path, "/api/v0/block/")) {
            handleBlockGet(allocator, ctx, stream, get_path, headers);
            return;
        }
    }

    // Route: GET /health (for Railway/Docker healthchecks)
    if (std.mem.eql(u8, target, "/health") or std.mem.eql(u8, target, "/")) {
        sendResp(stream, "HTTP/1.1 200 OK", "text/plain", "ok\n");
        return;
    }

    // Route: GET /ipfs/...
    if (!std.mem.startsWith(u8, target, "/ipfs/")) {
        if (std.mem.startsWith(u8, target, "/ipns/")) {
            sendResp(stream, "HTTP/1.1 501 Not Implemented", "text/plain", "ipns not enabled\n");
            return;
        }
        sendResp(stream, "HTTP/1.1 404 Not Found", "text/plain", "not found\n");
        return;
    }

    const rest = target["/ipfs/".len..];
    const slash = std.mem.indexOfScalar(u8, rest, '/') orelse rest.len;
    const cid_str = rest[0..slash];
    const subpath = if (slash < rest.len) rest[slash + 1 ..] else "";

    blk_dir: {
        // No external lock — Blockstore is internally thread-safe via cache_mu
        var dir = resolver.listDirAtPath(allocator, ctx.store, cid_str, subpath) catch break :blk_dir;
        defer dir.deinit();
        var html = std.ArrayList(u8).empty;
        defer html.deinit(allocator);
        const w = html.writer(allocator);
        w.writeAll("<!DOCTYPE html><html><body><ul>\n") catch break :blk_dir;
        for (dir.entries) |e| {
            w.print("<li><a href=\"{s}/{s}\">{s}</a> ({d} bytes)</li>\n", .{ target, e.name, e.name, e.size }) catch break :blk_dir;
        }
        w.writeAll("</ul></body></html>\n") catch break :blk_dir;
        sendResp(stream, "HTTP/1.1 200 OK", "text/html; charset=utf-8", html.items);
        return;
    }

    // No external lock — Blockstore is internally thread-safe via cache_mu
    const body = resolver.catFileAtPath(allocator, ctx.store, cid_str, subpath) catch |err| switch (err) {
        error.NotFound => {
            sendResp(stream, "HTTP/1.1 404 Not Found", "text/plain", "not found\n");
            return;
        },
        error.NotADirectory => {
            sendResp(stream, "HTTP/1.1 400 Bad Request", "text/plain", "bad request\n");
            return;
        },
        else => {
            sendResp(stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "error\n");
            return;
        },
    };
    defer allocator.free(body);
    sendResp(stream, "HTTP/1.1 200 OK", "application/octet-stream", body);
}

/// Multi-threaded accept loop: spawns a thread per connection up to max_conns.
pub fn run(_: std.mem.Allocator, ctx: *const GatewayCtx) !void {
    const addr = try std.net.Address.parseIp("0.0.0.0", ctx.port);
    var server = try addr.listen(.{ .reuse_address = true });
    defer server.deinit();

    var active = std.atomic.Value(u32).init(0);

    while (true) {
        const conn = server.accept() catch |err| switch (err) {
            error.ConnectionAborted => continue,
            error.ProcessFdQuotaExceeded, error.SystemFdQuotaExceeded => {
                std.log.err("accept: fd quota exceeded, retrying", .{});
                std.Thread.sleep(100 * std.time.ns_per_ms);
                continue;
            },
            else => |e| return e,
        };

        // Enforce concurrency limit — send 503 instead of silent close
        if (active.load(.acquire) >= ctx.max_conns) {
            sendResp(conn.stream, "HTTP/1.1 503 Service Unavailable", "text/plain", "server busy\n");
            conn.stream.close();
            continue;
        }
        _ = active.fetchAdd(1, .release);

        const t = std.Thread.spawn(.{}, handleConnection, .{ ctx, conn.stream, &active }) catch {
            _ = active.fetchSub(1, .release);
            conn.stream.close();
            continue;
        };
        t.detach();
    }
}
