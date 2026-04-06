//! HTTP gateway: `GET /ipfs/<cid>/<path>` + `POST /api/v0/add` (Kubo-compatible).

const std = @import("std");
const Blockstore = @import("blockstore.zig").Blockstore;
const resolver = @import("resolver.zig");
const importer = @import("importer.zig");
const repo = @import("repo.zig");
const pin = @import("pin.zig");

/// Context for the gateway server, carrying mutable state needed for writes.
pub const GatewayCtx = struct {
    store: *Blockstore,
    store_sync: ?*std.Thread.Mutex = null,
    repo_root: []const u8,
    chunk_size: u32 = 262144,
    port: u16 = 8080,
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
    const max_body: usize = 100 * 1024 * 1024; // 100MB
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
            sendResp(stream, "HTTP/1.1 413 Payload Too Large", "text/plain", "max upload size is 100MB\n");
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

    // Import file into blockstore and get CID string (under scoped lock)
    var cid_buf: [128]u8 = undefined;
    var cid_len: usize = 0;
    {
        if (ctx.store_sync) |m| m.lock();
        defer if (ctx.store_sync) |m| m.unlock();

        const root_cid = importer.addFileWithChunk(allocator, ctx.store, parsed.data, ctx.chunk_size) catch {
            sendResp(stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "import failed\n");
            return;
        };
        defer root_cid.deinit(allocator);

        // Export to disk repo
        repo.exportStore(ctx.store, ctx.repo_root) catch |err| {
            std.log.err("exportStore failed: {}", .{err});
        };

        // Copy CID string to stack buffer so it outlives the lock scope
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

    // Parse query params for ?pin= (default true)
    var do_pin = true;
    if (std.mem.indexOf(u8, target, "?")) |qmark| {
        const query = target[qmark + 1 ..];
        if (getQueryParam(query, "pin")) |val| {
            if (std.mem.eql(u8, val, "false") or std.mem.eql(u8, val, "0")) {
                do_pin = false;
            }
        }
    }

    // Pin + notify replication (outside store lock — file I/O only)
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
        pin.notifyInbox(allocator, ctx.repo_root, cid_str) catch {};
    }

    // Kubo-compatible NDJSON response (with JSON-escaped filename)
    const escaped_name = jsonEscapeString(allocator, parsed.filename) catch {
        sendResp(stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "format error\n");
        return;
    };
    defer allocator.free(escaped_name);

    const json_resp = std.fmt.allocPrint(
        allocator,
        "{{\"Name\":\"{s}\",\"Hash\":\"{s}\",\"Size\":\"{d}\"}}\n",
        .{ escaped_name, cid_str, parsed.data.len },
    ) catch {
        sendResp(stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "json format error\n");
        return;
    };
    defer allocator.free(json_resp);

    sendRespWithHeaders(
        stream,
        "HTTP/1.1 200 OK",
        "Content-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\n",
        json_resp,
    );
}

/// Blocking loop: accept connections and serve `/ipfs/...` reads + `/api/v0/add` writes.
pub fn run(allocator: std.mem.Allocator, ctx: *const GatewayCtx) !void {
    const addr = try std.net.Address.parseIp("0.0.0.0", ctx.port);
    var server = try addr.listen(.{ .reuse_address = true });
    defer server.deinit();

    var buf: [65536]u8 = undefined;
    accept: while (true) {
        const conn = server.accept() catch |err| switch (err) {
            error.ConnectionAborted => continue,
            error.ProcessFdQuotaExceeded, error.SystemFdQuotaExceeded => {
                std.log.err("accept: fd quota exceeded, retrying", .{});
                std.Thread.sleep(100 * std.time.ns_per_ms);
                continue;
            },
            else => |e| return e,
        };
        defer conn.stream.close();

        const n = conn.stream.read(&buf) catch continue;
        if (n == 0) continue;
        const req = buf[0..n];

        // Find end of request line
        const line_end = std.mem.indexOf(u8, req, "\r\n") orelse continue;
        const line = req[0..line_end];
        if (line.len < 10) continue;

        // Determine method
        const is_get = std.mem.startsWith(u8, line, "GET ");
        const is_post = std.mem.startsWith(u8, line, "POST ");
        if (!is_get and !is_post) {
            sendResp(conn.stream, "HTTP/1.1 405 Method Not Allowed", "text/plain", "method not allowed\n");
            continue;
        }

        // Parse target path
        const method_len: usize = if (is_get) 4 else 5;
        const path_end = std.mem.indexOfScalar(u8, line[method_len..], ' ') orelse continue;
        const target = line[method_len .. method_len + path_end];

        // Find headers section (everything after request line)
        const headers = req[line_end + 2 ..];

        // Route: POST /api/v0/add
        if (is_post) {
            const target_path = if (std.mem.indexOf(u8, target, "?")) |q| target[0..q] else target;
            if (std.mem.eql(u8, target_path, "/api/v0/add")) {
                // Find where body starts (after \r\n\r\n)
                const hdrs_end = std.mem.indexOf(u8, req, "\r\n\r\n") orelse continue;
                const body_start = hdrs_end + 4;
                const initial_body = if (body_start < n) req[body_start..n] else "";
                handleAdd(allocator, ctx, conn.stream, headers, initial_body, target);
                continue;
            }
            sendResp(conn.stream, "HTTP/1.1 404 Not Found", "text/plain", "not found\n");
            continue;
        }

        // Route: GET /health (for Railway/Docker healthchecks)
        if (std.mem.eql(u8, target, "/health") or std.mem.eql(u8, target, "/")) {
            sendResp(conn.stream, "HTTP/1.1 200 OK", "text/plain", "ok\n");
            continue;
        }

        // Route: GET /ipfs/...
        if (!std.mem.startsWith(u8, target, "/ipfs/")) {
            if (std.mem.startsWith(u8, target, "/ipns/")) {
                sendResp(conn.stream, "HTTP/1.1 501 Not Implemented", "text/plain", "ipns not enabled\n");
                continue;
            }
            sendResp(conn.stream, "HTTP/1.1 404 Not Found", "text/plain", "not found\n");
            continue;
        }

        const rest = target["/ipfs/".len..];
        const slash = std.mem.indexOfScalar(u8, rest, '/') orelse rest.len;
        const cid_str = rest[0..slash];
        const subpath = if (slash < rest.len) rest[slash + 1 ..] else "";

        if (ctx.store_sync) |m| m.lock();
        defer if (ctx.store_sync) |m| m.unlock();

        blk_dir: {
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
            sendResp(conn.stream, "HTTP/1.1 200 OK", "text/html; charset=utf-8", html.items);
            continue :accept;
        }

        const body = resolver.catFileAtPath(allocator, ctx.store, cid_str, subpath) catch |err| switch (err) {
            error.NotFound => {
                sendResp(conn.stream, "HTTP/1.1 404 Not Found", "text/plain", "not found\n");
                continue;
            },
            error.NotADirectory => {
                sendResp(conn.stream, "HTTP/1.1 400 Bad Request", "text/plain", "bad request\n");
                continue;
            },
            else => {
                sendResp(conn.stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "error\n");
                continue;
            },
        };
        defer allocator.free(body);
        sendResp(conn.stream, "HTTP/1.1 200 OK", "application/octet-stream", body);
    }
}
