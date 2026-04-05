//! Minimal read-only HTTP gateway: `GET /ipfs/<cid>/<path>`.

const std = @import("std");
const Blockstore = @import("blockstore.zig").Blockstore;
const resolver = @import("resolver.zig");

fn sendResp(stream: std.net.Stream, status: []const u8, content_type: []const u8, body: []const u8) !void {
    var hdr: [512]u8 = undefined;
    const h = try std.fmt.bufPrint(&hdr,
        "{s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
        .{ status, content_type, body.len },
    );
    try stream.writeAll(h);
    try stream.writeAll(body);
}

/// Blocking loop: accept connections and serve `/ipfs/...` until error.
/// When `store_sync` is set, it is locked around blockstore reads (pair with libp2p swarm thread).
pub fn run(allocator: std.mem.Allocator, store: *const Blockstore, port: u16, store_sync: ?*std.Thread.Mutex) !void {
    const addr = try std.net.Address.parseIp("0.0.0.0", port);
    var server = try addr.listen(.{ .reuse_address = true });
    defer server.deinit();

    var buf: [16384]u8 = undefined;
    accept: while (true) {
        const conn = server.accept() catch |err| switch (err) {
            error.ConnectionAborted => continue,
            else => |e| return e,
        };
        defer conn.stream.close();

        const n = conn.stream.read(&buf) catch continue;
        if (n == 0) continue;
        const req = buf[0..n];
        const line_end = std.mem.indexOf(u8, req, "\r\n") orelse continue;
        const line = req[0..line_end];
        if (line.len < 10) continue;
        if (!std.mem.startsWith(u8, line, "GET ")) continue;
        const path_start = 4;
        const path_end = std.mem.indexOfScalar(u8, line[path_start..], ' ') orelse continue;
        const target = line[path_start .. path_start + path_end];

        if (!std.mem.startsWith(u8, target, "/ipfs/")) {
            try sendResp(conn.stream, "HTTP/1.1 501 Not Implemented", "text/plain", "only /ipfs/ supported\n");
            continue;
        }
        if (std.mem.startsWith(u8, target, "/ipns/")) {
            try sendResp(conn.stream, "HTTP/1.1 501 Not Implemented", "text/plain", "ipns not enabled\n");
            continue;
        }

        const rest = target["/ipfs/".len..];
        const slash = std.mem.indexOfScalar(u8, rest, '/') orelse rest.len;
        const cid_str = rest[0..slash];
        const subpath = if (slash < rest.len) rest[slash + 1 ..] else "";

        if (store_sync) |m| m.lock();
        defer if (store_sync) |m| m.unlock();

        blk_dir: {
            var dir = resolver.listDirAtPath(allocator, store, cid_str, subpath) catch break :blk_dir;
            defer dir.deinit();
            var html = std.ArrayList(u8).empty;
            defer html.deinit(allocator);
            const w = html.writer(allocator);
            try w.writeAll("<!DOCTYPE html><html><body><ul>\n");
            for (dir.entries) |e| {
                const href = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ target, e.name });
                defer allocator.free(href);
                try w.print("<li><a href=\"{s}\">{s}</a> ({d} bytes)</li>\n", .{ href, e.name, e.size });
            }
            try w.writeAll("</ul></body></html>\n");
            try sendResp(conn.stream, "HTTP/1.1 200 OK", "text/html; charset=utf-8", html.items);
            continue :accept;
        }

        const body = resolver.catFileAtPath(allocator, store, cid_str, subpath) catch |err| switch (err) {
            error.NotFound => {
                try sendResp(conn.stream, "HTTP/1.1 404 Not Found", "text/plain", "not found\n");
                continue;
            },
            error.NotADirectory => {
                try sendResp(conn.stream, "HTTP/1.1 400 Bad Request", "text/plain", "bad request\n");
                continue;
            },
            else => {
                try sendResp(conn.stream, "HTTP/1.1 500 Internal Server Error", "text/plain", "error\n");
                continue;
            },
        };
        defer allocator.free(body);
        try sendResp(conn.stream, "HTTP/1.1 200 OK", "application/octet-stream", body);
    }
}
