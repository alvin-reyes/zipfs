//! TCP accept + multistream negotiate + echo line (no Noise yet).

const std = @import("std");
const multistream = @import("multistream.zig");

pub const echo_protocol = "/zig-ipfs/echo/1.0.0\n";

pub fn serveEcho(port: u16) !void {
    const addr = try std.net.Address.parseIp("0.0.0.0", port);
    var server = try addr.listen(.{ .reuse_address = true });
    defer server.deinit();

    var line_buf: [256]u8 = undefined;
    while (true) {
        const conn = try server.accept();
        defer conn.stream.close();

        _ = try multistream.readLine(conn.stream, &line_buf);
        try multistream.writeProtocol(conn.stream, multistream.multistream_1_0);
        _ = try multistream.readLine(conn.stream, &line_buf);
        try multistream.writeProtocol(conn.stream, echo_protocol);

        var msg: [64]u8 = undefined;
        const m = try multistream.readLine(conn.stream, &msg);
        if (std.mem.eql(u8, m, "PING")) {
            try conn.stream.writeAll("PONG\n");
        }
    }
}

pub fn dialEcho(host: []const u8, port: u16) !void {
    const peer = try std.net.Address.parseIp(host, port);
    const stream = try std.net.tcpConnectToAddress(peer);
    defer stream.close();

    var line_buf: [256]u8 = undefined;
    try multistream.writeProtocol(stream, multistream.multistream_1_0);
    _ = try multistream.readLine(stream, &line_buf);
    try multistream.writeProtocol(stream, echo_protocol);
    _ = try multistream.readLine(stream, &line_buf);
    try stream.writeAll("PING\n");
    const resp = try multistream.readLine(stream, &line_buf);
    if (!std.mem.eql(u8, resp, "PONG")) return error.EchoMismatch;
}
