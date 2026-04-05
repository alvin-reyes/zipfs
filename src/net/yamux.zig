//! Yamux frame header (v0): parse and build ping/pong.

const std = @import("std");

pub const FrameType = enum(u8) {
    data = 0,
    window_update = 1,
    ping = 2,
    go_away = 3,
};

pub const Header = packed struct(u32) {
    version: u8,
    typ: u8,
    flags: u8,
    stream_id: u8,
};

/// Read 8-byte yamux frame header (version, type, flags, stream_id, length u32 BE).
pub fn readFrameHeader(stream: std.net.Stream, buf: *[8]u8) !struct { typ: u8, length: u32 } {
    const n = try stream.readAll(buf);
    if (n != 8) return error.UnexpectedEof;
    const typ = buf[1];
    const length = std.mem.readInt(u32, buf[4..8], .big);
    return .{ .typ = typ, .length = length };
}

pub fn writePing(stream: std.net.Stream, buf: *[8]u8, stream_id: u32) !void {
    buf[0] = 0;
    buf[1] = @intFromEnum(FrameType.ping);
    buf[2] = 0;
    buf[3] = @truncate(stream_id & 0xff);
    std.mem.writeInt(u32, buf[4..8], 0, .big);
    try stream.writeAll(buf);
}
