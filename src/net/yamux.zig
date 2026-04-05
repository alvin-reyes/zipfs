//! Yamux v1 framing (Hashicorp / libp2p): 12-byte header, DATA / WINDOW_UPDATE / PING / GO_AWAY.

const std = @import("std");

pub const FrameType = enum(u8) {
    data = 0,
    window_update = 1,
    ping = 2,
    go_away = 3,
};

pub const flag_syn: u16 = 0x0001;
pub const flag_ack: u16 = 0x0002;
pub const flag_fin: u16 = 0x0004;
pub const flag_rst: u16 = 0x0008;

pub const Header = struct {
    version: u8,
    typ: FrameType,
    flags: u16,
    stream_id: u32,
    length: u32,

    pub fn encode(self: Header, out: *[12]u8) void {
        out[0] = self.version;
        out[1] = @intFromEnum(self.typ);
        std.mem.writeInt(u16, out[2..4], self.flags, .big);
        std.mem.writeInt(u32, out[4..8], self.stream_id, .big);
        std.mem.writeInt(u32, out[8..12], self.length, .big);
    }

    pub fn decode(buf: *const [12]u8) Header {
        return .{
            .version = buf[0],
            .typ = @enumFromInt(buf[1]),
            .flags = std.mem.readInt(u16, buf[2..4], .big),
            .stream_id = std.mem.readInt(u32, buf[4..8], .big),
            .length = std.mem.readInt(u32, buf[8..12], .big),
        };
    }
};

/// Read a full frame: 12-byte header + `length` bytes body into caller buffer (must be sized >= length).
pub fn readFrame(stream: std.net.Stream, header_buf: *[12]u8, body: []u8) !Header {
    try readFull(stream, header_buf);
    const h = Header.decode(header_buf);
    if (h.length > body.len) return error.BufferTooSmall;
    if (h.length > 0) try readFull(stream, body[0..h.length]);
    return h;
}

fn readFull(s: std.net.Stream, buf: []u8) std.net.Stream.ReadError!void {
    var off: usize = 0;
    while (off < buf.len) {
        const n = try s.read(buf[off..]);
        if (n == 0) return error.ConnectionResetByPeer;
        off += n;
    }
}

pub fn writeFrame(stream: std.net.Stream, header_buf: *[12]u8, h: Header, body: []const u8) !void {
    if (body.len != h.length) return error.LengthMismatch;
    h.encode(header_buf);
    try stream.writeAll(header_buf);
    if (body.len > 0) try stream.writeAll(body);
}

pub fn writePing(stream: std.net.Stream, header_buf: *[12]u8, stream_id: u32) !void {
    const h = Header{
        .version = 0,
        .typ = .ping,
        .flags = 0,
        .stream_id = stream_id,
        .length = 0,
    };
    h.encode(header_buf);
    try stream.writeAll(header_buf);
}

/// Back-compat: previous 8-byte layout (incorrect for interop). Prefer `writePing` + `Header`.
pub fn writePingLegacy(stream: std.net.Stream, buf: *[8]u8, stream_id: u32) !void {
    buf[0] = 0;
    buf[1] = @intFromEnum(FrameType.ping);
    buf[2] = 0;
    buf[3] = @truncate(stream_id & 0xff);
    std.mem.writeInt(u32, buf[4..8], 0, .big);
    try stream.writeAll(buf);
}
