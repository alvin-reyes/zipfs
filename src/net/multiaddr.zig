//! Minimal multiaddr parsing for Kubo interop: `/ip4/.../tcp/...`, `/dns4/.../tcp/...`, or binary packed.

const std = @import("std");
const varint = @import("../varint.zig");
const multibase = @import("../multibase.zig");

pub const TcpTarget = struct {
    host: []const u8,
    port: u16,

    pub fn deinit(self: TcpTarget, allocator: std.mem.Allocator) void {
        allocator.free(self.host);
    }
};

pub const ParseError = (error{ BadMultiaddr, Truncated } || std.mem.Allocator.Error);

fn allAsciiPrintable(b: []const u8) bool {
    for (b) |c| {
        if (c < 32 or c == 127) return false;
    }
    return true;
}

/// Parse `/ip4/a.b.c.d/tcp/port` or `/dns4/host/tcp/port` (optional trailing `/p2p/...`).
pub fn parseStringTcp(allocator: std.mem.Allocator, s: []const u8) ParseError!TcpTarget {
    var host: ?[]const u8 = null;
    var port: ?u16 = null;
    var it = std.mem.splitScalar(u8, s, '/');
    _ = it.first();
    while (it.next()) |p| {
        if (std.mem.eql(u8, p, "ip4")) {
            const h = it.next() orelse return error.BadMultiaddr;
            host = try allocator.dupe(u8, h);
        } else if (std.mem.eql(u8, p, "dns4") or std.mem.eql(u8, p, "dns6")) {
            const h = it.next() orelse return error.BadMultiaddr;
            if (host) |old| allocator.free(old);
            host = try allocator.dupe(u8, h);
        } else if (std.mem.eql(u8, p, "tcp")) {
            const ps = it.next() orelse return error.BadMultiaddr;
            port = std.fmt.parseInt(u16, ps, 10) catch return error.BadMultiaddr;
        }
    }
    const h = host orelse return error.BadMultiaddr;
    const po = port orelse return error.BadMultiaddr;
    return .{ .host = h, .port = po };
}

/// IP4=4, TCP=6, IP6=41, P2P=421 (varint-length payload).
pub fn parseBinaryTcp(allocator: std.mem.Allocator, b: []const u8) ParseError!TcpTarget {
    var i: usize = 0;
    var ip4: ?[4]u8 = null;
    var port: ?u16 = null;
    while (i < b.len) {
        const code = varint.decodeU64(b, &i) catch return error.Truncated;
        switch (code) {
            4 => {
                if (i + 4 > b.len) return error.BadMultiaddr;
                ip4 = b[i..][0..4].*;
                i += 4;
            },
            6 => {
                if (i + 2 > b.len) return error.BadMultiaddr;
                port = std.mem.readInt(u16, b[i..][0..2], .big);
                i += 2;
            },
            41 => {
                if (i + 16 > b.len) return error.BadMultiaddr;
                i += 16;
            },
            421 => {
                const ln_u = varint.decodeU64(b, &i) catch return error.Truncated;
                const ln: usize = @intCast(ln_u);
                if (i + ln > b.len) return error.BadMultiaddr;
                i += ln;
            },
            else => return error.BadMultiaddr,
        }
    }
    const ip = ip4 orelse return error.BadMultiaddr;
    const po = port orelse return error.BadMultiaddr;
    const hs = try std.fmt.allocPrint(allocator, "{d}.{d}.{d}.{d}", .{ ip[0], ip[1], ip[2], ip[3] });
    return .{ .host = hs, .port = po };
}

/// Best-effort TCP dial target from DHT `Peer.addrs` entry (UTF-8 path or binary multiaddr).
/// Base58btc `Qm…` libp2p peer id → raw multihash bytes.
pub fn decodePeerIdFromMultiaddrComponent(allocator: std.mem.Allocator, s: []const u8) (ParseError || error{InvalidChar, Overflow})![]u8 {
    if (s.len >= 2 and s[0] == 'Q' and s[1] == 'm')
        return try multibase.decodeBase58Btc(allocator, s);
    return error.BadMultiaddr;
}

/// TCP dial target plus optional `/p2p/<Qm…>` multihash (for DHT XOR distance).
pub fn parseStringTcpAndP2p(allocator: std.mem.Allocator, s: []const u8) ParseError!struct { target: TcpTarget, p2p_mh: ?[]u8 } {
    var peer_comp: ?[]const u8 = null;
    var it = std.mem.splitScalar(u8, s, '/');
    _ = it.first();
    while (it.next()) |p| {
        if (std.mem.eql(u8, p, "p2p")) {
            peer_comp = it.next();
        }
    }
    var target = try parseStringTcp(allocator, s);
    errdefer target.deinit(allocator);
    if (peer_comp) |pc| {
        const mh = decodePeerIdFromMultiaddrComponent(allocator, pc) catch |err| switch (err) {
            error.BadMultiaddr, error.InvalidChar, error.Overflow => {
                target.deinit(allocator);
                return error.BadMultiaddr;
            },
            else => |e| {
                target.deinit(allocator);
                return e;
            },
        };
        return .{ .target = target, .p2p_mh = mh };
    }
    return .{ .target = target, .p2p_mh = null };
}

pub fn tcpTargetFromAddrBytes(allocator: std.mem.Allocator, addr: []const u8) ParseError!TcpTarget {
    if (addr.len > 0 and addr[0] == '/') {
        return parseStringTcp(allocator, addr);
    }
    if (allAsciiPrintable(addr)) {
        return parseStringTcp(allocator, addr);
    }
    return parseBinaryTcp(allocator, addr);
}

/// Pack `/ip4/d.d.d.d/tcp/port` into binary multiaddr (codes 4 + 6). Kubo uses these bytes in Identify `listenAddrs`.
pub fn stringIp4TcpToBinary(allocator: std.mem.Allocator, s: []const u8) ParseError![]u8 {
    var ip: ?[4]u8 = null;
    var port: ?u16 = null;
    var it = std.mem.splitScalar(u8, s, '/');
    _ = it.first();
    while (it.next()) |p| {
        if (std.mem.eql(u8, p, "ip4")) {
            const h = it.next() orelse return error.BadMultiaddr;
            var oct = std.mem.splitScalar(u8, h, '.');
            var b: [4]u8 = undefined;
            var i: usize = 0;
            while (i < 4) : (i += 1) {
                const o = oct.next() orelse return error.BadMultiaddr;
                b[i] = std.fmt.parseInt(u8, o, 10) catch return error.BadMultiaddr;
            }
            if (oct.next() != null) return error.BadMultiaddr;
            ip = b;
        } else if (std.mem.eql(u8, p, "tcp")) {
            const ps = it.next() orelse return error.BadMultiaddr;
            port = std.fmt.parseInt(u16, ps, 10) catch return error.BadMultiaddr;
        }
    }
    const ipv = ip orelse return error.BadMultiaddr;
    const po = port orelse return error.BadMultiaddr;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try varint.encodeU64(&buf, allocator, 4);
    try buf.appendSlice(allocator, &ipv);
    try varint.encodeU64(&buf, allocator, 6);
    var pb: [2]u8 = undefined;
    std.mem.writeInt(u16, &pb, po, .big);
    try buf.appendSlice(allocator, &pb);
    return try buf.toOwnedSlice(allocator);
}
