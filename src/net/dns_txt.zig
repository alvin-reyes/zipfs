//! Minimal DNS-over-UDP (TXT) for `_dnsaddr.*` bootstrap resolution.

const std = @import("std");

pub const DnsTxtError = error{
    Truncated,
    UnexpectedResponse,
    Io,
    InvalidName,
};

fn encodeDnsName(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, fqdn: []const u8) (DnsTxtError || std.mem.Allocator.Error)!void {
    var start: usize = 0;
    while (start < fqdn.len) {
        const end = std.mem.indexOfScalarPos(u8, fqdn, start, '.') orelse fqdn.len;
        const label = fqdn[start..end];
        if (label.len == 0) {
            start = end + 1;
            continue;
        }
        if (label.len > 63) return error.InvalidName;
        try buf.append(allocator, @truncate(label.len));
        try buf.appendSlice(allocator, label);
        start = end + 1;
    }
    try buf.append(allocator, 0);
}

fn skipName(msg: []const u8, i: *usize) DnsTxtError!void {
    while (true) {
        if (i.* >= msg.len) return error.Truncated;
        const len = msg[i.*];
        if (len == 0) {
            i.* += 1;
            return;
        }
        if ((len & 0xc0) == 0xc0) {
            i.* += 2;
            return;
        }
        i.* += 1 + @as(usize, len);
        if (i.* > msg.len) return error.Truncated;
    }
}

fn readU16(msg: []const u8, i: *usize) DnsTxtError!u16 {
    if (i.* + 2 > msg.len) return error.Truncated;
    const v = std.mem.readInt(u16, msg[i.*..][0..2], .big);
    i.* += 2;
    return v;
}

fn readU32(msg: []const u8, i: *usize) DnsTxtError!u32 {
    if (i.* + 4 > msg.len) return error.Truncated;
    const v = std.mem.readInt(u32, msg[i.*..][0..4], .big);
    i.* += 4;
    return v;
}

/// First `nameserver` from `/etc/resolv.conf`, else `null`.
pub fn firstNameserverFromResolvConf(allocator: std.mem.Allocator) !?[]const u8 {
    const f = std.fs.openFileAbsolute("/etc/resolv.conf", .{}) catch return null;
    defer f.close();
    const data = f.readToEndAlloc(allocator, 16 * 1024) catch return null;
    defer allocator.free(data);
    var it = std.mem.splitScalar(u8, data, '\n');
    while (it.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0 or line[0] == '#') continue;
        const prefix = "nameserver ";
        if (!std.mem.startsWith(u8, line, prefix)) continue;
        const rest = std.mem.trim(u8, line[prefix.len..], " \t");
        if (rest.len == 0) continue;
        return try allocator.dupe(u8, rest);
    }
    return null;
}

pub fn freeNameserverString(allocator: std.mem.Allocator, s: []const u8) void {
    allocator.free(s);
}

/// TXT RDATA → one logical string (concatenate length-prefixed segments).
fn parseTxtRdata(rdata: []const u8, allocator: std.mem.Allocator) ![]u8 {
    var acc = std.ArrayList(u8).empty;
    errdefer acc.deinit(allocator);
    var i: usize = 0;
    while (i < rdata.len) {
        const seg = rdata[i];
        i += 1;
        if (i + seg > rdata.len) return error.Truncated;
        try acc.appendSlice(allocator, rdata[i..][0..seg]);
        i += seg;
    }
    return try acc.toOwnedSlice(allocator);
}

/// Query `fqdn` for TXT records; each item is one RR’s full text (owned).
pub fn queryTxtRecords(allocator: std.mem.Allocator, fqdn: []const u8, nameserver_ip: []const u8) ![][]u8 {
    var q = std.ArrayList(u8).empty;
    defer q.deinit(allocator);

    const id: u16 = std.crypto.random.int(u16);
    try q.appendSlice(allocator, &std.mem.toBytes(std.mem.nativeToBig(u16, id)));
    try q.appendSlice(allocator, &[_]u8{ 0x01, 0x00 });
    try q.appendSlice(allocator, &[_]u8{ 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });
    try encodeDnsName(&q, allocator, fqdn);
    try q.appendSlice(allocator, &[_]u8{ 0x00, 16, 0x00, 1 });

    const addr = try std.net.Address.parseIp(nameserver_ip, 53);
    const sock_flags = std.posix.SOCK.DGRAM | std.posix.SOCK.CLOEXEC;
    const sock = try std.posix.socket(addr.any.family, sock_flags, std.posix.IPPROTO.UDP);
    defer std.posix.close(sock);

    const tv = std.posix.timeval{ .sec = 5, .usec = 0 };
    try std.posix.setsockopt(sock, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv));

    const sa = addr.any;
    _ = try std.posix.sendto(sock, q.items, 0, &sa, addr.getOsSockLen());

    var resp: [4096]u8 = undefined;
    const n = try std.posix.recv(sock, &resp, 0);
    if (n < 12) return error.UnexpectedResponse;
    const msg = resp[0..n];

    const rid = std.mem.readInt(u16, msg[0..2], .big);
    if (rid != id) return error.UnexpectedResponse;
    const qdcount = std.mem.readInt(u16, msg[4..6], .big);
    const ancount = std.mem.readInt(u16, msg[6..8], .big);

    var off: usize = 12;
    var qn: u16 = 0;
    while (qn < qdcount) : (qn += 1) {
        try skipName(msg, &off);
        if (off + 4 > msg.len) return error.Truncated;
        off += 4;
    }

    var out = std.ArrayList([]u8).empty;
    errdefer {
        for (out.items) |s| allocator.free(s);
        out.deinit(allocator);
    }

    var an: u16 = 0;
    while (an < ancount) : (an += 1) {
        try skipName(msg, &off);
        const typ = try readU16(msg, &off);
        _ = try readU16(msg, &off);
        _ = try readU32(msg, &off);
        const rdlen = try readU16(msg, &off);
        if (off + rdlen > msg.len) return error.Truncated;
        const rdata = msg[off..][0..rdlen];
        off += rdlen;
        if (typ != 16) continue;
        const txt = try parseTxtRdata(rdata, allocator);
        errdefer allocator.free(txt);
        try out.append(allocator, txt);
    }

    return try out.toOwnedSlice(allocator);
}

pub fn freeTxtRecords(allocator: std.mem.Allocator, rs: [][]u8) void {
    for (rs) |s| allocator.free(s);
    allocator.free(rs);
}
