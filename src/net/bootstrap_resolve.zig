//! Expand `/dnsaddr/.../p2p/...` bootstrap entries via DNS TXT (`_dnsaddr.<domain>`).

const std = @import("std");
const dns_txt = @import("dns_txt.zig");

fn parseDnsaddrDomainP2p(s: []const u8) ?struct { domain: []const u8, p2p_id: ?[]const u8 } {
    if (!std.mem.startsWith(u8, s, "/")) return null;
    var domain: ?[]const u8 = null;
    var p2p_id: ?[]const u8 = null;
    var it = std.mem.splitScalar(u8, s, '/');
    _ = it.first();
    while (it.next()) |p| {
        if (std.mem.eql(u8, p, "dnsaddr")) {
            domain = it.next();
        } else if (std.mem.eql(u8, p, "p2p")) {
            p2p_id = it.next();
        }
    }
    if (domain) |d| return .{ .domain = d, .p2p_id = p2p_id };
    return null;
}

fn normalizeDnsaddrLine(allocator: std.mem.Allocator, line: []const u8, p2p_fallback: ?[]const u8) error{ Empty, OutOfMemory }![]u8 {
    var m = std.mem.trim(u8, line, " \t\r\n");
    if (m.len == 0) return error.Empty;
    if (std.mem.startsWith(u8, m, "dnsaddr=")) m = m["dnsaddr=".len..];
    m = std.mem.trim(u8, m, " \t\r\n");
    if (m.len == 0) return error.Empty;

    const owned: []u8 = if (m[0] != '/')
        try std.fmt.allocPrint(allocator, "/{s}", .{m})
    else
        try allocator.dupe(u8, m);
    errdefer allocator.free(owned);

    if (std.mem.indexOf(u8, owned, "/p2p/") == null) {
        if (p2p_fallback) |pid| {
            const with = try std.fmt.allocPrint(allocator, "{s}/p2p/{s}", .{ owned, pid });
            allocator.free(owned);
            return with;
        }
    }
    return owned;
}

/// Resolve one bootstrap multiaddr; if not dnsaddr, returns a single copy of `s`.
pub fn expandOneBootstrapAddr(allocator: std.mem.Allocator, s: []const u8) ![][]const u8 {
    const parsed = parseDnsaddrDomainP2p(s) orelse {
        const one = try allocator.dupe(u8, s);
        const sl = try allocator.alloc([]const u8, 1);
        sl[0] = one;
        return sl;
    };

    const fqdn = try std.fmt.allocPrint(allocator, "_dnsaddr.{s}", .{parsed.domain});
    defer allocator.free(fqdn);

    var ns_owned: ?[]u8 = null;
    defer if (ns_owned) |x| dns_txt.freeNameserverString(allocator, x);
    const ns_ip: []const u8 = if (try dns_txt.firstNameserverFromResolvConf(allocator)) |x| blk: {
        ns_owned = x;
        break :blk x;
    } else "8.8.8.8";

    const txts = dns_txt.queryTxtRecords(allocator, fqdn, ns_ip) catch {
        const one = try allocator.dupe(u8, s);
        const sl = try allocator.alloc([]const u8, 1);
        sl[0] = one;
        return sl;
    };
    defer dns_txt.freeTxtRecords(allocator, txts);

    var out = std.ArrayList([]const u8).empty;
    errdefer {
        for (out.items) |x| allocator.free(x);
        out.deinit(allocator);
    }

    for (txts) |ln| {
        const norm = normalizeDnsaddrLine(allocator, ln, parsed.p2p_id) catch continue;
        try out.append(allocator, norm);
    }

    if (out.items.len == 0) {
        const one = try allocator.dupe(u8, s);
        const sl = try allocator.alloc([]const u8, 1);
        sl[0] = one;
        return sl;
    }

    return try out.toOwnedSlice(allocator);
}

/// Expand every peer string (dnsaddr → concrete `/ip4/.../tcp/.../p2p/...`). Caller frees with `freeResolved`.
pub fn resolveBootstrapPeers(allocator: std.mem.Allocator, peers: []const []const u8) ![][]const u8 {
    var acc = std.ArrayList([]const u8).empty;
    errdefer {
        for (acc.items) |x| allocator.free(x);
        acc.deinit(allocator);
    }
    for (peers) |p| {
        const expanded = try expandOneBootstrapAddr(allocator, p);
        defer allocator.free(expanded);
        for (expanded) |e| try acc.append(allocator, try allocator.dupe(u8, e));
    }
    return try acc.toOwnedSlice(allocator);
}

pub fn freeResolved(allocator: std.mem.Allocator, peers: [][]const u8) void {
    for (peers) |p| allocator.free(p);
    allocator.free(peers);
}
