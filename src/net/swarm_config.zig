//! Swarm listen addresses for Identify (Kubo expects binary multiaddrs peers can dial).

const std = @import("std");
const config = @import("../config.zig");
const multiaddr = @import("multiaddr.zig");

/// Owned slices (caller holds until process exit for daemon thread).
pub fn buildIdentifyListenBinaries(allocator: std.mem.Allocator, cfg: *const config.Config, swarm_port: u16) ![][]u8 {
    var list: std.ArrayList([]u8) = .empty;
    errdefer {
        for (list.items) |b| allocator.free(b);
        list.deinit(allocator);
    }
    if (cfg.announce_addrs.len > 0) {
        for (cfg.announce_addrs) |s| {
            const b = multiaddr.stringIp4TcpToBinary(allocator, s) catch continue;
            try list.append(allocator, b);
        }
    } else if (cfg.listen_addrs.len > 0) {
        if (multiaddr.parseStringTcp(allocator, cfg.listen_addrs[0])) |t| {
            defer t.deinit(allocator);
            const host = if (std.mem.eql(u8, t.host, "0.0.0.0")) "127.0.0.1" else t.host;
            const canon = try std.fmt.allocPrint(allocator, "/ip4/{s}/tcp/{d}", .{ host, t.port });
            defer allocator.free(canon);
            try list.append(allocator, try multiaddr.stringIp4TcpToBinary(allocator, canon));
        } else |_| {
            const fb = try std.fmt.allocPrint(allocator, "/ip4/127.0.0.1/tcp/{d}", .{swarm_port});
            defer allocator.free(fb);
            try list.append(allocator, try multiaddr.stringIp4TcpToBinary(allocator, fb));
        }
    } else {
        const fb = try std.fmt.allocPrint(allocator, "/ip4/127.0.0.1/tcp/{d}", .{swarm_port});
        defer allocator.free(fb);
        try list.append(allocator, try multiaddr.stringIp4TcpToBinary(allocator, fb));
    }
    if (list.items.len == 0) {
        const fb = try std.fmt.allocPrint(allocator, "/ip4/127.0.0.1/tcp/{d}", .{swarm_port});
        defer allocator.free(fb);
        try list.append(allocator, try multiaddr.stringIp4TcpToBinary(allocator, fb));
    }
    return try list.toOwnedSlice(allocator);
}
