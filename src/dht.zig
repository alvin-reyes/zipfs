//! Kademlia-style XOR distance (256-bit) and k-bucket placeholder.

const std = @import("std");

pub fn distanceXor256(a: [32]u8, b: [32]u8) u256 {
    var d: u256 = 0;
    var i: usize = 0;
    while (i < 32) : (i += 1) {
        const x = @as(u256, a[i] ^ b[i]) << @intCast((31 - i) * 8);
        d |= x;
    }
    return d;
}

pub const KBucket = struct {
    max_peers: usize = 20,
    peers: std.ArrayListUnmanaged([32]u8) = .empty,

    pub fn deinit(self: *KBucket, allocator: std.mem.Allocator) void {
        self.peers.deinit(allocator);
        self.* = .{};
    }

    pub fn insert(self: *KBucket, allocator: std.mem.Allocator, id: [32]u8) !void {
        if (self.peers.items.len >= self.max_peers) return error.BucketFull;
        try self.peers.append(allocator, id);
    }
};
