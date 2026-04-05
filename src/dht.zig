//! Kademlia XOR distance + DHT `Message` protobuf (go-libp2p-kad-dht `dht.proto`).

const std = @import("std");
const wireproto = @import("wireproto.zig");

/// 32-byte key space (go-libp2p XOR keyspace: SHA-256 of raw key / peer-id bytes).
pub fn sha256Key(material: []const u8) [32]u8 {
    var out: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(material, &out, .{});
    return out;
}

/// XOR distance between `sha256(routing_key)` and `sha256(peer_id_field_bytes)`.
pub fn xorDistanceRoutingToPeer(routing_key: []const u8, peer_id_field: []const u8) u256 {
    const k = sha256Key(routing_key);
    const p = sha256Key(peer_id_field);
    return distanceXor256(k, p);
}

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

pub const MessageType = enum(u32) {
    put_value = 0,
    get_value = 1,
    add_provider = 2,
    get_providers = 3,
    find_node = 4,
    ping = 5,
};

pub const ConnectionType = enum(u32) {
    not_connected = 0,
    connected = 1,
    can_connect = 2,
    cannot_connect = 3,
};

pub const Peer = struct {
    id: []const u8,
    addrs: []const []const u8 = &.{},
    connection: ConnectionType = .not_connected,

    fn encodeInto(self: Peer, allocator: std.mem.Allocator) ![]u8 {
        var buf = std.ArrayList(u8).empty;
        defer buf.deinit(allocator);
        try wireproto.appendBytesField(&buf, allocator, 1, self.id);
        for (self.addrs) |a| {
            try wireproto.appendBytesField(&buf, allocator, 2, a);
        }
        try wireproto.appendVarintField(&buf, allocator, 3, @intFromEnum(self.connection));
        return try buf.toOwnedSlice(allocator);
    }
};

pub const Message = struct {
    typ: MessageType,
    cluster_level_raw: i32 = 0,
    key: ?[]const u8 = null,
    /// Embedded `record.pb.Record` bytes (field 3) — pass through opaque.
    record: ?[]const u8 = null,
    closer_peers: []const Peer = &.{},
    provider_peers: []const Peer = &.{},

    pub fn encode(self: Message, allocator: std.mem.Allocator) ![]u8 {
        var buf = std.ArrayList(u8).empty;
        errdefer buf.deinit(allocator);
        try wireproto.appendVarintField(&buf, allocator, 1, @intFromEnum(self.typ));
        if (self.key) |k| try wireproto.appendBytesField(&buf, allocator, 2, k);
        if (self.record) |r| try wireproto.appendBytesField(&buf, allocator, 3, r);
        for (self.closer_peers) |p| {
            const inner = try p.encodeInto(allocator);
            defer allocator.free(inner);
            try wireproto.appendBytesField(&buf, allocator, 8, inner);
        }
        for (self.provider_peers) |p| {
            const inner = try p.encodeInto(allocator);
            defer allocator.free(inner);
            try wireproto.appendBytesField(&buf, allocator, 9, inner);
        }
        if (self.cluster_level_raw != 0)
            try wireproto.appendVarintField(&buf, allocator, 10, @as(u64, @bitCast(@as(i64, self.cluster_level_raw))));
        return try buf.toOwnedSlice(allocator);
    }

    pub fn decode(data: []const u8, allocator: std.mem.Allocator) !Message {
        var typ: ?MessageType = null;
        var cluster: i32 = 0;
        var key: ?[]const u8 = null;
        var record: ?[]const u8 = null;
        var closer = std.ArrayList(Peer).empty;
        var prov = std.ArrayList(Peer).empty;
        var ok = false;
        errdefer {
            if (!ok) {
                for (closer.items) |p| peerDealloc(p, allocator);
                closer.deinit(allocator);
                for (prov.items) |p| peerDealloc(p, allocator);
                prov.deinit(allocator);
                if (key) |k| allocator.free(k);
                if (record) |r| allocator.free(r);
            }
        }

        var it = wireproto.FieldIter{ .data = data };
        while (try it.next()) |e| {
            switch (e.field_num) {
                1 => typ = std.meta.intToEnum(MessageType, @as(u32, @truncate(e.int_val))) catch return error.BadDhtMessage,
                2 => {
                    if (key) |k| allocator.free(k);
                    key = try allocator.dupe(u8, e.bytes);
                },
                3 => {
                    if (record) |r| allocator.free(r);
                    record = try allocator.dupe(u8, e.bytes);
                },
                8 => try closer.append(allocator, try decodePeer(e.bytes, allocator)),
                9 => try prov.append(allocator, try decodePeer(e.bytes, allocator)),
                10 => cluster = @bitCast(@as(u32, @truncate(e.int_val))),
                else => {},
            }
        }
        const t = typ orelse return error.BadDhtMessage;
        const closer_owned = try closer.toOwnedSlice(allocator);
        errdefer {
            for (closer_owned) |p| peerDealloc(p, allocator);
            allocator.free(closer_owned);
        }
        const prov_owned = try prov.toOwnedSlice(allocator);
        ok = true;
        return .{
            .typ = t,
            .cluster_level_raw = cluster,
            .key = key,
            .record = record,
            .closer_peers = closer_owned,
            .provider_peers = prov_owned,
        };
    }

    pub fn deinit(self: *Message, allocator: std.mem.Allocator) void {
        if (self.key) |k| allocator.free(k);
        if (self.record) |r| allocator.free(r);
        for (self.closer_peers) |p| peerDealloc(p, allocator);
        allocator.free(self.closer_peers);
        for (self.provider_peers) |p| peerDealloc(p, allocator);
        allocator.free(self.provider_peers);
        self.* = undefined;
    }
};

fn peerDealloc(p: Peer, allocator: std.mem.Allocator) void {
    allocator.free(p.id);
    for (p.addrs) |a| allocator.free(a);
    allocator.free(p.addrs);
}

pub fn peerFree(allocator: std.mem.Allocator, p: Peer) void {
    peerDealloc(p, allocator);
}

fn decodePeer(data: []const u8, allocator: std.mem.Allocator) !Peer {
    var id: ?[]const u8 = null;
    var addrs = std.ArrayList([]const u8).empty;
    defer addrs.deinit(allocator);
    var conn: ConnectionType = .not_connected;
    var it = wireproto.FieldIter{ .data = data };
    while (try it.next()) |e| {
        switch (e.field_num) {
            1 => id = try allocator.dupe(u8, e.bytes),
            2 => try addrs.append(allocator, try allocator.dupe(u8, e.bytes)),
            3 => conn = std.meta.intToEnum(ConnectionType, @as(u32, @truncate(e.int_val))) catch .not_connected,
            else => {},
        }
    }
    const idv = id orelse return error.BadDhtMessage;
    return .{
        .id = idv,
        .addrs = try addrs.toOwnedSlice(allocator),
        .connection = conn,
    };
}

pub fn encodePing(allocator: std.mem.Allocator) ![]u8 {
    const m = Message{ .typ = .ping };
    return try m.encode(allocator);
}

pub fn encodeFindNode(allocator: std.mem.Allocator, key: []const u8) ![]u8 {
    const m = Message{ .typ = .find_node, .key = key };
    return try m.encode(allocator);
}

/// Kubo/go-libp2p-kad-dht provider routing key: `/providers/` + multihash bytes (`cid.Hash()`).
pub fn providerKeyForMultihash(allocator: std.mem.Allocator, multihash_bytes: []const u8) ![]u8 {
    const prefix = "/providers/";
    const out = try allocator.alloc(u8, prefix.len + multihash_bytes.len);
    @memcpy(out[0..prefix.len], prefix);
    @memcpy(out[prefix.len..], multihash_bytes);
    return out;
}

pub fn encodeGetProviders(allocator: std.mem.Allocator, routing_key: []const u8) ![]u8 {
    const m = Message{ .typ = .get_providers, .key = routing_key };
    return try m.encode(allocator);
}

/// ADD_PROVIDER with `providerPeers` (field 9): identity multihash + binary multiaddrs, same as Kubo/go-libp2p-kad-dht.
pub fn encodeAddProvider(
    allocator: std.mem.Allocator,
    routing_key: []const u8,
    provider_peer_id: []const u8,
    provider_addrs: []const []const u8,
) ![]u8 {
    const p = Peer{ .id = provider_peer_id, .addrs = provider_addrs, .connection = .not_connected };
    const m = Message{
        .typ = .add_provider,
        .key = routing_key,
        .provider_peers = &[_]Peer{p},
    };
    return try m.encode(allocator);
}

pub fn clonePeer(allocator: std.mem.Allocator, p: Peer) !Peer {
    const id = try allocator.dupe(u8, p.id);
    errdefer allocator.free(id);
    const addrs = try allocator.alloc([]const u8, p.addrs.len);
    var n: usize = 0;
    errdefer {
        for (addrs[0..n]) |x| allocator.free(x);
        allocator.free(addrs);
    }
    for (p.addrs) |a| {
        addrs[n] = try allocator.dupe(u8, a);
        n += 1;
    }
    return .{
        .id = id,
        .addrs = addrs,
        .connection = p.connection,
    };
}

pub const DecodeError = error{ BadDhtMessage, Truncated };
