//! Zig IPFS: content-addressed blocks, CID v0/v1, UnixFS files (local blockstore).

const std = @import("std");
const wireproto = @import("wireproto.zig");

pub const varint = @import("varint.zig");
pub const multihash = @import("multihash.zig");
pub const multibase = @import("multibase.zig");
pub const cid = @import("cid.zig");
pub const unixfs = @import("unixfs.zig");
pub const dag_pb = @import("dag_pb.zig");
pub const blockstore = @import("blockstore.zig");
pub const importer = @import("importer.zig");
pub const resolver = @import("resolver.zig");
pub const repo = @import("repo.zig");
pub const config = @import("config.zig");
pub const car = @import("car.zig");
pub const pin = @import("pin.zig");
pub const gateway = @import("gateway.zig");
pub const bitswap = @import("bitswap.zig");
pub const dht = @import("dht.zig");
pub const ipns = @import("ipns.zig");
pub const mfs = @import("mfs.zig");
pub const net_peer_id = @import("net/peer_id.zig");
pub const net_swarm = @import("net/swarm_tcp.zig");
pub const net_noise = @import("net/noise.zig");
pub const net_yamux = @import("net/yamux.zig");
pub const net_multistream = @import("net/multistream.zig");
pub const net_libp2p_dial = @import("net/libp2p_dial.zig");
pub const net_multiaddr = @import("net/multiaddr.zig");
pub const net_identity = @import("net/identity.zig");
pub const net_libp2p_fetch = @import("net/libp2p_fetch.zig");
pub const net_libp2p_serve = @import("net/libp2p_serve.zig");
pub const net_dht_walk = @import("net/dht_walk.zig");
pub const net_libp2p_provide = @import("net/libp2p_provide.zig");
pub const net_swarm_config = @import("net/swarm_config.zig");
pub const net_identify = @import("net/identify.zig");
pub const net_bootstrap_resolve = @import("net/bootstrap_resolve.zig");

pub const Cid = cid.Cid;
pub const Blockstore = blockstore.Blockstore;

pub const Node = struct {
    store: Blockstore = .{},

    pub fn deinit(self: *Node, allocator: std.mem.Allocator) void {
        self.store.deinit(allocator);
    }

    pub fn addFile(self: *Node, allocator: std.mem.Allocator, data: []const u8) !Cid {
        return importer.addFile(allocator, &self.store, data);
    }

    pub fn addFileWithConfig(self: *Node, allocator: std.mem.Allocator, data: []const u8, cfg: *const config.Config) !Cid {
        return importer.addFileWithChunk(allocator, &self.store, data, cfg.chunk_size);
    }

    pub fn catFile(self: *const Node, allocator: std.mem.Allocator, cid_str: []const u8) ![]u8 {
        return resolver.catFile(allocator, &self.store, cid_str);
    }

    pub fn catFileAtPath(self: *const Node, allocator: std.mem.Allocator, cid_str: []const u8, path: []const u8) ![]u8 {
        return resolver.catFileAtPath(allocator, &self.store, cid_str, path);
    }

    pub fn listDir(self: *const Node, allocator: std.mem.Allocator, cid_str: []const u8, path: []const u8) !resolver.DirList {
        return resolver.listDirAtPath(allocator, &self.store, cid_str, path);
    }

    pub fn addDirectory(self: *Node, allocator: std.mem.Allocator, dir_path: []const u8, cfg: *const config.Config) !Cid {
        return @import("importer_dir.zig").addDirectory(allocator, &self.store, dir_path, cfg.chunk_size);
    }

    pub fn blockPut(self: *Node, allocator: std.mem.Allocator, data: []const u8) !Cid {
        const id = try cid.hashRawBlock(allocator, data);
        errdefer id.deinit(allocator);
        try self.store.put(allocator, id, data);
        return id;
    }

    pub fn blockGet(self: *const Node, allocator: std.mem.Allocator, cid_str: []const u8) ![]u8 {
        const b = self.store.get(cid_str) orelse return error.NotFound;
        return try allocator.dupe(u8, b);
    }
};

test "cid roundtrip v1" {
    const gpa = std.testing.allocator;
    const digest = multihash.digestSha256("hello");
    const c = try Cid.rawSha256(gpa, &digest);
    defer c.deinit(gpa);
    const s = try c.toString(gpa);
    defer gpa.free(s);
    const c2 = try Cid.parse(gpa, s);
    defer c2.deinit(gpa);
    try std.testing.expectEqual(c.version, c2.version);
    try std.testing.expectEqual(c.codec, c2.codec);
    try std.testing.expectEqualSlices(u8, c.hash, c2.hash);
}

test "add cat small file" {
    const gpa = std.testing.allocator;
    var node: Node = .{};
    defer node.deinit(gpa);
    const payload = "hello ipfs from zig";
    const root = try node.addFile(gpa, payload);
    defer root.deinit(gpa);
    const key = try root.toString(gpa);
    defer gpa.free(key);
    const out = try node.catFile(gpa, key);
    defer gpa.free(out);
    try std.testing.expectEqualStrings(payload, out);
}

test "bitswap encode empty" {
    const gpa = std.testing.allocator;
    const e = try bitswap.encodeEmptyMessage(gpa);
    defer gpa.free(e);
    try std.testing.expectEqual(@as(usize, 0), e.len);
}

test "dht xor distance" {
    var a: [32]u8 = .{0} ** 32;
    const b: [32]u8 = .{0} ** 32;
    a[0] = 1;
    try std.testing.expect(dht.distanceXor256(a, b) != 0);
}

test "dht ping protobuf roundtrip" {
    const gpa = std.testing.allocator;
    const enc = try dht.encodePing(gpa);
    defer gpa.free(enc);
    var msg = try dht.Message.decode(enc, gpa);
    defer msg.deinit(gpa);
    try std.testing.expectEqual(dht.MessageType.ping, msg.typ);
}

test "ipns verify v2 synthetic" {
    const gpa = std.testing.allocator;
    const kp = std.crypto.sign.Ed25519.KeyPair.generate();
    const data = "fake-dag-cbor-payload";
    var sign_input: [256]u8 = undefined;
    const pref = ipns.ipns_signature_prefix;
    @memcpy(sign_input[0..pref.len], pref);
    @memcpy(sign_input[pref.len..][0..data.len], data);
    const sig = try kp.sign(sign_input[0 .. pref.len + data.len], null);
    const sigb = sig.toBytes();
    const id_pb = net_peer_id.marshalPublicKeyEd25519(&kp.public_key.toBytes());

    var rec = std.ArrayList(u8).empty;
    defer rec.deinit(gpa);
    try wireproto.appendBytesField(&rec, gpa, 7, &id_pb);
    try wireproto.appendBytesField(&rec, gpa, 8, &sigb);
    try wireproto.appendBytesField(&rec, gpa, 9, data);
    const owned = try rec.toOwnedSlice(gpa);
    defer gpa.free(owned);
    try ipns.verifyRecord(owned, null);
}

test "dht xor distance uses sha256 keyspace" {
    const rk = "/providers/test";
    var mh: [34]u8 = undefined;
    mh[0] = 0x12;
    mh[1] = 0x20;
    @memset(mh[2..], 0xaa);
    const d = dht.xorDistanceRoutingToPeer(rk, &mh);
    try std.testing.expect(d != 0);
    try std.testing.expectEqual(d, dht.xorDistanceRoutingToPeer(rk, &mh));
}

test "bitswap wantlist have and block decode" {
    const gpa = std.testing.allocator;
    const digest = multihash.digestSha256("x");
    const c = try Cid.rawSha256(gpa, &digest);
    defer c.deinit(gpa);
    const cb = try c.toBytes(gpa);
    defer gpa.free(cb);
    const framed = try bitswap.encodeFramedWant(gpa, cb, true);
    defer gpa.free(framed);
    var off: usize = 0;
    const plen = try varint.decodeU64(framed, &off);
    const inner = framed[off..][0..plen];
    const items = try bitswap.decodeWantlistItems(inner, gpa);
    defer {
        for (items) |*it| it.deinit(gpa);
        gpa.free(items);
    }
    try std.testing.expectEqual(@as(usize, 2), items.len);
    try std.testing.expectEqual(bitswap.WantType.have, items[0].want_type);
    try std.testing.expectEqual(bitswap.WantType.block, items[1].want_type);
}

test "identify decode agent" {
    const gpa = std.testing.allocator;
    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(gpa);
    try wireproto.appendBytesField(&buf, gpa, 6, "kubo/0.27");
    const pb = try buf.toOwnedSlice(gpa);
    defer gpa.free(pb);
    var info = try net_identify.decodeIdentify(gpa, pb);
    defer info.deinit(gpa);
    try std.testing.expectEqualStrings("kubo/0.27", info.agent_version.?);
}

test "provider key and multiaddr parse" {
    const gpa = std.testing.allocator;
    const mh = try gpa.dupe(u8, &[_]u8{ 0x12, 0x20 } ++ [_]u8{0xab} ** 32);
    defer gpa.free(mh);
    const pk = try dht.providerKeyForMultihash(gpa, mh);
    defer gpa.free(pk);
    try std.testing.expect(std.mem.startsWith(u8, pk, "/providers/"));

    const t = try net_multiaddr.parseStringTcp(gpa, "/ip4/104.131.131.82/tcp/4001/p2p/QmX");
    defer t.deinit(gpa);
    try std.testing.expectEqual(@as(u16, 4001), t.port);
}

test "peer id from key" {
    const gpa = std.testing.allocator;
    const kp = net_peer_id.generateKeyPair();
    const s = try net_peer_id.peerIdString(gpa, &kp.public_key);
    defer gpa.free(s);
    try std.testing.expect(s.len > 10);
}

test "car export import roundtrip" {
    const gpa = std.testing.allocator;
    var node: Node = .{};
    defer node.deinit(gpa);
    const c1 = try node.blockPut(gpa, "block-a");
    defer c1.deinit(gpa);
    const c2 = try node.blockPut(gpa, "block-bbb");
    defer c2.deinit(gpa);
    const tmp = "zig-ipfs-test.car";
    defer std.fs.cwd().deleteFile(tmp) catch {};
    try car.exportStoreToFile(gpa, &node.store, tmp);
    var node2: Node = .{};
    defer node2.deinit(gpa);
    var f = try std.fs.cwd().openFile(tmp, .{});
    defer f.close();
    try car.importFromSeekableFile(gpa, f, &node2.store);
    try std.testing.expectEqual(@as(usize, 2), node2.store.count());
}

test "add cat chunked file" {
    const gpa = std.testing.allocator;
    var node: Node = .{};
    defer node.deinit(gpa);
    var payload = std.ArrayList(u8).empty;
    defer payload.deinit(gpa);
    try payload.appendNTimes(gpa, 'x', importer.chunk_size + 1234);
    const root = try node.addFile(gpa, payload.items);
    defer root.deinit(gpa);
    const key = try root.toString(gpa);
    defer gpa.free(key);
    const out = try node.catFile(gpa, key);
    defer gpa.free(out);
    try std.testing.expectEqualSlices(u8, payload.items, out);
}