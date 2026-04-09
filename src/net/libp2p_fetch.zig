//! Discover providers via DHT (bootstrap + iterative walk) and fetch raw blocks with bitswap.

const std = @import("std");
const blockstore = @import("../blockstore.zig");
const cid_mod = @import("../cid.zig");
const dht = @import("../dht.zig");
const multiaddr = @import("multiaddr.zig");
const libp2p_dial = @import("libp2p_dial.zig");
const dht_walk = @import("dht_walk.zig");
const bootstrap_resolve = @import("bootstrap_resolve.zig");

pub const FetchError = error{NoBlockFromNetwork};

/// Try DHT walk + bitswap until the block is in `store`. Returns true if a block was fetched.
pub fn fetchBlockIntoStore(
    allocator: std.mem.Allocator,
    store: *blockstore.Blockstore,
    cid_str: []const u8,
    bootstrap_peers: []const []const u8,
    ed25519_secret64: [64]u8,
) !bool {
    if (store.has(cid_str)) return false;

    var c = try cid_mod.Cid.parse(allocator, cid_str);
    defer c.deinit(allocator);

    const routing_key = try dht.providerKeyForMultihash(allocator, c.hash);
    defer allocator.free(routing_key);

    var providers: std.ArrayList(dht.Peer) = .empty;
    defer {
        for (providers.items) |p| dht.peerFree(allocator, p);
        providers.deinit(allocator);
    }

    const resolved = try bootstrap_resolve.resolveBootstrapPeers(allocator, bootstrap_peers);
    defer bootstrap_resolve.freeResolved(allocator, resolved);
    try dht_walk.walkGetProviders(allocator, routing_key, resolved, ed25519_secret64, .{}, &providers);

    for (providers.items) |prov| {
        for (prov.addrs) |ab| {
            const pt = multiaddr.tcpTargetFromAddrBytes(allocator, ab) catch |err| switch (err) {
                error.BadMultiaddr, error.Truncated => continue,
                else => |e| return e,
            };
            defer pt.deinit(allocator);

            const blk = libp2p_dial.dialBitswapWant(allocator, pt.host, pt.port, cid_str, ed25519_secret64) catch continue;
            defer if (blk) |b| allocator.free(b);
            if (blk) |b| {
                try store.put(allocator, c, b);
                return true;
            }
        }
    }
    return error.NoBlockFromNetwork;
}
