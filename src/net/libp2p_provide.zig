//! Publish provider records (ADD_PROVIDER) for a CID, Kubo-style.

const std = @import("std");
const cid_mod = @import("../cid.zig");
const dht = @import("../dht.zig");
const dht_walk = @import("dht_walk.zig");
const bootstrap_resolve = @import("bootstrap_resolve.zig");

/// Walk the DHT toward `cid_str` and send ADD_PROVIDER to the closest `replicate_k` peers we discover.
pub fn provideCid(
    allocator: std.mem.Allocator,
    cid_str: []const u8,
    bootstrap: []const []const u8,
    ed25519_secret64: [64]u8,
    opts: dht_walk.WalkOpts,
    replicate_k: usize,
    provider_addrs_bin: []const []const u8,
) !void {
    var c = try cid_mod.Cid.parse(allocator, cid_str);
    defer c.deinit(allocator);
    const routing_key = try dht.providerKeyForMultihash(allocator, c.hash);
    defer allocator.free(routing_key);
    const resolved = try bootstrap_resolve.resolveBootstrapPeers(allocator, bootstrap);
    defer bootstrap_resolve.freeResolved(allocator, resolved);
    try dht_walk.walkAndAddProvider(allocator, routing_key, resolved, ed25519_secret64, opts, replicate_k, provider_addrs_bin);
}
