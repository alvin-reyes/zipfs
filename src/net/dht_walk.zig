//! Iterative DHT walk using GET_PROVIDERS + closer_peers (Kubo-compatible), and ADD_PROVIDER replication.

const std = @import("std");
const dht = @import("../dht.zig");
const multiaddr = @import("multiaddr.zig");
const libp2p_dial = @import("libp2p_dial.zig");

pub const WalkOpts = struct {
    max_rounds: u32 = 15,
    alpha: u32 = 6,
};

pub const QueryNode = struct {
    id: []u8,
    host: []u8,
    port: u16,

    pub fn deinit(self: *QueryNode, a: std.mem.Allocator) void {
        a.free(self.id);
        a.free(self.host);
        self.* = undefined;
    }

    fn dist(self: *const QueryNode, routing_key: []const u8) u256 {
        if (self.id.len == 0) return std.math.maxInt(u256);
        return dht.xorDistanceRoutingToPeer(routing_key, self.id);
    }
};

fn cmpQuery(routing_key: []const u8, a: QueryNode, b: QueryNode) bool {
    return a.dist(routing_key) < b.dist(routing_key);
}

fn endpointKey(allocator: std.mem.Allocator, host: []const u8, port: u16) ![]u8 {
    return try std.fmt.allocPrint(allocator, "{s}:{d}", .{ host, port });
}

/// On duplicate endpoint, frees `id` and `host`.
fn addCandidate(
    allocator: std.mem.Allocator,
    nodes: *std.ArrayList(QueryNode),
    seen_ep: *std.StringHashMapUnmanaged(void),
    id: []u8,
    host: []u8,
    port: u16,
) !void {
    const ek = try endpointKey(allocator, host, port);
    defer allocator.free(ek);
    if (seen_ep.contains(ek)) {
        allocator.free(id);
        allocator.free(host);
        return;
    }
    const k = try allocator.dupe(u8, ek);
    errdefer allocator.free(k);
    try seen_ep.put(allocator, k, {});
    try nodes.append(allocator, .{ .id = id, .host = host, .port = port });
}

fn addCandidatesFromDhtPeer(
    allocator: std.mem.Allocator,
    nodes: *std.ArrayList(QueryNode),
    seen_ep: *std.StringHashMapUnmanaged(void),
    p: dht.Peer,
) !void {
    for (p.addrs) |ad| {
        const t = multiaddr.tcpTargetFromAddrBytes(allocator, ad) catch continue;
        defer t.deinit(allocator);
        const id_copy = try allocator.dupe(u8, p.id);
        errdefer allocator.free(id_copy);
        const host_copy = try allocator.dupe(u8, t.host);
        errdefer allocator.free(host_copy);
        try addCandidate(allocator, nodes, seen_ep, id_copy, host_copy, t.port);
    }
}

fn expandFromBootstrapFindNode(
    allocator: std.mem.Allocator,
    routing_key: []const u8,
    bootstrap: []const []const u8,
    secret: [64]u8,
    nodes: *std.ArrayList(QueryNode),
    seen_ep: *std.StringHashMapUnmanaged(void),
) !void {
    for (bootstrap) |bp| {
        const pr = multiaddr.parseStringTcpAndP2p(allocator, bp) catch continue;
        defer {
            pr.target.deinit(allocator);
            if (pr.p2p_mh) |m| allocator.free(m);
        }
        var msg = libp2p_dial.dialDhtFindNode(allocator, pr.target.host, pr.target.port, routing_key, secret) catch continue;
        defer msg.deinit(allocator);
        for (msg.closer_peers) |p| {
            addCandidatesFromDhtPeer(allocator, nodes, seen_ep, p) catch continue;
        }
    }
}

fn seedFromBootstrap(
    allocator: std.mem.Allocator,
    bootstrap: []const []const u8,
    nodes: *std.ArrayList(QueryNode),
    seen_ep: *std.StringHashMapUnmanaged(void),
) !void {
    for (bootstrap) |bp| {
        const pr = multiaddr.parseStringTcpAndP2p(allocator, bp) catch continue;
        defer {
            pr.target.deinit(allocator);
            if (pr.p2p_mh) |m| allocator.free(m);
        }
        const id = if (pr.p2p_mh) |m| try allocator.dupe(u8, m) else try allocator.dupe(u8, "");
        errdefer allocator.free(id);
        const host = try allocator.dupe(u8, pr.target.host);
        errdefer allocator.free(host);
        try addCandidate(allocator, nodes, seen_ep, id, host, pr.target.port);
    }
}

/// Merge unique providers (by peer id) into `out_providers`.
pub fn walkGetProviders(
    allocator: std.mem.Allocator,
    routing_key: []const u8,
    bootstrap: []const []const u8,
    secret: [64]u8,
    opts: WalkOpts,
    out_providers: *std.ArrayList(dht.Peer),
) !void {
    var nodes: std.ArrayList(QueryNode) = .empty;
    defer {
        for (nodes.items) |*n| n.deinit(allocator);
        nodes.deinit(allocator);
    }
    var seen_ep: std.StringHashMapUnmanaged(void) = .empty;
    defer {
        var it = seen_ep.keyIterator();
        while (it.next()) |k| allocator.free(k.*);
        seen_ep.deinit(allocator);
    }
    try seedFromBootstrap(allocator, bootstrap, &nodes, &seen_ep);
    try expandFromBootstrapFindNode(allocator, routing_key, bootstrap, secret, &nodes, &seen_ep);

    var seen_queried: std.StringHashMapUnmanaged(void) = .empty;
    defer {
        var it = seen_queried.keyIterator();
        while (it.next()) |k| allocator.free(k.*);
        seen_queried.deinit(allocator);
    }
    var seen_prov: std.StringHashMapUnmanaged(void) = .empty;
    defer {
        var it = seen_prov.keyIterator();
        while (it.next()) |k| allocator.free(k.*);
        seen_prov.deinit(allocator);
    }

    var round: u32 = 0;
    while (round < opts.max_rounds) : (round += 1) {
        if (nodes.items.len == 0) break;
        std.sort.pdq(QueryNode, nodes.items, routing_key, struct {
            pub fn less(rk: []const u8, a: QueryNode, b: QueryNode) bool {
                return cmpQuery(rk, a, b);
            }
        }.less);

        var n_query: u32 = 0;
        var qix: usize = 0;
        while (qix < nodes.items.len and n_query < opts.alpha) {
            const cand = nodes.items[qix];
            qix += 1;
            const qk = try endpointKey(allocator, cand.host, cand.port);
            defer allocator.free(qk);
            if (seen_queried.contains(qk)) continue;

            const qk_own = try allocator.dupe(u8, qk);
            errdefer allocator.free(qk_own);
            var msg = libp2p_dial.dialDhtGetProviders(allocator, cand.host, cand.port, routing_key, secret) catch {
                allocator.free(qk_own);
                continue;
            };
            defer msg.deinit(allocator);
            try seen_queried.put(allocator, qk_own, {});
            n_query += 1;

            for (msg.provider_peers) |p| {
                if (seen_prov.contains(p.id)) continue;
                const pk = try allocator.dupe(u8, p.id);
                errdefer allocator.free(pk);
                try seen_prov.put(allocator, pk, {});
                try out_providers.append(allocator, try dht.clonePeer(allocator, p));
            }

            for (msg.closer_peers) |p| {
                addCandidatesFromDhtPeer(allocator, &nodes, &seen_ep, p) catch continue;
            }
        }
        if (n_query == 0) break;
    }
}

/// After walking, send ADD_PROVIDER to the `replicate_k` closest distinct endpoints.
pub fn walkAndAddProvider(
    allocator: std.mem.Allocator,
    routing_key: []const u8,
    bootstrap: []const []const u8,
    secret: [64]u8,
    opts: WalkOpts,
    replicate_k: usize,
) !void {
    var nodes: std.ArrayList(QueryNode) = .empty;
    defer {
        for (nodes.items) |*n| n.deinit(allocator);
        nodes.deinit(allocator);
    }
    var seen_ep: std.StringHashMapUnmanaged(void) = .empty;
    defer {
        var it = seen_ep.keyIterator();
        while (it.next()) |k| allocator.free(k.*);
        seen_ep.deinit(allocator);
    }
    try seedFromBootstrap(allocator, bootstrap, &nodes, &seen_ep);
    try expandFromBootstrapFindNode(allocator, routing_key, bootstrap, secret, &nodes, &seen_ep);

    var seen_queried: std.StringHashMapUnmanaged(void) = .empty;
    defer {
        var it = seen_queried.keyIterator();
        while (it.next()) |k| allocator.free(k.*);
        seen_queried.deinit(allocator);
    }

    var round: u32 = 0;
    while (round < opts.max_rounds) : (round += 1) {
        if (nodes.items.len == 0) break;
        std.sort.pdq(QueryNode, nodes.items, routing_key, struct {
            pub fn less(rk: []const u8, a: QueryNode, b: QueryNode) bool {
                return cmpQuery(rk, a, b);
            }
        }.less);

        var n_query: u32 = 0;
        var qix: usize = 0;
        while (qix < nodes.items.len and n_query < opts.alpha) {
            const cand = nodes.items[qix];
            qix += 1;
            const qk = try endpointKey(allocator, cand.host, cand.port);
            defer allocator.free(qk);
            if (seen_queried.contains(qk)) continue;

            const qk_own = try allocator.dupe(u8, qk);
            errdefer allocator.free(qk_own);
            var msg = libp2p_dial.dialDhtGetProviders(allocator, cand.host, cand.port, routing_key, secret) catch {
                allocator.free(qk_own);
                continue;
            };
            defer msg.deinit(allocator);
            try seen_queried.put(allocator, qk_own, {});
            n_query += 1;

            for (msg.closer_peers) |p| {
                addCandidatesFromDhtPeer(allocator, &nodes, &seen_ep, p) catch continue;
            }
        }
        if (n_query == 0) break;
    }

    std.sort.pdq(QueryNode, nodes.items, routing_key, struct {
        pub fn less(rk: []const u8, a: QueryNode, b: QueryNode) bool {
            return cmpQuery(rk, a, b);
        }
    }.less);

    var sent: usize = 0;
    var seen_send: std.StringHashMapUnmanaged(void) = .empty;
    defer {
        var it = seen_send.keyIterator();
        while (it.next()) |k| allocator.free(k.*);
        seen_send.deinit(allocator);
    }
    for (nodes.items) |cand| {
        if (sent >= replicate_k) break;
        const sk = try endpointKey(allocator, cand.host, cand.port);
        defer allocator.free(sk);
        if (seen_send.contains(sk)) continue;
        libp2p_dial.dialDhtAddProvider(allocator, cand.host, cand.port, routing_key, secret) catch continue;
        const sko = try allocator.dupe(u8, sk);
        try seen_send.put(allocator, sko, {});
        sent += 1;
    }
}
