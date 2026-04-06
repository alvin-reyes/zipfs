//! Inbound Noise + yamux + Identify + bitswap (Kubo-style multiprotocol server).

const std = @import("std");
const multistream = @import("multistream.zig");
const yamux = @import("yamux.zig");
const bitswap = @import("../bitswap.zig");
const blockstore = @import("../blockstore.zig");
const cid_mod = @import("../cid.zig");
const varint = @import("../varint.zig");
const libp2p_dial = @import("libp2p_dial.zig");
const identify = @import("identify.zig");
const peer_id = @import("peer_id.zig");
const cluster_push = @import("cluster_push.zig");

pub const SwarmThreadCtx = struct {
    server: std.net.Server,
    secret: [64]u8,
    public_key: [32]u8,
    /// Binary multiaddrs; owned for process lifetime (daemon).
    listen_addrs_bin: [][]u8,
    store: *blockstore.Blockstore,
    mu: *std.Thread.Mutex,
    cluster_secret: ?[]const u8 = null,
};

/// Background accept loop (use with `gateway.run` + same `mu` on the blockstore).
pub fn swarmAcceptLoop(ctx: *SwarmThreadCtx) void {
    const alloc = std.heap.page_allocator;
    while (true) {
        const conn = ctx.server.accept() catch continue;
        serveSwarmConnection(alloc, conn.stream, ctx.secret, ctx.public_key, ctx.listen_addrs_bin, ctx.store, ctx.cluster_secret, ctx.mu) catch {};
        conn.stream.close();
    }
}

const Phase = enum {
    ms_line,
    proto_line,
    bitswap,
    identify_read,
    identify_done,
    cluster_push,
    discard,
};

const StreamState = struct {
    acc: std.ArrayList(u8),
    phase: Phase,

    fn deinit(self: *StreamState, allocator: std.mem.Allocator) void {
        self.acc.deinit(allocator);
        self.* = undefined;
    }
};

fn takeLine(allocator: std.mem.Allocator, acc: *std.ArrayList(u8)) !?[]u8 {
    if (acc.items.len > 8192) return error.LineTooLong;
    const idx = std.mem.indexOfScalar(u8, acc.items, '\n') orelse return null;
    const line = try allocator.dupe(u8, acc.items[0..idx]);
    errdefer allocator.free(line);
    try acc.replaceRange(allocator, 0, idx + 1, &.{});
    return line;
}

const IdentifyCtx = struct {
    pubkey_pb: []const u8,
    listen_addrs_bin: []const []const u8,
};

fn appendPresenceForStoreKey(
    allocator: std.mem.Allocator,
    pres: *std.ArrayList(bitswap.BlockPresence),
    store_key: []const u8,
    typ: bitswap.BlockPresenceType,
) !void {
    var c = try cid_mod.Cid.parse(allocator, store_key);
    defer c.deinit(allocator);
    const cid_wire = try c.toBytes(allocator);
    errdefer allocator.free(cid_wire);
    try pres.append(allocator, .{ .cid = cid_wire, .typ = typ });
}

fn sendIdentifyResponse(
    mux: *libp2p_dial.YamuxOverNoise,
    allocator: std.mem.Allocator,
    stream_id: u32,
    ictx: IdentifyCtx,
) !void {
    const body = try identify.encodeIdentify(allocator, ictx.pubkey_pb, ictx.listen_addrs_bin, &identify.default_protocols);
    defer allocator.free(body);
    var framed: std.ArrayList(u8) = .empty;
    defer framed.deinit(allocator);
    try varint.encodeU64(&framed, allocator, body.len);
    try framed.appendSlice(allocator, body);
    try mux.streamWrite(stream_id, framed.items);
}

fn advanceStream(
    mux: *libp2p_dial.YamuxOverNoise,
    allocator: std.mem.Allocator,
    store: *blockstore.Blockstore,
    stream_id: u32,
    st: *StreamState,
    ictx: IdentifyCtx,
    cluster_secret: ?[]const u8,
    mu: *std.Thread.Mutex,
) !void {
    while (true) {
        switch (st.phase) {
            .ms_line => {
                const line = takeLine(allocator, &st.acc) catch {
                    st.phase = .discard;
                    return;
                } orelse return;
                defer allocator.free(line);
                if (!std.mem.eql(u8, line, multistream.multistream_1_0[0 .. multistream.multistream_1_0.len - 1])) {
                    st.phase = .discard;
                    return;
                }
                try mux.streamWrite(stream_id, multistream.multistream_1_0);
                st.phase = .proto_line;
            },
            .proto_line => {
                const line = takeLine(allocator, &st.acc) catch {
                    st.phase = .discard;
                    return;
                } orelse return;
                defer allocator.free(line);
                var echo = std.ArrayList(u8).empty;
                defer echo.deinit(allocator);
                try echo.appendSlice(allocator, line);
                try echo.append(allocator, '\n');
                if (std.mem.startsWith(u8, line, "/ipfs/bitswap/")) {
                    try mux.streamWrite(stream_id, echo.items);
                    st.phase = .bitswap;
                } else if (std.mem.startsWith(u8, line, "/ipfs/id/")) {
                    try mux.streamWrite(stream_id, echo.items);
                    st.phase = .identify_read;
                } else if (std.mem.startsWith(u8, line, "/zipfs/cluster/push/")) {
                    try mux.streamWrite(stream_id, echo.items);
                    st.phase = .cluster_push;
                } else {
                    st.phase = .discard;
                    return;
                }
            },
            .identify_read => {
                if (st.acc.items.len == 0) return;
                var off: usize = 0;
                const plen = varint.decodeU64(st.acc.items, &off) catch return;
                if (plen > 256 * 1024) {
                    st.phase = .discard;
                    return;
                }
                const need = off + plen;
                if (st.acc.items.len < need) return;
                try st.acc.replaceRange(allocator, 0, need, &.{});
                try sendIdentifyResponse(mux, allocator, stream_id, ictx);
                st.phase = .identify_done;
            },
            .bitswap => {
                if (st.acc.items.len == 0) return;
                var off: usize = 0;
                const plen = varint.decodeU64(st.acc.items, &off) catch return;
                if (plen > 4 * 1024 * 1024) {
                    st.phase = .discard;
                    return;
                }
                const need = off + plen;
                if (st.acc.items.len < need) return;
                const msg_pb = st.acc.items[off..need];
                try st.acc.replaceRange(allocator, 0, need, &.{});
                const want_items = try bitswap.decodeWantlistItems(msg_pb, allocator);
                defer {
                    for (want_items) |*wi| wi.deinit(allocator);
                    allocator.free(want_items);
                }
                var entries: std.ArrayList(bitswap.BlockPayload) = .empty;
                // Track owned data copies for deferred free
                var owned_data: std.ArrayList([]const u8) = .empty;
                defer {
                    for (entries.items) |e| allocator.free(e.prefix);
                    entries.deinit(allocator);
                    for (owned_data.items) |d| allocator.free(d);
                    owned_data.deinit(allocator);
                }
                var pres: std.ArrayList(bitswap.BlockPresence) = .empty;
                defer {
                    for (pres.items) |bp| allocator.free(bp.cid);
                    pres.deinit(allocator);
                }
                var have_presence_key: std.StringHashMapUnmanaged(void) = .empty;
                defer {
                    var key_it = have_presence_key.keyIterator();
                    while (key_it.next()) |k| allocator.free(k.*);
                    have_presence_key.deinit(allocator);
                }

                for (want_items) |wi| {
                    mu.lock();
                    const data_copy = store.get(allocator, wi.store_key);
                    mu.unlock();

                    switch (wi.want_type) {
                        .have => {
                            if (data_copy) |dc| allocator.free(dc); // not needed for HAVE
                            if (have_presence_key.contains(wi.store_key)) continue;
                            const kdup = try allocator.dupe(u8, wi.store_key);
                            errdefer allocator.free(kdup);
                            try have_presence_key.put(allocator, kdup, {});
                            const typ: bitswap.BlockPresenceType = if (data_copy != null) .have else .dont_have;
                            try appendPresenceForStoreKey(allocator, &pres, wi.store_key, typ);
                        },
                        .block => {
                            if (data_copy) |d| {
                                try owned_data.append(allocator, d);
                                var c = try cid_mod.Cid.parse(allocator, wi.store_key);
                                defer c.deinit(allocator);
                                const prefix = try c.toBytes(allocator);
                                errdefer allocator.free(prefix);
                                try entries.append(allocator, .{ .prefix = prefix, .data = d });
                            } else if (wi.send_dont_have) {
                                if (have_presence_key.contains(wi.store_key)) continue;
                                const kdup = try allocator.dupe(u8, wi.store_key);
                                errdefer allocator.free(kdup);
                                try have_presence_key.put(allocator, kdup, {});
                                try appendPresenceForStoreKey(allocator, &pres, wi.store_key, .dont_have);
                            }
                        },
                    }
                }
                const resp_pb = blk: {
                    const pl = entries.items;
                    const bp = pres.items;
                    if (pl.len == 0 and bp.len == 0) {
                        break :blk try bitswap.encodeEmptyMessage(allocator);
                    }
                    const msg = bitswap.Message{ .payload = pl, .block_presences = bp };
                    break :blk try msg.encode(allocator);
                };
                defer allocator.free(resp_pb);
                var framed: std.ArrayList(u8) = .empty;
                defer framed.deinit(allocator);
                try varint.encodeU64(&framed, allocator, resp_pb.len);
                try framed.appendSlice(allocator, resp_pb);
                try mux.streamWrite(stream_id, framed.items);
            },
            .cluster_push => {
                if (st.acc.items.len == 0) return;
                var off: usize = 0;
                const plen = varint.decodeU64(st.acc.items, &off) catch return;
                if (plen > 4 * 1024 * 1024) {
                    st.phase = .discard;
                    return;
                }
                const need = off + plen;
                if (st.acc.items.len < need) return;
                const msg_pb = st.acc.items[off..need];
                try st.acc.replaceRange(allocator, 0, need, &.{});

                var decoded = cluster_push.decodeMessage(allocator, msg_pb) catch {
                    st.phase = .discard;
                    return;
                };
                defer decoded.deinit(allocator);

                // Validate cluster secret (constant-time comparison to prevent timing attacks)
                if (cluster_secret) |expected| {
                    const provided = decoded.cluster_secret orelse {
                        st.phase = .discard;
                        return;
                    };
                    if (expected.len != provided.len) {
                        st.phase = .discard;
                        return;
                    }
                    var diff: u8 = 0;
                    for (expected, provided) |a, b| {
                        diff |= a ^ b;
                    }
                    if (diff != 0) {
                        st.phase = .discard;
                        return;
                    }
                }

                switch (decoded.msg_type) {
                    .push_blocks => {
                        var stored: u32 = 0;
                        for (decoded.block_entries) |entry| {
                            const c = cid_mod.Cid.fromBytes(allocator, entry.cid_bytes) catch continue;
                            defer c.deinit(allocator);
                            // Verify content-address integrity (reject unverifiable hashes)
                            if (c.hash.len >= 34 and c.hash[0] == 0x12 and c.hash[1] == 0x20) {
                                // SHA-256 multihash: verify hash matches data
                                var computed: [32]u8 = undefined;
                                std.crypto.hash.sha2.Sha256.hash(entry.data, &computed, .{});
                                if (!std.mem.eql(u8, c.hash[2..34], &computed)) continue;
                            } else {
                                // Reject non-SHA-256 CIDs — cannot verify integrity
                                continue;
                            }
                            mu.lock();
                            store.put(allocator, c, entry.data) catch {
                                mu.unlock();
                                continue;
                            };
                            mu.unlock();
                            stored += 1;
                        }
                        const ack = cluster_push.encodePushAck(allocator, stored) catch return;
                        defer allocator.free(ack);
                        var framed: std.ArrayList(u8) = .empty;
                        defer framed.deinit(allocator);
                        varint.encodeU64(&framed, allocator, ack.len) catch return;
                        framed.appendSlice(allocator, ack) catch return;
                        mux.streamWrite(stream_id, framed.items) catch return;
                    },
                    .have_check => {
                        var have_list = std.ArrayList([]const u8).empty;
                        defer have_list.deinit(allocator);
                        for (decoded.cid_list) |cid_bytes| {
                            // Convert binary CID to string key for blockstore lookup
                            const c = cid_mod.Cid.fromBytes(allocator, cid_bytes) catch continue;
                            defer c.deinit(allocator);
                            const key = c.toString(allocator) catch continue;
                            defer allocator.free(key);
                            mu.lock();
                            const has_block = store.has(key);
                            mu.unlock();
                            if (has_block) {
                                have_list.append(allocator, cid_bytes) catch continue;
                            }
                        }
                        const resp = cluster_push.encodeHaveResponse(allocator, have_list.items) catch return;
                        defer allocator.free(resp);
                        var framed: std.ArrayList(u8) = .empty;
                        defer framed.deinit(allocator);
                        varint.encodeU64(&framed, allocator, resp.len) catch return;
                        framed.appendSlice(allocator, resp) catch return;
                        mux.streamWrite(stream_id, framed.items) catch return;
                    },
                    .ping => {
                        const pong = cluster_push.encodePong(allocator) catch return;
                        defer allocator.free(pong);
                        var framed: std.ArrayList(u8) = .empty;
                        defer framed.deinit(allocator);
                        varint.encodeU64(&framed, allocator, pong.len) catch return;
                        framed.appendSlice(allocator, pong) catch return;
                        mux.streamWrite(stream_id, framed.items) catch return;
                    },
                    .block_pull => {
                        // Serve a single block to a pulling peer
                        if (decoded.root_cid) |cid_str| {
                            mu.lock();
                            const block_data = store.get(allocator, cid_str);
                            mu.unlock();
                            const resp = if (block_data) |data| blk_resp: {
                                defer allocator.free(data);
                                break :blk_resp cluster_push.encodeBlockPullResp(allocator, cid_str, data) catch return;
                            } else cluster_push.encodeBlockPullResp(allocator, cid_str, null) catch return;
                            defer allocator.free(resp);
                            var framed: std.ArrayList(u8) = .empty;
                            defer framed.deinit(allocator);
                            varint.encodeU64(&framed, allocator, resp.len) catch return;
                            framed.appendSlice(allocator, resp) catch return;
                            mux.streamWrite(stream_id, framed.items) catch return;
                        }
                    },
                    .manifest_notify => {
                        // Acknowledge manifest notification
                        const ack = cluster_push.encodeManifestAck(allocator) catch return;
                        defer allocator.free(ack);
                        var framed: std.ArrayList(u8) = .empty;
                        defer framed.deinit(allocator);
                        varint.encodeU64(&framed, allocator, ack.len) catch return;
                        framed.appendSlice(allocator, ack) catch return;
                        mux.streamWrite(stream_id, framed.items) catch return;
                    },
                    else => {},
                }
                st.phase = .discard;
            },
            .identify_done, .discard => return,
        }
    }
}

fn serveYamuxMultiprotocol(
    mux: *libp2p_dial.YamuxOverNoise,
    allocator: std.mem.Allocator,
    store: *blockstore.Blockstore,
    pubkey_pb: []const u8,
    listen_addrs_bin: []const []const u8,
    cluster_secret: ?[]const u8,
    mu: *std.Thread.Mutex,
) !void {
    var streams: std.AutoHashMapUnmanaged(u32, StreamState) = .empty;
    defer {
        var it = streams.valueIterator();
        while (it.next()) |st| st.acc.deinit(allocator);
        streams.deinit(allocator);
    }

    const ictx = IdentifyCtx{ .pubkey_pb = pubkey_pb, .listen_addrs_bin = listen_addrs_bin };
    var iter: u32 = 0;
    while (iter < 100000) : (iter += 1) {
        const fr = try mux.readFrame(allocator);
        defer allocator.free(fr.body);
        switch (fr.h.typ) {
            .ping => try mux.handlePing(fr.h),
            .window_update => {
                if (fr.h.stream_id != 0 and (fr.h.flags & yamux.flag_syn) != 0) {
                    const max_streams: u32 = 256;
                    if (streams.count() >= max_streams) continue;
                    const g = try streams.getOrPut(allocator, fr.h.stream_id);
                    if (!g.found_existing) {
                        g.value_ptr.* = .{ .acc = .empty, .phase = .ms_line };
                    }
                    try mux.writeFrame(.{
                        .version = 0,
                        .typ = .window_update,
                        .flags = yamux.flag_ack,
                        .stream_id = fr.h.stream_id,
                        .length = 0,
                    }, &.{});
                }
            },
            .data => {
                const sid = fr.h.stream_id;
                if (sid == 0) continue;
                const gop = try streams.getOrPut(allocator, sid);
                if (!gop.found_existing) {
                    // Enforce stream limit for data frames too (matches window_update check)
                    const max_streams: u32 = 256;
                    if (streams.count() > max_streams) {
                        // Remove the just-inserted entry and skip
                        streams.removeByPtr(gop.key_ptr);
                        continue;
                    }
                    gop.value_ptr.* = .{ .acc = .empty, .phase = .ms_line };
                }
                try gop.value_ptr.acc.appendSlice(allocator, fr.body);
                advanceStream(mux, allocator, store, sid, gop.value_ptr, ictx, cluster_secret, mu) catch {
                    gop.value_ptr.phase = .discard;
                };
            },
            .go_away => return,
        }
    }
}

/// `listen_addrs_bin`: dialable `/ip4/.../tcp/...` packed multiaddrs for Identify.
pub fn serveSwarmConnection(
    allocator: std.mem.Allocator,
    stream: std.net.Stream,
    ed25519_secret64: [64]u8,
    ed25519_pub32: [32]u8,
    listen_addrs_bin: []const []const u8,
    store: *blockstore.Blockstore,
    cluster_secret: ?[]const u8,
    mu: *std.Thread.Mutex,
) !void {
    var ns = try libp2p_dial.acceptNoiseHandshake(allocator, stream, ed25519_secret64);
    try libp2p_dial.negotiateYamuxOnNoiseResponder(&ns, allocator);

    var mux = libp2p_dial.YamuxOverNoise{ .sess = &ns, .allocator = allocator };
    defer mux.deinit();

    const pk_pb = peer_id.marshalPublicKeyEd25519(&ed25519_pub32);
    try serveYamuxMultiprotocol(&mux, allocator, store, pk_pb[0..], listen_addrs_bin, cluster_secret, mu);
}
