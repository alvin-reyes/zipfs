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

pub const SwarmThreadCtx = struct {
    server: std.net.Server,
    secret: [64]u8,
    public_key: [32]u8,
    /// Binary multiaddrs; owned for process lifetime (daemon).
    listen_addrs_bin: [][]u8,
    store: *blockstore.Blockstore,
    mu: *std.Thread.Mutex,
};

/// Background accept loop (use with `gateway.run` + same `mu` on the blockstore).
pub fn swarmAcceptLoop(ctx: *SwarmThreadCtx) void {
    const alloc = std.heap.page_allocator;
    while (true) {
        const conn = ctx.server.accept() catch continue;
        ctx.mu.lock();
        serveSwarmConnection(alloc, conn.stream, ctx.secret, ctx.public_key, ctx.listen_addrs_bin, ctx.store) catch {};
        ctx.mu.unlock();
        conn.stream.close();
    }
}

const Phase = enum {
    ms_line,
    proto_line,
    bitswap,
    identify_read,
    identify_done,
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
                defer {
                    for (entries.items) |e| allocator.free(e.prefix);
                    entries.deinit(allocator);
                }
                var pres: std.ArrayList(bitswap.BlockPresence) = .empty;
                defer {
                    for (pres.items) |bp| allocator.free(bp.cid);
                    pres.deinit(allocator);
                }
                var have_presence_key: std.StringHashMapUnmanaged(void) = .empty;
                defer have_presence_key.deinit(allocator);

                for (want_items) |wi| {
                    const raw = store.get(wi.store_key);
                    switch (wi.want_type) {
                        .have => {
                            if (have_presence_key.contains(wi.store_key)) continue;
                            const kdup = try allocator.dupe(u8, wi.store_key);
                            errdefer allocator.free(kdup);
                            try have_presence_key.put(allocator, kdup, {});
                            const typ: bitswap.BlockPresenceType = if (raw != null) .have else .dont_have;
                            try appendPresenceForStoreKey(allocator, &pres, wi.store_key, typ);
                        },
                        .block => {
                            if (raw) |d| {
                                var c = try cid_mod.Cid.parse(allocator, wi.store_key);
                                defer c.deinit(allocator);
                                const prefix = try c.toBytes(allocator);
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
                    gop.value_ptr.* = .{ .acc = .empty, .phase = .ms_line };
                }
                try gop.value_ptr.acc.appendSlice(allocator, fr.body);
                advanceStream(mux, allocator, store, sid, gop.value_ptr, ictx) catch {
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
) !void {
    var ns = try libp2p_dial.acceptNoiseHandshake(allocator, stream, ed25519_secret64);
    try libp2p_dial.negotiateYamuxOnNoiseResponder(&ns, allocator);

    var mux = libp2p_dial.YamuxOverNoise{ .sess = &ns, .allocator = allocator };
    defer mux.deinit();

    const pk_pb = peer_id.marshalPublicKeyEd25519(&ed25519_pub32);
    try serveYamuxMultiprotocol(&mux, allocator, store, pk_pb[0..], listen_addrs_bin);
}
