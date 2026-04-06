//! Dial Kubo/libp2p: multistream → Noise → multistream → Yamux → bitswap (minimal client).

const std = @import("std");
const multistream = @import("multistream.zig");
const noise = @import("noise.zig");
const yamux = @import("yamux.zig");
const peer_id = @import("peer_id.zig");
const cid_mod = @import("../cid.zig");
const bitswap = @import("../bitswap.zig");
const dht = @import("../dht.zig");
const varint = @import("../varint.zig");
const net_identify = @import("identify.zig");

pub const proto_noise = "/noise\n";
pub const proto_yamux = "/yamux/1.0.0\n";
pub const proto_bitswap = "/ipfs/bitswap/1.2.0\n";
pub const proto_kad = "/ipfs/kad/1.0.0\n";
pub const proto_identify = "/ipfs/id/1.0.0\n";

pub const DialError = error{
    LineTooLong,
    YamuxGoAway,
    BitswapTimeout,
    UnexpectedMultistream,
    UnexpectedKad,
    LengthMismatch,
    MessageTooLong,
};

pub fn readLineNoise(s: *noise.Session, allocator: std.mem.Allocator) ![]u8 {
    var acc = std.ArrayList(u8).empty;
    defer acc.deinit(allocator);
    while (acc.items.len < 8192) {
        const p = try s.readTransport(allocator);
        defer allocator.free(p);
        try acc.appendSlice(allocator, p);
        if (std.mem.indexOfScalar(u8, acc.items, '\n')) |i| {
            return try allocator.dupe(u8, acc.items[0..i]);
        }
    }
    return error.LineTooLong;
}

pub fn writeLineNoise(s: *noise.Session, allocator: std.mem.Allocator, line: []const u8) !void {
    if (line.len == 0 or line[line.len - 1] != '\n') {
        var buf = std.ArrayList(u8).empty;
        defer buf.deinit(allocator);
        try buf.appendSlice(allocator, line);
        try buf.append(allocator, '\n');
        try s.writeTransport(allocator, buf.items);
    } else {
        try s.writeTransport(allocator, line);
    }
}

pub const YamuxOverNoise = struct {
    sess: *noise.Session,
    rx: std.ArrayList(u8) = .empty,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *YamuxOverNoise) void {
        self.rx.deinit(self.allocator);
    }

    fn feed(self: *YamuxOverNoise) !void {
        const chunk = self.sess.readTransport(self.allocator) catch return error.BitswapTimeout;
        defer self.allocator.free(chunk);
        try self.rx.appendSlice(self.allocator, chunk);
    }

    fn ensureLen(self: *YamuxOverNoise, n: usize) !void {
        while (self.rx.items.len < n) try self.feed();
    }

    pub fn writeFrame(self: *YamuxOverNoise, h: yamux.Header, body: []const u8) !void {
        if (body.len != h.length) return error.LengthMismatch;
        var hdr: [12]u8 = undefined;
        h.encode(&hdr);
        var plain = std.ArrayList(u8).empty;
        defer plain.deinit(self.allocator);
        try plain.appendSlice(self.allocator, &hdr);
        try plain.appendSlice(self.allocator, body);
        try self.sess.writeTransport(self.allocator, plain.items);
    }

    pub fn readFrame(self: *YamuxOverNoise, allocator: std.mem.Allocator) !struct { h: yamux.Header, body: []u8 } {
        try self.ensureLen(12);
        var hbuf: [12]u8 = undefined;
        @memcpy(&hbuf, self.rx.items[0..12]);
        const h = yamux.Header.decode(&hbuf) orelse return error.InvalidFrame;
        const max_frame_size: u32 = 4 * 1024 * 1024; // 4MB — matches protocol-level limits
        if (h.length > max_frame_size) return error.MessageTooLong;
        const total = 12 + @as(usize, h.length);
        try self.ensureLen(total);
        const body = try allocator.dupe(u8, self.rx.items[12..total]);
        errdefer allocator.free(body);
        try self.rx.replaceRange(self.allocator, 0, total, &.{});
        return .{ .h = h, .body = body };
    }

    pub fn handlePing(self: *YamuxOverNoise, rh: yamux.Header) !void {
        if ((rh.flags & yamux.flag_syn) != 0) {
            try self.writeFrame(.{
                .version = 0,
                .typ = .ping,
                .flags = yamux.flag_ack,
                .stream_id = 0,
                .length = rh.length,
            }, &.{});
        }
    }

    /// Client-side stream open (odd `stream_id`, e.g. 1).
    pub fn openClientStream(self: *YamuxOverNoise, stream_id: u32) !void {
        try self.writeFrame(.{
            .version = 0,
            .typ = .window_update,
            .flags = yamux.flag_syn,
            .stream_id = stream_id,
            .length = 0,
        }, &.{});
        var iter: u32 = 0;
        while (iter < 10000) : (iter += 1) {
            const fr = try self.readFrame(self.allocator);
            defer self.allocator.free(fr.body);
            switch (fr.h.typ) {
                .ping => try self.handlePing(fr.h),
                .window_update => {
                    if (fr.h.stream_id == stream_id and (fr.h.flags & yamux.flag_ack) != 0)
                        return;
                },
                .go_away => return error.YamuxGoAway,
                else => {},
            }
        }
        return error.BitswapTimeout;
    }

    pub fn streamWrite(self: *YamuxOverNoise, stream_id: u32, payload: []const u8) !void {
        if (payload.len > 256 * 1024) return error.MessageTooLong;
        try self.writeFrame(.{
            .version = 0,
            .typ = .data,
            .flags = 0,
            .stream_id = stream_id,
            .length = @truncate(payload.len),
        }, payload);
    }

    pub fn streamReadLine(self: *YamuxOverNoise, stream_id: u32) ![]u8 {
        var acc = std.ArrayList(u8).empty;
        defer acc.deinit(self.allocator);
        var iter: u32 = 0;
        while (iter < 10000) : (iter += 1) {
            if (std.mem.indexOfScalar(u8, acc.items, '\n')) |i| {
                return try self.allocator.dupe(u8, acc.items[0..i]);
            }
            const fr = try self.readFrame(self.allocator);
            defer self.allocator.free(fr.body);
            switch (fr.h.typ) {
                .ping => try self.handlePing(fr.h),
                .data => {
                    if (fr.h.stream_id == stream_id)
                        try acc.appendSlice(self.allocator, fr.body);
                },
                .go_away => return error.YamuxGoAway,
                else => {},
            }
        }
        return error.BitswapTimeout;
    }

    /// Accumulate DATA on `stream_id` until a varint-length protobuf payload is complete (bitswap / DHT).
    pub fn readLengthPrefixedProtobuf(self: *YamuxOverNoise, stream_id: u32) ![]u8 {
        var acc = std.ArrayList(u8).empty;
        defer acc.deinit(self.allocator);
        var iter: u32 = 0;
        while (iter < 20000) : (iter += 1) {
            if (acc.items.len > 0) {
                var off: usize = 0;
                const du = varint.decodeU64(acc.items, &off);
                if (du) |plen| {
                    if (plen > 4 * 1024 * 1024) return error.BitswapTimeout;
                    const need = off + plen;
                    if (acc.items.len >= need) {
                        return try self.allocator.dupe(u8, acc.items[off..need]);
                    }
                } else |_| {}
            }
            const fr = try self.readFrame(self.allocator);
            defer self.allocator.free(fr.body);
            switch (fr.h.typ) {
                .ping => try self.handlePing(fr.h),
                .data => {
                    if (fr.h.stream_id == stream_id)
                        try acc.appendSlice(self.allocator, fr.body);
                },
                .go_away => return error.YamuxGoAway,
                else => {},
            }
        }
        return error.BitswapTimeout;
    }

    pub fn readBitswapMessage(self: *YamuxOverNoise, stream_id: u32) ![]u8 {
        return self.readLengthPrefixedProtobuf(stream_id);
    }
};

fn multistreamSelectPlain(stream: std.net.Stream, buf: []u8) !void {
    try multistream.writeProtocol(stream, multistream.multistream_1_0);
    const a = try multistream.readLine(stream, buf);
    if (!std.mem.eql(u8, a, multistream.multistream_1_0[0 .. multistream.multistream_1_0.len - 1]))
        return error.UnexpectedMultistream;
    try multistream.writeProtocol(stream, proto_noise);
    _ = try multistream.readLine(stream, buf); // peer echoes `/noise` (or `na`)
}

/// TCP listener side: read client's multistream + `/noise`, echo selections (before Noise handshake).
pub fn multistreamRespondNoise(stream: std.net.Stream, buf: []u8) !void {
    _ = try multistream.readLine(stream, buf);
    try multistream.writeProtocol(stream, multistream.multistream_1_0);
    _ = try multistream.readLine(stream, buf);
    try multistream.writeProtocol(stream, proto_noise);
}

fn negotiateYamuxOnNoise(s: *noise.Session, allocator: std.mem.Allocator) !void {
    try writeLineNoise(s, allocator, multistream.multistream_1_0);
    const l1 = try readLineNoise(s, allocator);
    defer allocator.free(l1);
    try writeLineNoise(s, allocator, proto_yamux);
    const l2 = try readLineNoise(s, allocator);
    defer allocator.free(l2);
}

/// After Noise handshake: remote is multistream initiator for yamux.
pub fn negotiateYamuxOnNoiseResponder(s: *noise.Session, allocator: std.mem.Allocator) !void {
    const l1 = try readLineNoise(s, allocator);
    defer allocator.free(l1);
    if (!std.mem.eql(u8, l1, multistream.multistream_1_0[0 .. multistream.multistream_1_0.len - 1]))
        return error.UnexpectedMultistream;
    try writeLineNoise(s, allocator, multistream.multistream_1_0);
    const l2 = try readLineNoise(s, allocator);
    defer allocator.free(l2);
    if (!std.mem.eql(u8, l2, proto_yamux[0 .. proto_yamux.len - 1]))
        return error.UnexpectedMultistream;
    try writeLineNoise(s, allocator, proto_yamux);
}

pub fn dialNoiseHandshake(
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
    ed25519_secret64: [64]u8,
) !struct { stream: std.net.Stream, session: noise.Session } {
    const stream = try std.net.tcpConnectToHost(allocator, host, port);
    var line_buf: [512]u8 = undefined;
    multistreamSelectPlain(stream, &line_buf) catch |err| {
        stream.close();
        return err;
    };
    const ns = noise.Session.handshakeInitiator(stream, allocator, ed25519_secret64, &.{}) catch |err| {
        stream.close();
        return err;
    };
    return .{ .stream = stream, .session = ns };
}

/// Inbound TCP after `accept`: multistream + Noise XX responder.
pub fn acceptNoiseHandshake(
    allocator: std.mem.Allocator,
    stream: std.net.Stream,
    ed25519_secret64: [64]u8,
) !noise.Session {
    var line_buf: [512]u8 = undefined;
    try multistreamRespondNoise(stream, &line_buf);
    return try noise.Session.handshakeResponder(stream, allocator, ed25519_secret64, &.{});
}

pub fn remotePeerIdString(allocator: std.mem.Allocator, remote_pub32: *const [32]u8) ![]u8 {
    return try peer_id.peerIdString(allocator, remote_pub32);
}

/// TCP + multistream Noise handshake + yamux (initiator). Caller `defer conn.mux.deinit(); defer conn.stream.close();`
pub fn dialNoiseYamuxClient(
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
    ed25519_secret64: [64]u8,
) !struct { stream: std.net.Stream, ns: noise.Session, mux: YamuxOverNoise } {
    const stream = try std.net.tcpConnectToHost(allocator, host, port);
    errdefer stream.close();
    var line_buf: [512]u8 = undefined;
    try multistreamSelectPlain(stream, &line_buf);
    var ns = try noise.Session.handshakeInitiator(stream, allocator, ed25519_secret64, &.{});
    try negotiateYamuxOnNoise(&ns, allocator);
    const mux = YamuxOverNoise{ .sess = &ns, .allocator = allocator };
    return .{ .stream = stream, .ns = ns, .mux = mux };
}

fn negotiateMuxLine(
    mux: *YamuxOverNoise,
    allocator: std.mem.Allocator,
    stream_id: u32,
    proto_line: []const u8,
    expect_ack_prefix: []const u8,
) !void {
    try mux.streamWrite(stream_id, multistream.multistream_1_0);
    const na = try mux.streamReadLine(stream_id);
    defer allocator.free(na);
    if (!std.mem.eql(u8, na, multistream.multistream_1_0[0 .. multistream.multistream_1_0.len - 1]))
        return error.UnexpectedMultistream;
    try mux.streamWrite(stream_id, proto_line);
    const nb = try mux.streamReadLine(stream_id);
    defer allocator.free(nb);
    if (!std.mem.startsWith(u8, nb, expect_ack_prefix))
        return error.UnexpectedMultistream;
}

fn negotiateMuxKad(
    mux: *YamuxOverNoise,
    allocator: std.mem.Allocator,
    stream_id: u32,
) !void {
    try mux.streamWrite(stream_id, multistream.multistream_1_0);
    const na = try mux.streamReadLine(stream_id);
    defer allocator.free(na);
    if (!std.mem.eql(u8, na, multistream.multistream_1_0[0 .. multistream.multistream_1_0.len - 1]))
        return error.UnexpectedMultistream;
    try mux.streamWrite(stream_id, proto_kad);
    const nb = try mux.streamReadLine(stream_id);
    defer allocator.free(nb);
    if (!std.mem.startsWith(u8, nb, "/ipfs/kad/"))
        return error.UnexpectedKad;
}

/// Read remote `Identify` after multistream selection on an opened yamux stream.
pub fn identifyReadRemote(
    allocator: std.mem.Allocator,
    mux: *YamuxOverNoise,
    stream_id: u32,
) !net_identify.IdentifyInfo {
    try negotiateMuxLine(mux, allocator, stream_id, proto_identify, "/ipfs/id/");
    try mux.streamWrite(stream_id, &[_]u8{0});
    const resp = try mux.readLengthPrefixedProtobuf(stream_id);
    defer allocator.free(resp);
    return try net_identify.decodeIdentify(allocator, resp);
}

/// TCP → Noise → Yamux → Identify (1) → bitswap HAVE+BLOCK then optional BLOCK-only (stream 3). Returns owned `block` or null.
pub fn dialBitswapWant(
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
    cid_str: []const u8,
    ed25519_secret64: [64]u8,
) !?[]u8 {
    var conn = try dialNoiseYamuxClient(allocator, host, port, ed25519_secret64);
    conn.mux.sess = &conn.ns; // Fix: point to the returned copy, not the dead stack local
    defer conn.mux.deinit();
    defer conn.stream.close();

    try conn.mux.openClientStream(1);
    var id_info = identifyReadRemote(allocator, &conn.mux, 1) catch net_identify.IdentifyInfo{};
    defer id_info.deinit(allocator);

    try conn.mux.openClientStream(3);
    try negotiateMuxLine(&conn.mux, allocator, 3, proto_bitswap, "/ipfs/bitswap/");

    var c = try cid_mod.Cid.parse(allocator, cid_str);
    defer c.deinit(allocator);
    const cid_bytes = try c.toBytes(allocator);
    defer allocator.free(cid_bytes);

    const framed1 = try bitswap.encodeFramedWant(allocator, cid_bytes, true);
    defer allocator.free(framed1);
    try conn.mux.streamWrite(3, framed1);

    const resp_pb = try conn.mux.readBitswapMessage(3);
    defer allocator.free(resp_pb);
    if (try bitswap.decodeFirstPayloadData(resp_pb, allocator)) |b| return b;

    const pres = try bitswap.decodeBlockPresences(resp_pb, allocator);
    defer {
        for (pres) |p| allocator.free(p.cid);
        allocator.free(pres);
    }
    if (!bitswap.blockPresencesHaveCid(pres, cid_bytes)) return null;

    const framed2 = try bitswap.encodeFramedWant(allocator, cid_bytes, false);
    defer allocator.free(framed2);
    try conn.mux.streamWrite(3, framed2);
    const resp2 = try conn.mux.readBitswapMessage(3);
    defer allocator.free(resp2);
    return try bitswap.decodeFirstPayloadData(resp2, allocator);
}

/// Varint-framed DHT `Message` round-trip on a new kad stream (caller builds `request_pb`).
pub fn dialDhtExchange(
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
    request_pb: []const u8,
    ed25519_secret64: [64]u8,
) !dht.Message {
    var conn = try dialNoiseYamuxClient(allocator, host, port, ed25519_secret64);
    conn.mux.sess = &conn.ns; // Fix: point to the returned copy, not the dead stack local
    defer conn.mux.deinit();
    defer conn.stream.close();

    try conn.mux.openClientStream(1);
    var id_info = identifyReadRemote(allocator, &conn.mux, 1) catch net_identify.IdentifyInfo{};
    defer id_info.deinit(allocator);

    try conn.mux.openClientStream(3);
    try negotiateMuxKad(&conn.mux, allocator, 3);

    var framed = std.ArrayList(u8).empty;
    defer framed.deinit(allocator);
    try varint.encodeU64(&framed, allocator, request_pb.len);
    try framed.appendSlice(allocator, request_pb);
    try conn.mux.streamWrite(3, framed.items);

    const resp_pb = try conn.mux.readLengthPrefixedProtobuf(3);
    defer allocator.free(resp_pb);
    return try dht.Message.decode(resp_pb, allocator);
}

/// Yamux stream `kad` GET_PROVIDERS; `routing_key` is `/providers/` + multihash (Kubo). Caller frees returned message.
pub fn dialDhtGetProviders(
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
    routing_key: []const u8,
    ed25519_secret64: [64]u8,
) !dht.Message {
    const req = try dht.encodeGetProviders(allocator, routing_key);
    defer allocator.free(req);
    return dialDhtExchange(allocator, host, port, req, ed25519_secret64);
}

/// FIND_NODE toward `key` (same key bytes as GET_PROVIDERS for content routing).
pub fn dialDhtFindNode(
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
    key: []const u8,
    ed25519_secret64: [64]u8,
) !dht.Message {
    const req = try dht.encodeFindNode(allocator, key);
    defer allocator.free(req);
    return dialDhtExchange(allocator, host, port, req, ed25519_secret64);
}

/// Announce provider for `routing_key` (ADD_PROVIDER with peer id + `provider_addrs_bin`). Response is decoded and discarded.
pub fn dialDhtAddProvider(
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
    routing_key: []const u8,
    ed25519_secret64: [64]u8,
    provider_addrs_bin: []const []const u8,
) !void {
    const sk = try std.crypto.sign.Ed25519.SecretKey.fromBytes(ed25519_secret64);
    const kp = try std.crypto.sign.Ed25519.KeyPair.fromSecretKey(sk);
    const pub_bytes = kp.public_key.toBytes();
    const id_mh = try peer_id.peerMultihashBytes(allocator, &pub_bytes);
    defer allocator.free(id_mh);
    const req = try dht.encodeAddProvider(allocator, routing_key, id_mh, provider_addrs_bin);
    defer allocator.free(req);
    var resp = try dialDhtExchange(allocator, host, port, req, ed25519_secret64);
    defer resp.deinit(allocator);
}
