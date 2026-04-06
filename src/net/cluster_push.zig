//! `/zipfs/cluster/push/1.0.0` — push blocks, check presence, and ping cluster peers.

const std = @import("std");
const wireproto = @import("../wireproto.zig");
const varint = @import("../varint.zig");
const blockstore = @import("../blockstore.zig");
const cid_mod = @import("../cid.zig");
const libp2p_dial = @import("libp2p_dial.zig");
const multiaddr = @import("multiaddr.zig");

pub const proto_cluster_push = "/zipfs/cluster/push/1.0.0\n";

/// Message types for the cluster push protocol.
pub const MsgType = enum(u8) {
    push_blocks = 0,
    push_ack = 1,
    have_check = 2,
    have_response = 3,
    ping = 4,
    pong = 5,
};

/// Block entry within a push message.
pub const BlockEntry = struct {
    cid_bytes: []const u8,
    data: []const u8,
};

/// Encode a PUSH_BLOCKS message: field 1=type, field 2=root_cid, field 3=block entries, field 4=cluster_secret.
pub fn encodePushBlocks(
    allocator: std.mem.Allocator,
    root_cid: []const u8,
    blocks: []const BlockEntry,
    cluster_secret: ?[]const u8,
) ![]u8 {
    var buf = std.ArrayList(u8).empty;
    errdefer buf.deinit(allocator);
    try wireproto.appendVarintField(&buf, allocator, 1, @intFromEnum(MsgType.push_blocks));
    try wireproto.appendBytesField(&buf, allocator, 2, root_cid);
    for (blocks) |b| {
        // Each block entry: sub-message with field 1=cid, field 2=data
        var entry_buf = std.ArrayList(u8).empty;
        defer entry_buf.deinit(allocator);
        try wireproto.appendBytesField(&entry_buf, allocator, 1, b.cid_bytes);
        try wireproto.appendBytesField(&entry_buf, allocator, 2, b.data);
        try wireproto.appendBytesField(&buf, allocator, 3, entry_buf.items);
    }
    if (cluster_secret) |s| {
        try wireproto.appendBytesField(&buf, allocator, 4, s);
    }
    return try buf.toOwnedSlice(allocator);
}

/// Encode a PUSH_ACK message: field 1=type, field 2=block_count.
pub fn encodePushAck(allocator: std.mem.Allocator, block_count: u32) ![]u8 {
    var buf = std.ArrayList(u8).empty;
    errdefer buf.deinit(allocator);
    try wireproto.appendVarintField(&buf, allocator, 1, @intFromEnum(MsgType.push_ack));
    try wireproto.appendVarintField(&buf, allocator, 2, block_count);
    return try buf.toOwnedSlice(allocator);
}

/// Encode a HAVE_CHECK message: field 1=type, field 3=cid entries.
pub fn encodeHaveCheck(allocator: std.mem.Allocator, cids: []const []const u8, cluster_secret: ?[]const u8) ![]u8 {
    var buf = std.ArrayList(u8).empty;
    errdefer buf.deinit(allocator);
    try wireproto.appendVarintField(&buf, allocator, 1, @intFromEnum(MsgType.have_check));
    for (cids) |c| {
        try wireproto.appendBytesField(&buf, allocator, 3, c);
    }
    if (cluster_secret) |s| {
        try wireproto.appendBytesField(&buf, allocator, 4, s);
    }
    return try buf.toOwnedSlice(allocator);
}

/// Encode a HAVE_RESPONSE: field 1=type, field 3=cid entries the peer has.
pub fn encodeHaveResponse(allocator: std.mem.Allocator, have_cids: []const []const u8) ![]u8 {
    var buf = std.ArrayList(u8).empty;
    errdefer buf.deinit(allocator);
    try wireproto.appendVarintField(&buf, allocator, 1, @intFromEnum(MsgType.have_response));
    for (have_cids) |c| {
        try wireproto.appendBytesField(&buf, allocator, 3, c);
    }
    return try buf.toOwnedSlice(allocator);
}

/// Encode PING: field 1=type.
pub fn encodePing(allocator: std.mem.Allocator, cluster_secret: ?[]const u8) ![]u8 {
    var buf = std.ArrayList(u8).empty;
    errdefer buf.deinit(allocator);
    try wireproto.appendVarintField(&buf, allocator, 1, @intFromEnum(MsgType.ping));
    if (cluster_secret) |s| {
        try wireproto.appendBytesField(&buf, allocator, 4, s);
    }
    return try buf.toOwnedSlice(allocator);
}

/// Encode PONG: field 1=type.
pub fn encodePong(allocator: std.mem.Allocator) ![]u8 {
    var buf = std.ArrayList(u8).empty;
    errdefer buf.deinit(allocator);
    try wireproto.appendVarintField(&buf, allocator, 1, @intFromEnum(MsgType.pong));
    return try buf.toOwnedSlice(allocator);
}

/// Decoded cluster push message.
pub const DecodedMessage = struct {
    msg_type: MsgType,
    root_cid: ?[]const u8 = null,
    block_entries: []DecodedBlockEntry = &.{},
    cid_list: [][]const u8 = &.{},
    block_count: u32 = 0,
    cluster_secret: ?[]const u8 = null,

    pub fn deinit(self: *DecodedMessage, allocator: std.mem.Allocator) void {
        for (self.block_entries) |e| {
            allocator.free(e.cid_bytes);
            allocator.free(e.data);
        }
        if (self.block_entries.len > 0) allocator.free(self.block_entries);
        for (self.cid_list) |c| allocator.free(c);
        if (self.cid_list.len > 0) allocator.free(self.cid_list);
        if (self.root_cid) |r| allocator.free(r);
        if (self.cluster_secret) |s| allocator.free(s);
        self.* = undefined;
    }
};

pub const DecodedBlockEntry = struct {
    cid_bytes: []const u8,
    data: []const u8,
};

/// Decode a cluster push protocol message.
pub fn decodeMessage(allocator: std.mem.Allocator, pb: []const u8) !DecodedMessage {
    var msg = DecodedMessage{ .msg_type = .ping };
    errdefer msg.deinit(allocator);

    var block_list = std.ArrayList(DecodedBlockEntry).empty;
    errdefer {
        for (block_list.items) |e| {
            allocator.free(e.cid_bytes);
            allocator.free(e.data);
        }
        block_list.deinit(allocator);
    }
    var cid_list = std.ArrayList([]const u8).empty;
    errdefer {
        for (cid_list.items) |c| allocator.free(c);
        cid_list.deinit(allocator);
    }

    var it = wireproto.FieldIter{ .data = pb };
    while (try it.next()) |e| {
        switch (e.field_num) {
            1 => {
                const v: u8 = @intCast(e.int_val & 0xFF);
                msg.msg_type = std.meta.intToEnum(MsgType, v) catch return error.Truncated;
            },
            2 => {
                if (msg.msg_type == .push_ack) {
                    msg.block_count = @intCast(e.int_val & 0xFFFFFFFF);
                } else {
                    if (msg.root_cid) |old| allocator.free(old);
                    msg.root_cid = try allocator.dupe(u8, e.bytes);
                }
            },
            3 => {
                if (msg.msg_type == .push_blocks) {
                    // Parse sub-message for block entry
                    var sub_it = wireproto.FieldIter{ .data = e.bytes };
                    var cid_b: ?[]const u8 = null;
                    var data_b: ?[]const u8 = null;
                    while (try sub_it.next()) |se| {
                        switch (se.field_num) {
                            1 => {
                                if (cid_b) |old| allocator.free(old);
                                cid_b = try allocator.dupe(u8, se.bytes);
                            },
                            2 => {
                                if (data_b) |old| allocator.free(old);
                                data_b = try allocator.dupe(u8, se.bytes);
                            },
                            else => {},
                        }
                    }
                    if (cid_b) |cb| {
                        try block_list.append(allocator, .{
                            .cid_bytes = cb,
                            .data = data_b orelse try allocator.dupe(u8, &.{}),
                        });
                    } else {
                        if (data_b) |db| allocator.free(db);
                    }
                } else {
                    // CID list entry (for have_check / have_response)
                    try cid_list.append(allocator, try allocator.dupe(u8, e.bytes));
                }
            },
            4 => {
                if (msg.cluster_secret) |old| allocator.free(old);
                msg.cluster_secret = try allocator.dupe(u8, e.bytes);
            },
            else => {},
        }
    }

    msg.block_entries = try block_list.toOwnedSlice(allocator);
    msg.cid_list = try cid_list.toOwnedSlice(allocator);
    return msg;
}

/// Send a varint-length-prefixed message on a yamux stream.
fn sendFramed(mux: *libp2p_dial.YamuxOverNoise, allocator: std.mem.Allocator, stream_id: u32, payload: []const u8) !void {
    var framed = std.ArrayList(u8).empty;
    defer framed.deinit(allocator);
    try varint.encodeU64(&framed, allocator, payload.len);
    try framed.appendSlice(allocator, payload);
    try mux.streamWrite(stream_id, framed.items);
}

/// Dial a cluster peer, negotiate the push protocol, and push blocks.
pub fn dialClusterPush(
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
    root_cid: []const u8,
    blocks: []const BlockEntry,
    cluster_secret: ?[]const u8,
    ed25519_secret64: [64]u8,
) !u32 {
    var conn = try libp2p_dial.dialNoiseYamuxClient(allocator, host, port, ed25519_secret64);
    conn.mux.sess = &conn.ns; // Fix: point to the returned copy, not the dead stack local
    defer conn.stream.close();
    defer conn.mux.deinit();

    try conn.mux.openClientStream(1);

    // Negotiate cluster push protocol on stream 1
    try conn.mux.streamWrite(1, "/multistream/1.0.0\n");
    const ms_resp = try conn.mux.streamReadLine(1);
    defer allocator.free(ms_resp);

    try conn.mux.streamWrite(1, proto_cluster_push);
    const proto_resp = try conn.mux.streamReadLine(1);
    defer allocator.free(proto_resp);
    if (!std.mem.startsWith(u8, proto_resp, "/zipfs/cluster/push/"))
        return error.UnexpectedMultistream;

    // Encode and send push message
    const msg = try encodePushBlocks(allocator, root_cid, blocks, cluster_secret);
    defer allocator.free(msg);
    try sendFramed(&conn.mux, allocator, 1, msg);

    // Read ACK
    const resp_pb = try conn.mux.readLengthPrefixedProtobuf(1);
    defer allocator.free(resp_pb);
    var decoded = try decodeMessage(allocator, resp_pb);
    defer decoded.deinit(allocator);

    if (decoded.msg_type != .push_ack) return error.UnexpectedResponse;
    return decoded.block_count;
}

/// Check which CIDs a peer already has.
pub fn dialClusterHaveCheck(
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
    cids: []const []const u8,
    cluster_secret: ?[]const u8,
    ed25519_secret64: [64]u8,
) ![][]u8 {
    var conn = try libp2p_dial.dialNoiseYamuxClient(allocator, host, port, ed25519_secret64);
    conn.mux.sess = &conn.ns; // Fix: point to the returned copy, not the dead stack local
    defer conn.stream.close();
    defer conn.mux.deinit();

    try conn.mux.openClientStream(1);
    try conn.mux.streamWrite(1, "/multistream/1.0.0\n");
    const ms_resp = try conn.mux.streamReadLine(1);
    defer allocator.free(ms_resp);
    try conn.mux.streamWrite(1, proto_cluster_push);
    const proto_resp = try conn.mux.streamReadLine(1);
    defer allocator.free(proto_resp);

    const msg = try encodeHaveCheck(allocator, cids, cluster_secret);
    defer allocator.free(msg);
    try sendFramed(&conn.mux, allocator, 1, msg);

    const resp_pb = try conn.mux.readLengthPrefixedProtobuf(1);
    defer allocator.free(resp_pb);
    var decoded = try decodeMessage(allocator, resp_pb);
    defer decoded.deinit(allocator);

    // Copy the CID list to owned memory
    var result = try allocator.alloc([]u8, decoded.cid_list.len);
    var written: usize = 0;
    errdefer {
        for (result[0..written]) |r| allocator.free(r);
        allocator.free(result);
    }
    for (decoded.cid_list, 0..) |c, i| {
        result[i] = try allocator.dupe(u8, c);
        written += 1;
    }
    return result;
}

/// Lightweight liveness check.
pub fn dialClusterPing(
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
    cluster_secret: ?[]const u8,
    ed25519_secret64: [64]u8,
) !bool {
    var conn = libp2p_dial.dialNoiseYamuxClient(allocator, host, port, ed25519_secret64) catch return false;
    conn.mux.sess = &conn.ns; // Fix: point to the returned copy, not the dead stack local
    defer conn.stream.close();
    defer conn.mux.deinit();

    conn.mux.openClientStream(1) catch return false;
    conn.mux.streamWrite(1, "/multistream/1.0.0\n") catch return false;
    const ms_resp = conn.mux.streamReadLine(1) catch return false;
    defer allocator.free(ms_resp);
    conn.mux.streamWrite(1, proto_cluster_push) catch return false;
    const proto_resp = conn.mux.streamReadLine(1) catch return false;
    defer allocator.free(proto_resp);

    const msg = encodePing(allocator, cluster_secret) catch return false;
    defer allocator.free(msg);
    sendFramed(&conn.mux, allocator, 1, msg) catch return false;

    const resp_pb = conn.mux.readLengthPrefixedProtobuf(1) catch return false;
    defer allocator.free(resp_pb);
    var decoded = decodeMessage(allocator, resp_pb) catch return false;
    defer decoded.deinit(allocator);
    return decoded.msg_type == .pong;
}

/// Parse host:port from a multiaddr string like `/ip4/1.2.3.4/tcp/4001`.
pub fn parseHostPort(allocator: std.mem.Allocator, addr: []const u8) !struct { host: []const u8, port: u16 } {
    const t = try multiaddr.parseStringTcp(allocator, addr);
    defer t.deinit(allocator);
    const host = try allocator.dupe(u8, t.host);
    return .{ .host = host, .port = t.port };
}
