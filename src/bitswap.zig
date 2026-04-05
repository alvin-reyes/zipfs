//! Bitswap `Message` protobuf (boxo `message.proto`) — Kubo-compatible wire format.

const std = @import("std");
const wireproto = @import("wireproto.zig");
const cid_mod = @import("cid.zig");

pub const WantType = enum(u32) {
    block = 0,
    have = 1,
};

pub const BlockPresenceType = enum(u32) {
    have = 0,
    dont_have = 1,
};

pub const WantlistEntry = struct {
    block: []const u8,
    priority: i32 = 1,
    cancel: bool = false,
    want_type: WantType = .block,
    send_dont_have: bool = false,
};

pub const Wantlist = struct {
    entries: []const WantlistEntry = &.{},
    full: bool = false,
};

pub const BlockPayload = struct {
    prefix: []const u8,
    data: []const u8,
};

pub const BlockPresence = struct {
    cid: []const u8,
    typ: BlockPresenceType,
};

pub const Message = struct {
    wantlist: ?Wantlist = null,
    /// Deprecated bitswap 1.0.0 raw block bytes (field 2).
    legacy_blocks: []const []const u8 = &.{},
    payload: []const BlockPayload = &.{},
    block_presences: []const BlockPresence = &.{},
    pending_bytes: i32 = 0,

    pub fn encode(self: Message, allocator: std.mem.Allocator) ![]u8 {
        var buf = std.ArrayList(u8).empty;
        errdefer buf.deinit(allocator);
        if (self.wantlist) |w| {
            var inner = std.ArrayList(u8).empty;
            defer inner.deinit(allocator);
            for (w.entries) |ent| {
                var entb = std.ArrayList(u8).empty;
                defer entb.deinit(allocator);
                try wireproto.appendBytesField(&entb, allocator, 1, ent.block);
                try wireproto.appendVarintField(&entb, allocator, 2, @as(u64, @bitCast(@as(i64, ent.priority))));
                if (ent.cancel)
                    try wireproto.appendVarintField(&entb, allocator, 3, 1);
                try wireproto.appendVarintField(&entb, allocator, 4, @intFromEnum(ent.want_type));
                if (ent.send_dont_have)
                    try wireproto.appendVarintField(&entb, allocator, 5, 1);
                const eslice = try entb.toOwnedSlice(allocator);
                defer allocator.free(eslice);
                try wireproto.appendBytesField(&inner, allocator, 1, eslice);
            }
            if (w.full)
                try wireproto.appendVarintField(&inner, allocator, 2, 1);
            const wslice = try inner.toOwnedSlice(allocator);
            defer allocator.free(wslice);
            try wireproto.appendBytesField(&buf, allocator, 1, wslice);
        }
        for (self.legacy_blocks) |b| {
            try wireproto.appendBytesField(&buf, allocator, 2, b);
        }
        for (self.payload) |bl| {
            var inner = std.ArrayList(u8).empty;
            defer inner.deinit(allocator);
            try wireproto.appendBytesField(&inner, allocator, 1, bl.prefix);
            try wireproto.appendBytesField(&inner, allocator, 2, bl.data);
            const p = try inner.toOwnedSlice(allocator);
            defer allocator.free(p);
            try wireproto.appendBytesField(&buf, allocator, 3, p);
        }
        for (self.block_presences) |bp| {
            var inner = std.ArrayList(u8).empty;
            defer inner.deinit(allocator);
            try wireproto.appendBytesField(&inner, allocator, 1, bp.cid);
            try wireproto.appendVarintField(&inner, allocator, 2, @intFromEnum(bp.typ));
            const p = try inner.toOwnedSlice(allocator);
            defer allocator.free(p);
            try wireproto.appendBytesField(&buf, allocator, 4, p);
        }
        if (self.pending_bytes != 0)
            try wireproto.appendVarintField(&buf, allocator, 5, @as(u64, @bitCast(@as(i64, self.pending_bytes))));
        return try buf.toOwnedSlice(allocator);
    }
};

/// Empty protobuf document (valid `Message`).
pub fn encodeEmptyMessage(allocator: std.mem.Allocator) ![]u8 {
    return try allocator.dupe(u8, "");
}

/// Varint-framed bitswap `Message` with wantlist (optional HAVE + BLOCK for the same `cid_bytes`).
pub fn encodeFramedWant(allocator: std.mem.Allocator, cid_bytes: []const u8, include_have: bool) ![]u8 {
    const varint = @import("varint.zig");
    var entries: [2]WantlistEntry = undefined;
    var n: usize = 0;
    if (include_have) {
        entries[n] = .{ .block = cid_bytes, .want_type = .have, .send_dont_have = true, .priority = 1 };
        n += 1;
    }
    entries[n] = .{ .block = cid_bytes, .want_type = .block, .send_dont_have = true, .priority = 1 };
    n += 1;
    const wl = Wantlist{ .entries = entries[0..n], .full = false };
    const msg = Message{ .wantlist = wl };
    const pb = try msg.encode(allocator);
    defer allocator.free(pb);
    var framed = std.ArrayList(u8).empty;
    defer framed.deinit(allocator);
    try varint.encodeU64(&framed, allocator, pb.len);
    try framed.appendSlice(allocator, pb);
    return try framed.toOwnedSlice(allocator);
}

pub fn blockPresencesHaveCid(pres: []const BlockPresence, cid_bytes: []const u8) bool {
    for (pres) |p| {
        if (p.typ == .have and std.mem.eql(u8, p.cid, cid_bytes)) return true;
    }
    return false;
}

pub const BlockEntry = struct {
    cid_prefix: []const u8,
    data: []const u8,
};

/// First `payload` block's `data` field (inner protobuf field 2), if any.
pub fn decodeFirstPayloadData(msg_pb: []const u8, allocator: std.mem.Allocator) !?[]u8 {
    var it = wireproto.FieldIter{ .data = msg_pb };
    while (try it.next()) |e| {
        if (e.field_num != 3) continue;
        var inner = wireproto.FieldIter{ .data = e.bytes };
        while (try inner.next()) |ie| {
            if (ie.field_num == 2)
                return try allocator.dupe(u8, ie.bytes);
        }
    }
    return null;
}

/// Encode bitswap 1.1.0 `payload` blocks (field 3), Kubo-compatible.
pub fn encodeBlocks(allocator: std.mem.Allocator, blocks: []const BlockEntry) ![]u8 {
    var pl = try allocator.alloc(BlockPayload, blocks.len);
    defer allocator.free(pl);
    for (blocks, 0..) |b, i| {
        pl[i] = .{ .prefix = b.cid_prefix, .data = b.data };
    }
    const msg = Message{ .payload = pl };
    return try msg.encode(allocator);
}

pub const WantlistItem = struct {
    store_key: []u8,
    want_type: WantType,
    send_dont_have: bool,

    pub fn deinit(self: *WantlistItem, allocator: std.mem.Allocator) void {
        allocator.free(self.store_key);
        self.* = undefined;
    }
};

/// Decode wantlist entries (field 1); each `store_key` is owned (UTF-8 CID string for the blockstore).
pub fn decodeWantlistItems(msg_pb: []const u8, allocator: std.mem.Allocator) ![]WantlistItem {
    var items = std.ArrayList(WantlistItem).empty;
    errdefer {
        for (items.items) |*it| it.deinit(allocator);
        items.deinit(allocator);
    }
    var it = wireproto.FieldIter{ .data = msg_pb };
    while (try it.next()) |e| {
        if (e.field_num != 1) continue;
        var it2 = wireproto.FieldIter{ .data = e.bytes };
        while (try it2.next()) |e2| {
            if (e2.field_num != 1) continue;
            var cid_bytes: ?[]const u8 = null;
            var wt: WantType = .block;
            var sdh: bool = false;
            var it3 = wireproto.FieldIter{ .data = e2.bytes };
            while (try it3.next()) |e3| switch (e3.field_num) {
                1 => cid_bytes = e3.bytes,
                4 => wt = @enumFromInt(@as(u32, @truncate(e3.int_val))),
                5 => sdh = e3.int_val != 0,
                else => {},
            };
            const cb = cid_bytes orelse continue;
            var c = try cid_mod.Cid.fromBytes(allocator, cb);
            defer c.deinit(allocator);
            const ks = try c.toString(allocator);
            try items.append(allocator, .{ .store_key = ks, .want_type = wt, .send_dont_have = sdh });
        }
    }
    return try items.toOwnedSlice(allocator);
}

/// Collect CID UTF-8 keys (for the blockstore) from a bitswap `Message` wantlist (field 1).
pub fn decodeWantlistStoreKeys(msg_pb: []const u8, allocator: std.mem.Allocator) ![][]const u8 {
    const items = try decodeWantlistItems(msg_pb, allocator);
    defer {
        for (items) |*it| it.deinit(allocator);
        allocator.free(items);
    }
    var keys = std.ArrayList([]const u8).empty;
    errdefer {
        for (keys.items) |k| allocator.free(k);
        keys.deinit(allocator);
    }
    for (items) |it| {
        const k = try allocator.dupe(u8, it.store_key);
        try keys.append(allocator, k);
    }
    return try keys.toOwnedSlice(allocator);
}

/// Decode `block_presences` (field 4) from a bitswap message; each `cid` is owned bytes (CID wire form).
pub fn decodeBlockPresences(msg_pb: []const u8, allocator: std.mem.Allocator) ![]BlockPresence {
    var out = std.ArrayList(BlockPresence).empty;
    errdefer {
        for (out.items) |bp| allocator.free(bp.cid);
        out.deinit(allocator);
    }
    var it = wireproto.FieldIter{ .data = msg_pb };
    while (try it.next()) |e| {
        if (e.field_num != 4) continue;
        var cid_b: ?[]const u8 = null;
        var typ: BlockPresenceType = .have;
        var inner = wireproto.FieldIter{ .data = e.bytes };
        while (try inner.next()) |ie| switch (ie.field_num) {
            1 => cid_b = ie.bytes,
            2 => typ = @enumFromInt(@as(u32, @truncate(ie.int_val))),
            else => {},
        };
        const c = cid_b orelse continue;
        const owned = try allocator.dupe(u8, c);
        errdefer allocator.free(owned);
        try out.append(allocator, .{ .cid = owned, .typ = typ });
    }
    return try out.toOwnedSlice(allocator);
}
