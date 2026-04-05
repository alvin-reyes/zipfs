//! libp2p Identify `identify.pb.Identify` (proto2 wire, go-libp2p compatible subset).

const std = @import("std");
const wireproto = @import("../wireproto.zig");

pub const default_protocols = [_][]const u8{
    "/multistream/1.0.0",
    "/noise",
    "/yamux/1.0.0",
    "/ipfs/id/1.0.0",
    "/ipfs/id/push/1.0.0",
    "/ipfs/bitswap/1.2.0",
    "/ipfs/kad/1.0.0",
};

pub fn encodeIdentify(
    allocator: std.mem.Allocator,
    public_key_proto: []const u8,
    listen_addrs_bin: []const []const u8,
    protocols: []const []const u8,
) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    try wireproto.appendBytesField(&buf, allocator, 1, public_key_proto);
    for (listen_addrs_bin) |a| {
        try wireproto.appendBytesField(&buf, allocator, 2, a);
    }
    for (protocols) |p| {
        try wireproto.appendBytesField(&buf, allocator, 3, p);
    }
    try wireproto.appendBytesField(&buf, allocator, 5, "ipfs/0.1.0");
    try wireproto.appendBytesField(&buf, allocator, 6, "zig-ipfs/0.2.0");
    return try buf.toOwnedSlice(allocator);
}

pub const IdentifyInfo = struct {
    agent_version: ?[]u8 = null,
    protocol_version: ?[]u8 = null,
    protocols: [][]u8 = &.{},

    pub fn deinit(self: *IdentifyInfo, allocator: std.mem.Allocator) void {
        if (self.agent_version) |s| allocator.free(s);
        if (self.protocol_version) |s| allocator.free(s);
        for (self.protocols) |p| allocator.free(p);
        if (self.protocols.len != 0) allocator.free(self.protocols);
        self.* = .{};
    }
};

/// Decode `identify.pb.Identify` subset (fields 3, 5, 6); strings are owned.
pub fn decodeIdentify(allocator: std.mem.Allocator, pb: []const u8) !IdentifyInfo {
    var info: IdentifyInfo = .{};
    errdefer info.deinit(allocator);
    var protos = std.ArrayList([]u8).empty;
    errdefer {
        for (protos.items) |p| allocator.free(p);
        protos.deinit(allocator);
    }
    var it = wireproto.FieldIter{ .data = pb };
    while (try it.next()) |e| switch (e.field_num) {
        3 => {
            const s = try allocator.dupe(u8, e.bytes);
            errdefer allocator.free(s);
            try protos.append(allocator, s);
        },
        5 => {
            if (info.protocol_version) |old| allocator.free(old);
            info.protocol_version = try allocator.dupe(u8, e.bytes);
        },
        6 => {
            if (info.agent_version) |old| allocator.free(old);
            info.agent_version = try allocator.dupe(u8, e.bytes);
        },
        else => {},
    };
    info.protocols = try protos.toOwnedSlice(allocator);
    const out = info;
    info = .{};
    return out;
}
