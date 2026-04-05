//! Bitswap `Message` protobuf subset (empty / wantlist hooks).

const std = @import("std");

/// Placeholder full message encode: empty protobuf document.
pub fn encodeEmptyMessage(allocator: std.mem.Allocator) ![]u8 {
    return try allocator.dupe(u8, "");
}

pub const BlockEntry = struct {
    cid_prefix: []const u8,
    data: []const u8,
};

/// Minimal non-empty message: repeated Block (field 2) with raw CID bytes + data.
pub fn encodeBlocks(allocator: std.mem.Allocator, blocks: []const BlockEntry) ![]u8 {
    var buf = std.ArrayList(u8).empty;
    errdefer buf.deinit(allocator);
    for (blocks) |b| {
        var inner = std.ArrayList(u8).empty;
        defer inner.deinit(allocator);
        try inner.append(allocator, 0x0a);
        try appendVarint(&inner, allocator, b.cid_prefix.len);
        try inner.appendSlice(allocator, b.cid_prefix);
        try inner.append(allocator, 0x12);
        try appendVarint(&inner, allocator, b.data.len);
        try inner.appendSlice(allocator, b.data);
        const enc = try inner.toOwnedSlice(allocator);
        defer allocator.free(enc);
        try buf.append(allocator, 0x12);
        try appendVarint(&buf, allocator, enc.len);
        try buf.appendSlice(allocator, enc);
    }
    return try buf.toOwnedSlice(allocator);
}

fn appendVarint(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, v: u64) !void {
    var x = v;
    while (x >= 0x80) {
        try buf.append(allocator, @truncate((x & 0x7f) | 0x80));
        x >>= 7;
    }
    try buf.append(allocator, @truncate(x));
}
