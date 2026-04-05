//! Minimal protobuf wire helpers (varint + length-delimited fields).

const std = @import("std");

pub fn appendVarint(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, v: u64) !void {
    var x = v;
    while (x >= 0x80) {
        try buf.append(allocator, @truncate((x & 0x7f) | 0x80));
        x >>= 7;
    }
    try buf.append(allocator, @truncate(x));
}

pub fn readVarint(data: []const u8, i: *usize) error{Truncated}!u64 {
    var shift: u6 = 0;
    var v: u64 = 0;
    while (shift <= 63) : (shift += 7) {
        if (i.* >= data.len) return error.Truncated;
        const b = data[i.*];
        i.* += 1;
        v |= @as(u64, b & 0x7f) << shift;
        if ((b & 0x80) == 0) return v;
    }
    return error.Truncated;
}

pub fn appendBytesField(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, field_num: u32, payload: []const u8) !void {
    const tag = (field_num << 3) | 2;
    try appendVarint(buf, allocator, tag);
    try appendVarint(buf, allocator, payload.len);
    try buf.appendSlice(allocator, payload);
}

pub fn appendVarintField(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, field_num: u32, v: u64) !void {
    const tag = (field_num << 3) | 0;
    try appendVarint(buf, allocator, tag);
    try appendVarint(buf, allocator, v);
}

pub const FieldIter = struct {
    data: []const u8,
    pos: usize = 0,

    pub const Entry = struct {
        field_num: u32,
        wire_type: u3,
        /// For wire 0: numeric value. For wire 2: slice into data.
        bytes: []const u8,
        int_val: u64,
    };

    pub fn next(self: *FieldIter) error{Truncated}!?Entry {
        if (self.pos >= self.data.len) return null;
        const tag = try readVarint(self.data, &self.pos);
        const field_num: u32 = @truncate(tag >> 3);
        const wire_type: u3 = @truncate(tag & 7);
        switch (wire_type) {
            0 => {
                const v = try readVarint(self.data, &self.pos);
                return .{ .field_num = field_num, .wire_type = wire_type, .bytes = &.{}, .int_val = v };
            },
            2 => {
                const len = try readVarint(self.data, &self.pos);
                if (self.pos + len > self.data.len) return error.Truncated;
                const sl = self.data[self.pos .. self.pos + len];
                self.pos += len;
                return .{ .field_num = field_num, .wire_type = wire_type, .bytes = sl, .int_val = 0 };
            },
            else => return error.Truncated,
        }
    }
};
