//! CAR v1 read/write (IPLD). CAR v2: pragma + seek to data offset.

const std = @import("std");
const cid_mod = @import("cid.zig");
const Blockstore = @import("blockstore.zig").Blockstore;

pub const v2_pragma = [4]u8{ 0x0a, 0xed, 0x02, 0x0a };

fn writeLeU64(w: anytype, v: u64) !void {
    var x = v;
    var i: u8 = 0;
    while (i < 8) : (i += 1) {
        try w.writeByte(@truncate(x & 0xff));
        x >>= 8;
    }
}

fn readLeU64FromReader(r: std.io.AnyReader) !u64 {
    var buf: [8]u8 = undefined;
    const n = try r.readAll(&buf);
    if (n != 8) return error.UnexpectedEof;
    return std.mem.readInt(u64, &buf, .little);
}

pub fn writeUvarint(w: anytype, v: u64) !void {
    var x = v;
    while (x >= 0x80) {
        try w.writeByte(@truncate((x & 0x7f) | 0x80));
        x >>= 7;
    }
    try w.writeByte(@truncate(x));
}

pub fn readUvarint(r: std.io.AnyReader, max_bytes: usize) !u64 {
    var result: u64 = 0;
    var shift: u6 = 0;
    var i: usize = 0;
    while (i < max_bytes) : (i += 1) {
        var b: [1]u8 = undefined;
        const n = try r.readAll(&b);
        if (n != 1) return error.UnexpectedEof;
        result |= @as(u64, b[0] & 0x7f) << shift;
        if ((b[0] & 0x80) == 0) return result;
        shift += 7;
        if (shift > 63) return error.BadCar;
    }
    return error.BadCar;
}

fn encodeV1Header(allocator: std.mem.Allocator, root_cid_bytes: []const []const u8) ![]u8 {
    var buf = std.ArrayList(u8).empty;
    errdefer buf.deinit(allocator);
    try buf.append(allocator, 0xa2);
    try buf.append(allocator, 0x67);
    try buf.appendSlice(allocator, "version");
    try buf.append(allocator, 0x01);
    try buf.append(allocator, 0x65);
    try buf.appendSlice(allocator, "roots");
    const n = root_cid_bytes.len;
    if (n > 23) return error.TooManyRoots;
    try buf.append(allocator, @truncate(0x80 + n));
    for (root_cid_bytes) |rb| {
        if (rb.len < 24) {
            try buf.append(allocator, @truncate(0x40 + rb.len));
        } else if (rb.len < 256) {
            try buf.append(allocator, 0x58);
            try buf.append(allocator, @truncate(rb.len));
        } else return error.CidTooLong;
        try buf.appendSlice(allocator, rb);
    }
    return try buf.toOwnedSlice(allocator);
}

fn decodeV1Header(allocator: std.mem.Allocator, raw: []const u8) error{ BadCar, OutOfMemory }![][]u8 {
    var i: usize = 0;
    if (i >= raw.len or raw[i] != 0xa2) return error.BadCar;
    i += 1;
    if (i + 8 > raw.len or raw[i] != 0x67 or !std.mem.eql(u8, raw[i + 1 .. i + 8], "version")) return error.BadCar;
    i += 8;
    if (i >= raw.len or raw[i] != 0x01) return error.BadCar;
    i += 1;
    if (i + 6 > raw.len or raw[i] != 0x65 or !std.mem.eql(u8, raw[i + 1 .. i + 6], "roots")) return error.BadCar;
    i += 6;
    if (i >= raw.len) return error.BadCar;
    const arr_tag = raw[i];
    i += 1;
    const count: usize = if (arr_tag >= 0x80 and arr_tag <= 0x97)
        @as(usize, arr_tag - 0x80)
    else
        return error.BadCar;

    var roots = try allocator.alloc([]u8, count);
    errdefer {
        for (roots) |s| allocator.free(s);
        allocator.free(roots);
    }
    var r: usize = 0;
    while (r < count) : (r += 1) {
        if (i >= raw.len) return error.BadCar;
        const bt = raw[i];
        i += 1;
        const slen: usize = if (bt >= 0x40 and bt < 0x58)
            @as(usize, bt - 0x40)
        else if (bt == 0x58) blk: {
            if (i >= raw.len) return error.BadCar;
            const l = raw[i];
            i += 1;
            break :blk @as(usize, l);
        } else return error.BadCar;
        if (i + slen > raw.len) return error.BadCar;
        roots[r] = try allocator.dupe(u8, raw[i .. i + slen]);
        i += slen;
    }
    if (i != raw.len) return error.BadCar;
    return roots;
}

pub const Block = struct {
    cid_bytes: []const u8,
    data: []const u8,
};

pub fn writeCarV1(
    allocator: std.mem.Allocator,
    writer: anytype,
    root_strings: []const []const u8,
    blocks: []const Block,
) !void {
    var root_bin = std.ArrayList([]const u8).empty;
    defer {
        for (root_bin.items) |x| allocator.free(x);
        root_bin.deinit(allocator);
    }
    for (root_strings) |rs| {
        var c = try cid_mod.Cid.parse(allocator, rs);
        defer c.deinit(allocator);
        const b = try c.toBytes(allocator);
        defer allocator.free(b);
        try root_bin.append(allocator, try allocator.dupe(u8, b));
    }

    const hdr = try encodeV1Header(allocator, root_bin.items);
    defer allocator.free(hdr);

    try writeLeU64(writer, hdr.len);
    try writer.writeAll(hdr);
    for (blocks) |bl| {
        try writeUvarint(writer, bl.cid_bytes.len);
        try writer.writeAll(bl.cid_bytes);
        try writeUvarint(writer, bl.data.len);
        try writer.writeAll(bl.data);
    }
}

fn parseCarV2DataOffset(cbor: []const u8) ?u64 {
    const needle = "data_offset";
    var i: usize = 0;
    while (i + 1 + needle.len < cbor.len) : (i += 1) {
        if (cbor[i] != 0x6b) continue;
        if (!std.mem.eql(u8, cbor[i + 1 .. i + 12], needle)) continue;
        var j = i + 12;
        if (j >= cbor.len) return null;
        const tag = cbor[j];
        j += 1;
        if (tag <= 0x17) return tag;
        if (tag == 0x18 and j < cbor.len) return cbor[j];
        if (tag == 0x19 and j + 1 < cbor.len)
            return std.mem.readInt(u16, cbor[j .. j + 2][0..2], .big);
        if (tag == 0x1a and j + 3 < cbor.len)
            return std.mem.readInt(u32, cbor[j .. j + 4][0..4], .big);
        if (tag == 0x1b and j + 7 < cbor.len)
            return std.mem.readInt(u64, cbor[j .. j + 8][0..8], .big);
    }
    return null;
}

/// Read CAR from a **seekable** file (v1 or v2). Imports blocks into `store`.
pub fn importFromSeekableFile(allocator: std.mem.Allocator, file: std.fs.File, store: *Blockstore) !void {
    var scratch: [8192]u8 = undefined;
    try file.seekTo(0);
    var r = file.reader(&scratch);
    const ar = std.Io.Reader.adaptToOldInterface(&r.interface);

    var pragma: [4]u8 = undefined;
    const pn = try ar.readAll(&pragma);
    if (pn != 4) return error.UnexpectedEof;

    if (std.mem.eql(u8, &pragma, &v2_pragma)) {
        const h2len = try readLeU64FromReader(ar);
        const h2 = try allocator.alloc(u8, @intCast(h2len));
        defer allocator.free(h2);
        const hn = try ar.readAll(h2);
        if (hn != h2.len) return error.UnexpectedEof;
        const data_off = parseCarV2DataOffset(h2) orelse return error.BadCar;
        try file.seekTo(data_off);
        var scratch2: [8192]u8 = undefined;
        var r2 = file.reader(&scratch2);
        try readCarV1BodyAfterHeaderLen(allocator, std.Io.Reader.adaptToOldInterface(&r2.interface), store);
    } else {
        try file.seekTo(0);
        var scratch0: [8192]u8 = undefined;
        var r0 = file.reader(&scratch0);
        try readCarV1BodyAfterHeaderLen(allocator, std.Io.Reader.adaptToOldInterface(&r0.interface), store);
    }
}

/// Export every block in `store` to a CAR v1 file (roots array empty).
pub fn exportStoreToFile(allocator: std.mem.Allocator, store: *const Blockstore, out_path: []const u8) !void {
    var list: std.ArrayList(Block) = .empty;
    defer {
        for (list.items) |b| {
            allocator.free(b.cid_bytes);
            allocator.free(b.data);
        }
        list.deinit(allocator);
    }

    const Ctx = struct {
        list: *std.ArrayList(Block),
        allocator: std.mem.Allocator,
        fn collect(ctx: *anyopaque, key: []const u8, val: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            var c = try cid_mod.Cid.parse(self.allocator, key);
            defer c.deinit(self.allocator);
            const cb = try c.toBytes(self.allocator);
            const data = try self.allocator.dupe(u8, val);
            try self.list.append(self.allocator, .{ .cid_bytes = cb, .data = data });
        }
    };
    var ctx = Ctx{ .list = &list, .allocator = allocator };
    try store.each(@ptrCast(&ctx), Ctx.collect);

    var file = try std.fs.cwd().createFile(out_path, .{ .truncate = true });
    defer file.close();
    var wbuf: [4096]u8 = undefined;
    var fw = file.writer(&wbuf);
    const writer = &fw.interface;
    const root_strings: []const []const u8 = &.{};
    try writeCarV1(allocator, writer, root_strings, list.items);
    try writer.flush();
}

fn readCarV1BodyAfterHeaderLen(allocator: std.mem.Allocator, r: std.io.AnyReader, store: *Blockstore) !void {
    const hdr_len = try readLeU64FromReader(r);
    if (hdr_len > 1 << 24) return error.BadCar;
    const hdr = try allocator.alloc(u8, @intCast(hdr_len));
    defer allocator.free(hdr);
    const hn = try r.readAll(hdr);
    if (hn != hdr.len) return error.UnexpectedEof;
    const roots_dec = try decodeV1Header(allocator, hdr);
    defer {
        for (roots_dec) |s| allocator.free(s);
        allocator.free(roots_dec);
    }

    while (true) {
        const cid_len = readUvarint(r, 10) catch |err| switch (err) {
            error.UnexpectedEof => return,
            else => |e| return e,
        };
        if (cid_len > 1 << 20) return error.BadCar;
        const cid_b = try allocator.alloc(u8, @intCast(cid_len));
        defer allocator.free(cid_b);
        const cn = try r.readAll(cid_b);
        if (cn != cid_b.len) return error.UnexpectedEof;

        const dlen = try readUvarint(r, 10);
        if (dlen > 1 << 30) return error.BadCar;
        const data = try allocator.alloc(u8, @intCast(dlen));
        defer allocator.free(data);
        const dn = try r.readAll(data);
        if (dn != data.len) return error.UnexpectedEof;

        var c = try cid_mod.Cid.fromBytes(allocator, cid_b);
        defer c.deinit(allocator);
        try store.put(allocator, c, data);
    }
}
