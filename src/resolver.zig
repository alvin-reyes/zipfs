//! Read UnixFS file bytes from the local blockstore (dag-pb + raw leaves).

const std = @import("std");
const cid_mod = @import("cid.zig");
const Blockstore = @import("blockstore.zig").Blockstore;

const Cid = cid_mod.Cid;
const codec_raw = cid_mod.codec_raw;
const codec_dag_pb = cid_mod.codec_dag_pb;

fn readVarint(slice: []const u8, i: *usize) error{Truncated}!u64 {
    if (i.* >= slice.len) return error.Truncated;
    var result: u64 = 0;
    var shift: u6 = 0;
    while (true) {
        const b = slice[i.*];
        i.* += 1;
        result |= @as(u64, b & 0x7f) << shift;
        if ((b & 0x80) == 0) break;
        shift += 7;
        if (shift > 63 or i.* >= slice.len) return error.Truncated;
    }
    return result;
}

fn readBytes(slice: []const u8, i: *usize) error{Truncated}![]const u8 {
    const len = try readVarint(slice, i);
    const n: usize = @intCast(len);
    if (i.* + n > slice.len) return error.Truncated;
    const r = slice[i.* .. i.* + n];
    i.* += n;
    return r;
}

const ParsedLink = struct {
    hash: []const u8,
    name: []const u8,
    tsize: u64,
};

fn parseLink(msg: []const u8) error{Truncated}!ParsedLink {
    var i: usize = 0;
    var hash: ?[]const u8 = null;
    var name: []const u8 = "";
    var tsize: u64 = 0;
    while (i < msg.len) {
        const tag = try readVarint(msg, &i);
        const field = tag >> 3;
        const wire = tag & 7;
        switch (field) {
            1 => {
                if (wire != 2) return error.Truncated;
                hash = try readBytes(msg, &i);
            },
            2 => {
                if (wire != 2) return error.Truncated;
                name = try readBytes(msg, &i);
            },
            3 => {
                if (wire != 0) return error.Truncated;
                tsize = try readVarint(msg, &i);
            },
            else => {
                switch (wire) {
                    0 => _ = try readVarint(msg, &i),
                    2 => {
                        const skip_len = try readVarint(msg, &i);
                        const sn: usize = @intCast(skip_len);
                        if (i + sn > msg.len) return error.Truncated;
                        i += sn;
                    },
                    else => return error.Truncated,
                }
            },
        }
    }
    return .{ .hash = hash orelse return error.Truncated, .name = name, .tsize = tsize };
}

const ParsedUnixFs = struct {
    typ: u64,
    data: []const u8,
};

fn parseUnixFs(msg: []const u8) error{Truncated}!ParsedUnixFs {
    var i: usize = 0;
    var typ: u64 = 0;
    var data: []const u8 = &.{};
    while (i < msg.len) {
        const tag = try readVarint(msg, &i);
        const field = tag >> 3;
        const wire = tag & 7;
        switch (field) {
            1 => {
                if (wire != 0) return error.Truncated;
                typ = try readVarint(msg, &i);
            },
            2 => {
                if (wire != 2) return error.Truncated;
                data = try readBytes(msg, &i);
            },
            else => {
                switch (wire) {
                    0 => _ = try readVarint(msg, &i),
                    2 => {
                        const skip_len = try readVarint(msg, &i);
                        const sn: usize = @intCast(skip_len);
                        if (i + sn > msg.len) return error.Truncated;
                        i += sn;
                    },
                    else => return error.Truncated,
                }
            },
        }
    }
    return .{ .typ = typ, .data = data };
}

/// UnixFS file type enum value (proto).
const unixfs_file: u64 = 2;
const unixfs_directory: u64 = 1;
const unixfs_hamt_shard: u64 = 5;

pub const DirEntry = struct {
    name: []const u8,
    /// Child CID string (owned by `DirList`).
    cid_str: []const u8,
    size: u64,
};

pub const DirList = struct {
    entries: []DirEntry,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *DirList) void {
        for (self.entries) |e| {
            self.allocator.free(e.cid_str);
        }
        self.allocator.free(self.entries);
        self.entries = &.{};
    }
};

fn pathSegments(allocator: std.mem.Allocator, path: []const u8) error{ OutOfMemory, BadPath }![][]const u8 {
    var it = std.mem.splitScalar(u8, path, '/');
    var list = std.ArrayList([]const u8).empty;
    errdefer list.deinit(allocator);
    while (it.next()) |p| {
        if (p.len == 0) continue;
        if (std.mem.eql(u8, p, ".")) continue;
        if (std.mem.eql(u8, p, "..")) return error.BadPath;
        try list.append(allocator, p);
    }
    return try list.toOwnedSlice(allocator);
}

/// Walk `root_key` + UnixFS path; returns owned CID string for the target node.
pub fn resolvePathToKey(allocator: std.mem.Allocator, store: *Blockstore, root_key_utf8: []const u8, path: []const u8) error{ OutOfMemory, NotFound, BadBlock, BadPath, NotADirectory, UnsupportedHamt }![]u8 {
    const segs = try pathSegments(allocator, path);
    defer allocator.free(segs);

    var cur: []u8 = try allocator.dupe(u8, root_key_utf8);
    errdefer allocator.free(cur);

    for (segs) |seg| {
        const block = store.get(allocator, cur) orelse return error.NotFound;
        defer allocator.free(block);
        var cidn = Cid.parse(allocator, cur) catch return error.BadBlock;
        defer cidn.deinit(allocator);
        if (cidn.codec != codec_dag_pb) return error.BadBlock;

        const parsed = parseDagPbNode(allocator, block) catch return error.BadBlock;
        defer allocator.free(parsed.links);
        const um = parsed.ufs_msg orelse return error.BadBlock;
        const ufs = parseUnixFs(um) catch return error.BadBlock;
        if (ufs.typ == unixfs_hamt_shard) return error.UnsupportedHamt;
        if (ufs.typ != unixfs_directory) return error.NotADirectory;

        var found: ?[]const u8 = null;
        for (parsed.links) |lnk| {
            if (std.mem.eql(u8, lnk.name, seg)) {
                const child = Cid.fromBytes(allocator, lnk.hash) catch return error.BadBlock;
                defer child.deinit(allocator);
                found = try child.toString(allocator);
                break;
            }
        }
        const next = found orelse return error.NotFound;
        allocator.free(cur);
        cur = try allocator.dupe(u8, next);
        allocator.free(next);
    }
    return cur;
}

pub fn catFileAtPath(allocator: std.mem.Allocator, store: *Blockstore, root_key_utf8: []const u8, path: []const u8) error{ OutOfMemory, NotFound, BadBlock, BadPath, NotADirectory, UnsupportedHamt }![]u8 {
    const key = try resolvePathToKey(allocator, store, root_key_utf8, path);
    defer allocator.free(key);
    return try catFile(allocator, store, key);
}

pub fn listDirAtPath(allocator: std.mem.Allocator, store: *Blockstore, root_key_utf8: []const u8, path: []const u8) error{ OutOfMemory, NotFound, BadBlock, BadPath, NotADirectory, UnsupportedHamt }!DirList {
    const key = try resolvePathToKey(allocator, store, root_key_utf8, path);
    defer allocator.free(key);

    const block = store.get(allocator, key) orelse return error.NotFound;
    defer allocator.free(block);
    var cidn = Cid.parse(allocator, key) catch return error.BadBlock;
    defer cidn.deinit(allocator);
    if (cidn.codec != codec_dag_pb) return error.BadBlock;

    const parsed = parseDagPbNode(allocator, block) catch return error.BadBlock;
    defer allocator.free(parsed.links);
    const um = parsed.ufs_msg orelse return error.BadBlock;
    const ufs = parseUnixFs(um) catch return error.BadBlock;
    if (ufs.typ == unixfs_hamt_shard) return error.UnsupportedHamt;
    if (ufs.typ != unixfs_directory) return error.NotADirectory;

    var out = try allocator.alloc(DirEntry, parsed.links.len);
    errdefer {
        for (out) |e| allocator.free(e.cid_str);
        allocator.free(out);
    }
    var n: usize = 0;
    for (parsed.links) |lnk| {
        const child = Cid.fromBytes(allocator, lnk.hash) catch return error.BadBlock;
        defer child.deinit(allocator);
        const cs = try child.toString(allocator);
        out[n] = .{ .name = lnk.name, .cid_str = cs, .size = lnk.tsize };
        n += 1;
    }
    return .{ .entries = out, .allocator = allocator };
}

fn parseDagPbNode(allocator: std.mem.Allocator, block: []const u8) error{ Truncated, OutOfMemory }!struct {
    ufs_msg: ?[]const u8,
    links: []ParsedLink,
} {
    var ufs_msg: ?[]const u8 = null;
    var links = std.ArrayList(ParsedLink).empty;
    errdefer links.deinit(allocator);

    var i: usize = 0;
    while (i < block.len) {
        const tag = try readVarint(block, &i);
        const field = tag >> 3;
        const wire = tag & 7;
        switch (field) {
            1 => {
                if (wire != 2) return error.Truncated;
                ufs_msg = try readBytes(block, &i);
            },
            2 => {
                if (wire != 2) return error.Truncated;
                const enc = try readBytes(block, &i);
                const lnk = try parseLink(enc);
                try links.append(allocator, lnk);
            },
            else => {
                switch (wire) {
                    0 => _ = try readVarint(block, &i),
                    2 => {
                        const skip_len = try readVarint(block, &i);
                        const sn: usize = @intCast(skip_len);
                        if (i + sn > block.len) return error.Truncated;
                        i += sn;
                    },
                    else => return error.Truncated,
                }
            },
        }
    }
    return .{ .ufs_msg = ufs_msg, .links = try links.toOwnedSlice(allocator) };
}

fn catInto(allocator: std.mem.Allocator, store: *Blockstore, key_utf8: []const u8, out: *std.ArrayList(u8)) error{ OutOfMemory, NotFound, BadBlock }!void {
    const block = store.get(allocator, key_utf8) orelse return error.NotFound;
    defer allocator.free(block);

    var root = Cid.parse(allocator, key_utf8) catch return error.BadBlock;
    defer root.deinit(allocator);

    if (root.codec == codec_raw) {
        try out.appendSlice(allocator, block);
        return;
    }
    if (root.codec != codec_dag_pb) return error.BadBlock;

    const parsed = parseDagPbNode(allocator, block) catch return error.BadBlock;
    defer allocator.free(parsed.links);

    const um = parsed.ufs_msg orelse return error.BadBlock;
    const ufs = parseUnixFs(um) catch return error.BadBlock;

    if (ufs.typ != unixfs_file) return error.BadBlock;

    if (ufs.data.len > 0) {
        try out.appendSlice(allocator, ufs.data);
        return;
    }

    for (parsed.links) |lnk| {
        const child = Cid.fromBytes(allocator, lnk.hash) catch return error.BadBlock;
        defer child.deinit(allocator);
        const ck = child.toString(allocator) catch return error.BadBlock;
        defer allocator.free(ck);
        try catInto(allocator, store, ck, out);
    }
}

/// Owned list of child CID strings referenced by a dag-pb block (empty for raw / non-dag).
pub fn dagChildKeys(allocator: std.mem.Allocator, store: *Blockstore, key_utf8: []const u8) error{ OutOfMemory, NotFound, BadBlock }![][]u8 {
    const block = store.get(allocator, key_utf8) orelse return error.NotFound;
    defer allocator.free(block);
    var root = Cid.parse(allocator, key_utf8) catch return error.BadBlock;
    defer root.deinit(allocator);
    if (root.codec == codec_raw) {
        const empty = try allocator.alloc([]u8, 0);
        return empty;
    }
    if (root.codec != codec_dag_pb) return error.BadBlock;
    const parsed = parseDagPbNode(allocator, block) catch return error.BadBlock;
    defer allocator.free(parsed.links);
    var out_arr = try allocator.alloc([]u8, parsed.links.len);
    errdefer {
        for (out_arr) |s| allocator.free(s);
        allocator.free(out_arr);
    }
    for (parsed.links, 0..) |lnk, i| {
        const child = Cid.fromBytes(allocator, lnk.hash) catch return error.BadBlock;
        defer child.deinit(allocator);
        out_arr[i] = try child.toString(allocator);
    }
    return out_arr;
}

/// Returns owned slice of file payload.
pub fn catFile(allocator: std.mem.Allocator, store: *Blockstore, root_key_utf8: []const u8) error{ OutOfMemory, NotFound, BadBlock }![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    try catInto(allocator, store, root_key_utf8, &buf);
    return try buf.toOwnedSlice(allocator);
}
