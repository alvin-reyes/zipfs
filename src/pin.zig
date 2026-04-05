//! Direct / recursive pins and mark-sweep GC vs blockstore + disk.

const std = @import("std");
const Blockstore = @import("blockstore.zig").Blockstore;
const resolver = @import("resolver.zig");
const repo = @import("repo.zig");

pub const PinSet = struct {
    direct: std.StringHashMapUnmanaged(void) = .empty,
    recursive: std.StringHashMapUnmanaged(void) = .empty,

    pub fn deinit(self: *PinSet, allocator: std.mem.Allocator) void {
        var it = self.direct.keyIterator();
        while (it.next()) |k| allocator.free(k.*);
        self.direct.deinit(allocator);
        var it2 = self.recursive.keyIterator();
        while (it2.next()) |k| allocator.free(k.*);
        self.recursive.deinit(allocator);
        self.* = .{};
    }

    pub fn load(allocator: std.mem.Allocator, repo_root: []const u8) !PinSet {
        const path = try std.fs.path.join(allocator, &.{ repo_root, "pins.json" });
        defer allocator.free(path);
        const data = std.fs.cwd().readFileAlloc(allocator, path, 1 << 20) catch |err| switch (err) {
            error.FileNotFound => return .{},
            else => |e| return e,
        };
        defer allocator.free(data);

        const J = struct {
            direct: ?[][]const u8 = null,
            recursive: ?[][]const u8 = null,
        };
        var p = try std.json.parseFromSlice(J, allocator, data, .{ .allocate = .alloc_always });
        defer p.deinit();

        var ps: PinSet = .{};
        errdefer ps.deinit(allocator);

        if (p.value.direct) |arr| {
            for (arr) |s| {
                const k = try allocator.dupe(u8, s);
                errdefer allocator.free(k);
                try ps.direct.put(allocator, k, {});
            }
        }
        if (p.value.recursive) |arr| {
            for (arr) |s| {
                const k = try allocator.dupe(u8, s);
                errdefer allocator.free(k);
                try ps.recursive.put(allocator, k, {});
            }
        }
        return ps;
    }

    pub fn save(self: *const PinSet, allocator: std.mem.Allocator, repo_root: []const u8) !void {
        var d = std.ArrayList([]const u8).empty;
        defer d.deinit(allocator);
        var it = self.direct.keyIterator();
        while (it.next()) |k| try d.append(allocator, k.*);

        var r = std.ArrayList([]const u8).empty;
        defer r.deinit(allocator);
        var it2 = self.recursive.keyIterator();
        while (it2.next()) |k| try r.append(allocator, k.*);

        var buf = std.ArrayList(u8).empty;
        defer buf.deinit(allocator);
        const Out = struct { direct: [][]const u8, recursive: [][]const u8 };
        var bw = buf.writer(allocator);
        try bw.print("{f}", .{std.json.fmt(Out{
            .direct = d.items,
            .recursive = r.items,
        }, .{ .whitespace = .indent_2 })});
        try buf.append(allocator, '\n');
        const path = try std.fs.path.join(allocator, &.{ repo_root, "pins.json" });
        defer allocator.free(path);
        try std.fs.cwd().makePath(repo_root);
        try std.fs.cwd().writeFile(.{ .sub_path = path, .data = buf.items });
    }

    pub fn pinDirect(self: *PinSet, allocator: std.mem.Allocator, cid_str: []const u8) !void {
        const k = try allocator.dupe(u8, cid_str);
        errdefer allocator.free(k);
        const gop = try self.direct.getOrPut(allocator, k);
        if (gop.found_existing) allocator.free(k);
    }

    pub fn pinRecursive(self: *PinSet, allocator: std.mem.Allocator, cid_str: []const u8) !void {
        const k = try allocator.dupe(u8, cid_str);
        errdefer allocator.free(k);
        const gop = try self.recursive.getOrPut(allocator, k);
        if (gop.found_existing) allocator.free(k);
    }

    pub fn unpinDirect(self: *PinSet, allocator: std.mem.Allocator, cid_str: []const u8) void {
        if (self.direct.fetchRemove(cid_str)) |kv| allocator.free(kv.key);
    }

    pub fn unpinRecursive(self: *PinSet, allocator: std.mem.Allocator, cid_str: []const u8) void {
        if (self.recursive.fetchRemove(cid_str)) |kv| allocator.free(kv.key);
    }
};

fn markRecursive(allocator: std.mem.Allocator, store: *const Blockstore, key: []const u8, marked: *std.StringHashMapUnmanaged(void)) !void {
    if (marked.contains(key)) return;
    const owned = try allocator.dupe(u8, key);
    marked.put(allocator, owned, {}) catch |err| {
        allocator.free(owned);
        return err;
    };

    const kids = try resolver.dagChildKeys(allocator, store, key);
    defer {
        for (kids) |c| allocator.free(c);
        allocator.free(kids);
    }
    for (kids) |c| try markRecursive(allocator, store, c, marked);
}

/// Remove unpinned blocks from memory store and sharded disk. Returns count removed.
pub fn gc(allocator: std.mem.Allocator, store: *Blockstore, pins: *const PinSet, repo_root: []const u8) !usize {
    var marked: std.StringHashMapUnmanaged(void) = .empty;
    defer {
        var it = marked.keyIterator();
        while (it.next()) |k| allocator.free(k.*);
        marked.deinit(allocator);
    }

    {
        var it = pins.recursive.keyIterator();
        while (it.next()) |k| try markRecursive(allocator, store, k.*, &marked);
    }
    {
        var it = pins.direct.keyIterator();
        while (it.next()) |k| {
            const dup = try allocator.dupe(u8, k.*);
            const gop = try marked.getOrPut(allocator, dup);
            if (gop.found_existing) allocator.free(dup);
        }
    }

    var keys = std.ArrayList([]const u8).empty;
    defer {
        for (keys.items) |x| allocator.free(x);
        keys.deinit(allocator);
    }
    var sit = store.map.iterator();
    while (sit.next()) |e| try keys.append(allocator, try allocator.dupe(u8, e.key_ptr.*));

    var removed: usize = 0;
    for (keys.items) |k| {
        if (marked.contains(k)) continue;
        _ = store.remove(allocator, k);
        try repo.removeBlockFile(repo_root, k);
        removed += 1;
    }
    return removed;
}
