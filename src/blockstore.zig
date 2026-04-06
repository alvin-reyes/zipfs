//! In-memory blockstore keyed by CID string.

const std = @import("std");
const cid_mod = @import("cid.zig");
const repo = @import("repo.zig");

const Cid = cid_mod.Cid;

pub const Blockstore = struct {
    map: std.StringHashMapUnmanaged([]u8) = .empty,
    repo_root: ?[]const u8 = null,

    pub fn deinit(self: *Blockstore, allocator: std.mem.Allocator) void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            allocator.free(e.key_ptr.*);
            allocator.free(e.value_ptr.*);
        }
        self.map.deinit(allocator);
        self.* = undefined;
    }

    pub fn put(self: *Blockstore, allocator: std.mem.Allocator, c: Cid, data: []const u8) !void {
        const key = try c.toString(allocator);
        errdefer allocator.free(key);
        if (self.map.get(key)) |old| {
            if (!std.mem.eql(u8, old, data)) return error.ConflictingBlock;
            allocator.free(key);
            return;
        }
        if (self.repo_root) |root| {
            try writeBlockFile(root, key, data);
        }
        const owned = try allocator.dupe(u8, data);
        errdefer allocator.free(owned);
        try self.map.put(allocator, key, owned);
    }

    pub fn get(self: *const Blockstore, key_utf8: []const u8) ?[]const u8 {
        return self.map.get(key_utf8);
    }

    pub fn has(self: *const Blockstore, key_utf8: []const u8) bool {
        return self.map.contains(key_utf8);
    }

    pub fn count(self: *const Blockstore) usize {
        return self.map.count();
    }

    /// Iterate all blocks (key = CID string, value = raw block bytes).
    pub fn each(self: *const Blockstore, ctx: *anyopaque, cb: *const fn (*anyopaque, []const u8, []const u8) anyerror!void) !void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            try cb(ctx, e.key_ptr.*, e.value_ptr.*);
        }
    }

    pub fn remove(self: *Blockstore, allocator: std.mem.Allocator, key_utf8: []const u8) bool {
        const kv = self.map.fetchRemove(key_utf8) orelse return false;
        if (self.repo_root) |root| {
            repo.removeBlockFile(root, kv.key) catch {};
        }
        allocator.free(kv.key);
        allocator.free(kv.value);
        return true;
    }

    /// Write every block to `dir` using the CID string as the file name.
    pub fn exportFlatDir(self: *const Blockstore, dir: std.fs.Dir) !void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            try dir.writeFile(.{ .sub_path = e.key_ptr.*, .data = e.value_ptr.* });
        }
    }

    /// Load blocks from `dir` (non-recursive). File names must be full CID strings.
    pub fn importFlatDir(self: *Blockstore, allocator: std.mem.Allocator, dir: std.fs.Dir) !void {
        var it = dir.iterate();
        while (try it.next()) |ent| {
            if (ent.kind != .file) continue;
            const data = try dir.readFileAlloc(allocator, ent.name, std.math.maxInt(usize));
            errdefer allocator.free(data);
            const key = try allocator.dupe(u8, ent.name);
            errdefer allocator.free(key);
            try self.map.put(allocator, key, data);
        }
    }

    fn writeBlockFile(root: []const u8, cid_key: []const u8, data: []const u8) !void {
        const path = try repo.joinBlocksPath(std.heap.page_allocator, root, cid_key);
        defer std.heap.page_allocator.free(path);
        if (std.fs.path.dirname(path)) |dir| {
            try std.fs.cwd().makePath(dir);
        }
        const file = try std.fs.cwd().createFile(path, .{ .truncate = true });
        file.writeAll(data) catch |e| {
            file.close();
            return e;
        };
        file.sync() catch |e| {
            file.close();
            return e;
        };
        file.close();
    }
};
