//! On-disk blocks: sharded layout `$IPFS_PATH/blocks/aa/bb/<cid-string>`.

const std = @import("std");
const Blockstore = @import("blockstore.zig").Blockstore;
const cid_mod = @import("cid.zig");

const hex = "0123456789abcdef";

pub fn shardParts(cid_key: []const u8) [4]u8 {
    var h: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(cid_key, &h, .{});
    return .{
        hex[h[0] >> 4], hex[h[0] & 0xf],
        hex[h[1] >> 4], hex[h[1] & 0xf],
    };
}

fn joinBlocksPath(allocator: std.mem.Allocator, repo_root: []const u8, cid_key: []const u8) ![]u8 {
    const sp = shardParts(cid_key);
    const a = sp[0..2];
    const b = sp[2..4];
    return std.fs.path.join(allocator, &.{ repo_root, "blocks", a, b, cid_key });
}

fn exportOneBlock(ctx: *anyopaque, key: []const u8, value: []const u8) !void {
    const repo_root: []const u8 = @as(*[]const u8, @ptrCast(@alignCast(ctx))).*;
    const rel = try joinBlocksPath(std.heap.page_allocator, repo_root, key);
    defer std.heap.page_allocator.free(rel);
    if (std.fs.path.dirname(rel)) |dir| try std.fs.cwd().makePath(dir);
    try std.fs.cwd().writeFile(.{ .sub_path = rel, .data = value });
}

/// Write all blocks using sharded paths.
pub fn exportStore(store: *const Blockstore, repo_root: []const u8) !void {
    var rr = repo_root;
    try store.each(@ptrCast(&rr), exportOneBlock);
}

/// Recursively load `.` and `aa/bb/` shard subdirs.
pub fn importStore(store: *Blockstore, allocator: std.mem.Allocator, repo_root: []const u8) !void {
    const blocks_base = try std.fs.path.join(std.heap.page_allocator, &.{ repo_root, "blocks" });
    defer std.heap.page_allocator.free(blocks_base);

    var base_dir = std.fs.cwd().openDir(blocks_base, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => |e| return e,
    };
    defer base_dir.close();

    // Legacy flat layout: files directly under blocks/
    try importDirFlat(store, allocator, base_dir);

    var it = base_dir.iterate();
    while (try it.next()) |ent| {
        if (ent.kind != .directory) continue;
        if (ent.name.len != 2) continue;
        var sub = base_dir.openDir(ent.name, .{ .iterate = true }) catch continue;
        defer sub.close();
        var it2 = sub.iterate();
        while (try it2.next()) |e2| {
            if (e2.kind != .directory) continue;
            if (e2.name.len != 2) continue;
            var sub2 = sub.openDir(e2.name, .{ .iterate = true }) catch continue;
            defer sub2.close();
            try importDirFlat(store, allocator, sub2);
        }
    }
}

fn importDirFlat(store: *Blockstore, allocator: std.mem.Allocator, dir: std.fs.Dir) !void {
    var it = dir.iterate();
    while (try it.next()) |ent| {
        if (ent.kind != .file) continue;
        const data = try dir.readFileAlloc(allocator, ent.name, std.math.maxInt(usize));
        defer allocator.free(data);
        var c = try cid_mod.Cid.parse(allocator, ent.name);
        defer c.deinit(allocator);
        try store.put(allocator, c, data);
    }
}

pub fn removeBlockFile(repo_root: []const u8, cid_key: []const u8) !void {
    const rel = try joinBlocksPath(std.heap.page_allocator, repo_root, cid_key);
    defer std.heap.page_allocator.free(rel);
    std.fs.cwd().deleteFile(rel) catch |err| switch (err) {
        error.FileNotFound => return,
        else => |e| return e,
    };
}

pub fn repoRootFromEnv(allocator: std.mem.Allocator) ![]u8 {
    if (std.process.getEnvVarOwned(allocator, "IPFS_PATH")) |p| return p else |_| {}
    return try allocator.dupe(u8, ".zig-ipfs");
}
