//! On-disk blocks: sharded layout `$IPFS_PATH/blocks/aa/bb/<cid-string>`.

const std = @import("std");

const hex = "0123456789abcdef";

pub fn shardParts(cid_key: []const u8) [4]u8 {
    var h: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(cid_key, &h, .{});
    return .{
        hex[h[0] >> 4], hex[h[0] & 0xf],
        hex[h[1] >> 4], hex[h[1] & 0xf],
    };
}

pub fn joinBlocksPath(allocator: std.mem.Allocator, repo_root: []const u8, cid_key: []const u8) ![]u8 {
    const sp = shardParts(cid_key);
    const a = sp[0..2];
    const b = sp[2..4];
    return std.fs.path.join(allocator, &.{ repo_root, "blocks", a, b, cid_key });
}

/// Read a block from the sharded disk layout. Returns owned data or null.
pub fn readBlockFromDisk(allocator: std.mem.Allocator, repo_root: []const u8, cid_key: []const u8) ?[]u8 {
    const path = joinBlocksPath(std.heap.page_allocator, repo_root, cid_key) catch return null;
    defer std.heap.page_allocator.free(path);
    return std.fs.cwd().readFileAlloc(allocator, path, 64 << 20) catch null;
}

/// Check if a block file exists on disk without reading it.
pub fn blockExistsOnDisk(repo_root: []const u8, cid_key: []const u8) bool {
    const path = joinBlocksPath(std.heap.page_allocator, repo_root, cid_key) catch return false;
    defer std.heap.page_allocator.free(path);
    std.fs.cwd().access(path, .{}) catch |err| {
        switch (err) {
            error.FileNotFound => {},
            else => std.log.warn("blockExistsOnDisk: access error for {s}: {}", .{ cid_key, err }),
        }
        return false;
    };
    return true;
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
