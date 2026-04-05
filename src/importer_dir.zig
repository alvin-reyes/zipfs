//! Recursive UnixFS directory import (deterministic name order).

const std = @import("std");
const cid_mod = @import("cid.zig");
const multihash = @import("multihash.zig");
const unixfs = @import("unixfs.zig");
const dag_pb = @import("dag_pb.zig");
const importer = @import("importer.zig");
const Blockstore = @import("blockstore.zig").Blockstore;

const Cid = cid_mod.Cid;

const Child = struct {
    name: []u8,
    key: []u8,
};

fn childListDeinit(allocator: std.mem.Allocator, list: *std.ArrayList(Child)) void {
    for (list.items) |c| {
        allocator.free(c.name);
        allocator.free(c.key);
    }
    list.deinit(allocator);
}

fn lessChildName(_: void, a: Child, b: Child) bool {
    return std.mem.lessThan(u8, a.name, b.name);
}

fn addDirFromOpenDir(allocator: std.mem.Allocator, store: *Blockstore, dir: std.fs.Dir, chunk_size: usize) !Cid {
    var children = std.ArrayList(Child).empty;
    defer childListDeinit(allocator, &children);

    var it = dir.iterate();
    while (try it.next()) |ent| {
        if (std.mem.eql(u8, ent.name, ".") or std.mem.eql(u8, ent.name, "..")) continue;
        switch (ent.kind) {
            .file => {
                const data = try dir.readFileAlloc(allocator, ent.name, std.math.maxInt(usize));
                defer allocator.free(data);
                const fc = try importer.addFileWithChunk(allocator, store, data, chunk_size);
                defer fc.deinit(allocator);
                const ks = try fc.toString(allocator);
                try children.append(allocator, .{
                    .name = try allocator.dupe(u8, ent.name),
                    .key = ks,
                });
            },
            .directory => {
                var sub = try dir.openDir(ent.name, .{ .iterate = true });
                defer sub.close();
                const subc = try addDirFromOpenDir(allocator, store, sub, chunk_size);
                defer subc.deinit(allocator);
                const ks = try subc.toString(allocator);
                try children.append(allocator, .{
                    .name = try allocator.dupe(u8, ent.name),
                    .key = ks,
                });
            },
            else => continue,
        }
    }

    std.mem.sortUnstable(Child, children.items, {}, lessChildName);

    var links = std.ArrayList(dag_pb.Link).empty;
    defer {
        for (links.items) |ln| allocator.free(ln.hash);
        links.deinit(allocator);
    }

    for (children.items) |ch| {
        var c = try Cid.parse(allocator, ch.key);
        defer c.deinit(allocator);
        const hb = try c.toBytes(allocator);
        const blk = store.get(ch.key) orelse return error.MissingBlock;
        try links.append(allocator, .{ .hash = hb, .name = ch.name, .tsize = @intCast(blk.len) });
    }

    const ufs = try unixfs.encodeData(allocator, .directory, null, null);
    defer allocator.free(ufs);
    const node = try dag_pb.encodeNode(allocator, ufs, links.items);
    defer allocator.free(node);
    const d = multihash.digestSha256(node);
    const id = try Cid.dagPbSha256(allocator, &d);
    try store.put(allocator, id, node);
    return id;
}

pub fn addDirectory(allocator: std.mem.Allocator, store: *Blockstore, dir_path: []const u8, chunk_size: usize) !Cid {
    var dir = try std.fs.cwd().openDir(dir_path, .{ .iterate = true });
    defer dir.close();
    return addDirFromOpenDir(allocator, store, dir, chunk_size);
}
