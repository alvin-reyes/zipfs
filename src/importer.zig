//! Add files: single dag-pb block (small) or chunked raw leaves + balanced dag-pb tree.

const std = @import("std");
const cid_mod = @import("cid.zig");
const multihash = @import("multihash.zig");
const unixfs = @import("unixfs.zig");
const dag_pb = @import("dag_pb.zig");
const Blockstore = @import("blockstore.zig").Blockstore;

const Cid = cid_mod.Cid;

pub const chunk_size: usize = 262144;
const max_links: usize = 174;

const Ref = struct {
    key: []u8,
    file_part: u64,
    block_len: usize,
};

fn refListDeinit(allocator: std.mem.Allocator, list: *std.ArrayList(Ref)) void {
    for (list.items) |r| allocator.free(r.key);
    list.deinit(allocator);
}

pub fn addFile(allocator: std.mem.Allocator, store: *Blockstore, data: []const u8) !Cid {
    return addFileWithChunk(allocator, store, data, chunk_size);
}

pub fn addFileWithChunk(allocator: std.mem.Allocator, store: *Blockstore, data: []const u8, file_chunk_size: usize) !Cid {
    if (file_chunk_size < 1024) return error.ChunkTooSmall;
    if (data.len <= file_chunk_size) {
        return addSingleBlockFile(allocator, store, data);
    }

    var level = std.ArrayList(Ref).empty;
    errdefer refListDeinit(allocator, &level);

    var off: usize = 0;
    while (off < data.len) {
        const end = @min(off + file_chunk_size, data.len);
        const chunk = data[off..end];
        off = end;
        const digest = multihash.digestSha256(chunk);
        const id = try Cid.rawSha256(allocator, &digest);
        defer id.deinit(allocator);
        try store.put(allocator, id, chunk);
        const key = try id.toString(allocator);
        try level.append(allocator, .{ .key = key, .file_part = @intCast(chunk.len), .block_len = chunk.len });
    }

    while (level.items.len > 1) {
        var next = std.ArrayList(Ref).empty;
        errdefer refListDeinit(allocator, &next);

        var i: usize = 0;
        while (i < level.items.len) {
            const remain = level.items.len - i;
            if (remain == 1) {
                const r = level.items[i];
                const k = try allocator.dupe(u8, r.key);
                try next.append(allocator, .{ .key = k, .file_part = r.file_part, .block_len = r.block_len });
                i += 1;
                break;
            }
            const take = @min(max_links, remain);
            const group = level.items[i .. i + take];
            i += take;

            var total_file: u64 = 0;
            var links = std.ArrayList(dag_pb.Link).empty;
            defer {
                for (links.items) |ln| allocator.free(ln.hash);
                links.deinit(allocator);
            }

            for (group) |r| {
                total_file += r.file_part;
                const child = try Cid.parse(allocator, r.key);
                defer child.deinit(allocator);
                const hb = try child.toBytes(allocator);
                try links.append(allocator, .{ .hash = hb, .name = "", .tsize = @intCast(r.block_len) });
            }

            const ufs = try unixfs.encodeData(allocator, .file, null, total_file);
            defer allocator.free(ufs);
            const node = try dag_pb.encodeNode(allocator, ufs, links.items);
            defer allocator.free(node);

            const d = multihash.digestSha256(node);
            const pid = try Cid.dagPbSha256(allocator, &d);
            defer pid.deinit(allocator);
            try store.put(allocator, pid, node);
            const pkey = try pid.toString(allocator);
            try next.append(allocator, .{ .key = pkey, .file_part = total_file, .block_len = node.len });
        }

        refListDeinit(allocator, &level);
        level = next;
    }

    const root_key = try allocator.dupe(u8, level.items[0].key);
    errdefer allocator.free(root_key);
    refListDeinit(allocator, &level);

    const root_cid = try Cid.parse(allocator, root_key);
    allocator.free(root_key);
    return root_cid;
}

fn addSingleBlockFile(allocator: std.mem.Allocator, store: *Blockstore, data: []const u8) !Cid {
    const ufs = try unixfs.encodeData(allocator, .file, data, @intCast(data.len));
    defer allocator.free(ufs);
    const node = try dag_pb.encodeNode(allocator, ufs, &.{});
    defer allocator.free(node);
    const d = multihash.digestSha256(node);
    const id = try Cid.dagPbSha256(allocator, &d);
    try store.put(allocator, id, node);
    return id;
}
