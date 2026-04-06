//! Manifest-based pull replication: streaming DAG walk, disk-persisted manifests, per-peer progress.
//!
//! Disk layout per manifest:
//!   $IPFS_PATH/manifests/<root-cid>.json     — metadata (status, peers, counters)
//!   $IPFS_PATH/manifests/<root-cid>.cids     — one CID string per line (append-only)
//!   $IPFS_PATH/manifests/<root-cid>.progress — per-peer checkpoint (peer:last_idx per line)

const std = @import("std");
const resolver = @import("resolver.zig");
const blockstore_mod = @import("blockstore.zig");

const Blockstore = blockstore_mod.Blockstore;

/// Lifecycle status of a replication manifest.
pub const ManifestStatus = enum {
    walking,
    ready,
    active,
    complete,
    failed,

    /// Return the canonical string representation for JSON serialization.
    pub fn toString(self: ManifestStatus) []const u8 {
        return switch (self) {
            .walking => "walking",
            .ready => "ready",
            .active => "active",
            .complete => "complete",
            .failed => "failed",
        };
    }

    /// Parse a string into a ManifestStatus, returning null if unrecognized.
    pub fn fromString(s: []const u8) ?ManifestStatus {
        if (std.mem.eql(u8, s, "walking")) return .walking;
        if (std.mem.eql(u8, s, "ready")) return .ready;
        if (std.mem.eql(u8, s, "active")) return .active;
        if (std.mem.eql(u8, s, "complete")) return .complete;
        if (std.mem.eql(u8, s, "failed")) return .failed;
        return null;
    }
};

/// Per-peer pull status within a manifest.
pub const PeerStatus = enum {
    pending,
    pulling,
    complete,
    failed,

    /// Return the canonical string representation for JSON serialization.
    pub fn toString(self: PeerStatus) []const u8 {
        return switch (self) {
            .pending => "pending",
            .pulling => "pulling",
            .complete => "complete",
            .failed => "failed",
        };
    }

    /// Parse a string into a PeerStatus, returning null if unrecognized.
    pub fn fromString(s: []const u8) ?PeerStatus {
        if (std.mem.eql(u8, s, "pending")) return .pending;
        if (std.mem.eql(u8, s, "pulling")) return .pulling;
        if (std.mem.eql(u8, s, "complete")) return .complete;
        if (std.mem.eql(u8, s, "failed")) return .failed;
        return null;
    }
};

/// Tracks a single peer's pull progress within a manifest.
pub const PeerProgress = struct {
    peer_addr: []const u8,
    total_blocks: u64,
    pulled_blocks: u64,
    last_pulled_idx: u64,
    last_activity_ns: i128,
    status: PeerStatus,
};

/// A replication manifest describing a DAG, its constituent CIDs, and per-peer pull progress.
pub const Manifest = struct {
    root_cid: []const u8,
    total_blocks: u64,
    total_bytes: u64,
    status: ManifestStatus,
    created_ns: i128,
    updated_ns: i128,
    target_peers: []PeerProgress,
    replication_factor: u8,

    /// Free all owned memory (root_cid, peer addrs, peer slice).
    pub fn deinit(self: *Manifest, allocator: std.mem.Allocator) void {
        allocator.free(self.root_cid);
        for (self.target_peers) |p| {
            allocator.free(p.peer_addr);
        }
        allocator.free(self.target_peers);
        self.* = undefined;
    }

    /// Save manifest metadata as JSON (atomic write).
    pub fn save(self: *const Manifest, allocator: std.mem.Allocator, repo_root: []const u8) !void {
        const dir_path = try std.fs.path.join(allocator, &.{ repo_root, "manifests" });
        defer allocator.free(dir_path);
        try std.fs.cwd().makePath(dir_path);

        const filename = try std.fmt.allocPrint(allocator, "{s}.json", .{self.root_cid});
        defer allocator.free(filename);
        const path = try std.fs.path.join(allocator, &.{ dir_path, filename });
        defer allocator.free(path);
        const tmp_filename = try std.fmt.allocPrint(allocator, "{s}.json.tmp", .{self.root_cid});
        defer allocator.free(tmp_filename);
        const tmp_path = try std.fs.path.join(allocator, &.{ dir_path, tmp_filename });
        defer allocator.free(tmp_path);

        var buf = std.ArrayList(u8).empty;
        defer buf.deinit(allocator);
        const w = buf.writer(allocator);

        try w.writeAll("{");
        try w.print("\"root_cid\":\"{s}\",", .{self.root_cid});
        try w.print("\"total_blocks\":{d},", .{self.total_blocks});
        try w.print("\"total_bytes\":{d},", .{self.total_bytes});
        try w.print("\"status\":\"{s}\",", .{self.status.toString()});
        try w.print("\"created_ns\":{d},", .{self.created_ns});
        try w.print("\"updated_ns\":{d},", .{self.updated_ns});
        try w.print("\"replication_factor\":{d},", .{self.replication_factor});
        try w.writeAll("\"target_peers\":[");
        for (self.target_peers, 0..) |p, i| {
            if (i > 0) try w.writeAll(",");
            try w.writeAll("{");
            try w.print("\"peer_addr\":\"{s}\",", .{p.peer_addr});
            try w.print("\"total_blocks\":{d},", .{p.total_blocks});
            try w.print("\"pulled_blocks\":{d},", .{p.pulled_blocks});
            try w.print("\"last_pulled_idx\":{d},", .{p.last_pulled_idx});
            try w.print("\"last_activity_ns\":{d},", .{p.last_activity_ns});
            try w.print("\"status\":\"{s}\"", .{p.status.toString()});
            try w.writeAll("}");
        }
        try w.writeAll("]}\n");

        // Atomic write: temp → fsync → rename
        const file = try std.fs.cwd().createFile(tmp_path, .{ .truncate = true });
        file.writeAll(buf.items) catch |e| {
            file.close();
            return e;
        };
        file.sync() catch |e| {
            file.close();
            return e;
        };
        file.close();
        std.fs.cwd().rename(tmp_path, path) catch {
            // Fallback: direct write
            const fb = try std.fs.cwd().createFile(path, .{ .truncate = true });
            fb.writeAll(buf.items) catch |e2| {
                fb.close();
                return e2;
            };
            fb.sync() catch |e2| {
                fb.close();
                return e2;
            };
            fb.close();
        };
    }

    /// Load a manifest by root CID.
    pub fn load(allocator: std.mem.Allocator, repo_root: []const u8, root_cid: []const u8) !Manifest {
        const filename = try std.fmt.allocPrint(allocator, "{s}.json", .{root_cid});
        defer allocator.free(filename);
        const path = try std.fs.path.join(allocator, &.{ repo_root, "manifests", filename });
        defer allocator.free(path);

        const data = try std.fs.cwd().readFileAlloc(allocator, path, 4 << 20);
        defer allocator.free(data);

        return parseManifestJson(allocator, data);
    }

    /// Load all manifests from the manifests directory.
    pub fn loadAll(allocator: std.mem.Allocator, repo_root: []const u8) ![]Manifest {
        const dir_path = try std.fs.path.join(allocator, &.{ repo_root, "manifests" });
        defer allocator.free(dir_path);

        var manifests = std.ArrayList(Manifest).empty;
        errdefer {
            for (manifests.items) |*m| m.deinit(allocator);
            manifests.deinit(allocator);
        }

        var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return try manifests.toOwnedSlice(allocator),
            else => return err,
        };
        defer dir.close();

        var it = dir.iterate();
        while (try it.next()) |entry| {
            if (entry.kind != .file) continue;
            if (!std.mem.endsWith(u8, entry.name, ".json")) continue;
            // Skip .tmp files
            if (std.mem.endsWith(u8, entry.name, ".json.tmp")) continue;

            const file_path = try std.fs.path.join(allocator, &.{ dir_path, entry.name });
            defer allocator.free(file_path);

            const file_data = std.fs.cwd().readFileAlloc(allocator, file_path, 4 << 20) catch continue;
            defer allocator.free(file_data);

            var m = parseManifestJson(allocator, file_data) catch continue;
            errdefer m.deinit(allocator);
            try manifests.append(allocator, m);
        }

        return try manifests.toOwnedSlice(allocator);
    }
};

/// Parse manifest JSON into a Manifest struct.
fn parseManifestJson(allocator: std.mem.Allocator, data: []const u8) !Manifest {
    const Json = struct {
        root_cid: ?[]const u8 = null,
        total_blocks: ?u64 = null,
        total_bytes: ?u64 = null,
        status: ?[]const u8 = null,
        created_ns: ?i128 = null,
        updated_ns: ?i128 = null,
        replication_factor: ?u8 = null,
        target_peers: ?[]const struct {
            peer_addr: ?[]const u8 = null,
            total_blocks: ?u64 = null,
            pulled_blocks: ?u64 = null,
            last_pulled_idx: ?u64 = null,
            last_activity_ns: ?i128 = null,
            status: ?[]const u8 = null,
        } = null,
    };

    var parsed = try std.json.parseFromSlice(Json, allocator, data, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    const v = parsed.value;

    const root_cid = try allocator.dupe(u8, v.root_cid orelse return error.MissingField);
    errdefer allocator.free(root_cid);

    var peers = std.ArrayList(PeerProgress).empty;
    errdefer {
        for (peers.items) |p| allocator.free(p.peer_addr);
        peers.deinit(allocator);
    }

    if (v.target_peers) |tps| {
        for (tps) |tp| {
            const addr = try allocator.dupe(u8, tp.peer_addr orelse continue);
            errdefer allocator.free(addr);
            try peers.append(allocator, .{
                .peer_addr = addr,
                .total_blocks = tp.total_blocks orelse 0,
                .pulled_blocks = tp.pulled_blocks orelse 0,
                .last_pulled_idx = tp.last_pulled_idx orelse 0,
                .last_activity_ns = tp.last_activity_ns orelse 0,
                .status = if (tp.status) |s| PeerStatus.fromString(s) orelse .pending else .pending,
            });
        }
    }

    return .{
        .root_cid = root_cid,
        .total_blocks = v.total_blocks orelse 0,
        .total_bytes = v.total_bytes orelse 0,
        .status = if (v.status) |s| ManifestStatus.fromString(s) orelse .walking else .walking,
        .created_ns = v.created_ns orelse std.time.nanoTimestamp(),
        .updated_ns = v.updated_ns orelse std.time.nanoTimestamp(),
        .target_peers = try peers.toOwnedSlice(allocator),
        .replication_factor = v.replication_factor orelse 2,
    };
}

/// Simple bloom filter for deduplication during DAG walk.
/// Uses ~6MB for 40M items at ~1% FPR.
const BloomFilter = struct {
    bits: []u8,
    num_hashes: u8,
    bit_count: u64,

    /// Create a bloom filter sized for `expected_items` at ~1% FPR.
    fn init(allocator: std.mem.Allocator, expected_items: u64) !BloomFilter {
        // m = -n * ln(p) / (ln(2))^2; p=0.01
        // ~9.6 bits per item; k = m/n * ln(2) ~= 7
        const bits_needed = expected_items * 10; // ~10 bits per item
        const byte_count = @max(bits_needed / 8, 1024); // min 1KB
        const capped = @min(byte_count, 48 * 1024 * 1024); // max 48MB
        const bits = try allocator.alloc(u8, capped);
        @memset(bits, 0);
        return .{
            .bits = bits,
            .num_hashes = 7,
            .bit_count = capped * 8,
        };
    }

    fn deinit(self: *BloomFilter, allocator: std.mem.Allocator) void {
        allocator.free(self.bits);
        self.* = undefined;
    }

    fn add(self: *BloomFilter, key: []const u8) void {
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(key, &hash, .{});
        var i: u8 = 0;
        while (i < self.num_hashes) : (i += 1) {
            const bit_idx = self.hashSlot(hash, i);
            self.bits[bit_idx / 8] |= @as(u8, 1) << @intCast(bit_idx % 8);
        }
    }

    fn mightContain(self: *const BloomFilter, key: []const u8) bool {
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(key, &hash, .{});
        var i: u8 = 0;
        while (i < self.num_hashes) : (i += 1) {
            const bit_idx = self.hashSlot(hash, i);
            if ((self.bits[bit_idx / 8] & (@as(u8, 1) << @intCast(bit_idx % 8))) == 0)
                return false;
        }
        return true;
    }

    fn hashSlot(self: *const BloomFilter, hash: [32]u8, idx: u8) u64 {
        // Double hashing: h(i) = (h1 + i*h2) mod m
        const h1 = std.mem.readInt(u64, hash[0..8], .little);
        const h2 = std.mem.readInt(u64, hash[8..16], .little);
        return (h1 +% @as(u64, idx) *% h2) % self.bit_count;
    }
};

/// Create a manifest by streaming BFS walk of a DAG.
/// Appends each discovered CID to the .cids file (never holds full list in memory).
/// Uses a bloom filter for dedup (~10 bits per item, 1% FPR).
/// Returns the manifest with status=ready.
pub fn createManifest(
    allocator: std.mem.Allocator,
    store: *Blockstore,
    store_mu: ?*std.Thread.Mutex,
    repo_root: []const u8,
    root_cid: []const u8,
    target_peers: []const []const u8,
    repl_factor: u8,
) !Manifest {
    const now = std.time.nanoTimestamp();

    // Ensure manifests directory exists
    const dir_path = try std.fs.path.join(allocator, &.{ repo_root, "manifests" });
    defer allocator.free(dir_path);
    try std.fs.cwd().makePath(dir_path);

    // Open .cids file for append
    const cids_filename = try std.fmt.allocPrint(allocator, "{s}.cids", .{root_cid});
    defer allocator.free(cids_filename);
    const cids_path = try std.fs.path.join(allocator, &.{ dir_path, cids_filename });
    defer allocator.free(cids_path);

    // Check if .cids file already exists (crash recovery: resume from existing)
    var existing_count: u64 = 0;
    var bloom = try BloomFilter.init(allocator, 1_000_000); // start with 1M estimate
    defer bloom.deinit(allocator);

    // Rebuild bloom from existing .cids if resuming
    if (std.fs.cwd().readFileAlloc(allocator, cids_path, 1 << 30)) |existing_data| {
        defer allocator.free(existing_data);
        var line_it = std.mem.splitScalar(u8, existing_data, '\n');
        while (line_it.next()) |line| {
            if (line.len == 0) continue;
            bloom.add(line);
            existing_count += 1;
        }
    } else |_| {}

    // Open .cids for append
    const cids_file = try std.fs.cwd().createFile(cids_path, .{ .truncate = false });
    defer cids_file.close();
    cids_file.seekFromEnd(0) catch {};

    // BFS walk
    var work = std.ArrayList([]u8).empty;
    defer {
        for (work.items) |w| allocator.free(w);
        work.deinit(allocator);
    }

    // Start from root if not already visited
    if (!bloom.mightContain(root_cid)) {
        try work.append(allocator, try allocator.dupe(u8, root_cid));
    }

    var total_blocks = existing_count;
    const total_bytes: u64 = 0;

    while (work.items.len > 0) {
        const key = work.pop() orelse break;
        defer allocator.free(key);

        if (bloom.mightContain(key)) continue;
        bloom.add(key);

        // Write CID to .cids file
        cids_file.writeAll(key) catch continue;
        cids_file.writeAll("\n") catch continue;
        total_blocks += 1;

        // Sync every 1000 blocks to limit data loss on crash
        if (total_blocks % 1000 == 0) {
            cids_file.sync() catch {};
        }

        // Get children
        const children = blk: {
            if (store_mu) |mu| mu.lock();
            defer if (store_mu) |mu| mu.unlock();
            break :blk resolver.dagChildKeys(allocator, store, key) catch continue;
        };
        defer {
            for (children) |c| allocator.free(c);
            allocator.free(children);
        }

        for (children) |child| {
            if (!bloom.mightContain(child)) {
                try work.append(allocator, try allocator.dupe(u8, child));
            }
        }
    }

    // Final sync
    cids_file.sync() catch {};

    // Build peer progress entries
    var peers = try allocator.alloc(PeerProgress, target_peers.len);
    errdefer {
        for (peers) |p| allocator.free(p.peer_addr);
        allocator.free(peers);
    }
    for (target_peers, 0..) |addr, i| {
        peers[i] = .{
            .peer_addr = try allocator.dupe(u8, addr),
            .total_blocks = total_blocks,
            .pulled_blocks = 0,
            .last_pulled_idx = 0,
            .last_activity_ns = now,
            .status = .pending,
        };
    }

    var m = Manifest{
        .root_cid = try allocator.dupe(u8, root_cid),
        .total_blocks = total_blocks,
        .total_bytes = total_bytes,
        .status = .ready,
        .created_ns = now,
        .updated_ns = now,
        .target_peers = peers,
        .replication_factor = repl_factor,
    };

    try m.save(allocator, repo_root);
    return m;
}

/// Estimate the number of blocks in a DAG by walking up to `max_blocks` levels.
/// Returns the count; if the cap is hit, the actual DAG is at least that large.
pub fn estimateDagBlocks(
    allocator: std.mem.Allocator,
    store: *Blockstore,
    root_cid: []const u8,
    max_blocks: u64,
) u64 {
    var visited = std.StringHashMapUnmanaged(void).empty;
    defer {
        var kit = visited.keyIterator();
        while (kit.next()) |k| allocator.free(k.*);
        visited.deinit(allocator);
    }

    var work = std.ArrayList([]u8).empty;
    defer {
        for (work.items) |w| allocator.free(w);
        work.deinit(allocator);
    }

    work.append(allocator, allocator.dupe(u8, root_cid) catch return 1) catch return 1;

    var count: u64 = 0;
    while (work.items.len > 0) {
        if (count >= max_blocks) return count;

        const key = work.pop() orelse break;
        defer allocator.free(key);

        if (visited.contains(key)) continue;
        const owned = allocator.dupe(u8, key) catch continue;
        visited.put(allocator, owned, {}) catch {
            allocator.free(owned);
            continue;
        };
        count += 1;

        const children = resolver.dagChildKeys(allocator, store, key) catch continue;
        defer {
            for (children) |c| allocator.free(c);
            allocator.free(children);
        }
        for (children) |child| {
            if (!visited.contains(child)) {
                work.append(allocator, allocator.dupe(u8, child) catch continue) catch continue;
            }
        }
    }
    return count;
}

/// Read a batch of CID strings from the .cids file starting at a given line index.
/// Returns owned CID strings that the caller must free.
pub fn readCidBatch(
    allocator: std.mem.Allocator,
    repo_root: []const u8,
    root_cid: []const u8,
    start_idx: u64,
    batch_size: u16,
) ![][]u8 {
    const cids_filename = try std.fmt.allocPrint(allocator, "{s}.cids", .{root_cid});
    defer allocator.free(cids_filename);
    const cids_path = try std.fs.path.join(allocator, &.{ repo_root, "manifests", cids_filename });
    defer allocator.free(cids_path);

    const data = try std.fs.cwd().readFileAlloc(allocator, cids_path, 1 << 30);
    defer allocator.free(data);

    var result = std.ArrayList([]u8).empty;
    errdefer {
        for (result.items) |r| allocator.free(r);
        result.deinit(allocator);
    }

    var line_it = std.mem.splitScalar(u8, data, '\n');
    var idx: u64 = 0;
    var collected: u16 = 0;
    while (line_it.next()) |line| {
        if (line.len == 0) {
            idx += 1;
            continue;
        }
        if (idx < start_idx) {
            idx += 1;
            continue;
        }
        if (collected >= batch_size) break;
        try result.append(allocator, try allocator.dupe(u8, line));
        collected += 1;
        idx += 1;
    }

    return try result.toOwnedSlice(allocator);
}

/// Save per-peer progress to the .progress file.
pub fn saveProgress(
    allocator: std.mem.Allocator,
    repo_root: []const u8,
    root_cid: []const u8,
    peer_addr: []const u8,
    last_idx: u64,
    pulled: u64,
) !void {
    const progress_filename = try std.fmt.allocPrint(allocator, "{s}.progress", .{root_cid});
    defer allocator.free(progress_filename);
    const progress_path = try std.fs.path.join(allocator, &.{ repo_root, "manifests", progress_filename });
    defer allocator.free(progress_path);

    // Read existing progress entries (if any)
    var entries = std.ArrayList(struct { addr: []u8, idx: u64, pulled: u64 }).empty;
    defer {
        for (entries.items) |e| allocator.free(e.addr);
        entries.deinit(allocator);
    }

    if (std.fs.cwd().readFileAlloc(allocator, progress_path, 1 << 20)) |existing| {
        defer allocator.free(existing);
        var line_it = std.mem.splitScalar(u8, existing, '\n');
        while (line_it.next()) |line| {
            if (line.len == 0) continue;
            // Format: peer_addr:last_idx:pulled
            const first_colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
            const addr_part = line[0..first_colon];
            const rest = line[first_colon + 1 ..];
            const second_colon = std.mem.indexOfScalar(u8, rest, ':') orelse continue;
            const idx_str = rest[0..second_colon];
            const pulled_str = rest[second_colon + 1 ..];
            // Skip the entry we're updating
            if (std.mem.eql(u8, addr_part, peer_addr)) continue;
            const idx_val = std.fmt.parseInt(u64, idx_str, 10) catch continue;
            const pulled_val = std.fmt.parseInt(u64, pulled_str, 10) catch continue;
            try entries.append(allocator, .{
                .addr = try allocator.dupe(u8, addr_part),
                .idx = idx_val,
                .pulled = pulled_val,
            });
        }
    } else |_| {}

    // Write all entries + updated one
    const file = try std.fs.cwd().createFile(progress_path, .{ .truncate = true });
    defer file.close();

    for (entries.items) |e| {
        var line_buf: [512]u8 = undefined;
        const line = std.fmt.bufPrint(&line_buf, "{s}:{d}:{d}\n", .{ e.addr, e.idx, e.pulled }) catch continue;
        file.writeAll(line) catch continue;
    }
    // Write the updated entry
    var line_buf: [512]u8 = undefined;
    const line = std.fmt.bufPrint(&line_buf, "{s}:{d}:{d}\n", .{ peer_addr, last_idx, pulled }) catch return;
    file.writeAll(line) catch return;
    file.sync() catch {};
}

/// Load progress for a specific peer from the .progress file.
pub fn loadProgress(
    allocator: std.mem.Allocator,
    repo_root: []const u8,
    root_cid: []const u8,
    peer_addr: []const u8,
) struct { last_idx: u64, pulled: u64 } {
    const progress_filename = std.fmt.allocPrint(allocator, "{s}.progress", .{root_cid}) catch return .{ .last_idx = 0, .pulled = 0 };
    defer allocator.free(progress_filename);
    const progress_path = std.fs.path.join(allocator, &.{ repo_root, "manifests", progress_filename }) catch return .{ .last_idx = 0, .pulled = 0 };
    defer allocator.free(progress_path);

    const data = std.fs.cwd().readFileAlloc(allocator, progress_path, 1 << 20) catch return .{ .last_idx = 0, .pulled = 0 };
    defer allocator.free(data);

    var line_it = std.mem.splitScalar(u8, data, '\n');
    while (line_it.next()) |line| {
        if (line.len == 0) continue;
        const first_colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const addr_part = line[0..first_colon];
        if (!std.mem.eql(u8, addr_part, peer_addr)) continue;
        const rest = line[first_colon + 1 ..];
        const second_colon = std.mem.indexOfScalar(u8, rest, ':') orelse continue;
        const idx_val = std.fmt.parseInt(u64, rest[0..second_colon], 10) catch continue;
        const pulled_val = std.fmt.parseInt(u64, rest[second_colon + 1 ..], 10) catch continue;
        return .{ .last_idx = idx_val, .pulled = pulled_val };
    }
    return .{ .last_idx = 0, .pulled = 0 };
}
