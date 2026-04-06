//! Cluster state: peer membership, replica tracking, retry queue, and persistent state.

const std = @import("std");

pub const ClusterMode = enum {
    replicate,
    shard,

    pub fn fromString(s: []const u8) ?ClusterMode {
        if (std.mem.eql(u8, s, "replicate")) return .replicate;
        if (std.mem.eql(u8, s, "shard")) return .shard;
        return null;
    }

    pub fn toString(self: ClusterMode) []const u8 {
        return switch (self) {
            .replicate => "replicate",
            .shard => "shard",
        };
    }
};

pub const ClusterPeer = struct {
    peer_id: []const u8,
    addr: []const u8,
    last_seen_ns: i128 = 0,
    is_alive: bool = true,
    consecutive_failures: u8 = 0,

    pub fn deinit(self: *ClusterPeer, allocator: std.mem.Allocator) void {
        allocator.free(self.peer_id);
        allocator.free(self.addr);
        self.* = undefined;
    }

    pub fn clone(self: ClusterPeer, allocator: std.mem.Allocator) !ClusterPeer {
        const pid = try allocator.dupe(u8, self.peer_id);
        errdefer allocator.free(pid);
        const a = try allocator.dupe(u8, self.addr);
        return .{
            .peer_id = pid,
            .addr = a,
            .last_seen_ns = self.last_seen_ns,
            .is_alive = self.is_alive,
            .consecutive_failures = self.consecutive_failures,
        };
    }
};

pub const ShardAssignment = struct {
    shard_index: u16,
    peer_id: []const u8,
    block_cids: [][]const u8,
    confirmed: bool = false,

    pub fn deinit(self: *ShardAssignment, allocator: std.mem.Allocator) void {
        allocator.free(self.peer_id);
        for (self.block_cids) |c| allocator.free(c);
        allocator.free(self.block_cids);
        self.* = undefined;
    }
};

pub const ReplicaRecord = struct {
    cid: []const u8,
    mode: ClusterMode,
    target_n: u8,
    confirmed_peers: [][]const u8,
    shard_assignments: []ShardAssignment,

    pub fn deinit(self: *ReplicaRecord, allocator: std.mem.Allocator) void {
        allocator.free(self.cid);
        for (self.confirmed_peers) |p| allocator.free(p);
        allocator.free(self.confirmed_peers);
        for (self.shard_assignments) |*sa| {
            var s = sa.*;
            s.deinit(allocator);
        }
        allocator.free(self.shard_assignments);
        self.* = undefined;
    }
};

pub const RetryOp = enum {
    push_blocks,
    push_shard,
    have_check,
};

pub const RetryEntry = struct {
    op: RetryOp,
    cid: []const u8,
    target_peer: []const u8,
    attempts: u8 = 0,
    next_retry_ns: i128 = 0,
    shard_index: ?u16 = null,

    pub fn deinit(self: *RetryEntry, allocator: std.mem.Allocator) void {
        allocator.free(self.cid);
        allocator.free(self.target_peer);
        self.* = undefined;
    }

    /// Exponential backoff: 5s, 10s, 20s, ... capped at 1 hour.
    pub fn nextBackoffNs(self: RetryEntry) i128 {
        const base_ns: i128 = 5 * std.time.ns_per_s;
        const shift: u6 = @intCast(@min(self.attempts, 20));
        const delay = base_ns << shift;
        const max_delay: i128 = 3600 * std.time.ns_per_s;
        return @min(delay, max_delay);
    }
};

pub const max_retry_attempts: u8 = 10;

/// Write a JSON-escaped string (with surrounding quotes) to the writer.
fn writeJsonString(w: anytype, s: []const u8) !void {
    try w.writeByte('"');
    for (s) |c| {
        switch (c) {
            '"' => try w.writeAll("\\\""),
            '\\' => try w.writeAll("\\\\"),
            '\n' => try w.writeAll("\\n"),
            '\r' => try w.writeAll("\\r"),
            '\t' => try w.writeAll("\\t"),
            else => {
                if (c < 0x20) {
                    try w.print("\\u{x:0>4}", .{c});
                } else {
                    try w.writeByte(c);
                }
            },
        }
    }
    try w.writeByte('"');
}

pub const ClusterState = struct {
    peers: std.ArrayList(ClusterPeer),
    replicas: std.StringHashMapUnmanaged(ReplicaRecord),
    retry_queue: std.ArrayList(RetryEntry),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) ClusterState {
        return .{
            .peers = std.ArrayList(ClusterPeer).empty,
            .replicas = .empty,
            .retry_queue = std.ArrayList(RetryEntry).empty,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ClusterState) void {
        for (self.peers.items) |*p| {
            var peer = p.*;
            peer.deinit(self.allocator);
        }
        self.peers.deinit(self.allocator);

        var it = self.replicas.iterator();
        while (it.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            var rec = e.value_ptr.*;
            rec.deinit(self.allocator);
        }
        self.replicas.deinit(self.allocator);

        for (self.retry_queue.items) |*r| {
            var entry = r.*;
            entry.deinit(self.allocator);
        }
        self.retry_queue.deinit(self.allocator);
    }

    pub fn addPeer(self: *ClusterState, peer_id: []const u8, addr: []const u8) !void {
        for (self.peers.items) |p| {
            if (std.mem.eql(u8, p.addr, addr)) return;
        }
        const pid = try self.allocator.dupe(u8, peer_id);
        errdefer self.allocator.free(pid);
        const a = try self.allocator.dupe(u8, addr);
        errdefer self.allocator.free(a);
        try self.peers.append(self.allocator, .{
            .peer_id = pid,
            .addr = a,
            .last_seen_ns = std.time.nanoTimestamp(),
            .is_alive = true,
            .consecutive_failures = 0,
        });
    }

    pub fn removePeer(self: *ClusterState, addr: []const u8) void {
        var i: usize = 0;
        while (i < self.peers.items.len) {
            if (std.mem.eql(u8, self.peers.items[i].addr, addr)) {
                var p = self.peers.orderedRemove(i);
                p.deinit(self.allocator);
            } else {
                i += 1;
            }
        }
    }

    pub fn alivePeers(self: *const ClusterState, allocator: std.mem.Allocator) ![]ClusterPeer {
        var list = std.ArrayList(ClusterPeer).empty;
        errdefer list.deinit(allocator);
        for (self.peers.items) |p| {
            if (p.is_alive) try list.append(allocator, p);
        }
        return try list.toOwnedSlice(allocator);
    }

    pub fn findPeerByAddr(self: *ClusterState, addr: []const u8) ?*ClusterPeer {
        for (self.peers.items) |*p| {
            if (std.mem.eql(u8, p.addr, addr)) return p;
        }
        return null;
    }

    pub fn addRetry(self: *ClusterState, op: RetryOp, cid_str: []const u8, target_peer: []const u8, shard_index: ?u16) !void {
        const c = try self.allocator.dupe(u8, cid_str);
        errdefer self.allocator.free(c);
        const t = try self.allocator.dupe(u8, target_peer);
        errdefer self.allocator.free(t);
        var entry = RetryEntry{
            .op = op,
            .cid = c,
            .target_peer = t,
            .attempts = 0,
            .shard_index = shard_index,
        };
        entry.next_retry_ns = std.time.nanoTimestamp() + entry.nextBackoffNs();
        try self.retry_queue.append(self.allocator, entry);
    }

    /// Atomic save: write to temp file then rename.
    pub fn save(self: *const ClusterState, repo_root: []const u8) !void {
        const allocator = self.allocator;
        var buf = std.ArrayList(u8).empty;
        defer buf.deinit(allocator);
        var w = buf.writer(allocator);

        try w.writeAll("{");

        // peers
        try w.writeAll("\"peers\":[");
        for (self.peers.items, 0..) |p, i| {
            if (i > 0) try w.writeAll(",");
            try w.writeAll("{\"peer_id\":");
            try writeJsonString(w, p.peer_id);
            try w.writeAll(",\"addr\":");
            try writeJsonString(w, p.addr);
            try w.print(",\"is_alive\":{},\"consecutive_failures\":{d}}}", .{
                p.is_alive, p.consecutive_failures,
            });
        }
        try w.writeAll("],");

        // replicas
        try w.writeAll("\"replicas\":[");
        var rit = self.replicas.iterator();
        var first_r = true;
        while (rit.next()) |e| {
            if (!first_r) try w.writeAll(",");
            first_r = false;
            const rec = e.value_ptr.*;
            try w.writeAll("{\"cid\":");
            try writeJsonString(w, rec.cid);
            try w.writeAll(",\"mode\":");
            try writeJsonString(w, rec.mode.toString());
            try w.print(",\"target_n\":{d},\"confirmed_peers\":[", .{rec.target_n});
            for (rec.confirmed_peers, 0..) |cp, j| {
                if (j > 0) try w.writeAll(",");
                try writeJsonString(w, cp);
            }
            try w.writeAll("],\"shard_assignments\":[");
            for (rec.shard_assignments, 0..) |sa, j| {
                if (j > 0) try w.writeAll(",");
                try w.writeAll("{\"shard_index\":");
                try w.print("{d}", .{sa.shard_index});
                try w.writeAll(",\"peer_id\":");
                try writeJsonString(w, sa.peer_id);
                try w.print(",\"confirmed\":{},\"block_cids\":[", .{sa.confirmed});
                for (sa.block_cids, 0..) |bc, k| {
                    if (k > 0) try w.writeAll(",");
                    try writeJsonString(w, bc);
                }
                try w.writeAll("]}");
            }
            try w.writeAll("]}");
        }
        try w.writeAll("],");

        // retry_queue
        try w.writeAll("\"retry_queue\":[");
        for (self.retry_queue.items, 0..) |re, i| {
            if (i > 0) try w.writeAll(",");
            const op_str: []const u8 = switch (re.op) {
                .push_blocks => "push_blocks",
                .push_shard => "push_shard",
                .have_check => "have_check",
            };
            try w.writeAll("{\"op\":");
            try writeJsonString(w, op_str);
            try w.writeAll(",\"cid\":");
            try writeJsonString(w, re.cid);
            try w.writeAll(",\"target_peer\":");
            try writeJsonString(w, re.target_peer);
            try w.print(",\"attempts\":{d}", .{re.attempts});
            if (re.shard_index) |si| {
                try w.print(",\"shard_index\":{d}", .{si});
            }
            try w.writeAll("}");
        }
        try w.writeAll("]");

        try w.writeAll("}\n");

        try std.fs.cwd().makePath(repo_root);

        // Atomic write: temp file + rename
        const tmp_path = try std.fs.path.join(allocator, &.{ repo_root, "cluster_state.json.tmp" });
        defer allocator.free(tmp_path);
        const final_path = try std.fs.path.join(allocator, &.{ repo_root, "cluster_state.json" });
        defer allocator.free(final_path);

        // Write temp file with fsync for durability
        const tmp_file = try std.fs.cwd().createFile(tmp_path, .{ .truncate = true });
        tmp_file.writeAll(buf.items) catch |e| {
            tmp_file.close();
            std.fs.cwd().deleteFile(tmp_path) catch {};
            return e;
        };
        tmp_file.sync() catch |e| {
            tmp_file.close();
            std.fs.cwd().deleteFile(tmp_path) catch {};
            return e;
        };
        tmp_file.close();

        std.fs.cwd().rename(tmp_path, final_path) catch {
            // Fallback: direct write with fsync if rename fails (e.g. cross-device)
            std.fs.cwd().deleteFile(tmp_path) catch {};
            const fallback = try std.fs.cwd().createFile(final_path, .{ .truncate = true });
            fallback.writeAll(buf.items) catch |e| {
                fallback.close();
                return e;
            };
            fallback.sync() catch |e| {
                fallback.close();
                return e;
            };
            fallback.close();
        };
    }

    pub fn load(allocator: std.mem.Allocator, repo_root: []const u8) !ClusterState {
        const path = try std.fs.path.join(allocator, &.{ repo_root, "cluster_state.json" });
        defer allocator.free(path);
        const data = std.fs.cwd().readFileAlloc(allocator, path, 4 << 20) catch |err| switch (err) {
            error.FileNotFound => return ClusterState.init(allocator),
            else => |e| return e,
        };
        defer allocator.free(data);

        const J = struct {
            peers: ?[]const struct {
                peer_id: []const u8 = "",
                addr: []const u8 = "",
                is_alive: bool = true,
                consecutive_failures: u8 = 0,
            } = null,
            replicas: ?[]const struct {
                cid: []const u8 = "",
                mode: []const u8 = "replicate",
                target_n: u8 = 1,
                confirmed_peers: ?[]const []const u8 = null,
                shard_assignments: ?[]const struct {
                    shard_index: u16 = 0,
                    peer_id: []const u8 = "",
                    confirmed: bool = false,
                    block_cids: ?[]const []const u8 = null,
                } = null,
            } = null,
            retry_queue: ?[]const struct {
                op: []const u8 = "push_blocks",
                cid: []const u8 = "",
                target_peer: []const u8 = "",
                attempts: u8 = 0,
                shard_index: ?u16 = null,
            } = null,
        };

        var p = try std.json.parseFromSlice(J, allocator, data, .{ .allocate = .alloc_always });
        defer p.deinit();

        var state = ClusterState.init(allocator);
        errdefer state.deinit();

        if (p.value.peers) |peers| {
            for (peers) |jp| {
                const pid = try allocator.dupe(u8, jp.peer_id);
                errdefer allocator.free(pid);
                const addr = try allocator.dupe(u8, jp.addr);
                errdefer allocator.free(addr);
                try state.peers.append(allocator, .{
                    .peer_id = pid,
                    .addr = addr,
                    .is_alive = jp.is_alive,
                    .consecutive_failures = jp.consecutive_failures,
                });
            }
        }

        if (p.value.replicas) |replicas| {
            for (replicas) |jr| {
                const cid_key = try allocator.dupe(u8, jr.cid);
                errdefer allocator.free(cid_key);
                const cid_val = try allocator.dupe(u8, jr.cid);
                errdefer allocator.free(cid_val);

                var confirmed = std.ArrayList([]const u8).empty;
                errdefer {
                    for (confirmed.items) |c| allocator.free(c);
                    confirmed.deinit(allocator);
                }
                if (jr.confirmed_peers) |cps| {
                    for (cps) |cp| {
                        try confirmed.append(allocator, try allocator.dupe(u8, cp));
                    }
                }

                var shards = std.ArrayList(ShardAssignment).empty;
                errdefer {
                    for (shards.items) |*s| s.deinit(allocator);
                    shards.deinit(allocator);
                }
                if (jr.shard_assignments) |sas| {
                    for (sas) |sa| {
                        const sa_pid = try allocator.dupe(u8, sa.peer_id);
                        errdefer allocator.free(sa_pid);
                        var bcids = std.ArrayList([]const u8).empty;
                        errdefer {
                            for (bcids.items) |c| allocator.free(c);
                            bcids.deinit(allocator);
                        }
                        if (sa.block_cids) |bcs| {
                            for (bcs) |bc| {
                                try bcids.append(allocator, try allocator.dupe(u8, bc));
                            }
                        }
                        try shards.append(allocator, .{
                            .shard_index = sa.shard_index,
                            .peer_id = sa_pid,
                            .block_cids = try bcids.toOwnedSlice(allocator),
                            .confirmed = sa.confirmed,
                        });
                    }
                }

                const mode = ClusterMode.fromString(jr.mode) orelse .replicate;
                try state.replicas.put(allocator, cid_key, .{
                    .cid = cid_val,
                    .mode = mode,
                    .target_n = jr.target_n,
                    .confirmed_peers = try confirmed.toOwnedSlice(allocator),
                    .shard_assignments = try shards.toOwnedSlice(allocator),
                });
            }
        }

        if (p.value.retry_queue) |retries| {
            for (retries) |jr| {
                const c = try allocator.dupe(u8, jr.cid);
                errdefer allocator.free(c);
                const t = try allocator.dupe(u8, jr.target_peer);
                errdefer allocator.free(t);
                const op: RetryOp = if (std.mem.eql(u8, jr.op, "push_shard"))
                    .push_shard
                else if (std.mem.eql(u8, jr.op, "have_check"))
                    .have_check
                else
                    .push_blocks;
                try state.retry_queue.append(allocator, .{
                    .op = op,
                    .cid = c,
                    .target_peer = t,
                    .attempts = jr.attempts,
                    .shard_index = jr.shard_index,
                });
            }
        }

        return state;
    }

    /// Add or update a replica record. Caller must not free cid_str (it will be duped).
    pub fn upsertReplica(self: *ClusterState, cid_str: []const u8, mode: ClusterMode, target_n: u8) !*ReplicaRecord {
        if (self.replicas.getPtr(cid_str)) |existing| {
            existing.mode = mode;
            existing.target_n = target_n;
            return existing;
        }
        const key = try self.allocator.dupe(u8, cid_str);
        errdefer self.allocator.free(key);
        const cid_val = try self.allocator.dupe(u8, cid_str);
        errdefer self.allocator.free(cid_val);
        const confirmed = try self.allocator.alloc([]const u8, 0);
        errdefer self.allocator.free(confirmed);
        const shards = try self.allocator.alloc(ShardAssignment, 0);
        errdefer self.allocator.free(shards);
        try self.replicas.put(self.allocator, key, .{
            .cid = cid_val,
            .mode = mode,
            .target_n = target_n,
            .confirmed_peers = confirmed,
            .shard_assignments = shards,
        });
        return self.replicas.getPtr(cid_str).?;
    }

    /// Add a confirmed peer to a replica record (deduplicates).
    pub fn addConfirmedPeer(self: *ClusterState, cid_str: []const u8, peer_addr: []const u8) !void {
        const rec = self.replicas.getPtr(cid_str) orelse return;
        for (rec.confirmed_peers) |cp| {
            if (std.mem.eql(u8, cp, peer_addr)) return;
        }
        const new_len = rec.confirmed_peers.len + 1;
        const new = try self.allocator.alloc([]const u8, new_len);
        errdefer self.allocator.free(new);
        for (rec.confirmed_peers, 0..) |cp, i| {
            new[i] = cp;
        }
        new[new_len - 1] = try self.allocator.dupe(u8, peer_addr);
        self.allocator.free(rec.confirmed_peers);
        rec.confirmed_peers = new;
    }

    /// Remove a replica record by CID.
    pub fn removeReplica(self: *ClusterState, cid_str: []const u8) void {
        if (self.replicas.fetchRemove(cid_str)) |kv| {
            self.allocator.free(kv.key);
            var rec = kv.value;
            rec.deinit(self.allocator);
        }
    }
};
