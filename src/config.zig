//! Repo config at `$IPFS_PATH/config.json`.

const std = @import("std");

/// One public IPv4 bootstrap (no DNS); add more via `config.json`.
pub const default_bootstrap_peers = [_][]const u8{
    "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
};

pub const default_listen_addrs = [_][]const u8{
    "/ip4/0.0.0.0/tcp/4001",
};

pub const Config = struct {
    chunk_size: u32 = 262144,
    gateway_port: u16 = 8080,
    listen_addrs: [][]const u8 = &.{},
    bootstrap_peers: [][]const u8 = &.{},
    /// Public `/ip4/HOST/tcp/PORT` addresses advertised in Identify (required for WAN discoverability).
    announce_addrs: [][]const u8 = &.{},
    /// Seconds between DHT reprovide passes for recursive pins (`null` = default 43200). `0` disables.
    reprovide_interval_secs: ?u32 = null,
    /// Cluster peer multiaddrs for replication/sharding (e.g. `/ip4/1.2.3.4/tcp/4001`).
    cluster_peers: [][]const u8 = &.{},
    /// Shared hex-encoded secret for cluster authentication (`null` = no auth).
    cluster_secret: ?[]const u8 = null,
    /// Replication factor N in N-of-M (`null` = disabled).
    replication_factor: ?u8 = null,
    /// Number of shards for capacity scaling (`null` = no sharding).
    shard_count: ?u16 = null,
    /// `"replicate"` | `"shard"` | `null` (disabled).
    cluster_mode: ?[]const u8 = null,
    /// Self-healing loop interval in seconds (default 300).
    self_heal_interval_secs: ?u32 = null,
    /// Replication inbox polling interval in seconds (default 2).
    repl_inbox_poll_secs: ?u32 = null,
    /// Max replication queue items before backpressure drops low-priority retries (default 4096).
    repl_queue_max_size: ?u32 = null,
    /// Max concurrent replication connections per peer (default 2).
    repl_max_per_peer: ?u8 = null,
    /// Global replication ops/second rate limit (default 20).
    repl_rate_limit: ?u32 = null,
    /// Max blocks per push batch (default 50).
    repl_batch_size: ?u16 = null,

    pub fn deinit(self: *Config, allocator: std.mem.Allocator) void {
        if (self.listen_addrs.len != 0) {
            for (self.listen_addrs) |s| allocator.free(s);
            allocator.free(self.listen_addrs);
            self.listen_addrs = &.{};
        }
        if (self.bootstrap_peers.len != 0) {
            for (self.bootstrap_peers) |s| allocator.free(s);
            allocator.free(self.bootstrap_peers);
            self.bootstrap_peers = &.{};
        }
        if (self.announce_addrs.len != 0) {
            for (self.announce_addrs) |s| allocator.free(s);
            allocator.free(self.announce_addrs);
            self.announce_addrs = &.{};
        }
        if (self.cluster_peers.len != 0) {
            for (self.cluster_peers) |s| allocator.free(s);
            allocator.free(self.cluster_peers);
            self.cluster_peers = &.{};
        }
        if (self.cluster_secret) |s| {
            allocator.free(s);
            self.cluster_secret = null;
        }
        if (self.cluster_mode) |s| {
            allocator.free(s);
            self.cluster_mode = null;
        }
    }

    pub fn load(allocator: std.mem.Allocator, repo_root: []const u8) !Config {
        const path = try std.fs.path.join(allocator, &.{ repo_root, "config.json" });
        defer allocator.free(path);
        const data = std.fs.cwd().readFileAlloc(allocator, path, 1 << 20) catch |err| switch (err) {
            error.FileNotFound => return .{},
            else => |e| return e,
        };
        defer allocator.free(data);

        const Json = struct {
            chunk_size: ?u32 = null,
            gateway_port: ?u16 = null,
            listen_addrs: ?[][]const u8 = null,
            bootstrap_peers: ?[][]const u8 = null,
            announce_addrs: ?[][]const u8 = null,
            reprovide_interval_secs: ?u32 = null,
            cluster_peers: ?[][]const u8 = null,
            cluster_secret: ?[]const u8 = null,
            replication_factor: ?u8 = null,
            shard_count: ?u16 = null,
            cluster_mode: ?[]const u8 = null,
            self_heal_interval_secs: ?u32 = null,
            repl_inbox_poll_secs: ?u32 = null,
            repl_queue_max_size: ?u32 = null,
            repl_max_per_peer: ?u8 = null,
            repl_rate_limit: ?u32 = null,
            repl_batch_size: ?u16 = null,
        };
        var p = try std.json.parseFromSlice(Json, allocator, data, .{ .allocate = .alloc_always });
        defer p.deinit();

        var cfg = Config{};
        errdefer cfg.deinit(allocator);

        if (p.value.chunk_size) |cs| {
            if (cs > 0 and cs < (1 << 30)) cfg.chunk_size = cs;
        }
        if (p.value.gateway_port) |gp| cfg.gateway_port = gp;
        if (p.value.listen_addrs) |addrs| {
            const copy = try allocator.alloc([]const u8, addrs.len);
            var i: usize = 0;
            errdefer {
                for (copy[0..i]) |s| allocator.free(s);
                allocator.free(copy);
            }
            for (addrs) |s| {
                copy[i] = try allocator.dupe(u8, s);
                i += 1;
            }
            cfg.listen_addrs = copy;
        }
        if (p.value.bootstrap_peers) |addrs| {
            const copy = try allocator.alloc([]const u8, addrs.len);
            var i: usize = 0;
            errdefer {
                for (copy[0..i]) |s| allocator.free(s);
                allocator.free(copy);
            }
            for (addrs) |s| {
                copy[i] = try allocator.dupe(u8, s);
                i += 1;
            }
            cfg.bootstrap_peers = copy;
        }
        if (p.value.announce_addrs) |addrs| {
            const copy = try allocator.alloc([]const u8, addrs.len);
            var i: usize = 0;
            errdefer {
                for (copy[0..i]) |s| allocator.free(s);
                allocator.free(copy);
            }
            for (addrs) |s| {
                copy[i] = try allocator.dupe(u8, s);
                i += 1;
            }
            cfg.announce_addrs = copy;
        }
        if (p.value.reprovide_interval_secs) |r| cfg.reprovide_interval_secs = r;
        if (p.value.cluster_peers) |addrs| {
            const copy = try allocator.alloc([]const u8, addrs.len);
            var i: usize = 0;
            errdefer {
                for (copy[0..i]) |s| allocator.free(s);
                allocator.free(copy);
            }
            for (addrs) |s| {
                copy[i] = try allocator.dupe(u8, s);
                i += 1;
            }
            cfg.cluster_peers = copy;
        }
        if (p.value.cluster_secret) |s| cfg.cluster_secret = try allocator.dupe(u8, s);
        if (p.value.replication_factor) |r| cfg.replication_factor = r;
        if (p.value.shard_count) |s| cfg.shard_count = s;
        if (p.value.cluster_mode) |m| {
            if (std.mem.eql(u8, m, "replicate") or std.mem.eql(u8, m, "shard")) {
                cfg.cluster_mode = try allocator.dupe(u8, m);
            } else {
                return error.InvalidClusterMode;
            }
        }
        if (p.value.self_heal_interval_secs) |s| cfg.self_heal_interval_secs = s;
        // Validate scheduler config: zero values cause deadlocks or busy-loops
        if (p.value.repl_inbox_poll_secs) |v| {
            if (v > 0) cfg.repl_inbox_poll_secs = v;
        }
        if (p.value.repl_queue_max_size) |v| {
            if (v > 0) cfg.repl_queue_max_size = v;
        }
        if (p.value.repl_max_per_peer) |v| {
            if (v > 0) cfg.repl_max_per_peer = v;
        }
        if (p.value.repl_rate_limit) |v| {
            if (v > 0) cfg.repl_rate_limit = v;
        }
        if (p.value.repl_batch_size) |v| {
            if (v > 0) cfg.repl_batch_size = v;
        }
        return cfg;
    }

    pub fn save(self: Config, allocator: std.mem.Allocator, repo_root: []const u8) !void {
        const Out = struct {
            chunk_size: u32,
            gateway_port: u16,
            listen_addrs: [][]const u8,
            bootstrap_peers: [][]const u8,
            announce_addrs: [][]const u8,
            reprovide_interval_secs: ?u32,
            cluster_peers: [][]const u8,
            cluster_secret: ?[]const u8,
            replication_factor: ?u8,
            shard_count: ?u16,
            cluster_mode: ?[]const u8,
            self_heal_interval_secs: ?u32,
            repl_inbox_poll_secs: ?u32,
            repl_queue_max_size: ?u32,
            repl_max_per_peer: ?u8,
            repl_rate_limit: ?u32,
            repl_batch_size: ?u16,
        };
        var buf = std.ArrayList(u8).empty;
        defer buf.deinit(allocator);
        var bw = buf.writer(allocator);
        try bw.print("{f}", .{std.json.fmt(Out{
            .chunk_size = self.chunk_size,
            .gateway_port = self.gateway_port,
            .listen_addrs = self.listen_addrs,
            .bootstrap_peers = self.bootstrap_peers,
            .announce_addrs = self.announce_addrs,
            .reprovide_interval_secs = self.reprovide_interval_secs,
            .cluster_peers = self.cluster_peers,
            .cluster_secret = self.cluster_secret,
            .replication_factor = self.replication_factor,
            .shard_count = self.shard_count,
            .cluster_mode = self.cluster_mode,
            .self_heal_interval_secs = self.self_heal_interval_secs,
            .repl_inbox_poll_secs = self.repl_inbox_poll_secs,
            .repl_queue_max_size = self.repl_queue_max_size,
            .repl_max_per_peer = self.repl_max_per_peer,
            .repl_rate_limit = self.repl_rate_limit,
            .repl_batch_size = self.repl_batch_size,
        }, .{ .whitespace = .indent_2 })});
        try buf.append(allocator, '\n');
        const path = try std.fs.path.join(allocator, &.{ repo_root, "config.json" });
        defer allocator.free(path);
        try std.fs.cwd().makePath(repo_root);
        try std.fs.cwd().writeFile(.{ .sub_path = path, .data = buf.items });
    }

    /// Allocated listen + bootstrap defaults suitable for `config init`.
    pub fn initWithMainnetDefaults(allocator: std.mem.Allocator) std.mem.Allocator.Error!Config {
        var la_list = std.ArrayList([]const u8).empty;
        errdefer {
            for (la_list.items) |s| allocator.free(s);
            la_list.deinit(allocator);
        }
        for (default_listen_addrs) |s| {
            try la_list.append(allocator, try allocator.dupe(u8, s));
        }
        const la = try la_list.toOwnedSlice(allocator);
        errdefer {
            for (la) |s| allocator.free(s);
            allocator.free(la);
        }

        var bs_list = std.ArrayList([]const u8).empty;
        errdefer {
            for (bs_list.items) |s| allocator.free(s);
            bs_list.deinit(allocator);
        }
        for (default_bootstrap_peers) |s| {
            try bs_list.append(allocator, try allocator.dupe(u8, s));
        }
        const bs = try bs_list.toOwnedSlice(allocator);
        return .{ .listen_addrs = la, .bootstrap_peers = bs };
    }
};
