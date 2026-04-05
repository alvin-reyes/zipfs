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
        };
        var p = try std.json.parseFromSlice(Json, allocator, data, .{ .allocate = .alloc_always });
        defer p.deinit();

        var cfg = Config{};
        if (p.value.chunk_size) |cs| {
            if (cs > 0 and cs < (1 << 30)) cfg.chunk_size = cs;
        }
        if (p.value.gateway_port) |gp| cfg.gateway_port = gp;
        if (p.value.listen_addrs) |addrs| {
            const copy = try allocator.alloc([]const u8, addrs.len);
            for (addrs, 0..) |s, i| copy[i] = try allocator.dupe(u8, s);
            cfg.listen_addrs = copy;
        }
        if (p.value.bootstrap_peers) |addrs| {
            const copy = try allocator.alloc([]const u8, addrs.len);
            for (addrs, 0..) |s, i| copy[i] = try allocator.dupe(u8, s);
            cfg.bootstrap_peers = copy;
        }
        if (p.value.announce_addrs) |addrs| {
            const copy = try allocator.alloc([]const u8, addrs.len);
            for (addrs, 0..) |s, i| copy[i] = try allocator.dupe(u8, s);
            cfg.announce_addrs = copy;
        }
        if (p.value.reprovide_interval_secs) |r| cfg.reprovide_interval_secs = r;
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
