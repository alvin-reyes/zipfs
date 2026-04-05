//! Repo config at `$IPFS_PATH/config.json`.

const std = @import("std");

pub const Config = struct {
    chunk_size: u32 = 262144,
    gateway_port: u16 = 8080,
    listen_addrs: [][]const u8 = &.{},

    pub fn deinit(self: *Config, allocator: std.mem.Allocator) void {
        if (self.listen_addrs.len == 0) return;
        for (self.listen_addrs) |s| allocator.free(s);
        allocator.free(self.listen_addrs);
        self.listen_addrs = &.{};
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
        return cfg;
    }

    pub fn save(self: Config, allocator: std.mem.Allocator, repo_root: []const u8) !void {
        const Out = struct {
            chunk_size: u32,
            gateway_port: u16,
            listen_addrs: [][]const u8,
        };
        var buf = std.ArrayList(u8).empty;
        defer buf.deinit(allocator);
        var bw = buf.writer(allocator);
        try bw.print("{f}", .{std.json.fmt(Out{
            .chunk_size = self.chunk_size,
            .gateway_port = self.gateway_port,
            .listen_addrs = self.listen_addrs,
        }, .{ .whitespace = .indent_2 })});
        try buf.append(allocator, '\n');
        const path = try std.fs.path.join(allocator, &.{ repo_root, "config.json" });
        defer allocator.free(path);
        try std.fs.cwd().makePath(repo_root);
        try std.fs.cwd().writeFile(.{ .sub_path = path, .data = buf.items });
    }
};
