//! Mutable filesystem root pointer (minimal; flush = pin new DAG).

const std = @import("std");

pub const MfsRoot = struct {
    root_cid: ?[]u8 = null,

    pub fn deinit(self: *MfsRoot, allocator: std.mem.Allocator) void {
        if (self.root_cid) |s| allocator.free(s);
        self.* = .{};
    }

    pub fn setRoot(self: *MfsRoot, allocator: std.mem.Allocator, cid_str: []const u8) !void {
        if (self.root_cid) |s| allocator.free(s);
        self.root_cid = try allocator.dupe(u8, cid_str);
    }
};
