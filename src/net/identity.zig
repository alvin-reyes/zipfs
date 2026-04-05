//! Persistent libp2p Ed25519 secret (`$IPFS_PATH/identity.key`, 64 bytes) for Noise + PeerID.

const std = @import("std");

const identity_file = "identity.key";

pub fn pathInRepo(allocator: std.mem.Allocator, repo_root: []const u8) ![]u8 {
    return std.fs.path.join(allocator, &.{ repo_root, identity_file });
}

/// Load 64-byte Ed25519 secret or create and persist a new keypair (same layout as `std.crypto.sign.Ed25519.KeyPair`).
pub fn loadOrCreateSecret64(allocator: std.mem.Allocator, repo_root: []const u8) ![64]u8 {
    const p = try pathInRepo(allocator, repo_root);
    defer allocator.free(p);

    const f = std.fs.cwd().openFile(p, .{}) catch |err| switch (err) {
        error.FileNotFound => {
            const kp = std.crypto.sign.Ed25519.KeyPair.generate();
            const sec = kp.secret_key.toBytes();
            try std.fs.cwd().makePath(repo_root);
            try std.fs.cwd().writeFile(.{ .sub_path = p, .data = &sec });
            return sec;
        },
        else => |e| return e,
    };
    defer f.close();
    const st = try f.stat();
    if (st.size != 64) return error.BadIdentityFile;
    var buf: [64]u8 = undefined;
    const n = try f.readAll(&buf);
    if (n != 64) return error.BadIdentityFile;
    _ = std.crypto.sign.Ed25519.SecretKey.fromBytes(buf) catch return error.BadIdentityFile;
    return buf;
}
