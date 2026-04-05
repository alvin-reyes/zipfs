//! libp2p-style peer id from Ed25519 public key (protobuf + SHA2-256 multihash, base58).

const std = @import("std");
const multibase = @import("../multibase.zig");

/// Protobuf `crypto.pb.PublicKey`: field1 KeyType=1 (Ed25519 per go-libp2p), field2 Data=key bytes.
pub fn marshalPublicKeyEd25519(pub32: *const [32]u8) [36]u8 {
    var out: [36]u8 = undefined;
    // field 1: varint 1 (Ed25519)
    out[0] = 0x08;
    out[1] = 0x01;
    // field 2: length-delimited 32 bytes
    out[2] = 0x12;
    out[3] = 32;
    @memcpy(out[4..][0..32], pub32);
    return out;
}

/// Multihash: 0x12 0x20 + sha256(proto).
/// Binary peer id: multihash `0x12 0x20` + SHA-256(libp2p `PublicKey` protobuf).
pub fn peerMultihashBytes(allocator: std.mem.Allocator, pub32: *const [32]u8) ![]u8 {
    const pb = marshalPublicKeyEd25519(pub32);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&pb, &digest, .{});
    const out = try allocator.alloc(u8, 34);
    out[0] = 0x12;
    out[1] = 0x20;
    @memcpy(out[2..], &digest);
    return out;
}

pub fn peerIdString(allocator: std.mem.Allocator, pub32: *const [32]u8) ![]u8 {
    const pb = marshalPublicKeyEd25519(pub32);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&pb, &digest, .{});
    var mh: [34]u8 = undefined;
    mh[0] = 0x12;
    mh[1] = 0x20;
    @memcpy(mh[2..], &digest);
    return multibase.encodeBase58Btc(allocator, &mh);
}

pub fn generateKeyPair() struct { public_key: [32]u8, secret_key: [64]u8 } {
    const kp = std.crypto.sign.Ed25519.KeyPair.generate();
    return .{
        .public_key = kp.public_key.toBytes(),
        .secret_key = kp.secret_key.toBytes(),
    };
}
