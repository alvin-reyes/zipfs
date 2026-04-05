//! IPNS `IpnsEntry` verification (spec: `ipns-signature:` + DAG-CBOR `data`).

const std = @import("std");
const crypto = std.crypto;
const Ed25519 = crypto.sign.Ed25519;
const wireproto = @import("wireproto.zig");

pub const ipns_signature_prefix = "ipns-signature:";

pub const IpnsError = error{
    InvalidRecord,
    UnsupportedKey,
    BadSignature,
};

fn parseLibp2pEd25519Pub(pb: []const u8) ?[32]u8 {
    var it = wireproto.FieldIter{ .data = pb };
    var key_type: ?u64 = null;
    var data: ?[]const u8 = null;
    while (it.next() catch return null) |e| {
        switch (e.field_num) {
            1 => key_type = e.int_val,
            2 => data = e.bytes,
            else => {},
        }
    }
    if (key_type != 1) return null; // Ed25519 in libp2p crypto.pb
    const d = data orelse return null;
    if (d.len != 32) return null;
    var out: [32]u8 = undefined;
    @memcpy(&out, d);
    return out;
}

/// Verify a V2 IPNS record: requires non-empty `data` and `signatureV2` (protobuf fields 9 and 8).
/// If `pub_key_ed25519` is null, uses embedded `pubKey` (field 7) when it holds an Ed25519 `crypto.pb.PublicKey`.
pub fn verifyRecord(record: []const u8, pub_key_ed25519: ?[32]u8) IpnsError!void {
    var data: ?[]const u8 = null;
    var sig2: ?[]const u8 = null;
    var pub_pb: ?[]const u8 = null;
    var it = wireproto.FieldIter{ .data = record };
    while (it.next() catch return error.InvalidRecord) |e| {
        switch (e.field_num) {
            7 => pub_pb = e.bytes,
            8 => sig2 = e.bytes,
            9 => data = e.bytes,
            else => {},
        }
    }
    const d = data orelse return error.InvalidRecord;
    if (d.len == 0) return error.InvalidRecord;
    const s2 = sig2 orelse return error.InvalidRecord;
    if (s2.len != Ed25519.Signature.encoded_length) return error.InvalidRecord;

    const pk_raw = if (pub_key_ed25519) |k| k else blk: {
        const pb = pub_pb orelse return error.InvalidRecord;
        break :blk parseLibp2pEd25519Pub(pb) orelse return error.UnsupportedKey;
    };
    const pk = Ed25519.PublicKey.fromBytes(pk_raw) catch return error.InvalidRecord;

    var stack_buf: [10240]u8 = undefined; // spec max record 10 KiB
    if (ipns_signature_prefix.len + d.len > stack_buf.len) return error.InvalidRecord;
    @memcpy(stack_buf[0..ipns_signature_prefix.len], ipns_signature_prefix);
    @memcpy(stack_buf[ipns_signature_prefix.len..][0..d.len], d);
    const to_verify = stack_buf[0 .. ipns_signature_prefix.len + d.len];

    const sig = Ed25519.Signature.fromBytes(s2[0..Ed25519.Signature.encoded_length].*);
    sig.verify(to_verify, pk) catch return error.BadSignature;
}

pub fn publishRecord(_: []const u8, _: []const u8) error{IpnsPublishNotImplemented}!void {
    return error.IpnsPublishNotImplemented;
}
