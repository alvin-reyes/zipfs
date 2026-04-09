//! libp2p Noise: `Noise_XX_25519_ChaChaPoly_SHA256` + signed `NoiseHandshakePayload` (Kubo-compatible).

const std = @import("std");
const crypto = std.crypto;
const Sha256 = crypto.hash.sha2.Sha256;
const HmacSha256 = crypto.auth.hmac.sha2.HmacSha256;
const X25519 = crypto.dh.X25519;
const ChaCha20Poly1305 = crypto.aead.chacha_poly.ChaCha20Poly1305;
const Ed25519 = crypto.sign.Ed25519;
const peer_id = @import("peer_id.zig");
const wireproto = @import("../wireproto.zig");

pub const payload_sig_prefix = "noise-libp2p-static-key:";

pub const NoiseError = error{
    HandshakeFailed,
    BadPayload,
};

const protocol_name = "Noise_XX_25519_ChaChaPoly_SHA256";

fn mixHash(h: *[32]u8, data: []const u8) void {
    var st = Sha256.init(.{});
    st.update(h);
    st.update(data);
    st.final(h);
}

fn hkdf2(ck_in: [32]u8, ikm: []const u8, ck_out: *[32]u8, k_out: *[32]u8) void {
    var t_key: [32]u8 = undefined;
    HmacSha256.create(&t_key, ikm, &ck_in);
    HmacSha256.create(ck_out, &[_]u8{0x01}, &t_key);
    var buf: [33]u8 = undefined;
    @memcpy(buf[0..32], ck_out);
    buf[32] = 0x02;
    HmacSha256.create(k_out, &buf, &t_key);
}

fn hkdf2_split(ck_in: [32]u8, k1: *[32]u8, k2: *[32]u8) void {
    var t_key: [32]u8 = undefined;
    HmacSha256.create(&t_key, &.{}, &ck_in);
    HmacSha256.create(k1, &[_]u8{0x01}, &t_key);
    var buf: [33]u8 = undefined;
    @memcpy(buf[0..32], k1);
    buf[32] = 0x02;
    HmacSha256.create(k2, &buf, &t_key);
}

fn noiseNonce(n: u64) [12]u8 {
    var np: [12]u8 = .{0} ** 12;
    std.mem.writeInt(u64, np[4..12], n, .little);
    return np;
}

const SymmetricState = struct {
    h: [32]u8,
    ck: [32]u8,
    k: [32]u8,
    n: u64 = 0,
    has_k: bool = false,
    c_key: [32]u8 = undefined,

    fn init(name: []const u8) SymmetricState {
        var h: [32]u8 = undefined;
        if (name.len <= 32) {
            @memcpy(h[0..name.len], name);
            @memset(h[name.len..], 0);
        } else {
            Sha256.hash(name, &h, .{});
        }
        return .{
            .h = h,
            .ck = h,
            .k = undefined,
            .c_key = undefined,
        };
    }

    fn mixKey(self: *SymmetricState, dh_out: [32]u8) void {
        self.n = 0;
        self.has_k = true;
        hkdf2(self.ck, &dh_out, &self.ck, &self.k);
        self.c_key = self.k;
    }

    fn encryptAead(self: *SymmetricState, plaintext: []const u8, out: *std.ArrayList(u8), allocator: std.mem.Allocator) !void {
        const nonce = noiseNonce(self.n);
        self.n += 1;
        const tag_len = ChaCha20Poly1305.tag_length;
        const base = out.items.len;
        try out.resize(allocator, base + plaintext.len + tag_len);
        const ct = out.items[base .. base + plaintext.len];
        var tag: [tag_len]u8 = undefined;
        ChaCha20Poly1305.encrypt(ct, &tag, plaintext, &self.h, nonce, self.c_key);
        @memcpy(out.items[base + plaintext.len ..][0..tag_len], &tag);
        mixHash(&self.h, out.items[base..]);
    }

    fn decryptAead(self: *SymmetricState, ciphertext: []const u8, out: *std.ArrayList(u8), allocator: std.mem.Allocator) !void {
        if (ciphertext.len < ChaCha20Poly1305.tag_length) return error.HandshakeFailed;
        const tag_len = ChaCha20Poly1305.tag_length;
        const ct = ciphertext[0 .. ciphertext.len - tag_len];
        const tag = ciphertext[ciphertext.len - tag_len ..][0..tag_len].*;
        const nonce = noiseNonce(self.n);
        self.n += 1;
        const base = out.items.len;
        try out.resize(allocator, base + ct.len);
        const plain = out.items[base..];
        ChaCha20Poly1305.decrypt(plain, ct, tag, &self.h, nonce, self.c_key) catch return error.HandshakeFailed;
        mixHash(&self.h, ciphertext);
    }

    fn encryptAndHash(self: *SymmetricState, plaintext: []const u8, out: *std.ArrayList(u8), allocator: std.mem.Allocator) !void {
        if (!self.has_k) {
            mixHash(&self.h, plaintext);
            try out.appendSlice(allocator, plaintext);
            return;
        }
        try self.encryptAead(plaintext, out, allocator);
    }

    fn decryptAndHash(self: *SymmetricState, data: []const u8, out: *std.ArrayList(u8), allocator: std.mem.Allocator) ![]const u8 {
        if (!self.has_k) {
            mixHash(&self.h, data);
            const start = out.items.len;
            try out.appendSlice(allocator, data);
            return out.items[start..];
        }
        const start = out.items.len;
        try self.decryptAead(data, out, allocator);
        return out.items[start..];
    }

    fn split(self: *SymmetricState, k1: *[32]u8, k2: *[32]u8) void {
        hkdf2_split(self.ck, k1, k2);
    }
};

const DhKey = struct {
    secret: [32]u8,
    public: [32]u8,
};

fn genDhKey() !DhKey {
    const kp = X25519.KeyPair.generate();
    return .{ .secret = kp.secret_key, .public = kp.public_key };
}

fn dh(sk: [32]u8, pk: [32]u8) ![32]u8 {
    return X25519.scalarmult(sk, pk);
}

fn buildExtensionsYamux(allocator: std.mem.Allocator) ![]u8 {
    var inner = std.ArrayList(u8).empty;
    defer inner.deinit(allocator);
    try wireproto.appendBytesField(&inner, allocator, 2, "/yamux/1.0.0");
    return try inner.toOwnedSlice(allocator);
}

fn buildHandshakePayload(allocator: std.mem.Allocator, ed25519_kp: Ed25519.KeyPair, noise_static_pub: *const [32]u8) ![]u8 {
    const id_pb = peer_id.marshalPublicKeyEd25519(&ed25519_kp.public_key.toBytes());
    var to_sign = std.ArrayList(u8).empty;
    defer to_sign.deinit(allocator);
    try to_sign.appendSlice(allocator, payload_sig_prefix);
    try to_sign.appendSlice(allocator, noise_static_pub);
    const sig = try ed25519_kp.sign(to_sign.items, null);
    const sig_bytes = sig.toBytes();
    const ext = try buildExtensionsYamux(allocator);
    defer allocator.free(ext);
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    try wireproto.appendBytesField(&out, allocator, 1, &id_pb);
    try wireproto.appendBytesField(&out, allocator, 2, &sig_bytes);
    try wireproto.appendBytesField(&out, allocator, 4, ext);
    return try out.toOwnedSlice(allocator);
}

fn parseLibp2pEd25519Pub(payload: []const u8) ?[32]u8 {
    var it = wireproto.FieldIter{ .data = payload };
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

fn parseHandshakePayload(payload: []const u8) ?struct { libp2p_pub_pb: []const u8, sig: []const u8 } {
    var it = wireproto.FieldIter{ .data = payload };
    var id_key: ?[]const u8 = null;
    var sig: ?[]const u8 = null;
    while (it.next() catch return null) |e| {
        switch (e.field_num) {
            1 => id_key = e.bytes,
            2 => sig = e.bytes,
            else => {},
        }
    }
    return .{ .libp2p_pub_pb = id_key orelse return null, .sig = sig orelse return null };
}

fn verifyRemotePayload(payload: []const u8, remote_noise_static: *const [32]u8, allocator: std.mem.Allocator) !Ed25519.PublicKey {
    const p = parseHandshakePayload(payload) orelse return error.BadPayload;
    const pk_raw = parseLibp2pEd25519Pub(p.libp2p_pub_pb) orelse return error.BadPayload;
    const pk = Ed25519.PublicKey.fromBytes(pk_raw) catch return error.BadPayload;
    if (p.sig.len != Ed25519.Signature.encoded_length) return error.BadPayload;
    var msg = std.ArrayList(u8).empty;
    defer msg.deinit(allocator);
    try msg.appendSlice(allocator, payload_sig_prefix);
    try msg.appendSlice(allocator, remote_noise_static);
    const sig = Ed25519.Signature.fromBytes(p.sig[0..Ed25519.Signature.encoded_length].*);
    sig.verify(msg.items, pk) catch return error.BadPayload;
    return pk;
}

/// Initiator: build first handshake message (`-> e`, empty payload).
fn writeInitiatorMsg1(ss: *SymmetricState, e: *DhKey, out: *std.ArrayList(u8), allocator: std.mem.Allocator) !void {
    e.* = try genDhKey();
    try out.appendSlice(allocator, &e.public);
    mixHash(&ss.h, &e.public);
    try ss.encryptAndHash(&.{}, out, allocator);
}

/// Responder: consume first message, extract `re`.
fn readResponderMsg1(ss: *SymmetricState, re: *[32]u8, msg: []const u8, payload_out: *std.ArrayList(u8), allocator: std.mem.Allocator) !void {
    if (msg.len < 32) return error.HandshakeFailed;
    @memcpy(re, msg[0..32]);
    mixHash(&ss.h, re);
    _ = try ss.decryptAndHash(msg[32..], payload_out, allocator);
}

/// Responder: second message `<- e, ee, s, es` + libp2p payload.
fn writeResponderMsg2(
    ss: *SymmetricState,
    s: *const DhKey,
    re: *const [32]u8,
    e: *DhKey,
    libp2p_payload: []const u8,
    out: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
) !void {
    e.* = try genDhKey();
    try out.appendSlice(allocator, &e.public);
    mixHash(&ss.h, &e.public);
    const ee = try dh(e.secret, re.*);
    ss.mixKey(ee);
    try ss.encryptAndHash(&s.public, out, allocator);
    const dh_es = try dh(s.secret, re.*);
    ss.mixKey(dh_es);
    try ss.encryptAndHash(libp2p_payload, out, allocator);
}

/// Initiator: read second message; returns decrypted libp2p payload slice (into `payload_out` buffer).
fn readInitiatorMsg2(
    ss: *SymmetricState,
    _: *const DhKey,
    e: *const DhKey,
    re: *[32]u8,
    rs: *[32]u8,
    msg: []const u8,
    payload_out: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
) !void {
    if (msg.len < 32) return error.HandshakeFailed;
    @memcpy(re, msg[0..32]);
    mixHash(&ss.h, re);
    const ee = try dh(e.secret, re.*);
    ss.mixKey(ee);
    var rest = msg[32..];
    if (rest.len < 48) return error.HandshakeFailed;
    var tmp = std.ArrayList(u8).empty;
    defer tmp.deinit(allocator);
    const dec_s = try ss.decryptAndHash(rest[0..48], &tmp, allocator);
    if (dec_s.len != 32) return error.HandshakeFailed;
    @memcpy(rs, dec_s[0..32]);
    rest = rest[48..];
    const dh_es = try dh(e.secret, rs.*);
    ss.mixKey(dh_es);
    payload_out.clearRetainingCapacity();
    _ = try ss.decryptAndHash(rest, payload_out, allocator);
}

/// Initiator: third message `-> s, se` + libp2p payload; fills transport keys.
fn writeInitiatorMsg3(
    ss: *SymmetricState,
    s: *const DhKey,
    re: *const [32]u8,
    libp2p_payload: []const u8,
    out: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    k1: *[32]u8,
    k2: *[32]u8,
) !void {
    try ss.encryptAndHash(&s.public, out, allocator);
    const dh_se = try dh(s.secret, re.*);
    ss.mixKey(dh_se);
    try ss.encryptAndHash(libp2p_payload, out, allocator);
    ss.split(k1, k2);
}

/// Responder: read third message; fills transport keys.
fn readResponderMsg3(
    ss: *SymmetricState,
    _: *const DhKey,
    e: *const DhKey,
    rs: *[32]u8,
    msg: []const u8,
    payload_out: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    k1: *[32]u8,
    k2: *[32]u8,
) !void {
    if (msg.len < 48) return error.HandshakeFailed;
    var tmp = std.ArrayList(u8).empty;
    defer tmp.deinit(allocator);
    const dec_s = try ss.decryptAndHash(msg[0..48], &tmp, allocator);
    if (dec_s.len != 32) return error.HandshakeFailed;
    @memcpy(rs, dec_s[0..32]);
    const rest = msg[48..];
    const dh_se = try dh(e.secret, rs.*);
    ss.mixKey(dh_se);
    payload_out.clearRetainingCapacity();
    _ = try ss.decryptAndHash(rest, payload_out, allocator);
    ss.split(k1, k2);
}

fn readFull(s: std.net.Stream, buf: []u8) std.net.Stream.ReadError!void {
    var off: usize = 0;
    while (off < buf.len) {
        const n = try s.read(buf[off..]);
        if (n == 0) return error.ConnectionResetByPeer;
        off += n;
    }
}

fn readFrameLen(s: std.net.Stream) !u16 {
    var lenb: [2]u8 = undefined;
    try readFull(s, &lenb);
    return std.mem.readInt(u16, &lenb, .big);
}

fn writeFrame(s: std.net.Stream, body: []const u8) !void {
    // Noise transport frames have a 2-byte BE length prefix: max 65535 bytes.
    // Callers (writeTransport, handshake) must chunk plaintext to fit.
    if (body.len > 65535) return error.NoiseFrameTooLarge;
    var hdr: [2]u8 = undefined;
    std.mem.writeInt(u16, &hdr, @intCast(body.len), .big);
    try s.writeAll(&hdr);
    try s.writeAll(body);
}

pub const CipherState = struct {
    key: [32]u8,
    n: u64 = 0,

    pub fn encrypt(self: *CipherState, plaintext: []const u8, allocator: std.mem.Allocator) ![]u8 {
        const nonce = noiseNonce(self.n);
        self.n += 1;
        const tlen = ChaCha20Poly1305.tag_length;
        var out = try allocator.alloc(u8, plaintext.len + tlen);
        errdefer allocator.free(out);
        var tag: [tlen]u8 = undefined;
        ChaCha20Poly1305.encrypt(out[0..plaintext.len], &tag, plaintext, &.{}, nonce, self.key);
        @memcpy(out[plaintext.len..][0..tlen], &tag);
        return out;
    }

    pub fn decrypt(self: *CipherState, ciphertext: []const u8, allocator: std.mem.Allocator) ![]u8 {
        if (ciphertext.len < ChaCha20Poly1305.tag_length) return error.HandshakeFailed;
        const tlen = ChaCha20Poly1305.tag_length;
        const nonce = noiseNonce(self.n);
        self.n += 1;
        const plain = try allocator.alloc(u8, ciphertext.len - tlen);
        errdefer allocator.free(plain);
        const ct = ciphertext[0 .. ciphertext.len - tlen];
        const tag = ciphertext[ciphertext.len - tlen ..][0..tlen].*;
        ChaCha20Poly1305.decrypt(plain, ct, tag, &.{}, nonce, self.key) catch return error.HandshakeFailed;
        return plain;
    }
};

pub const Session = struct {
    stream: std.net.Stream,
    enc: CipherState,
    dec: CipherState,
    remote_ed25519_pub: [32]u8,

    /// Initiator after `/noise` multistream. `prologue` is usually empty.
    pub fn handshakeInitiator(stream: std.net.Stream, allocator: std.mem.Allocator, ed25519_secret64: [64]u8, prologue: []const u8) !Session {
        const sk = Ed25519.SecretKey.fromBytes(ed25519_secret64) catch return error.BadPayload;
        const ed_kp = Ed25519.KeyPair.fromSecretKey(sk) catch return error.BadPayload;
        const noise_s = try genDhKey();
        var e: DhKey = undefined;
        var ss = SymmetricState.init(protocol_name);
        mixHash(&ss.h, prologue);

        var m1 = std.ArrayList(u8).empty;
        defer m1.deinit(allocator);
        try writeInitiatorMsg1(&ss, &e, &m1, allocator);
        writeFrame(stream, m1.items) catch return error.HandshakeFailed;

        const l2 = readFrameLen(stream) catch return error.HandshakeFailed;
        const buf2 = allocator.alloc(u8, l2) catch return error.HandshakeFailed;
        defer allocator.free(buf2);
        readFull(stream, buf2) catch return error.HandshakeFailed;

        var re: [32]u8 = undefined;
        var rs: [32]u8 = undefined;
        var pay2 = std.ArrayList(u8).empty;
        defer pay2.deinit(allocator);
        readInitiatorMsg2(&ss, &noise_s, &e, &re, &rs, buf2, &pay2, allocator) catch return error.HandshakeFailed;

        const remote_pub = try verifyRemotePayload(pay2.items, &rs, allocator);

        const pl3 = buildHandshakePayload(allocator, ed_kp, &noise_s.public) catch return error.BadPayload;
        defer allocator.free(pl3);
        var m3 = std.ArrayList(u8).empty;
        defer m3.deinit(allocator);
        var tk1: [32]u8 = undefined;
        var tk2: [32]u8 = undefined;
        writeInitiatorMsg3(&ss, &noise_s, &re, pl3, &m3, allocator, &tk1, &tk2) catch return error.HandshakeFailed;
        writeFrame(stream, m3.items) catch return error.HandshakeFailed;

        return .{
            .stream = stream,
            .enc = .{ .key = tk1 },
            .dec = .{ .key = tk2 },
            .remote_ed25519_pub = remote_pub.toBytes(),
        };
    }

    /// Responder after `/noise` multistream.
    pub fn handshakeResponder(stream: std.net.Stream, allocator: std.mem.Allocator, ed25519_secret64: [64]u8, prologue: []const u8) !Session {
        const sk = Ed25519.SecretKey.fromBytes(ed25519_secret64) catch return error.BadPayload;
        const ed_kp = Ed25519.KeyPair.fromSecretKey(sk) catch return error.BadPayload;
        const noise_s = try genDhKey();
        var ss = SymmetricState.init(protocol_name);
        mixHash(&ss.h, prologue);

        const l1 = readFrameLen(stream) catch return error.HandshakeFailed;
        const buf1 = allocator.alloc(u8, l1) catch return error.HandshakeFailed;
        defer allocator.free(buf1);
        readFull(stream, buf1) catch return error.HandshakeFailed;

        var re: [32]u8 = undefined;
        var pay1 = std.ArrayList(u8).empty;
        defer pay1.deinit(allocator);
        readResponderMsg1(&ss, &re, buf1, &pay1, allocator) catch return error.HandshakeFailed;

        const pl2 = buildHandshakePayload(allocator, ed_kp, &noise_s.public) catch return error.BadPayload;
        defer allocator.free(pl2);
        var e: DhKey = undefined;
        var m2 = std.ArrayList(u8).empty;
        defer m2.deinit(allocator);
        writeResponderMsg2(&ss, &noise_s, &re, &e, pl2, &m2, allocator) catch return error.HandshakeFailed;
        writeFrame(stream, m2.items) catch return error.HandshakeFailed;

        const l3 = readFrameLen(stream) catch return error.HandshakeFailed;
        const buf3 = allocator.alloc(u8, l3) catch return error.HandshakeFailed;
        defer allocator.free(buf3);
        readFull(stream, buf3) catch return error.HandshakeFailed;

        var rs: [32]u8 = undefined;
        var pay3 = std.ArrayList(u8).empty;
        defer pay3.deinit(allocator);
        var tk1: [32]u8 = undefined;
        var tk2: [32]u8 = undefined;
        readResponderMsg3(&ss, &noise_s, &e, &rs, buf3, &pay3, allocator, &tk1, &tk2) catch return error.HandshakeFailed;

        const remote_pk = try verifyRemotePayload(pay3.items, &rs, allocator);

        return .{
            .stream = stream,
            .enc = .{ .key = tk2 },
            .dec = .{ .key = tk1 },
            .remote_ed25519_pub = remote_pk.toBytes(),
        };
    }

    /// Encrypt+send transport frames (2-byte BE length + ciphertext).
    /// Chunks plaintext to respect the 65535-byte Noise transport frame limit.
    /// Each chunk is encrypted independently (nonce auto-increments per call),
    /// and the receiver accumulates chunks via YamuxOverNoise.feed.
    pub fn writeTransport(self: *Session, allocator: std.mem.Allocator, plaintext: []const u8) !void {
        const tlen = ChaCha20Poly1305.tag_length;
        const max_plaintext: usize = 65535 - tlen; // 65519
        var off: usize = 0;
        while (true) {
            const n = @min(max_plaintext, plaintext.len - off);
            const ct = try self.enc.encrypt(plaintext[off .. off + n], allocator);
            defer allocator.free(ct);
            try writeFrame(self.stream, ct);
            off += n;
            if (off >= plaintext.len) break;
        }
    }

    /// Read+decrypt one transport frame.
    pub fn readTransport(self: *Session, allocator: std.mem.Allocator) ![]u8 {
        const n = try readFrameLen(self.stream);
        const buf = try allocator.alloc(u8, n);
        defer allocator.free(buf); // ciphertext buffer; decrypt returns a fresh plaintext buffer
        try readFull(self.stream, buf);
        return self.dec.decrypt(buf, allocator);
    }
};

test "noise XX local handshake" {
    const gpa = std.testing.allocator;
    const kp = Ed25519.KeyPair.generate();
    const sec = kp.secret_key.toBytes();

    const addr = try std.net.Address.parseIp("127.0.0.1", 0);
    var srv = try addr.listen(.{ .reuse_address = true });
    defer srv.deinit();
    const port = srv.listen_address.in.getPort();

    const Client = struct {
        fn run(p: u16, sk: [64]u8) void {
            const conn = std.net.tcpConnectToHost(std.heap.page_allocator, "127.0.0.1", p) catch unreachable;
            defer conn.close();
            var cli = Session.handshakeInitiator(conn, std.heap.page_allocator, sk, &.{}) catch unreachable;
            cli.writeTransport(std.heap.page_allocator, "ping") catch unreachable;
            const pong = cli.readTransport(std.heap.page_allocator) catch unreachable;
            defer std.heap.page_allocator.free(pong);
            std.testing.expectEqualStrings("pong", pong) catch unreachable;
        }
    };

    var th = try std.Thread.spawn(.{}, Client.run, .{ port, sec });
    defer th.join();

    const conn = try srv.accept();
    defer conn.stream.close();
    var ser = try Session.handshakeResponder(conn.stream, gpa, sec, &.{});
    const ping = try ser.readTransport(gpa);
    defer gpa.free(ping);
    try std.testing.expectEqualStrings("ping", ping);
    try ser.writeTransport(gpa, "pong");
}

test "noise transport chunks plaintext > 64KB" {
    const gpa = std.testing.allocator;
    const kp = Ed25519.KeyPair.generate();
    const sec = kp.secret_key.toBytes();

    const addr = try std.net.Address.parseIp("127.0.0.1", 0);
    var srv = try addr.listen(.{ .reuse_address = true });
    defer srv.deinit();
    const port = srv.listen_address.in.getPort();

    // 200KB payload with a recognizable byte pattern
    const payload_size: usize = 200 * 1024;
    const payload = try gpa.alloc(u8, payload_size);
    defer gpa.free(payload);
    for (payload, 0..) |*b, i| b.* = @intCast(i & 0xff);

    const Client = struct {
        fn run(p: u16, sk: [64]u8, pl: []const u8) void {
            const conn = std.net.tcpConnectToHost(std.heap.page_allocator, "127.0.0.1", p) catch unreachable;
            defer conn.close();
            var cli = Session.handshakeInitiator(conn, std.heap.page_allocator, sk, &.{}) catch unreachable;
            cli.writeTransport(std.heap.page_allocator, pl) catch unreachable;
        }
    };

    var th = try std.Thread.spawn(.{}, Client.run, .{ port, sec, payload });
    defer th.join();

    const conn = try srv.accept();
    defer conn.stream.close();
    var ser = try Session.handshakeResponder(conn.stream, gpa, sec, &.{});

    // Accumulate across multiple transport frames (receiver-side chunking reassembly).
    var acc = std.ArrayList(u8).empty;
    defer acc.deinit(gpa);
    while (acc.items.len < payload_size) {
        const chunk = try ser.readTransport(gpa);
        defer gpa.free(chunk);
        try acc.appendSlice(gpa, chunk);
    }
    try std.testing.expectEqual(payload_size, acc.items.len);
    try std.testing.expectEqualSlices(u8, payload, acc.items);
}
