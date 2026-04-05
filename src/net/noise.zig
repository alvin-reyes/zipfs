//! Noise XX handshake (libp2p) — placeholder for full crypto handoff.

const std = @import("std");

pub const NoiseSession = struct {
    pub fn init() NoiseSession {
        return .{};
    }

    /// Real interop requires Curve25519 + ChaChaPoly; not wired yet.
    pub fn handshakeComplete(_: *NoiseSession) error{NoiseNotImplemented}!void {
        return error.NoiseNotImplemented;
    }
};
