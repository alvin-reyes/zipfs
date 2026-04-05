//! Release metadata (libp2p Identify agent, CLI, `build.zig.zon` should match `semver`).

/// Semantic version; keep in sync with `build.zig.zon` `version`.
pub const semver = "0.1.0";

/// `agentVersion` in libp2p Identify and `zipfs id` output.
pub const agent_version = "zipfs/0.1.0";
