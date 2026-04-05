# zipfs

<img width="400" height="400" alt="zipfs" src="https://github.com/user-attachments/assets/08870e43-14f6-43d7-b195-35fc80d9bd72" />

An [IPFS](https://ipfs.tech/) content-addressed node written in [Zig](https://ziglang.org/). It implements local UnixFS-style files and directories, a blockstore, CID v0/v1, and enough **Kubo-compatible libp2p wire formats** (Noise, yamux, multistream, bitswap 1.2.0, Kademlia DHT, Identify) to **dial the public network**, **serve inbound peers**, and **fetch or provide** content in a way that interoperates with mainstream IPFS nodes.

This is **not** a full Kubo replacement: scope is intentionally smaller, but the project aims for correct stacks and keyspaces where it touches the network.

## Requirements

- **Zig 0.15.1** or newer (see `build.zig.zon` → `minimum_zig_version`)
- No external runtime dependencies for the binary (pure Zig + libc where the platform requires it)

## Build

```sh
zig build
```

The CLI is installed to `zig-out/bin/zipfs` by default.

```sh
zig build -Doptimize=ReleaseFast
```

## Run tests

```sh
zig build test
```

## Quick start

### Repository path

All persistent data lives under a repo directory:

- If **`IPFS_PATH`** is set, that directory is used.
- Otherwise the default is **`.zig-ipfs`** in the current working directory.

Typical layout:

| Path | Purpose |
|------|---------|
| `config.json` | Listen addresses, bootstrap peers, gateway port, announce addresses, reprovide interval |
| `identity.key` | 64-byte Ed25519 secret for libp2p (Noise, PeerID); created on first use |
| `blocks/` | Sharded raw blocks (exported after mutating commands) |
| `pins.json` | Direct and recursive pins |

### Initialize config

```sh
export IPFS_PATH=/path/to/my-repo   # optional
zig-out/bin/zipfs config init
```

This writes `config.json` with default **listen** (`/ip4/0.0.0.0/tcp/4001`) and **bootstrap** peers.

### Add and read a file

```sh
zig-out/bin/zipfs add ./README.md
zig-out/bin/zipfs cat <cid>
```

Recursive directory add:

```sh
zig-out/bin/zipfs add -r ./some-dir
```

### HTTP gateway (read-only)

```sh
zig-out/bin/zipfs gateway
```

Serves `http://0.0.0.0:<gateway_port>/ipfs/<cid>/...` (default port **8080** from config).

### Daemon (gateway + libp2p swarm)

```sh
zig-out/bin/zipfs daemon
```

Runs:

- The same HTTP gateway (with a mutex shared with the swarm thread for the blockstore).
- A **TCP swarm listener** on `listen_addrs` (default all interfaces, port **4001**): multistream → Noise → yamux → **Identify** + **bitswap** + (inbound multiprotocol negotiation per stream).

For **WAN discoverability**, set **`announce_addrs`** in `config.json` to your public dialable multiaddrs (for example `/ip4/<public-ip>/tcp/4001`). Without this, Identify may only advertise loopback-derived addresses.

## Configuration (`config.json`)

| Field | Meaning |
|-------|---------|
| `chunk_size` | UnixFS file chunk size in bytes (default `262144`) |
| `gateway_port` | HTTP gateway port (default `8080`) |
| `listen_addrs` | Multiaddrs for the swarm listener (text form, e.g. `/ip4/0.0.0.0/tcp/4001`) |
| `bootstrap_peers` | Bootstrap multiaddrs for DHT operations. Supports **`/dnsaddr/.../p2p/...`** (resolved via DNS TXT `_dnsaddr.<domain>`) as well as `/ip4/.../tcp/.../p2p/...` |
| `announce_addrs` | Binary Identify `listenAddrs` built from these strings; use public IPs for internet peers |
| `reprovide_interval_secs` | Daemon-only: seconds between **ADD_PROVIDER** passes for **recursive** pins. Omitted or `null` behaves like **43200** (12h). Set to **0** to disable |

## Command reference

Run with no arguments to print a short help summary.

### Data and repo

| Command | Description |
|---------|-------------|
| `add [-r] <path>` | Add a file or directory (UnixFS); exports blocks to `$IPFS_PATH` |
| `cat <cid> [subpath]` | Cat file or path inside a directory CID |
| `cat --net <cid>` | If the block is missing locally, **DHT + bitswap** fetch, then cat |
| `ls <cid> [path]` | List directory |
| `block put <path>` / `block get <cid>` | Raw block I/O |
| `dag car import <path>` / `dag car export <path>` | CAR import/export |
| `pin add [-r] <cid>` / `pin rm [-r] <cid>` / `pin ls` | Pins |
| `repo gc` | Mark-sweep GC vs pins; updates on-disk blocks |

### Config

| Command | Description |
|---------|-------------|
| `config init` | Write default `config.json` |

### Gateway and daemon

| Command | Description |
|---------|-------------|
| `gateway` | HTTP gateway only |
| `daemon` | Gateway + libp2p swarm + optional periodic reprovide |

### Network debugging and publishing

| Command | Description |
|---------|-------------|
| `id` | Print a **new** ephemeral Ed25519 PeerID (does not use repo identity) |
| `net fetch <cid>` | Iterative **GET_PROVIDERS** walk + bitswap into local repo |
| `net provide <cid>` | **ADD_PROVIDER** for the CID to closest DHT peers |
| `net dial-noise <host> <port>` | Noise handshake; print remote PeerID |
| `net dial-bitswap <host> <port> <cid>` | Bitswap want-have / want-block against a single peer |
| `net echo-serve <port>` / `net echo-dial <host> <port>` | TCP echo (dev aid) |

## Networking and Kubo interoperability

### Stack (outbound)

Typical dial path: **TCP** → multistream **`/noise`** → **Noise XX** → multistream **`/yamux/1.0.0`** → **yamux** → per-stream multistream → **`/ipfs/id/1.0.0`** (Identify) and/or **`/ipfs/bitswap/1.2.0`** or **`/ipfs/kad/1.0.0`**.

### Bitswap

- Wantlist supports **HAVE** and **BLOCK** want types and **block presences** (have / don’t-have), aligned with Kubo-style messages.
- Fetch uses a HAVE+BLOCK round, then a block-only round when the peer signals **have** without sending data in the first response.

### DHT

- Provider records use the **`/providers/` + multihash** keyspace consistent with Kubo.
- Walks use **GET_PROVIDERS**, **FIND_NODE** (including bootstrap expansion), and closest-peer replication for **ADD_PROVIDER**.

### Bootstrap resolution

Entries like `/dnsaddr/bootstrap.libp2p.io/p2p/Qm…` are expanded by querying **`_dnsaddr.<domain>`** TXT records (first `nameserver` in `/etc/resolv.conf`, or **8.8.8.8**), then normalizing `dnsaddr=/ip4/...` style answers into concrete multiaddrs.

### Security note

You are opening **encrypted TCP** to the public IPFS network. Use an isolated repo and identity for experiments; keep **`identity.key`** private.

## Library

The core logic is exposed as the Zig module **`zipfs`** (`src/root.zig`), importable from this package or by path in another `build.zig`. It includes:

- CID, multihash, multibase, varint, protobuf wire helpers  
- UnixFS import/resolution, DAG-PB, blockstore  
- Bitswap and DHT message types, DHT walks, libp2p dial/serve helpers  

The CLI (`src/main.zig`) is a thin wrapper over that module.

Example dependency in another project (Zig package manager): add this package and depend on the `zipfs` module from `build.zig` (see Zig documentation for `addModule` / `lazyDependency`).

## Project layout (source)

```
src/
  main.zig          # CLI entry
  root.zig          # Library exports + tests
  bitswap.zig       # Bitswap protobuf messages
  dht.zig           # DHT protobuf messages / provider keys
  cid.zig           # CID parsing and bytes
  repo.zig          # On-disk block layout, IPFS_PATH
  config.zig        # config.json
  gateway.zig       # HTTP gateway
  net/              # libp2p: noise, yamux, multistream, dial, serve, dnsaddr, etc.
```

## Limitations (non-exhaustive)

- Not all Kubo subsystems (full bitswap sessions, relay, QUIC, mDNS, comprehensive observability, etc.) are implemented.
- IPNS / MFS in the tree are partial or demo-level compared to Kubo.
- You should validate behavior against your own threat model before production use.

## Contributing

Issues and PRs welcome. Run **`zig build test`** before submitting changes.

## License

No `LICENSE` file is present in this repository yet; clarify terms with the maintainers before redistributing.
