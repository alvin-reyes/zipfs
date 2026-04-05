# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-04-04

### Added

- Local IPFS-style repo: sharded blockstore, `IPFS_PATH` / default `.zig-ipfs`, `config.json`.
- UnixFS import (`add`, `add -r`), `cat`, `ls`, `block put|get`, DAG CAR import/export.
- Pins (`pin add|rm|ls`), `repo gc`.
- Read-only HTTP gateway and `daemon` mode with libp2p swarm (Noise, yamux, multistream, Identify, bitswap 1.2.0, Kademlia DHT).
- Network: `net fetch`, `net provide`, `cat --net`, DHT walks (GET_PROVIDERS, FIND_NODE, ADD_PROVIDER with dialable multiaddrs), `/dnsaddr` bootstrap resolution, periodic reprovide for recursive pins.
- Kubo-oriented wire formats where implemented (provider keyspace, bitswap want-have / presences).

### Notes

- Not a full Kubo replacement; validate for your use case before relying on it in production.
