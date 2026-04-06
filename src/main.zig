const std = @import("std");
const zipfs = @import("zipfs");

fn printPeerIdP2pLine(
    allocator: std.mem.Allocator,
    stderr: anytype,
    secret64: [64]u8,
) !void {
    const sk = try std.crypto.sign.Ed25519.SecretKey.fromBytes(secret64);
    const kp = try std.crypto.sign.Ed25519.KeyPair.fromSecretKey(sk);
    const pub_bytes = kp.public_key.toBytes();
    const pid = try zipfs.net_peer_id.peerIdString(allocator, &pub_bytes);
    defer allocator.free(pid);
    try stderr.print("PeerID: /p2p/{s}\n", .{pid});
}

fn daemonReprovideLoop(repo_owned: []const u8, secret: [64]u8, interval_ns: u64) void {
    const a = std.heap.page_allocator;
    while (true) {
        var maybe_cfg = zipfs.config.Config.load(a, repo_owned) catch null;
        if (maybe_cfg) |*cfg| {
            defer cfg.deinit(a);
            const default_bs = zipfs.config.default_bootstrap_peers;
            const peer_list: []const []const u8 = if (cfg.bootstrap_peers.len > 0) cfg.bootstrap_peers else default_bs[0..];
            const resolved = zipfs.net_bootstrap_resolve.resolveBootstrapPeers(a, peer_list) catch continue;
            defer zipfs.net_bootstrap_resolve.freeResolved(a, resolved);
            const swarm_port = zipfs.net_swarm_config.swarmTcpPortFromConfig(a, cfg);
            const bins = zipfs.net_swarm_config.buildIdentifyListenBinaries(a, cfg, swarm_port) catch continue;
            defer {
                for (bins) |b| a.free(b);
                a.free(bins);
            }
            var pins = zipfs.pin.PinSet.load(a, repo_owned) catch continue;
            defer pins.deinit(a);
            var it = pins.recursive.keyIterator();
            while (it.next()) |k| {
                zipfs.net_libp2p_provide.provideCid(a, k.*, resolved, secret, .{}, 8, bins) catch {};
            }
        }
        std.Thread.sleep(interval_ns);
    }
}

pub fn main() !void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();

    var args = try std.process.argsWithAllocator(gpa);
    defer args.deinit();

    _ = args.skip();

    var stderr_buf: [512]u8 = undefined;
    var stderr_filew = std.fs.File.stderr().writer(&stderr_buf);
    const stderr = &stderr_filew.interface;
    defer stderr.flush() catch {};

    const repo_root = try zipfs.repo.repoRootFromEnv(gpa);
    defer gpa.free(repo_root);

    var cfg = try zipfs.config.Config.load(gpa, repo_root);
    defer cfg.deinit(gpa);

    const cmd = args.next() orelse {
        try stderr.writeAll(
            \\zipfs — IPFS-style node (Zig, local + gateway + Kubo wire formats)
            \\
            \\Data:   add [-r] <path>  cat <cid>  ls <cid> [path]  block put|get
            \\        dag car import|export <path>  config init
            \\        pin add [-r] <cid>  pin rm [-r] <cid>  pin ls  repo gc
            \\        gateway | daemon   (HTTP + libp2p swarm on listen_addrs from config)
            \\Net:    net echo-serve <port>  net echo-dial <host> <port>
            \\        net dial-noise <host> <port>   net dial-bitswap <host> <port> <cid>
            \\        net fetch <cid>  (iterative DHT + bitswap)  net provide <cid>  (ADD_PROVIDER to closest peers)
            \\        id  (peer id from new Ed25519 key)
            \\        cat --net <cid>   (fetch over IPFS if missing locally, then cat)
            \\Cluster: cluster peers add <multiaddr>  cluster peers rm <multiaddr>  cluster peers ls
            \\         cluster status  cluster replicate <cid> [--factor N]
            \\         cluster shard <cid> [--shards M]  cluster sync
            \\         cluster manifest ls  cluster manifest status <cid>
            \\
        );
        return;
    };

    if (std.mem.eql(u8, cmd, "config")) {
        const sub = args.next() orelse {
            try stderr.writeAll("usage: zipfs config init\n");
            return error.BadArgs;
        };
        if (std.mem.eql(u8, sub, "init")) {
            var c = try zipfs.config.Config.initWithMainnetDefaults(gpa);
            defer c.deinit(gpa);
            try c.save(gpa, repo_root);
            try stderr.writeAll("wrote config.json (listen_addrs + bootstrap_peers)\n");
            return;
        }
        try stderr.print("unknown config subcommand: {s}\n", .{sub});
        return error.BadArgs;
    }

    if (std.mem.eql(u8, cmd, "id")) {
        const kp = zipfs.net_peer_id.generateKeyPair();
        const pid = try zipfs.net_peer_id.peerIdString(gpa, &kp.public_key);
        defer gpa.free(pid);
        const line = try std.fmt.allocPrint(gpa, "AgentVersion: {s}\nPeerID: {s}\n", .{ zipfs.version.agent_version, pid });
        defer gpa.free(line);
        try std.fs.File.stdout().writeAll(line);
        return;
    }

    if (std.mem.eql(u8, cmd, "add")) {
        var recurse = false;
        const p1 = args.next() orelse {
            try stderr.writeAll("add: missing path\n");
            return error.BadArgs;
        };
        const path = if (std.mem.eql(u8, p1, "-r")) blk: {
            recurse = true;
            break :blk args.next() orelse {
                try stderr.writeAll("add -r: missing path\n");
                return error.BadArgs;
            };
        } else p1;

        var node: zipfs.Node = .{};
        defer node.deinit(gpa);
        node.store.repo_root = repo_root;
        const root = if (recurse)
            try node.addDirectory(gpa, path, &cfg)
        else blk: {
            const data = try std.fs.cwd().readFileAlloc(gpa, path, std.math.maxInt(usize));
            defer gpa.free(data);
            break :blk try node.addFileWithConfig(gpa, data, &cfg);
        };
        defer root.deinit(gpa);
        const s = try root.toString(gpa);
        defer gpa.free(s);
        try std.fs.File.stdout().writeAll(s);
        try std.fs.File.stdout().writeAll("\n");
        return;
    }

    if (std.mem.eql(u8, cmd, "cat")) {
        const cid_arg = args.next() orelse {
            try stderr.writeAll("cat: missing cid\n");
            return error.BadArgs;
        };
        const use_net = std.mem.eql(u8, cid_arg, "--net");
        const cid_str = if (use_net) args.next() orelse {
            try stderr.writeAll("cat --net: missing cid\n");
            return error.BadArgs;
        } else cid_arg;
        const rpath = args.next() orelse "";
        var node: zipfs.Node = .{};
        defer node.deinit(gpa);
        node.store.repo_root = repo_root;
        if (use_net and !node.store.has(cid_str)) {
            const sec = try zipfs.net_identity.loadOrCreateSecret64(gpa, repo_root);
            const default_bs = zipfs.config.default_bootstrap_peers;
            const peers = if (cfg.bootstrap_peers.len > 0) cfg.bootstrap_peers else default_bs[0..];
            _ = zipfs.net_libp2p_fetch.fetchBlockIntoStore(gpa, &node.store, cid_str, peers, sec) catch |err| {
                try stderr.print("cat --net: fetch failed: {}\n", .{err});
                return err;
            };
        }
        const out = if (rpath.len == 0)
            try node.catFile(gpa, cid_str)
        else
            try node.catFileAtPath(gpa, cid_str, rpath);
        defer gpa.free(out);
        try std.fs.File.stdout().writeAll(out);
        return;
    }

    if (std.mem.eql(u8, cmd, "ls")) {
        const cid_str = args.next() orelse {
            try stderr.writeAll("ls: missing cid\n");
            return error.BadArgs;
        };
        const rpath = args.next() orelse "";
        var node: zipfs.Node = .{};
        defer node.deinit(gpa);
        node.store.repo_root = repo_root;
        var dir = try node.listDir(gpa, cid_str, rpath);
        defer dir.deinit();
        for (dir.entries) |e| {
            const line = try std.fmt.allocPrint(gpa, "{s}\t{d}\n", .{ e.name, e.size });
            defer gpa.free(line);
            try std.fs.File.stdout().writeAll(line);
        }
        return;
    }

    if (std.mem.eql(u8, cmd, "dag")) {
        const sub = args.next() orelse {
            try stderr.writeAll("dag: need car import|export\n");
            return error.BadArgs;
        };
        if (!std.mem.eql(u8, sub, "car")) {
            try stderr.writeAll("only 'dag car' supported\n");
            return error.BadArgs;
        }
        const op = args.next() orelse {
            try stderr.writeAll("dag car: import|export <path>\n");
            return error.BadArgs;
        };
        const path = args.next() orelse {
            try stderr.writeAll("dag car: missing path\n");
            return error.BadArgs;
        };
        if (std.mem.eql(u8, op, "export")) {
            var node: zipfs.Node = .{};
            defer node.deinit(gpa);
            node.store.repo_root = repo_root;
            try zipfs.car.exportStoreToFile(gpa, &node.store, path);
            return;
        }
        if (std.mem.eql(u8, op, "import")) {
            var node: zipfs.Node = .{};
            defer node.deinit(gpa);
            node.store.repo_root = repo_root;
            var f = try std.fs.cwd().openFile(path, .{});
            defer f.close();
            try zipfs.car.importFromSeekableFile(gpa, f, &node.store);
            try stderr.writeAll("imported CAR into repo\n");
            return;
        }
        try stderr.writeAll("dag car: use import or export\n");
        return error.BadArgs;
    }

    if (std.mem.eql(u8, cmd, "pin")) {
        const sub = args.next() orelse {
            try stderr.writeAll("pin: add|rm|ls\n");
            return error.BadArgs;
        };
        var pins = try zipfs.pin.PinSet.load(gpa, repo_root);
        defer pins.deinit(gpa);
        if (std.mem.eql(u8, sub, "ls")) {
            try stderr.writeAll("recursive:\n");
            var it = pins.recursive.keyIterator();
            while (it.next()) |k| try stderr.print("  {s}\n", .{k.*});
            try stderr.writeAll("direct:\n");
            var it2 = pins.direct.keyIterator();
            while (it2.next()) |k| try stderr.print("  {s}\n", .{k.*});
            return;
        }
        if (std.mem.eql(u8, sub, "add")) {
            var rec = false;
            const a1 = args.next() orelse {
                try stderr.writeAll("pin add [-r] <cid>\n");
                return error.BadArgs;
            };
            const cidv = if (std.mem.eql(u8, a1, "-r")) blk: {
                rec = true;
                break :blk args.next() orelse {
                    try stderr.writeAll("pin add -r: missing cid\n");
                    return error.BadArgs;
                };
            } else a1;
            if (rec) try pins.pinRecursive(gpa, cidv) else try pins.pinDirect(gpa, cidv);
            try pins.save(gpa, repo_root);
            // Notify replication inbox for immediate cluster replication
            zipfs.pin.notifyInbox(gpa, repo_root, cidv) catch {};
            return;
        }
        if (std.mem.eql(u8, sub, "rm")) {
            var rec = false;
            const a1 = args.next() orelse {
                try stderr.writeAll("pin rm [-r] <cid>\n");
                return error.BadArgs;
            };
            const cidv = if (std.mem.eql(u8, a1, "-r")) blk: {
                rec = true;
                break :blk args.next() orelse {
                    try stderr.writeAll("pin rm -r: missing cid\n");
                    return error.BadArgs;
                };
            } else a1;
            if (rec) pins.unpinRecursive(gpa, cidv) else pins.unpinDirect(gpa, cidv);
            try pins.save(gpa, repo_root);
            return;
        }
        try stderr.print("pin: unknown {s}\n", .{sub});
        return error.BadArgs;
    }

    if (std.mem.eql(u8, cmd, "repo")) {
        const sub = args.next() orelse {
            try stderr.writeAll("repo: gc\n");
            return error.BadArgs;
        };
        if (std.mem.eql(u8, sub, "gc")) {
            var pins = try zipfs.pin.PinSet.load(gpa, repo_root);
            defer pins.deinit(gpa);
            var node: zipfs.Node = .{};
            defer node.deinit(gpa);
            node.store.repo_root = repo_root;
            const n = try zipfs.pin.gc(gpa, &node.store, &pins, repo_root);
            try stderr.print("removed {d} blocks\n", .{n});
            return;
        }
        try stderr.print("repo: unknown {s}\n", .{sub});
        return error.BadArgs;
    }

    if (std.mem.eql(u8, cmd, "gateway") or std.mem.eql(u8, cmd, "daemon")) {
        // Auto-init: persist default config if none exists (needed for Railway/Docker first boot)
        {
            const cfg_path = try std.fs.path.join(gpa, &.{ repo_root, "config.json" });
            defer gpa.free(cfg_path);
            std.fs.cwd().access(cfg_path, .{}) catch |err| {
                if (err == error.FileNotFound) {
                    cfg.save(gpa, repo_root) catch |se| {
                        try stderr.print("warn: failed to auto-init config: {}\n", .{se});
                    };
                }
            };
        }

        var node: zipfs.Node = .{};
        defer node.deinit(gpa);
        node.store.repo_root = repo_root;
        try node.store.initCache(gpa, cfg.block_cache_size orelse 1024);

        if (std.mem.eql(u8, cmd, "daemon")) {
            const sec = try zipfs.net_identity.loadOrCreateSecret64(gpa, repo_root);
            const swarm_port: u16 = blk: {
                if (cfg.listen_addrs.len == 0) break :blk 4001;
                if (zipfs.net_multiaddr.parseStringTcp(gpa, cfg.listen_addrs[0])) |t| {
                    defer t.deinit(gpa);
                    break :blk t.port;
                } else |_| {}
                break :blk 4001;
            };
            const saddr = try std.net.Address.parseIp("0.0.0.0", swarm_port);
            const swarm_srv = try saddr.listen(.{ .reuse_address = true });
            // Server stays open for the accept thread for process lifetime (no defer deinit).

            const listen_bins = try zipfs.net_swarm_config.buildIdentifyListenBinaries(gpa, &cfg, swarm_port);
            const sk = try std.crypto.sign.Ed25519.SecretKey.fromBytes(sec);
            const kp = try std.crypto.sign.Ed25519.KeyPair.fromSecretKey(sk);
            const pub_bytes = kp.public_key.toBytes();

            var sync_mu = std.Thread.Mutex{};
            var swarm_ctx = zipfs.net_libp2p_serve.SwarmThreadCtx{
                .server = swarm_srv,
                .secret = sec,
                .public_key = pub_bytes,
                .listen_addrs_bin = listen_bins,
                .store = &node.store,
                .mu = &sync_mu,
                .cluster_secret = cfg.cluster_secret,
            };
            _ = try std.Thread.spawn(.{}, zipfs.net_libp2p_serve.swarmAcceptLoop, .{&swarm_ctx});

            const reprov_secs = cfg.reprovide_interval_secs orelse 43200;
            if (reprov_secs > 0) {
                const rr = try gpa.dupe(u8, repo_root);
                _ = try std.Thread.spawn(.{}, daemonReprovideLoop, .{ rr, sec, @as(u64, reprov_secs) * std.time.ns_per_s });
            }

            // Cluster self-healing thread
            var heal_ctx: zipfs.replication.SelfHealCtx = undefined;
            const cluster_enabled = cfg.cluster_mode != null and cfg.cluster_peers.len > 0;

            // Replication scheduler components (initialized if cluster enabled)
            var repl_q: zipfs.repl_queue.ReplQueue = undefined;
            var rate_lim: zipfs.repl_queue.RateLimiter = undefined;
            var peer_conc: zipfs.repl_queue.PeerConcurrency = undefined;
            var push_pool: zipfs.net_conn_pool.ConnPool = undefined;
            var sched_ctx: zipfs.repl_scheduler.SchedulerCtx = undefined;
            var pull_ctx: zipfs.pull_engine.PullCtx = undefined;
            var state_mu = std.Thread.Mutex{}; // Guards ClusterState load/save across threads

            if (cluster_enabled) {
                const c_mode = if (cfg.cluster_mode) |m| zipfs.cluster.ClusterMode.fromString(m) orelse .replicate else .replicate;
                const repl_factor = cfg.replication_factor orelse 2;
                const shard_cnt = cfg.shard_count orelse 4;

                // Initialize replication queue and scheduler
                repl_q = zipfs.repl_queue.ReplQueue.init(gpa, cfg.repl_queue_max_size orelse 4096);
                rate_lim = zipfs.repl_queue.RateLimiter.init(cfg.repl_rate_limit orelse 20);
                peer_conc = zipfs.repl_queue.PeerConcurrency.init(gpa, cfg.repl_max_per_peer orelse 2);
                push_pool = zipfs.net_conn_pool.ConnPool.init(gpa);

                heal_ctx = .{
                    .store = &node.store,
                    .mu = &sync_mu,
                    .repo_root = try gpa.dupe(u8, repo_root),
                    .ed25519_secret64 = sec,
                    .interval_ns = @as(u64, cfg.self_heal_interval_secs orelse 300) * std.time.ns_per_s,
                    .cluster_secret = cfg.cluster_secret,
                    .cluster_mode = c_mode,
                    .replication_factor = repl_factor,
                    .shard_count = shard_cnt,
                    .queue = &repl_q,
                    .state_mu = &state_mu,
                };
                _ = try std.Thread.spawn(.{}, zipfs.replication.selfHealingLoop, .{&heal_ctx});

                sched_ctx = .{
                    .queue = &repl_q,
                    .store = &node.store,
                    .mu = &sync_mu,
                    .repo_root = try gpa.dupe(u8, repo_root),
                    .ed25519_secret64 = sec,
                    .cluster_secret = cfg.cluster_secret,
                    .cluster_mode = c_mode,
                    .replication_factor = repl_factor,
                    .shard_count = shard_cnt,
                    .inbox_poll_ns = @as(u64, cfg.repl_inbox_poll_secs orelse 2) * std.time.ns_per_s,
                    .rate_limiter = &rate_lim,
                    .peer_concurrency = &peer_conc,
                    .max_per_peer = cfg.repl_max_per_peer orelse 2,
                    .batch_size = cfg.repl_batch_size orelse 50,
                    .cluster_peers = cfg.cluster_peers,
                    .state_mu = &state_mu,
                    .pool = &push_pool,
                    .pull_replication_threshold = cfg.pull_replication_threshold orelse 0,
                    .gateway_port = cfg.gateway_port,
                    .local_gateway_host = if (cfg.announce_addrs.len > 0) blk: {
                        const hp = zipfs.net_cluster_push.parseHostPort(gpa, cfg.announce_addrs[0]) catch break :blk "127.0.0.1";
                        break :blk hp.host; // process-lifetime, intentionally not freed
                    } else "127.0.0.1",
                };
                _ = try std.Thread.spawn(.{}, zipfs.repl_scheduler.inboxPollLoop, .{&sched_ctx});
                _ = try std.Thread.spawn(.{}, zipfs.repl_scheduler.schedulerLoop, .{&sched_ctx});

                // Pull replication worker thread (watches for manifest notifications)
                if (cfg.pull_replication_threshold != null) {
                    pull_ctx = .{
                        .store = &node.store,
                        .mu = &sync_mu,
                        .repo_root = try gpa.dupe(u8, repo_root),
                        .cluster_secret = cfg.cluster_secret,
                        .max_concurrent_pulls = cfg.pull_concurrency orelse 4,
                        .pull_batch_size = cfg.pull_batch_size orelse 32,
                    };
                    _ = try std.Thread.spawn(.{}, zipfs.pull_engine.pullWorkerLoop, .{&pull_ctx});
                }
            }

            try stderr.print("zipfs {s} daemon starting...\n", .{zipfs.version.semver});
            try printPeerIdP2pLine(gpa, stderr, sec);
            try stderr.print("Gateway (read-only HTTP): http://127.0.0.1:{d}/ipfs/<cid>/...\n", .{cfg.gateway_port});
            try stderr.print("Libp2p swarm (Noise+yamux+identify+bitswap): 0.0.0.0:{d}\n", .{swarm_port});
            if (cfg.announce_addrs.len == 0) {
                try stderr.writeAll("Hint: set \"announce_addrs\" in config.json to your public /ip4/HOST/tcp/PORT for WAN peers (Identify).\n");
            }
            if (reprov_secs > 0) {
                try stderr.print("DHT reprovide for recursive pins every {d}s (set reprovide_interval_secs to 0 to disable).\n", .{reprov_secs});
            } else {
                try stderr.writeAll("DHT periodic reprovide disabled (reprovide_interval_secs: 0).\n");
            }
            if (cluster_enabled) {
                const mode_str = if (cfg.cluster_mode) |m| m else "replicate";
                try stderr.print("Cluster mode: {s} | peers: {d} | heal interval: {d}s\n", .{
                    mode_str, cfg.cluster_peers.len, cfg.self_heal_interval_secs orelse 300,
                });
                try stderr.print("Replication scheduler: inbox poll {d}s | queue max {d} | rate {d}/s | batch {d}\n", .{
                    cfg.repl_inbox_poll_secs orelse 2,
                    cfg.repl_queue_max_size orelse 4096,
                    cfg.repl_rate_limit orelse 20,
                    cfg.repl_batch_size orelse 50,
                });
                if (cfg.sync_replication orelse false) {
                    try stderr.writeAll("Synchronous replication: ENABLED (uploads block until peers confirm)\n");
                } else {
                    try stderr.writeAll("Direct queue injection: ENABLED (bypasses file inbox)\n");
                }
                if (cfg.pull_replication_threshold) |thresh| {
                    try stderr.print("Pull replication: ENABLED (threshold {d} bytes, concurrency {d}, batch {d})\n", .{
                        thresh,
                        cfg.pull_concurrency orelse 4,
                        cfg.pull_batch_size orelse 32,
                    });
                }
            }
            try stderr.print("HTTP API: POST http://127.0.0.1:{d}/api/v0/add (Kubo-compatible)\n", .{cfg.gateway_port});
            try stderr.writeAll("Ctrl+C to stop.\n\n");
            try stderr.flush();
            const pid_str = try zipfs.net_peer_id.peerIdString(gpa, &pub_bytes);
            const gw_ctx = zipfs.gateway.GatewayCtx{
                .store = &node.store,
                .store_sync = &sync_mu,
                .repo_root = repo_root,
                .chunk_size = cfg.chunk_size,
                .port = cfg.gateway_port,
                .peer_id = pid_str,
                .listen_addrs = cfg.listen_addrs,
                .announce_addrs = cfg.announce_addrs,
                .cluster_peers = cfg.cluster_peers,
                .replication_factor = cfg.replication_factor orelse 2,
                .max_conns = cfg.max_gateway_conns orelse 64,
                .queue = if (cluster_enabled) &repl_q else null,
                .cluster_mode = if (cluster_enabled) (if (cfg.cluster_mode) |m| zipfs.cluster.ClusterMode.fromString(m) orelse .replicate else .replicate) else .replicate,
                .sync_repl = if (cluster_enabled) (cfg.sync_replication orelse false) else false,
                .ed25519_secret64 = if (cluster_enabled) sec else null,
                .cluster_secret = cfg.cluster_secret,
                .state_mu = if (cluster_enabled) &state_mu else null,
            };
            try zipfs.gateway.run(gpa, &gw_ctx);
        } else {
            const sec = try zipfs.net_identity.loadOrCreateSecret64(gpa, repo_root);
            try printPeerIdP2pLine(gpa, stderr, sec);
            try stderr.print("gateway on 0.0.0.0:{d}\n", .{cfg.gateway_port});
            try stderr.print("HTTP API: POST http://127.0.0.1:{d}/api/v0/add (Kubo-compatible)\n", .{cfg.gateway_port});
            try stderr.flush();
            const gw_sk = try std.crypto.sign.Ed25519.SecretKey.fromBytes(sec);
            const gw_kp = try std.crypto.sign.Ed25519.KeyPair.fromSecretKey(gw_sk);
            const gw_pub = gw_kp.public_key.toBytes();
            const gw_pid = try zipfs.net_peer_id.peerIdString(gpa, &gw_pub);
            const gw_ctx = zipfs.gateway.GatewayCtx{
                .store = &node.store,
                .store_sync = null,
                .repo_root = repo_root,
                .chunk_size = cfg.chunk_size,
                .port = cfg.gateway_port,
                .peer_id = gw_pid,
                .listen_addrs = cfg.listen_addrs,
                .announce_addrs = cfg.announce_addrs,
                .cluster_peers = cfg.cluster_peers,
                .replication_factor = cfg.replication_factor orelse 2,
                .max_conns = cfg.max_gateway_conns orelse 64,
            };
            try zipfs.gateway.run(gpa, &gw_ctx);
        }
        return;
    }

    if (std.mem.eql(u8, cmd, "net")) {
        const sub = args.next() orelse {
            try stderr.writeAll("net: echo-serve <port> | echo-dial <host> <port> | dial-noise <host> <port> | dial-bitswap <host> <port> <cid> | fetch <cid> | provide <cid>\n");
            return error.BadArgs;
        };
        if (std.mem.eql(u8, sub, "echo-serve")) {
            const p = args.next() orelse {
                try stderr.writeAll("net echo-serve: missing port\n");
                return error.BadArgs;
            };
            const port = try std.fmt.parseInt(u16, p, 10);
            try zipfs.net_swarm.serveEcho(port);
            return;
        }
        if (std.mem.eql(u8, sub, "echo-dial")) {
            const host = args.next() orelse {
                try stderr.writeAll("net echo-dial: missing host\n");
                return error.BadArgs;
            };
            const p = args.next() orelse {
                try stderr.writeAll("net echo-dial: missing port\n");
                return error.BadArgs;
            };
            const port = try std.fmt.parseInt(u16, p, 10);
            try zipfs.net_swarm.dialEcho(host, port);
            try stderr.writeAll("echo ok\n");
            return;
        }
        if (std.mem.eql(u8, sub, "dial-noise")) {
            const host = args.next() orelse {
                try stderr.writeAll("net dial-noise: missing host\n");
                return error.BadArgs;
            };
            const p = args.next() orelse {
                try stderr.writeAll("net dial-noise: missing port\n");
                return error.BadArgs;
            };
            const port = try std.fmt.parseInt(u16, p, 10);
            const kp = std.crypto.sign.Ed25519.KeyPair.generate();
            const sec = kp.secret_key.toBytes();
            var dh = try zipfs.net_libp2p_dial.dialNoiseHandshake(gpa, host, port, sec);
            defer dh.stream.close();
            const pid = try zipfs.net_libp2p_dial.remotePeerIdString(gpa, &dh.session.remote_ed25519_pub);
            defer gpa.free(pid);
            try stderr.print("remote PeerID: {s}\n", .{pid});
            return;
        }
        if (std.mem.eql(u8, sub, "fetch")) {
            const cid_str = args.next() orelse {
                try stderr.writeAll("net fetch: missing cid\n");
                return error.BadArgs;
            };
            var node: zipfs.Node = .{};
            defer node.deinit(gpa);
            node.store.repo_root = repo_root;
            if (node.store.has(cid_str)) {
                try stderr.writeAll("already in local blockstore\n");
                return;
            }
            const sec = try zipfs.net_identity.loadOrCreateSecret64(gpa, repo_root);
            const default_bs = zipfs.config.default_bootstrap_peers;
            const peers = if (cfg.bootstrap_peers.len > 0) cfg.bootstrap_peers else default_bs[0..];
            const got = try zipfs.net_libp2p_fetch.fetchBlockIntoStore(gpa, &node.store, cid_str, peers, sec);
            if (got) {
                try stderr.writeAll("fetched block into repo\n");
            } else {
                try stderr.writeAll("block already present\n");
            }
            return;
        }
        if (std.mem.eql(u8, sub, "provide")) {
            const cid_str = args.next() orelse {
                try stderr.writeAll("net provide: missing cid\n");
                return error.BadArgs;
            };
            const sec = try zipfs.net_identity.loadOrCreateSecret64(gpa, repo_root);
            const default_bs = zipfs.config.default_bootstrap_peers;
            const peers = if (cfg.bootstrap_peers.len > 0) cfg.bootstrap_peers else default_bs[0..];
            const swarm_port = zipfs.net_swarm_config.swarmTcpPortFromConfig(gpa, &cfg);
            const bins = try zipfs.net_swarm_config.buildIdentifyListenBinaries(gpa, &cfg, swarm_port);
            defer {
                for (bins) |b| gpa.free(b);
                gpa.free(bins);
            }
            try zipfs.net_libp2p_provide.provideCid(gpa, cid_str, peers, sec, .{}, 8, bins);
            try stderr.writeAll("ADD_PROVIDER sent to closest peers (best-effort; see bootstrap_peers)\n");
            return;
        }
        if (std.mem.eql(u8, sub, "dial-bitswap")) {
            const host = args.next() orelse {
                try stderr.writeAll("net dial-bitswap: missing host\n");
                return error.BadArgs;
            };
            const p = args.next() orelse {
                try stderr.writeAll("net dial-bitswap: missing port\n");
                return error.BadArgs;
            };
            const port = try std.fmt.parseInt(u16, p, 10);
            const cid_str = args.next() orelse {
                try stderr.writeAll("net dial-bitswap: missing cid\n");
                return error.BadArgs;
            };
            const kp = std.crypto.sign.Ed25519.KeyPair.generate();
            const sec = kp.secret_key.toBytes();
            const block = zipfs.net_libp2p_dial.dialBitswapWant(gpa, host, port, cid_str, sec) catch |err| {
                try stderr.print("dial-bitswap failed: {}\n", .{err});
                return err;
            };
            defer if (block) |b| gpa.free(b);
            if (block) |b| {
                try stderr.print("received block: {d} bytes\n", .{b.len});
                try std.fs.File.stdout().writeAll(b);
            } else {
                try stderr.writeAll("peer did not send block (want-haves / dont-have / timeout)\n");
            }
            return;
        }
        try stderr.print("net: unknown {s}\n", .{sub});
        return error.BadArgs;
    }

    if (std.mem.eql(u8, cmd, "cluster")) {
        const sub = args.next() orelse {
            try stderr.writeAll("cluster: peers|status|replicate|shard|sync\n");
            return error.BadArgs;
        };
        if (std.mem.eql(u8, sub, "peers")) {
            const peer_cmd = args.next() orelse {
                try stderr.writeAll("cluster peers: add|rm|ls\n");
                return error.BadArgs;
            };
            var cstate = try zipfs.cluster.ClusterState.load(gpa, repo_root);
            defer cstate.deinit();

            if (std.mem.eql(u8, peer_cmd, "add")) {
                const addr = args.next() orelse {
                    try stderr.writeAll("cluster peers add: missing multiaddr\n");
                    return error.BadArgs;
                };
                try cstate.addPeer("unknown", addr);
                try cstate.save(repo_root);
                try stderr.print("added cluster peer: {s}\n", .{addr});
                return;
            }
            if (std.mem.eql(u8, peer_cmd, "rm")) {
                const addr = args.next() orelse {
                    try stderr.writeAll("cluster peers rm: missing multiaddr\n");
                    return error.BadArgs;
                };
                cstate.removePeer(addr);
                try cstate.save(repo_root);
                try stderr.print("removed cluster peer: {s}\n", .{addr});
                return;
            }
            if (std.mem.eql(u8, peer_cmd, "ls")) {
                if (cstate.peers.items.len == 0) {
                    try stderr.writeAll("no cluster peers configured\n");
                    return;
                }
                try stderr.writeAll("ADDR\tALIVE\tFAILS\n");
                for (cstate.peers.items) |p| {
                    try stderr.print("{s}\t{}\t{d}\n", .{ p.addr, p.is_alive, p.consecutive_failures });
                }
                return;
            }
            try stderr.print("cluster peers: unknown {s}\n", .{peer_cmd});
            return error.BadArgs;
        }
        if (std.mem.eql(u8, sub, "status")) {
            var cstate = try zipfs.cluster.ClusterState.load(gpa, repo_root);
            defer cstate.deinit();

            var rit = cstate.replicas.iterator();
            var count: usize = 0;
            if (cstate.replicas.count() == 0) {
                try stderr.writeAll("no replicated CIDs\n");
                return;
            }
            try stderr.writeAll("CID\tMODE\tTARGET\tCONFIRMED\tRETRIES\n");
            while (rit.next()) |entry| {
                const rec = entry.value_ptr.*;
                // Count pending retries for this CID
                var retries: usize = 0;
                for (cstate.retry_queue.items) |r| {
                    if (std.mem.eql(u8, r.cid, rec.cid)) retries += 1;
                }
                const short_cid = if (rec.cid.len > 16) rec.cid[0..16] else rec.cid;
                try stderr.print("{s}...\t{s}\t{d}\t{d}\t{d}\n", .{
                    short_cid, rec.mode.toString(), rec.target_n, rec.confirmed_peers.len, retries,
                });
                count += 1;
            }
            try stderr.print("total: {d} replicated CIDs, {d} pending retries\n", .{ count, cstate.retry_queue.items.len });
            return;
        }
        if (std.mem.eql(u8, sub, "replicate")) {
            const cid_str = args.next() orelse {
                try stderr.writeAll("cluster replicate: missing cid\n");
                return error.BadArgs;
            };
            var factor: u8 = cfg.replication_factor orelse 2;
            if (args.next()) |flag| {
                if (std.mem.eql(u8, flag, "--factor")) {
                    if (args.next()) |nstr| {
                        factor = try std.fmt.parseInt(u8, nstr, 10);
                    }
                }
            }
            var node: zipfs.Node = .{};
            defer node.deinit(gpa);
            node.store.repo_root = repo_root;

            const sec = try zipfs.net_identity.loadOrCreateSecret64(gpa, repo_root);
            var cstate = try zipfs.cluster.ClusterState.load(gpa, repo_root);
            defer cstate.deinit();

            // Add configured peers if not already tracked
            for (cfg.cluster_peers) |addr| {
                cstate.addPeer("unknown", addr) catch {};
            }

            // factor = total copies; subtract 1 for origin to get peer target
            const peer_target: u8 = if (factor > 1) factor - 1 else 1;
            try zipfs.replication.replicateCid(gpa, &cstate, &node.store, cid_str, peer_target, cfg.cluster_secret, sec);
            try cstate.save(repo_root);
            try stderr.print("replicate {s} (factor={d}, peers={d}) initiated\n", .{ cid_str, factor, peer_target });
            return;
        }
        if (std.mem.eql(u8, sub, "shard")) {
            const cid_str = args.next() orelse {
                try stderr.writeAll("cluster shard: missing cid\n");
                return error.BadArgs;
            };
            var shard_count: u16 = cfg.shard_count orelse 4;
            if (args.next()) |flag| {
                if (std.mem.eql(u8, flag, "--shards")) {
                    if (args.next()) |nstr| {
                        shard_count = try std.fmt.parseInt(u16, nstr, 10);
                    }
                }
            }
            var node: zipfs.Node = .{};
            defer node.deinit(gpa);
            node.store.repo_root = repo_root;

            const sec = try zipfs.net_identity.loadOrCreateSecret64(gpa, repo_root);
            var cstate = try zipfs.cluster.ClusterState.load(gpa, repo_root);
            defer cstate.deinit();

            for (cfg.cluster_peers) |addr| {
                cstate.addPeer("unknown", addr) catch {};
            }

            try zipfs.replication.shardCid(gpa, &cstate, &node.store, cid_str, shard_count, cfg.cluster_secret, sec);
            try cstate.save(repo_root);
            try stderr.print("shard {s} (shards={d}) initiated\n", .{ cid_str, shard_count });
            return;
        }
        if (std.mem.eql(u8, sub, "manifest")) {
            const manifest_cmd = args.next() orelse {
                try stderr.writeAll("cluster manifest: ls|status <cid>\n");
                return error.BadArgs;
            };
            if (std.mem.eql(u8, manifest_cmd, "ls")) {
                var manifests = try zipfs.manifest.Manifest.loadAll(gpa, repo_root);
                defer {
                    for (manifests) |*m| m.deinit(gpa);
                    gpa.free(manifests);
                }
                if (manifests.len == 0) {
                    try stderr.writeAll("no manifests\n");
                    return;
                }
                try stderr.writeAll("ROOT_CID\tSTATUS\tBLOCKS\tPEERS\n");
                for (manifests) |m| {
                    const short_cid = if (m.root_cid.len > 16) m.root_cid[0..16] else m.root_cid;
                    try stderr.print("{s}...\t{s}\t{d}\t{d}\n", .{
                        short_cid, m.status.toString(), m.total_blocks, m.target_peers.len,
                    });
                }
                return;
            }
            if (std.mem.eql(u8, manifest_cmd, "status")) {
                const mcid = args.next() orelse {
                    try stderr.writeAll("cluster manifest status: missing cid\n");
                    return error.BadArgs;
                };
                var m = zipfs.manifest.Manifest.load(gpa, repo_root, mcid) catch |err| {
                    try stderr.print("manifest not found: {}\n", .{err});
                    return error.BadArgs;
                };
                defer m.deinit(gpa);
                try stderr.print("Root CID:    {s}\n", .{m.root_cid});
                try stderr.print("Status:      {s}\n", .{m.status.toString()});
                try stderr.print("Blocks:      {d}\n", .{m.total_blocks});
                try stderr.print("Repl Factor: {d}\n", .{m.replication_factor});
                if (m.target_peers.len > 0) {
                    try stderr.writeAll("\nPEER\tSTATUS\tPULLED\tTOTAL\n");
                    for (m.target_peers) |p| {
                        const short_addr = if (p.peer_addr.len > 20) p.peer_addr[0..20] else p.peer_addr;
                        try stderr.print("{s}...\t{s}\t{d}\t{d}\n", .{
                            short_addr, p.status.toString(), p.pulled_blocks, p.total_blocks,
                        });
                    }
                }
                return;
            }
            try stderr.print("cluster manifest: unknown {s}\n", .{manifest_cmd});
            return error.BadArgs;
        }
        if (std.mem.eql(u8, sub, "sync")) {
            var node: zipfs.Node = .{};
            defer node.deinit(gpa);
            node.store.repo_root = repo_root;

            const sec = try zipfs.net_identity.loadOrCreateSecret64(gpa, repo_root);
            const c_mode = if (cfg.cluster_mode) |m| zipfs.cluster.ClusterMode.fromString(m) orelse .replicate else .replicate;
            var sync_mu_local = std.Thread.Mutex{};
            var heal_ctx = zipfs.replication.SelfHealCtx{
                .store = &node.store,
                .mu = &sync_mu_local,
                .repo_root = repo_root,
                .ed25519_secret64 = sec,
                .interval_ns = 0,
                .cluster_secret = cfg.cluster_secret,
                .cluster_mode = c_mode,
                .replication_factor = cfg.replication_factor orelse 2,
                .shard_count = cfg.shard_count orelse 4,
            };
            // Run one cycle synchronously
            try zipfs.replication.selfHealOnce(gpa, &heal_ctx);
            try stderr.writeAll("cluster sync completed\n");
            return;
        }
        try stderr.print("cluster: unknown {s}\n", .{sub});
        return error.BadArgs;
    }

    if (std.mem.eql(u8, cmd, "mfs")) {
        const sub = args.next() orelse {
            try stderr.writeAll("mfs: root <cid> — set MFS root pointer (in-memory demo)\n");
            return error.BadArgs;
        };
        if (std.mem.eql(u8, sub, "root")) {
            const cidv = args.next() orelse {
                try stderr.writeAll("mfs root: missing cid\n");
                return error.BadArgs;
            };
            var m = zipfs.mfs.MfsRoot{};
            defer m.deinit(gpa);
            try m.setRoot(gpa, cidv);
            try stderr.writeAll("mfs root set (session only; persist via pin)\n");
            return;
        }
        try stderr.print("mfs: unknown {s}\n", .{sub});
        return error.BadArgs;
    }

    if (std.mem.eql(u8, cmd, "block")) {
        const sub = args.next() orelse {
            try stderr.writeAll("block: missing subcommand (put|get)\n");
            return error.BadArgs;
        };
        if (std.mem.eql(u8, sub, "put")) {
            const path = args.next() orelse {
                try stderr.writeAll("block put: missing path\n");
                return error.BadArgs;
            };
            const data = try std.fs.cwd().readFileAlloc(gpa, path, std.math.maxInt(usize));
            defer gpa.free(data);
            var node: zipfs.Node = .{};
            defer node.deinit(gpa);
            node.store.repo_root = repo_root;
            const id = try node.blockPut(gpa, data);
            defer id.deinit(gpa);
            const s = try id.toString(gpa);
            defer gpa.free(s);
            try std.fs.File.stdout().writeAll(s);
            try std.fs.File.stdout().writeAll("\n");
            return;
        }
        if (std.mem.eql(u8, sub, "get")) {
            const cid_str = args.next() orelse {
                try stderr.writeAll("block get: missing cid\n");
                return error.BadArgs;
            };
            var node: zipfs.Node = .{};
            defer node.deinit(gpa);
            node.store.repo_root = repo_root;
            const raw = try node.blockGet(gpa, cid_str);
            defer gpa.free(raw);
            try std.fs.File.stdout().writeAll(raw);
            return;
        }
        try stderr.print("block: unknown subcommand {s}\n", .{sub});
        return error.BadArgs;
    }

    try stderr.print("unknown command: {s}\n", .{cmd});
    return error.BadArgs;
}

pub const std_options: std.Options = .{
    .log_level = .warn,
};
