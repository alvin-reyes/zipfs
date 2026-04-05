const std = @import("std");
const zig_ipfs = @import("zig_ipfs");

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

    const repo_root = try zig_ipfs.repo.repoRootFromEnv(gpa);
    defer gpa.free(repo_root);

    var cfg = try zig_ipfs.config.Config.load(gpa, repo_root);
    defer cfg.deinit(gpa);

    const cmd = args.next() orelse {
        try stderr.writeAll(
            \\zig-ipfs — IPFS-style node (Zig, local + gateway + net stubs)
            \\
            \\Data:   add [-r] <path>  cat <cid>  ls <cid> [path]  block put|get
            \\        dag car import|export <path>  config init
            \\        pin add [-r] <cid>  pin rm [-r] <cid>  pin ls  repo gc
            \\        gateway
            \\Net:    net echo-serve <port>  net echo-dial <host> <port>
            \\        id  (peer id from new Ed25519 key)
            \\
        );
        return;
    };

    if (std.mem.eql(u8, cmd, "config")) {
        const sub = args.next() orelse {
            try stderr.writeAll("usage: zig-ipfs config init\n");
            return error.BadArgs;
        };
        if (std.mem.eql(u8, sub, "init")) {
            var c = zig_ipfs.config.Config{};
            try c.save(gpa, repo_root);
            try stderr.writeAll("wrote config.json\n");
            return;
        }
        try stderr.print("unknown config subcommand: {s}\n", .{sub});
        return error.BadArgs;
    }

    if (std.mem.eql(u8, cmd, "id")) {
        const kp = zig_ipfs.net_peer_id.generateKeyPair();
        const pid = try zig_ipfs.net_peer_id.peerIdString(gpa, &kp.public_key);
        defer gpa.free(pid);
        const line = try std.fmt.allocPrint(gpa, "AgentVersion: zig-ipfs/0.2.0\nPeerID: {s}\n", .{pid});
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

        var node: zig_ipfs.Node = .{};
        defer node.deinit(gpa);
        const root = if (recurse)
            try node.addDirectory(gpa, path, &cfg)
        else blk: {
            const data = try std.fs.cwd().readFileAlloc(gpa, path, std.math.maxInt(usize));
            defer gpa.free(data);
            break :blk try node.addFileWithConfig(gpa, data, &cfg);
        };
        defer root.deinit(gpa);
        try zig_ipfs.repo.exportStore(&node.store, repo_root);
        const s = try root.toString(gpa);
        defer gpa.free(s);
        try std.fs.File.stdout().writeAll(s);
        try std.fs.File.stdout().writeAll("\n");
        return;
    }

    if (std.mem.eql(u8, cmd, "cat")) {
        const cid_str = args.next() orelse {
            try stderr.writeAll("cat: missing cid\n");
            return error.BadArgs;
        };
        const rpath = args.next() orelse "";
        var node: zig_ipfs.Node = .{};
        defer node.deinit(gpa);
        try zig_ipfs.repo.importStore(&node.store, gpa, repo_root);
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
        var node: zig_ipfs.Node = .{};
        defer node.deinit(gpa);
        try zig_ipfs.repo.importStore(&node.store, gpa, repo_root);
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
            var node: zig_ipfs.Node = .{};
            defer node.deinit(gpa);
            try zig_ipfs.repo.importStore(&node.store, gpa, repo_root);
            try zig_ipfs.car.exportStoreToFile(gpa, &node.store, path);
            return;
        }
        if (std.mem.eql(u8, op, "import")) {
            var node: zig_ipfs.Node = .{};
            defer node.deinit(gpa);
            var f = try std.fs.cwd().openFile(path, .{});
            defer f.close();
            try zig_ipfs.car.importFromSeekableFile(gpa, f, &node.store);
            try zig_ipfs.repo.exportStore(&node.store, repo_root);
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
        var pins = try zig_ipfs.pin.PinSet.load(gpa, repo_root);
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
            var pins = try zig_ipfs.pin.PinSet.load(gpa, repo_root);
            defer pins.deinit(gpa);
            var node: zig_ipfs.Node = .{};
            defer node.deinit(gpa);
            try zig_ipfs.repo.importStore(&node.store, gpa, repo_root);
            const n = try zig_ipfs.pin.gc(gpa, &node.store, &pins, repo_root);
            try zig_ipfs.repo.exportStore(&node.store, repo_root);
            try stderr.print("removed {d} blocks\n", .{n});
            return;
        }
        try stderr.print("repo: unknown {s}\n", .{sub});
        return error.BadArgs;
    }

    if (std.mem.eql(u8, cmd, "gateway")) {
        var node: zig_ipfs.Node = .{};
        defer node.deinit(gpa);
        try zig_ipfs.repo.importStore(&node.store, gpa, repo_root);
        try stderr.print("gateway on 0.0.0.0:{d}\n", .{cfg.gateway_port});
        try zig_ipfs.gateway.run(gpa, &node.store, cfg.gateway_port);
        return;
    }

    if (std.mem.eql(u8, cmd, "net")) {
        const sub = args.next() orelse {
            try stderr.writeAll("net: echo-serve <port> | echo-dial <host> <port>\n");
            return error.BadArgs;
        };
        if (std.mem.eql(u8, sub, "echo-serve")) {
            const p = args.next() orelse {
                try stderr.writeAll("net echo-serve: missing port\n");
                return error.BadArgs;
            };
            const port = try std.fmt.parseInt(u16, p, 10);
            try zig_ipfs.net_swarm.serveEcho(port);
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
            try zig_ipfs.net_swarm.dialEcho(host, port);
            try stderr.writeAll("echo ok\n");
            return;
        }
        try stderr.print("net: unknown {s}\n", .{sub});
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
            var m = zig_ipfs.mfs.MfsRoot{};
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
            var node: zig_ipfs.Node = .{};
            defer node.deinit(gpa);
            const id = try node.blockPut(gpa, data);
            defer id.deinit(gpa);
            try zig_ipfs.repo.exportStore(&node.store, repo_root);
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
            var node: zig_ipfs.Node = .{};
            defer node.deinit(gpa);
            try zig_ipfs.repo.importStore(&node.store, gpa, repo_root);
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
