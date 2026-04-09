//! Connection pool for reusable Yamux connections to cluster peers.
//! Avoids the overhead of TCP+Noise+Yamux handshake on every push.

const std = @import("std");
const noise = @import("noise.zig");
const libp2p_dial = @import("libp2p_dial.zig");
const multistream = @import("multistream.zig");

pub const PooledConn = struct {
    stream: std.net.Stream,
    ns: *noise.Session,
    mux: *libp2p_dial.YamuxOverNoise,
    next_stream_id: u32 = 3,
    last_used_ns: i128 = 0,

    /// Get the next available yamux stream ID (odd numbers for client-initiated).
    /// Returns null if stream IDs are exhausted (connection should be evicted).
    pub fn nextStreamId(self: *PooledConn) ?u32 {
        if (self.next_stream_id > std.math.maxInt(u32) - 2) return null;
        const sid = self.next_stream_id;
        self.next_stream_id += 2;
        return sid;
    }

    fn destroy(self: *PooledConn, allocator: std.mem.Allocator) void {
        self.mux.deinit();
        allocator.destroy(self.mux);
        allocator.destroy(self.ns);
        self.stream.close();
    }
};

pub const ConnPool = struct {
    pool: std.StringHashMapUnmanaged(PooledConn) = .empty,
    mu: std.Thread.Mutex = .{},
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) ConnPool {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *ConnPool) void {
        var it = self.pool.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.destroy(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.pool.deinit(self.allocator);
    }

    /// Get an existing pooled connection, or dial a new one.
    /// Returns the PooledConn pointer and the pool key (for release/evict).
    /// On error, nothing needs to be cleaned up.
    /// On success, caller must call release(key) on success or evictByKey(key) on failure.
    pub fn getOrDial(
        self: *ConnPool,
        allocator: std.mem.Allocator,
        host: []const u8,
        port: u16,
        ed25519_secret64: [64]u8,
    ) !struct { conn: *PooledConn, key: []const u8 } {
        const lookup_key = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ host, port });

        // Check for existing connection (under lock)
        {
            self.mu.lock();
            defer self.mu.unlock();
            if (self.pool.getPtr(lookup_key)) |existing| {
                existing.last_used_ns = std.time.nanoTimestamp();
                const owned_key = self.pool.getKey(lookup_key).?;
                self.allocator.free(lookup_key);
                return .{ .conn = existing, .key = owned_key };
            }
        }

        // Dial new connection (outside lock — network I/O)
        // Use explicit catch-block cleanup (no errdefer) to prevent double-free.
        const ns_ptr = self.allocator.create(noise.Session) catch {
            self.allocator.free(lookup_key);
            return error.OutOfMemory;
        };

        const mux_ptr = self.allocator.create(libp2p_dial.YamuxOverNoise) catch {
            self.allocator.destroy(ns_ptr);
            self.allocator.free(lookup_key);
            return error.OutOfMemory;
        };

        const stream = std.net.tcpConnectToHost(allocator, host, port) catch |err| {
            self.allocator.destroy(mux_ptr);
            self.allocator.destroy(ns_ptr);
            self.allocator.free(lookup_key);
            return err;
        };

        var line_buf: [512]u8 = undefined;
        libp2p_dial.multistreamSelectPlain(stream, &line_buf) catch |err| {
            self.allocator.destroy(mux_ptr);
            self.allocator.destroy(ns_ptr);
            stream.close();
            self.allocator.free(lookup_key);
            return err;
        };
        ns_ptr.* = noise.Session.handshakeInitiator(stream, allocator, ed25519_secret64, &.{}) catch |err| {
            self.allocator.destroy(mux_ptr);
            self.allocator.destroy(ns_ptr);
            stream.close();
            self.allocator.free(lookup_key);
            return err;
        };
        libp2p_dial.negotiateYamuxOnNoise(ns_ptr, allocator) catch |err| {
            self.allocator.destroy(mux_ptr);
            self.allocator.destroy(ns_ptr);
            stream.close();
            self.allocator.free(lookup_key);
            return err;
        };
        mux_ptr.* = .{ .sess = ns_ptr, .allocator = allocator };

        // Insert into pool (under lock)
        self.mu.lock();
        defer self.mu.unlock();

        // Check for race: another thread may have inserted while we were dialing
        if (self.pool.getPtr(lookup_key)) |race_winner| {
            // Destroy our connection, use the one that won the race
            mux_ptr.deinit();
            self.allocator.destroy(mux_ptr);
            self.allocator.destroy(ns_ptr);
            stream.close();
            race_winner.last_used_ns = std.time.nanoTimestamp();
            const owned_key = self.pool.getKey(lookup_key).?;
            self.allocator.free(lookup_key);
            return .{ .conn = race_winner, .key = owned_key };
        }

        const pooled = PooledConn{
            .stream = stream,
            .ns = ns_ptr,
            .mux = mux_ptr,
            .next_stream_id = 3,
            .last_used_ns = std.time.nanoTimestamp(),
        };
        self.pool.put(self.allocator, lookup_key, pooled) catch |err| {
            mux_ptr.deinit();
            self.allocator.destroy(mux_ptr);
            self.allocator.destroy(ns_ptr);
            stream.close();
            self.allocator.free(lookup_key);
            return err;
        };
        const ptr = self.pool.getPtr(lookup_key).?;
        return .{ .conn = ptr, .key = lookup_key };
    }

    /// Mark a connection as available (update last-used timestamp).
    pub fn release(self: *ConnPool, key: []const u8) void {
        self.mu.lock();
        defer self.mu.unlock();
        if (self.pool.getPtr(key)) |c| {
            c.last_used_ns = std.time.nanoTimestamp();
        } else {
            std.log.debug("conn_pool: release called for evicted key: {s}", .{key});
        }
    }

    /// Remove and destroy a broken connection.
    pub fn evictByKey(self: *ConnPool, key: []const u8) void {
        self.mu.lock();
        defer self.mu.unlock();
        if (self.pool.fetchRemove(key)) |kv| {
            var conn = kv.value;
            conn.destroy(self.allocator);
            self.allocator.free(kv.key);
        }
    }

    /// Evict connections idle longer than max_idle_ns.
    pub fn evictStale(self: *ConnPool, max_idle_ns: i128) void {
        self.mu.lock();
        defer self.mu.unlock();

        const now = std.time.nanoTimestamp();
        // Collect stale keys first, then remove
        var stale_keys = std.ArrayList([]const u8).empty;
        defer stale_keys.deinit(self.allocator);

        var it = self.pool.iterator();
        while (it.next()) |entry| {
            if (now - entry.value_ptr.last_used_ns > max_idle_ns) {
                stale_keys.append(self.allocator, entry.key_ptr.*) catch continue;
            }
        }

        for (stale_keys.items) |key| {
            if (self.pool.fetchRemove(key)) |kv| {
                var conn = kv.value;
                conn.destroy(self.allocator);
                self.allocator.free(kv.key);
            }
        }
    }
};
