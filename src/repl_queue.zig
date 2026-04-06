//! Thread-safe priority replication queue, token-bucket rate limiter, and per-peer concurrency tracker.

const std = @import("std");
const cluster_mod = @import("cluster.zig");
const ClusterMode = cluster_mod.ClusterMode;

/// Priority levels for replication work items (lower numeric value = higher priority).
pub const Priority = enum(u8) {
    immediate = 0,
    normal = 1,
    heal = 2,
    retry_high = 3,
    retry_low = 4,
};

/// Maximum retries before dropping a replication item.
pub const max_repl_retries: u8 = 10;

/// A single replication work item.
pub const ReplItem = struct {
    cid: []const u8,
    mode: ClusterMode,
    priority: Priority,
    enqueued_ns: i128,
    target_peer: ?[]const u8,
    shard_index: ?u16,
    retry_count: u8 = 0,

    pub fn deinit(self: *ReplItem, allocator: std.mem.Allocator) void {
        allocator.free(self.cid);
        if (self.target_peer) |tp| allocator.free(tp);
        self.* = undefined;
    }

    /// Ordering: by priority ASC, then by timestamp (ASC for retries, DESC for immediate).
    fn orderLessThan(_: void, a: ReplItem, b: ReplItem) bool {
        const pa = @intFromEnum(a.priority);
        const pb = @intFromEnum(b.priority);
        if (pa != pb) return pa < pb;
        // Within same priority: immediate items newest-first, retries oldest-first
        if (a.priority == .immediate) return a.enqueued_ns > b.enqueued_ns;
        return a.enqueued_ns < b.enqueued_ns;
    }
};

/// Thread-safe priority queue for replication work items.
pub const ReplQueue = struct {
    items: std.ArrayList(ReplItem),
    mutex: std.Thread.Mutex = .{},
    cond: std.Thread.Condition = .{},
    max_size: u32,
    dropped_count: u64 = 0,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, max_size: u32) ReplQueue {
        return .{
            .items = std.ArrayList(ReplItem).empty,
            .max_size = max_size,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ReplQueue) void {
        for (self.items.items) |*item| item.deinit(self.allocator);
        self.items.deinit(self.allocator);
    }

    /// Push an item into the queue. If the queue is full, drops the lowest-priority (last) item.
    /// Takes ownership of the item's allocations.
    pub fn push(self: *ReplQueue, item: ReplItem) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Backpressure: if full, drop lowest-priority item using full comparator
        if (self.items.items.len >= self.max_size) {
            if (self.items.items.len > 0) {
                const last = &self.items.items[self.items.items.len - 1];
                // Use full ordering: new item must sort before the last (worst) item
                if (ReplItem.orderLessThan({}, item, last.*)) {
                    var removed = self.items.pop();
                    removed.deinit(self.allocator);
                    self.dropped_count += 1;
                } else {
                    // New item is lower or equal priority — drop it
                    var dropped = item;
                    dropped.deinit(self.allocator);
                    self.dropped_count += 1;
                    return;
                }
            }
        }

        // Binary search for correct insertion position: O(log n) instead of O(n log n) full sort
        const item_count = self.items.items.len;
        const insert_pos = blk: {
            var lo: usize = 0;
            var hi: usize = item_count;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (ReplItem.orderLessThan({}, self.items.items[mid], item)) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            break :blk lo;
        };

        // Append to end (may grow the backing array), then rotate into position
        self.items.append(self.allocator, item) catch {
            var dropped = item;
            dropped.deinit(self.allocator);
            self.dropped_count += 1;
            return;
        };
        // Shift elements [insert_pos..len] right by one to make room
        if (insert_pos < len) {
            const items = self.items.items;
            const new_item = items[len]; // the just-appended item
            std.mem.copyBackwards(ReplItem, items[insert_pos + 1 .. len + 1], items[insert_pos..len]);
            items[insert_pos] = new_item;
        }

        // Signal waiting consumers
        self.cond.signal();
    }

    /// Pop up to `max_count` items from the front of the queue.
    /// Blocks (with timeout) if queue is empty. Returns owned slice (caller must free).
    pub fn popBatch(self: *ReplQueue, max_count: usize, timeout_ns: u64) []ReplItem {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Wait for items if empty (loop guards against spurious wakeups)
        while (self.items.items.len == 0) {
            self.cond.timedWait(&self.mutex, timeout_ns) catch break;
        }

        if (self.items.items.len == 0) return &.{};

        const count = @min(max_count, self.items.items.len);
        const batch = self.allocator.alloc(ReplItem, count) catch return &.{};

        // Copy from front
        @memcpy(batch, self.items.items[0..count]);

        // Remove from front by shifting remaining
        const remaining = self.items.items.len - count;
        if (remaining > 0) {
            std.mem.copyForwards(ReplItem, self.items.items[0..remaining], self.items.items[count..]);
        }
        self.items.items.len = remaining;

        return batch;
    }

    /// Current queue length.
    pub fn len(self: *ReplQueue) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.items.items.len;
    }
};

/// Token-bucket rate limiter.
pub const RateLimiter = struct {
    tokens: u32,
    max_tokens: u32,
    refill_rate: u32,
    last_refill_ns: i128,
    mutex: std.Thread.Mutex = .{},

    pub fn init(rate_per_sec: u32) RateLimiter {
        return .{
            .tokens = rate_per_sec,
            .max_tokens = rate_per_sec,
            .refill_rate = rate_per_sec,
            .last_refill_ns = std.time.nanoTimestamp(),
        };
    }

    /// Try to acquire a token. Returns true if acquired.
    pub fn tryAcquire(self: *RateLimiter) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.refill();
        if (self.tokens > 0) {
            self.tokens -= 1;
            return true;
        }
        return false;
    }

    /// Block until a token is available (with a spin-sleep).
    pub fn waitAcquire(self: *RateLimiter) void {
        while (true) {
            if (self.tryAcquire()) return;
            std.Thread.sleep(10 * std.time.ns_per_ms); // 10ms spin
        }
    }

    fn refill(self: *RateLimiter) void {
        const now = std.time.nanoTimestamp();
        const elapsed_ns = now - self.last_refill_ns;
        if (elapsed_ns <= 0) return;
        const elapsed_secs = @as(u64, @intCast(@divFloor(elapsed_ns, std.time.ns_per_s)));
        if (elapsed_secs == 0) return;
        const new_tokens = elapsed_secs * self.refill_rate;
        self.tokens = @min(self.max_tokens, self.tokens +| @as(u32, @intCast(@min(new_tokens, std.math.maxInt(u32)))));
        self.last_refill_ns = now;
    }
};

/// Per-peer connection concurrency tracker.
pub const PeerConcurrency = struct {
    counts: std.StringHashMapUnmanaged(u8) = .empty,
    max_per_peer: u8,
    mutex: std.Thread.Mutex = .{},
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, max_per_peer: u8) PeerConcurrency {
        return .{
            .max_per_peer = max_per_peer,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *PeerConcurrency) void {
        var kit = self.counts.keyIterator();
        while (kit.next()) |k| self.allocator.free(k.*);
        self.counts.deinit(self.allocator);
    }

    /// Try to acquire a connection slot for the given peer. Returns true if under limit.
    pub fn acquire(self: *PeerConcurrency, peer_addr: []const u8) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.counts.getPtr(peer_addr)) |count_ptr| {
            if (count_ptr.* >= self.max_per_peer) return false;
            count_ptr.* += 1;
            return true;
        }
        // New peer — insert with count 1
        const key = self.allocator.dupe(u8, peer_addr) catch return false;
        self.counts.put(self.allocator, key, 1) catch {
            self.allocator.free(key);
            return false;
        };
        return true;
    }

    /// Release a connection slot for the given peer.
    pub fn release(self: *PeerConcurrency, peer_addr: []const u8) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.counts.getPtr(peer_addr)) |count_ptr| {
            if (count_ptr.* > 1) {
                count_ptr.* -= 1;
            } else {
                // Remove the entry entirely
                if (self.counts.fetchRemove(peer_addr)) |kv| {
                    self.allocator.free(kv.key);
                }
            }
        }
    }
};

test "priority ordering" {
    const gpa = std.testing.allocator;
    var q = ReplQueue.init(gpa, 100);
    defer q.deinit();

    // Push items with different priorities
    q.push(.{ .cid = try gpa.dupe(u8, "cid1"), .mode = .replicate, .priority = .retry_low, .enqueued_ns = 100, .target_peer = null, .shard_index = null });
    q.push(.{ .cid = try gpa.dupe(u8, "cid2"), .mode = .replicate, .priority = .immediate, .enqueued_ns = 200, .target_peer = null, .shard_index = null });
    q.push(.{ .cid = try gpa.dupe(u8, "cid3"), .mode = .replicate, .priority = .heal, .enqueued_ns = 150, .target_peer = null, .shard_index = null });

    const batch = q.popBatch(10, 0);
    defer {
        for (batch) |*item| {
            var it = item.*;
            it.deinit(gpa);
        }
        gpa.free(batch);
    }

    try std.testing.expectEqual(@as(usize, 3), batch.len);
    try std.testing.expectEqual(Priority.immediate, batch[0].priority);
    try std.testing.expectEqual(Priority.heal, batch[1].priority);
    try std.testing.expectEqual(Priority.retry_low, batch[2].priority);
}

test "backpressure drops lowest priority" {
    const gpa = std.testing.allocator;
    var q = ReplQueue.init(gpa, 2);
    defer q.deinit();

    q.push(.{ .cid = try gpa.dupe(u8, "a"), .mode = .replicate, .priority = .normal, .enqueued_ns = 1, .target_peer = null, .shard_index = null });
    q.push(.{ .cid = try gpa.dupe(u8, "b"), .mode = .replicate, .priority = .retry_low, .enqueued_ns = 2, .target_peer = null, .shard_index = null });

    // Queue is full (2 items). Push immediate — should drop retry_low
    q.push(.{ .cid = try gpa.dupe(u8, "c"), .mode = .replicate, .priority = .immediate, .enqueued_ns = 3, .target_peer = null, .shard_index = null });

    try std.testing.expectEqual(@as(usize, 2), q.items.items.len);
    try std.testing.expectEqual(@as(u64, 1), q.dropped_count);

    const batch = q.popBatch(10, 0);
    defer {
        for (batch) |*item| {
            var it = item.*;
            it.deinit(gpa);
        }
        gpa.free(batch);
    }
    try std.testing.expectEqual(Priority.immediate, batch[0].priority);
    try std.testing.expectEqual(Priority.normal, batch[1].priority);
}

test "rate limiter basic" {
    var rl = RateLimiter.init(5);
    // Should be able to acquire up to 5 tokens
    var acquired: u32 = 0;
    for (0..10) |_| {
        if (rl.tryAcquire()) acquired += 1;
    }
    try std.testing.expectEqual(@as(u32, 5), acquired);
}

test "peer concurrency limit" {
    const gpa = std.testing.allocator;
    var pc = PeerConcurrency.init(gpa, 2);
    defer pc.deinit();

    try std.testing.expect(pc.acquire("peer1"));
    try std.testing.expect(pc.acquire("peer1"));
    try std.testing.expect(!pc.acquire("peer1")); // at limit

    pc.release("peer1");
    try std.testing.expect(pc.acquire("peer1")); // slot freed
}
