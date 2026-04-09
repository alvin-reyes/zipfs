//! Disk-backed blockstore with LRU cache, keyed by CID string.

const std = @import("std");
const cid_mod = @import("cid.zig");
const repo = @import("repo.zig");

const Cid = cid_mod.Cid;

const CacheEntry = struct {
    key: []u8,
    data: []u8,
};

pub const Blockstore = struct {
    repo_root: ?[]const u8 = null,

    // LRU cache fields
    cache: ?[]?CacheEntry = null,
    cache_index: std.StringHashMapUnmanaged(usize) = .empty,
    cache_pos: usize = 0,
    cache_capacity: usize = 0,
    cache_mu: std.Thread.Mutex = .{},

    /// Allocator used for cache entries (set during initCache).
    cache_allocator: ?std.mem.Allocator = null,

    pub fn initCache(self: *Blockstore, allocator: std.mem.Allocator, capacity: usize) !void {
        const cap = if (capacity == 0) 1024 else capacity;
        const slots = try allocator.alloc(?CacheEntry, cap);
        @memset(slots, null);
        self.cache = slots;
        self.cache_capacity = cap;
        self.cache_allocator = allocator;
    }

    pub fn deinit(self: *Blockstore, allocator: std.mem.Allocator) void {
        _ = allocator;
        {
            self.cache_mu.lock();
            defer self.cache_mu.unlock();
            if (self.cache) |slots| {
                const ca = self.cache_allocator orelse return;
                for (slots) |slot| {
                    if (slot) |entry| {
                        ca.free(entry.key); // shared with cache_index — frees both
                        ca.free(entry.data);
                    }
                }
                ca.free(slots);
                // Index keys are shared with slot keys (already freed above)
                self.cache_index.deinit(ca);
            }
        } // defer unlock runs HERE, before self.* = undefined clobbers the mutex
        self.* = undefined;
    }

    pub fn put(self: *Blockstore, allocator: std.mem.Allocator, c: Cid, data: []const u8) !void {
        const key = try c.toString(allocator);
        defer allocator.free(key);

        // Write to disk
        if (self.repo_root) |root| {
            try writeBlockFile(root, key, data);
        }

        // Insert into cache
        self.cacheInsert(key, data);
    }

    /// Get block data from cache or disk. Returns owned data that caller must free.
    pub fn get(self: *Blockstore, allocator: std.mem.Allocator, key_utf8: []const u8) ?[]u8 {
        // Check cache first
        {
            self.cache_mu.lock();
            defer self.cache_mu.unlock();
            if (self.cache_index.get(key_utf8)) |slot_idx| {
                if (self.cache) |slots| {
                    if (slots[slot_idx]) |entry| {
                        return allocator.dupe(u8, entry.data) catch null;
                    }
                }
            }
        }

        // Read from disk
        const root = self.repo_root orelse return null;
        const data = repo.readBlockFromDisk(allocator, root, key_utf8) orelse return null;

        // Insert into cache (data is already owned by caller, cache gets its own copy)
        self.cacheInsert(key_utf8, data);

        return data;
    }

    pub fn has(self: *Blockstore, key_utf8: []const u8) bool {
        // Check cache
        {
            self.cache_mu.lock();
            defer self.cache_mu.unlock();
            if (self.cache_index.contains(key_utf8)) return true;
        }
        // Check disk
        const root = self.repo_root orelse return false;
        return repo.blockExistsOnDisk(root, key_utf8);
    }

    pub fn count(self: *Blockstore) usize {
        self.cache_mu.lock();
        defer self.cache_mu.unlock();
        return self.cache_index.count();
    }

    pub fn remove(self: *Blockstore, allocator: std.mem.Allocator, key_utf8: []const u8) bool {
        _ = allocator;
        // Remove from cache
        {
            self.cache_mu.lock();
            defer self.cache_mu.unlock();
            if (self.cache_index.fetchRemove(key_utf8)) |kv| {
                const ca = self.cache_allocator orelse return false;
                // kv.key is shared with slot entry.key — free via slot only
                if (self.cache) |slots| {
                    if (slots[kv.value]) |entry| {
                        ca.free(entry.key);
                        ca.free(entry.data);
                        slots[kv.value] = null;
                    }
                } else {
                    // No slots but index had entry — free the shared key
                    ca.free(kv.key);
                }
            }
        }
        // Remove from disk
        if (self.repo_root) |root| {
            repo.removeBlockFile(root, key_utf8) catch {};
            return true;
        }
        return false;
    }

    // -- Internal cache operations --

    fn cacheInsert(self: *Blockstore, key: []const u8, data: []const u8) void {
        self.cache_mu.lock();
        defer self.cache_mu.unlock();

        const ca = self.cache_allocator orelse return;
        const slots = self.cache orelse return;
        if (self.cache_capacity == 0) return;

        // Already cached?
        if (self.cache_index.contains(key)) return;

        // Evict current slot if occupied
        const pos = self.cache_pos;
        if (slots[pos]) |old| {
            // Remove from index — key is shared with slot, free once
            _ = self.cache_index.remove(old.key);
            ca.free(old.key);
            ca.free(old.data);
            slots[pos] = null;
        }

        // Insert new entry — single key allocation shared between slot and index
        const key_dup = ca.dupe(u8, key) catch {
            std.log.warn("blockstore cache: OOM allocating key ({d} bytes)", .{key.len});
            return;
        };
        const data_dup = ca.dupe(u8, data) catch {
            std.log.warn("blockstore cache: OOM allocating data ({d} bytes)", .{data.len});
            ca.free(key_dup);
            return;
        };

        slots[pos] = .{ .key = key_dup, .data = data_dup };
        self.cache_index.put(ca, key_dup, pos) catch {
            std.log.warn("blockstore cache: OOM inserting index entry", .{});
            ca.free(key_dup);
            ca.free(data_dup);
            slots[pos] = null;
            return;
        };

        self.cache_pos = (pos + 1) % self.cache_capacity;
    }

    fn writeBlockFile(root: []const u8, cid_key: []const u8, data: []const u8) !void {
        const path = try repo.joinBlocksPath(std.heap.page_allocator, root, cid_key);
        defer std.heap.page_allocator.free(path);
        if (std.fs.path.dirname(path)) |dir| {
            try std.fs.cwd().makePath(dir);
        }
        const file = try std.fs.cwd().createFile(path, .{ .truncate = true });
        file.writeAll(data) catch |e| {
            file.close();
            return e;
        };
        file.sync() catch |e| {
            file.close();
            return e;
        };
        file.close();
    }
};
