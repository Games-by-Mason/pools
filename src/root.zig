const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;

/// A heap of handles. Allocates a free list of handles up front with length equal to the pool
/// capacity.
///
/// See `handle_type`.
pub fn HandleHeap(Handle: type) type {
    return struct {
        const Len = handle_type.CapacityType(Handle);
        pub const capacity = handle_type.capacity(Handle);

        free: *[capacity]Handle,
        free_len: Len,
        allocated: Len,

        pub fn init(allocator: Allocator) Allocator.Error!@This() {
            return .{
                .free = try allocator.create([capacity]Handle),
                .allocated = 0,
                .free_len = 0,
            };
        }

        pub fn deinit(self: *@This(), allocator: Allocator) void {
            allocator.destroy(self.free);
            self.* = undefined;
        }

        pub fn alloc(self: *@This()) error{OutOfMemory}!Handle {
            // First, try allocating from the free list
            if (self.free_len > 0) {
                self.free_len -= 1;
                return self.free[self.free_len];
            }

            // If that fails, try appending to the buffer
            if (self.allocated >= capacity) return error.OutOfMemory;
            const index = self.allocated;
            self.allocated += 1;
            return handle_type.fromIndex(Handle, index);
        }

        pub fn remove(self: *@This(), handle: Handle) void {
            std.debug.assert(self.free_len < capacity);
            self.free[self.free_len] = handle;
            self.free_len += 1;
        }

        pub fn count(self: *const @This()) usize {
            return self.allocated - self.free_len;
        }
    };
}

/// Helper functions for working with handles.
///
/// `Handle` may be given an integer type, or a non-exhaustive enum with no fields. The capacity of
/// the pool is equivalent to the maximum value of this integer or enum.
pub const handle_type = struct {
    /// Checks if the given type can be used as a handle.
    pub fn typeCheck(Handle: type) void {
        switch (@typeInfo(Handle)) {
            .int => {},
            .@"enum" => |e| {
                if (e.is_exhaustive) @compileError("Handle enums must be non-exhaustive");
                if (e.fields.len > 0) @compileError("Handle enums may not have fields");
            },
            else => @compileError("Handle must be a fixed size integer, or an enum"),
        }
    }

    /// Converts the given index into the given handle type.
    pub fn fromIndex(Handle: type, index: usize) Handle {
        handle_type.typeCheck(Handle);
        switch (@typeInfo(Handle)) {
            .int => return @intCast(index),
            .@"enum" => return @enumFromInt(index),
            else => unreachable,
        }
    }

    /// Converts the given handle into an index.
    pub fn toIndex(handle: anytype) usize {
        handle_type.typeCheck(@TypeOf(handle));
        switch (@typeInfo(@TypeOf(handle))) {
            .int => return @intCast(handle),
            .@"enum" => return @intFromEnum(handle),
            else => unreachable,
        }
    }

    /// Returns the backing integer of the given handle type.
    pub fn backingInt(Handle: type) type {
        handle_type.typeCheck(Handle);
        return switch (@typeInfo(Handle)) {
            .int => Handle,
            .@"enum" => |e| e.tag_type,
            else => unreachable,
        };
    }

    /// Returns the capacity type a given handle type.
    pub fn CapacityType(Handle: type) type {
        handle_type.typeCheck(Handle);
        return @Type(.{ .int = .{
            .signedness = .unsigned,
            .bits = @typeInfo(backingInt(Handle)).int.bits + 1,
        } });
    }

    /// Returns the capacity of a given handle type.
    pub fn capacity(Handle: type) CapacityType(Handle) {
        handle_type.typeCheck(Handle);
        const backing_int = backingInt(Handle);
        return @as(CapacityType(Handle), @intCast(std.math.maxInt(backing_int))) + 1;
    }
};

/// See `ArrayBackedAligned`.
pub fn ArrayBacked(Handle: type, Item: type, empty: Item) type {
    return ArrayBackedAligned(Handle, Item, .fromByteUnits(@alignOf(Item)), empty);
}

/// A pool backed by an array and a `HandleHeap`. Useful when external iteration over the allocated
// items is needed.
///
/// Allocates two arrays internally:
///  1. An array of items
///  2. A an array of free handles (as part of handle heap)
///
/// The free list is not stored inline in the items list so that the items list can be exposed as
/// type `[]Item` with holes marked. If items lots were reused as free list indices, they either
/// could not store state representing whether the slot is empty, or they could not be `Item` sized.
/// gives up the ability to mark holes in the with the empty.
pub fn ArrayBackedAligned(
    /// See `handle_type`.
    Handle: type,
    /// The item type stored by this pool. May be set to void if all you need are handles, or you
    /// want to create a multi pool.
    Item: type,
    /// The alignment of the pool.
    alignment: std.mem.Alignment,
    /// The value to write over removed items in the items array, often but not always `null`. May
    /// be set to `undefined` if this feature is not needed.
    empty: Item,
) type {
    return struct {
        pub const Handles = HandleHeap(Handle);

        items: *align(alignment.toByteUnits()) [Handles.capacity]Item,
        handles: Handles,

        pub fn init(allocator: Allocator) Allocator.Error!@This() {
            const items_slice = try allocator.alignedAlloc([Handles.capacity]Item, alignment, 1);
            const items = &items_slice[0];
            errdefer allocator.destroy(items);
            const handles = try Handles.init(allocator);
            errdefer handles.deinit();
            return .{
                .items = items,
                .handles = handles,
            };
        }

        pub fn deinit(self: *@This(), allocator: Allocator) void {
            self.handles.deinit(allocator);
            allocator.free(self.items);
            self.* = undefined;
        }

        pub fn addOne(self: *@This()) error{OutOfMemory}!Handle {
            return self.put(undefined);
        }

        pub fn put(self: *@This(), value: Item) error{OutOfMemory}!Handle {
            const handle = try self.handles.alloc();
            const index = handle_type.toIndex(handle);
            self.items[index] = value;
            return handle;
        }

        pub fn remove(self: *@This(), handle: Handle) void {
            self.handles.remove(handle);
            const index = handle_type.toIndex(handle);
            self.items[index] = empty;
        }
    };
}

fn testFillPool(Handle: type) !void {
    const empty = std.math.maxInt(usize);
    const P = ArrayBacked(Handle, usize, empty);
    var pool: P = try P.init(std.testing.allocator);
    defer pool.deinit(std.testing.allocator);
    try testing.expectEqual(0, pool.handles.count());

    // Half fill the pool with even numbers
    for (0..P.Handles.capacity / 2) |i| {
        try testing.expectEqual(handle_type.fromIndex(Handle, i), try pool.put(i * 2));
    }
    try testing.expectEqual(P.Handles.capacity / 2, pool.handles.count());
    for (0..P.Handles.capacity / 2) |i| {
        try testing.expectEqual(i * 2, pool.items[i]);
    }

    // Fill the rest of the pool
    for (P.Handles.capacity / 2..P.Handles.capacity) |i| {
        try testing.expectEqual(handle_type.fromIndex(Handle, i), try pool.put(i * 2));
    }
    try testing.expectEqual(P.Handles.capacity, pool.handles.count());

    try testing.expectError(error.OutOfMemory, pool.put(0));
    try testing.expectError(error.OutOfMemory, pool.put(0));
    try testing.expectEqual(P.Handles.capacity, pool.handles.count());

    for (0..P.Handles.capacity) |i| {
        try testing.expectEqual(i * 2, pool.items[i]);
    }

    // Free half the pool
    for (0..P.Handles.capacity / 2) |i| {
        pool.remove(handle_type.fromIndex(Handle, i));
    }

    try testing.expectEqual(P.Handles.capacity - (P.Handles.capacity / 2), pool.handles.count());

    for (0..P.Handles.capacity / 2) |i| {
        try testing.expectEqual(empty, pool.items[i]);
    }

    for (P.Handles.capacity / 2..P.Handles.capacity) |i| {
        try testing.expectEqual(i * 2, pool.items[i]);
    }

    // Allocate from the free list
    for (0..P.Handles.capacity / 2) |i| {
        const expected = handle_type.fromIndex(Handle, P.Handles.capacity / 2 - 1 - i);
        const found = pool.put(i * 10);
        try testing.expectEqual(expected, found);
    }
    try testing.expectEqual(P.Handles.capacity, pool.handles.count());

    for (0..P.Handles.capacity / 2) |i| {
        try testing.expectEqual(((P.Handles.capacity / 2) - 1 - i) * 10, pool.items[i]);
    }

    for (P.Handles.capacity / 2..P.Handles.capacity) |i| {
        try testing.expectEqual(i * 2, pool.items[i]);
    }

    // Free the second half of the pool
    for (P.Handles.capacity / 2..P.Handles.capacity) |i| {
        pool.remove(handle_type.fromIndex(Handle, i));
    }

    try testing.expectEqual(P.Handles.capacity / 2, pool.handles.count());

    for (P.Handles.capacity / 2..P.Handles.capacity) |i| {
        try testing.expectEqual(empty, pool.items[i]);
    }

    for (0..P.Handles.capacity / 2) |i| {
        try testing.expectEqual(((P.Handles.capacity / 2) - 1 - i) * 10, pool.items[i]);
    }

    // Allocate from the free list
    for (0..(P.Handles.capacity - P.Handles.capacity / 2)) |i| {
        const expected = handle_type.fromIndex(Handle, P.Handles.capacity - 1 - i);
        const found = try pool.put(i);
        try testing.expectEqual(expected, found);
    }
    try testing.expectEqual(P.Handles.capacity, pool.handles.count());

    for (0..P.Handles.capacity / 2) |i| {
        try testing.expectEqual(((P.Handles.capacity / 2) - 1 - i) * 10, pool.items[i]);
    }

    for (P.Handles.capacity / 2..P.Handles.capacity) |i| {
        try testing.expectEqual(P.Handles.capacity - 1 - i, pool.items[i]);
    }

    // Free all values
    for (0..P.Handles.capacity) |i| pool.remove(handle_type.fromIndex(Handle, i));
    try testing.expectEqual(0, pool.handles.count());
    for (0..P.Handles.capacity) |i| {
        try testing.expectEqual(empty, pool.items[i]);
    }

    // Reallocate all values
    for (0..P.Handles.capacity) |i| {
        const expected = handle_type.fromIndex(Handle, P.Handles.capacity - 1 - i);
        const found = pool.put(i * 100);
        try testing.expectEqual(expected, found);
    }
    try testing.expectEqual(P.Handles.capacity, pool.handles.count());

    for (0..P.Handles.capacity) |i| {
        try testing.expectEqual((P.Handles.capacity - 1 - i) * 100, pool.items[i]);
    }
}

test "fill pool" {
    try testFillPool(u8);
    try testFillPool(enum(u8) { _ });
}

/// Bump allocated handles within a runtime set range. See `HandleArean` and `handle_type`.
pub fn HandleRegion(Handle: type) type {
    return struct {
        const Len = handle_type.CapacityType(Handle);

        min: Len,
        capacity: Len,
        allocated: Len,

        pub fn init(min: Len, capacity: usize) error{OutOfMemory}!@This() {
            if (capacity > handle_type.capacity(Handle) or min > handle_type.capacity(Handle) - capacity) {
                return error.OutOfMemory;
            }
            return .{
                .min = min,
                .capacity = @intCast(capacity),
                .allocated = 0,
            };
        }

        pub fn alloc(self: *@This()) error{OutOfMemory}!Handle {
            if (self.allocated >= self.capacity) return error.OutOfMemory;
            const index = handle_type.toIndex(self.min) + self.allocated;
            const result = handle_type.fromIndex(Handle, index);
            self.allocated += 1;
            return result;
        }

        pub fn reset(self: *@This()) void {
            self.allocated = 0;
        }
    };
}

test "handle bump range" {
    // Test from 0 to the end
    {
        const Handle = enum(u8) { _ };
        var handles = try HandleRegion(Handle).init(0, std.math.maxInt(handle_type.backingInt(Handle)));
        for (0..255) |i| {
            const expected: Handle = @enumFromInt(i);
            try testing.expectEqual(expected, handles.alloc());
        }
        try testing.expectError(error.OutOfMemory, handles.alloc());
        try testing.expectError(error.OutOfMemory, handles.alloc());

        handles.reset();

        for (0..255) |i| {
            const expected: Handle = @enumFromInt(i);
            try testing.expectEqual(expected, handles.alloc());
        }
        try testing.expectError(error.OutOfMemory, handles.alloc());
        try testing.expectError(error.OutOfMemory, handles.alloc());
    }

    // Test from 0
    {
        const Handle = enum(u8) { _ };
        var handles = try HandleRegion(Handle).init(0, 10);
        for (0..10) |i| {
            const expected: Handle = @enumFromInt(i);
            try testing.expectEqual(expected, handles.alloc());
        }
        try testing.expectError(error.OutOfMemory, handles.alloc());
        try testing.expectError(error.OutOfMemory, handles.alloc());

        handles.reset();

        for (0..10) |i| {
            const expected: Handle = @enumFromInt(i);
            try testing.expectEqual(expected, handles.alloc());
        }
        try testing.expectError(error.OutOfMemory, handles.alloc());
        try testing.expectError(error.OutOfMemory, handles.alloc());
    }

    // Test the middle
    {
        const Handle = enum(u8) { _ };
        var handles = try HandleRegion(Handle).init(50, 100);
        for (50..150) |i| {
            const expected: Handle = @enumFromInt(i);
            try testing.expectEqual(expected, handles.alloc());
        }
        try testing.expectError(error.OutOfMemory, handles.alloc());
        try testing.expectError(error.OutOfMemory, handles.alloc());

        handles.reset();

        for (50..150) |i| {
            const expected: Handle = @enumFromInt(i);
            try testing.expectEqual(expected, handles.alloc());
        }
        try testing.expectError(error.OutOfMemory, handles.alloc());
        try testing.expectError(error.OutOfMemory, handles.alloc());
    }

    // Test from the middle to the end
    {
        const Handle = enum(u8) { _ };
        var handles = try HandleRegion(Handle).init(200, 55);
        for (200..255) |i| {
            const expected: Handle = @enumFromInt(i);
            try testing.expectEqual(expected, handles.alloc());
        }
        try testing.expectError(error.OutOfMemory, handles.alloc());
        try testing.expectError(error.OutOfMemory, handles.alloc());

        handles.reset();

        for (200..255) |i| {
            const expected: Handle = @enumFromInt(i);
            try testing.expectEqual(expected, handles.alloc());
        }
        try testing.expectError(error.OutOfMemory, handles.alloc());
        try testing.expectError(error.OutOfMemory, handles.alloc());
    }

    // Test out of range inits
    {
        const Handle = enum(u8) { _ };
        try testing.expectError(error.OutOfMemory, HandleRegion(Handle).init(0, 257));
        try testing.expectError(error.OutOfMemory, HandleRegion(Handle).init(1, 256));
        try testing.expectError(error.OutOfMemory, HandleRegion(Handle).init(2, 255));
    }
}

/// An arena of handles. Can be used to reserve regions, and then use bump allocation on the
/// individual regions for quick region based handle management.
pub fn HandleArena(Handle: type) type {
    return struct {
        pub const capacity = handle_type.capacity(Handle);
        const Len = handle_type.CapacityType(Handle);

        /// The number of handles that have been reserved. May be directly set to reset the
        /// allocator to a previous state.
        reserved: Len = 0,

        pub fn alloc(self: *@This(), len: usize) error{OutOfMemory}!HandleRegion(Handle) {
            const result = try HandleRegion(Handle).init(self.reserved, len);
            self.reserved += @intCast(len);
            return result;
        }
    };
}

test "handle bump ranges" {
    const Handle = u8;
    var ranges: HandleArena(Handle) = .{};

    const r0 = try ranges.alloc(200);
    try std.testing.expectEqual(0, r0.min);
    try std.testing.expectEqual(200, r0.capacity);

    try std.testing.expectError(error.OutOfMemory, ranges.alloc(200));

    const r1 = try ranges.alloc(56);
    try std.testing.expectEqual(200, r1.min);
    try std.testing.expectEqual(56, r1.capacity);

    try std.testing.expectError(error.OutOfMemory, ranges.alloc(1));

    var r2 = try ranges.alloc(0);
    try std.testing.expectEqual(256, r2.min);
    try std.testing.expectEqual(0, r2.capacity);
    try std.testing.expectError(error.OutOfMemory, r2.alloc());

    const r3 = try ranges.alloc(0);
    try std.testing.expectEqual(256, r3.min);
    try std.testing.expectEqual(0, r3.capacity);
}

/// A bump allocator for handles. See `handle_type`.
pub fn HandleBump(Handle: type) type {
    return struct {
        pub const capacity = handle_type.capacity(Handle);
        const Len = handle_type.CapacityType(Handle);
        allocated: Len = 0,

        pub fn alloc(self: *@This()) error{OutOfMemory}!Handle {
            const index = std.math.cast(handle_type.backingInt(Handle), self.allocated) orelse return error.OutOfMemory;
            self.allocated += 1;
            return handle_type.fromIndex(Handle, index);
        }

        pub fn clear(self: *@This()) void {
            self.allocated = 0;
        }
    };
}

test "handle bump" {
    var handles: HandleBump(u8) = .{};
    for (0..256) |i| {
        try std.testing.expectEqual(@as(u8, @intCast(i)), handles.alloc());
    }
    try std.testing.expectError(error.OutOfMemory, handles.alloc());
    try std.testing.expectError(error.OutOfMemory, handles.alloc());
}
