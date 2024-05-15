const std = @import("std");

const parser = @import("parser.zig");

test "Test parse bulk strings" {
    // input, value, len
    const expected = [_]struct { []const u8, []const u8, usize }{
        .{ "$12\r\nheyheyheyhey\r\n", "heyheyheyhey", 12 },
        .{ "$3\r\nhey\r\n", "hey", 3 },
    };

    for (expected) |exp| {
        const res: parser.DataBulkString = try parser.parse_bulkstring_data(exp[0]);
        try std.testing.expect(parser.DataBulkString == @TypeOf(res));
        try std.testing.expectEqualStrings(exp[1], res.value);
        try std.testing.expectEqual(exp[2], res.length);
    }
}

test "Test parse arrays" {
    // input, size, elems
    const expected = [_]struct { []const u8, usize, []const parser.DataItem }{
        .{
            "*2\r\n$4\r\nECHO\r\n$12\r\nheyheyheyhey\r\n",
            2,
            &[_]parser.DataItem{
                parser.DataItem{
                    .bulk_string = parser.DataBulkString{ .value = "ECHO", .length = 4 },
                },
                parser.DataItem{
                    .bulk_string = parser.DataBulkString{ .value = "heyheyheyhey", .length = 12 },
                },
            },
        },
        .{
            "*3\r\n$4\r\nECHO\r\n$3\r\nhey\r\n$5\r\nhello\r\n",
            3,
            &[_]parser.DataItem{
                parser.DataItem{
                    .bulk_string = parser.DataBulkString{ .value = "ECHO", .length = 4 },
                },
                parser.DataItem{
                    .bulk_string = parser.DataBulkString{ .value = "hey", .length = 3 },
                },
                parser.DataItem{
                    .bulk_string = parser.DataBulkString{ .value = "hello", .length = 5 },
                },
            },
        },
    };

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    for (expected) |exp| {
        const res: parser.DataArrays = try parser.parse_arrays_data(&alloc, exp[0]);
        try std.testing.expectEqual(parser.DataArrays, @TypeOf(res));
        try std.testing.expectEqual(exp[1], res.size);
        for (exp[2], res.elems) |exp_elem, elem| {
            try std.testing.expectEqualStrings(exp_elem.bulk_string.value, elem.bulk_string.value);
            try std.testing.expectEqual(exp_elem.bulk_string.length, elem.bulk_string.length);
        }
    }
}
