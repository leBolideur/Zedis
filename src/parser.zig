const std = @import("std");

pub const DataType = enum(u8) { STRING, B_STRING, ARRAYS };
pub const DataString = struct { value: []const u8 };
pub const DataBulkString = struct { value: []const u8, length: usize };
pub const DataArrays = struct { size: usize, elems: []DataItem };

pub const DataItem = union(enum) { string: DataString, bulk_string: DataBulkString, arrays: DataArrays };

pub fn parse_data_type(msg: []const u8) DataType {
    const data_type = msg[0..1];

    if (std.mem.eql(u8, data_type, "+")) {
        return DataType.STRING;
    } else if (std.mem.eql(u8, data_type, "*")) {
        return DataType.ARRAYS;
    } else if (std.mem.eql(u8, data_type, "$")) {
        return DataType.B_STRING;
    }

    unreachable;
}

pub fn parse_string_data(input: []const u8) DataString {
    return DataString{ .value = input[1 .. input.len - 1] }; // Exclude \r\n
}

pub fn parse_bulkstring_data(input: []const u8) !DataBulkString {
    var split = std.mem.split(u8, input[1..], "\r\n");
    const len: usize = try std.fmt.parseInt(usize, split.next().?, 10);
    const value = split.next().?;

    return DataBulkString{ .value = value, .length = len };
}

pub fn parse_arrays_data(allocator: *const std.mem.Allocator, input: []const u8) !DataArrays {
    var split = std.mem.split(u8, input[1..], "\r\n");
    const size = try std.fmt.parseInt(usize, split.next().?, 10);

    var elems = std.ArrayList(DataItem).init(allocator.*);
    var idx: usize = 0;
    while (idx < size) : (idx += 1) {
        const elem = split.next().?;
        const type_ = parse_data_type(elem);
        switch (type_) {
            .B_STRING => {
                const len: usize = try std.fmt.parseInt(usize, elem[1..], 10);
                const bulk = DataBulkString{ .value = split.next().?, .length = len };

                try elems.append(DataItem{ .bulk_string = bulk });
            },
            else => continue,
        }
    }

    const slice_elems = try elems.toOwnedSlice();
    return DataArrays{ .size = size, .elems = slice_elems };
}
