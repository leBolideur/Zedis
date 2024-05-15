const std = @import("std");
const net = std.net;

const parser = @import("parser.zig");
const DataArrays = parser.DataArrays;
const DataString = parser.DataString;
const DataItem = parser.DataItem;

const RDBParser = @import("rdb_parser.zig").RDBParser;

const Config = struct {
    dir: ?[]const u8,
    dbfilename: ?[]const u8,
};

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    var config = Config{ .dir = null, .dbfilename = null };
    var args = std.process.args().inner;
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--dir")) {
            const dir = args.next().?;
            config.dir = dir;
        } else if (std.mem.eql(u8, arg, "--dbfilename")) {
            const dbfilename = args.next().?;
            config.dbfilename = dbfilename;
        }
    }

    try stdout.print("Logs from your program will appear here!", .{});

    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var map = try parse_rdb_file(&allocator, config);

    while (true) {
        const connection = try listener.accept();

        _ = try std.Thread.spawn(.{}, handle_client, .{ &allocator, connection, &config, &map });

        try stdout.print("accepted new connection", .{});
    }
}

fn parse_rdb_file(allocator: *const std.mem.Allocator, config: Config) !?std.StringHashMap([]const u8) {
    if (config.dir != null and config.dbfilename != null) {
        const filepath = try std.fmt.allocPrint(allocator.*, "{s}/{s}", .{ config.dir.?, config.dbfilename.? });

        var rdb_parser = RDBParser.init(allocator, filepath) catch return null;
        try rdb_parser.parse();
        return rdb_parser.map;
    }

    return null;
}

const MapValue = struct {
    value: []const u8,
    add_time: i64,
    px: ?i64,
};

fn handle_client(
    allocator: *const std.mem.Allocator,
    client: net.Server.Connection,
    config: *Config,
    rdb_map: *?std.StringHashMap([]const u8),
) !void {
    const reader = client.stream.reader();
    var buf: [128]u8 = undefined;
    var map = std.StringHashMap(MapValue).init(allocator.*);

    while (true) {
        const read = try reader.read(&buf);
        if (read == 0) break;

        const msg = buf[0..read];
        const data_type = parser.parse_data_type(msg);
        switch (data_type) {
            .STRING => {
                const data_string = parser.parse_string_data(msg);
                _ = data_string;
            },
            .ARRAYS => {
                const arrays = try parser.parse_arrays_data(allocator, msg);
                const cmd = get_cmd(arrays);
                switch (cmd.?) {
                    .PING => _ = try client.stream.write("+PONG\r\n"),
                    .ECHO => {
                        _ = try client.stream.writer().print(
                            "${d}\r\n{s}\r\n",
                            .{ arrays.elems[1].bulk_string.length, arrays.elems[1].bulk_string.value },
                        );
                    },
                    .GET => try execute_get_command(arrays.elems, &map, client, rdb_map),
                    .SET => try execute_set_command(arrays.elems, &map, client),
                    .CONFIG => try execute_config_command(arrays.elems, client, config),
                    .KEYS => {
                        // if (rdb_map == null) {
                        //     return;
                        // }

                        const map_count = rdb_map.*.?.count();
                        var iter = rdb_map.*.?.iterator();
                        var key_buf = std.ArrayList(u8).init(allocator.*);
                        while (iter.next()) |item| {
                            const key = item.key_ptr.*;
                            try std.fmt.format(key_buf.writer(), "${d}\r\n{s}\r\n", .{ key.len, key });
                        }

                        const key_slice = try key_buf.toOwnedSlice();
                        _ = try client.stream.writer().print(
                            "*{d}\r\n{s}",
                            .{ map_count, key_slice },
                        );
                    },
                }
            },
            .B_STRING => {
                const data_bulk = try parser.parse_bulkstring_data(&buf);
                _ = data_bulk;
            },
        }
    }

    client.stream.close();
}

const Command = enum { PING, ECHO, GET, SET, CONFIG, KEYS };

fn get_cmd(arrays: DataArrays) ?Command {
    const first = arrays.elems[0].bulk_string.value;

    if (std.mem.eql(u8, first, "ECHO")) {
        return Command.ECHO;
    } else if (std.mem.eql(u8, first, "PING")) {
        return Command.PING;
    } else if (std.mem.eql(u8, first, "SET")) {
        return Command.SET;
    } else if (std.mem.eql(u8, first, "GET")) {
        return Command.GET;
    } else if (std.mem.eql(u8, first, "CONFIG")) {
        return Command.CONFIG;
    } else if (std.mem.eql(u8, first, "KEYS")) {
        return Command.KEYS;
    }

    return null;
}

fn execute_config_command(elems: []DataItem, client: net.Server.Connection, config: *Config) !void {
    const method = elems[1].bulk_string.value;
    _ = method;
    const field = elems[2].bulk_string.value;

    // std.debug.print("\nmethod: {s}\tfield: {s}\n", .{ method, field });

    if (std.mem.eql(u8, field, "dir")) {
        _ = try client.stream.writer().print(
            "*2\r\n$3\r\ndir\r\n${d}\r\n{s}\r\n",
            .{ config.*.dir.?.len, config.*.dir.? },
        );
    } else if (std.mem.eql(u8, field, "dbfilename")) {
        _ = try client.stream.writer().print(
            "*2\r\n$10\r\ndbfilename\r\n${d}\r\n{s}\r\n",
            .{ config.*.dbfilename.?.len, config.*.dbfilename.? },
        );
    }
}

fn execute_set_command(elems: []DataItem, map: *std.StringHashMap(MapValue), client: net.Server.Connection) !void {
    const key = elems[1].bulk_string.value;
    const value = elems[2].bulk_string.value;

    var px: ?i64 = null;
    if (elems.len == 5) {
        const raw_value = elems[4].bulk_string.value;
        const int = try std.fmt.parseInt(i64, raw_value, 10);
        px = int;
    }

    const map_value = MapValue{
        .value = value,
        .add_time = std.time.milliTimestamp(),
        .px = px,
    };
    try map.*.put(key, map_value);
    _ = try client.stream.write("+OK\r\n");
}

fn execute_get_command(
    elems: []DataItem,
    map: *std.StringHashMap(MapValue),
    client: net.Server.Connection,
    rdb_map: *?std.StringHashMap([]const u8),
) !void {
    const key = elems[1].bulk_string.value;
    // const map_value: ?MapValue = map.*.get(key);

    var value: ?[]const u8 = undefined;
    if (rdb_map.* != null) {
        value = rdb_map.*.?.get(key);
    } else {
        // _ = map;
        // _ = try client.stream.write("$-1\r\n");
        // return;
        const local_value = map.*.get(key).?;

        const now = std.time.milliTimestamp();
        const add_time = local_value.add_time;
        const px = local_value.px;
        if (px == null or now < (add_time + px.?)) {
            _ = try client.stream.writer().print(
                "${d}\r\n{s}\r\n",
                .{ local_value.value.len, local_value.value },
            );
            return;
        } else {
            _ = try client.stream.write("$-1\r\n");
            return;
        }
    }

    if (value == null) {
        _ = try client.stream.write("$-1\r\n");
        return;
    }

    _ = try client.stream.writer().print(
        "${d}\r\n{s}\r\n",
        .{ value.?.len, value.? },
    );

    // const now = std.time.milliTimestamp();
    // const add_time = map_value.?.add_time;
    // const px = map_value.?.px;
    // if (px == null or now < (add_time + px.?)) {
    //     _ = try client.stream.writer().print(
    //         "${d}\r\n{s}\r\n",
    //         .{ map_value.?.value.len, map_value.?.value },
    //     );
    // } else {
    //     _ = try client.stream.write("$-1\r\n");
    // }
}
