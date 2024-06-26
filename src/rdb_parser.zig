const std = @import("std");

pub const RDBParser = struct {
    buffer: []const u8,
    cursor: usize,
    map: std.StringHashMap([]const u8),

    pub fn init(allocator: *const std.mem.Allocator, filepath: []const u8) !RDBParser {
        const file = try std.fs.cwd().openFile(filepath, .{ .mode = .read_only });
        const stat = try file.stat();
        var reader = file.reader();
        const buffer = try allocator.alloc(u8, stat.size);
        _ = try reader.readAll(buffer);

        return RDBParser{
            .buffer = buffer,
            .cursor = 0,
            .map = std.StringHashMap([]const u8).init(allocator.*),
        };
    }

    fn read_byte(self: *RDBParser) usize {
        const byte = self.buffer[self.cursor];
        self.cursor += 1;
        return byte;
    }

    fn read_u32(self: *RDBParser) u32 {
        var result: u32 = 0;
        const bytes = self.buffer[self.cursor..(self.cursor + 4)];
        for (bytes, 0..) |byte, i| {
            const shift: u5 = @as(u5, @intCast(i * 8));
            result |= @as(u32, @intCast(byte)) << shift;
        }
        self.cursor += 4;
        return result;
    }

    fn read_u64(self: *RDBParser) u64 {
        var result: u64 = 0;
        const bytes = self.buffer[self.cursor..(self.cursor + 8)];
        for (bytes, 0..) |byte, i| {
            const shift: u6 = @as(u6, @intCast(i * 8));
            result |= @as(u64, @intCast(byte)) << shift;
        }
        self.cursor += 8;
        return result;
    }

    fn decode_length(self: *RDBParser, byte: usize) []const usize {
        const two = byte >> 6;

        var value: []const usize = undefined;
        switch (two) {
            0b00 => value = &[_]usize{byte & 0x00111111},
            0b01 => {
                const sec_byte = self.read_byte();
                value = &[_]usize{((byte & 0b00111111) << 8) | sec_byte};
            },
            // 0b10 => value = self.read_u32(),
            // Special format
            0b11 => value = &[_]usize{byte & 0x00111111},
            else => {},
        }

        return value;
    }

    pub fn parse(self: *RDBParser) !void {
        while (true) {
            if (self.cursor >= self.buffer.len) return;

            const byte = self.read_byte();
            switch (byte) {
                0xFF => {
                    return;
                },
                0xFE => {
                    const encoded_len: usize = self.read_byte();
                    const db_number = self.decode_length(encoded_len);
                    _ = db_number;
                },
                0xFD => {},
                0xFC => {},
                0xFB => {
                    const b = self.read_byte();
                    _ = self.decode_length(b);
                    _ = self.decode_length(self.read_byte());

                    try self.read_data(b);
                },
                0xFA => {},
                else => {},
            }
        }
    }

    pub fn read_data(self: *RDBParser, count: usize) !void {
        for (0..count) |_| {
            const key_type = self.read_byte();
            var expire: ?usize = null;
            if (key_type == 0xfc) {
                //expire in millisecond
                expire = self.read_u64();
                _ = self.read_byte();
            } else if (key_type == 0xfd) {
                // expire in second
                expire = self.read_u32();
                _ = self.read_byte();
            }

            const key_len = self.read_byte();
            const key = (self.buffer[self.cursor..(self.cursor + key_len)]);

            self.cursor += key_len;

            const value_len = self.read_byte();
            const value = (self.buffer[self.cursor..(self.cursor + value_len)]);
            self.cursor += value_len;

            const now = std.time.milliTimestamp();
            if (expire == null or now < expire.?) {
                try self.map.put(key, value);
            }
        }
    }
};
