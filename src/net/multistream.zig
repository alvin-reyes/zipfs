//! Multistream-select 1.0 line framing (`<proto>\\n`).

const std = @import("std");

pub const multistream_1_0 = "/multistream/1.0.0\n";

pub fn writeProtocol(stream: std.net.Stream, name: []const u8) !void {
    try stream.writeAll(name);
    if (name.len == 0 or name[name.len - 1] != '\n')
        try stream.writeAll("\n");
}

/// Read one LF-terminated line into `buf`; returns slice without `\n`.
pub fn readLine(stream: std.net.Stream, buf: []u8) ![]const u8 {
    var i: usize = 0;
    while (i < buf.len) : (i += 1) {
        var b: [1]u8 = undefined;
        const n = try stream.read(&b);
        if (n == 0) return error.UnexpectedEof;
        if (b[0] == '\n') return buf[0..i];
        buf[i] = b[0];
    }
    return error.LineTooLong;
}
