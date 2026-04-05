//! IPNS record verification / publish (stub until DHT wiring).

pub fn verifyRecord(_: []const u8) error{IpnsNotImplemented}!void {
    return error.IpnsNotImplemented;
}

pub fn publishRecord(_: []const u8, _: []const u8) error{IpnsNotImplemented}!void {
    return error.IpnsNotImplemented;
}
