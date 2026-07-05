/// Reason why one or more messages were dropped from the outbound buffer.
enum BufferDropReason {
  /// Message Time-To-Live (TTL) expired before transmission.
  ttlExpired,

  /// Message dropped due to buffer overflow or byte capacity limits.
  overflow,

  /// Buffer cleared because the socket or buffer was disposed/closed.
  disposed,
}
