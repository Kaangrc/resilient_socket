import 'package:meta/meta.dart';

/// Immutable wrapper for a message enqueued in the outbound buffer.
@immutable
final class BufferedMessage {
  /// Creates a buffered message with its metadata.
  const BufferedMessage({
    required this.payload,
    required this.enqueuedAt,
    required this.ttl,
    required this.priority,
  });

  /// The underlying message payload.
  final Object payload;

  /// The exact time when the message was enqueued.
  final DateTime enqueuedAt;

  /// Time-To-Live duration for this message.
  final Duration ttl;

  /// Priority of this message (higher value means higher priority).
  final int priority;

  /// Returns `true` if this message has expired at [now] using strict greater-than evaluation.
  bool isExpiredAt(DateTime now) => now.difference(enqueuedAt) > ttl;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is BufferedMessage &&
          runtimeType == other.runtimeType &&
          payload == other.payload &&
          enqueuedAt == other.enqueuedAt &&
          ttl == other.ttl &&
          priority == other.priority;

  @override
  int get hashCode =>
      payload.hashCode ^ enqueuedAt.hashCode ^ ttl.hashCode ^ priority.hashCode;

  @override
  String toString() =>
      'BufferedMessage(payload: $payload, enqueuedAt: $enqueuedAt, ttl: $ttl, priority: $priority)';
}
