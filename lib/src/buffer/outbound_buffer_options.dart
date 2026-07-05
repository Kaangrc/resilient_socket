import 'package:meta/meta.dart';
import 'package:resilient_socket/src/buffer/overflow_strategy.dart';

/// Configuration options for the outbound message buffer.
@immutable
class OutboundBufferOptions {
  /// Creates outbound buffer configuration options.
  const OutboundBufferOptions({
    this.maxMessages = 500,
    this.maxBytes,
    this.defaultTtl = const Duration(seconds: 20),
    this.overflow = OverflowStrategy.dropOldest,
    this.sizeEstimator,
  });

  /// Maximum number of messages allowed in the buffer.
  final int maxMessages;

  /// Maximum total estimated bytes allowed in the buffer.
  /// Enforced only when [sizeEstimator] is provided and non-null.
  final int? maxBytes;

  /// Default Time-To-Live (TTL) for enqueued messages.
  final Duration defaultTtl;

  /// Strategy used when the buffer exceeds [maxMessages] or [maxBytes].
  final OverflowStrategy overflow;

  /// Function used to estimate the size in bytes of a payload object.
  final int Function(Object payload)? sizeEstimator;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is OutboundBufferOptions &&
          runtimeType == other.runtimeType &&
          maxMessages == other.maxMessages &&
          maxBytes == other.maxBytes &&
          defaultTtl == other.defaultTtl &&
          overflow == other.overflow &&
          sizeEstimator == other.sizeEstimator;

  @override
  int get hashCode =>
      maxMessages.hashCode ^
      maxBytes.hashCode ^
      defaultTtl.hashCode ^
      overflow.hashCode ^
      sizeEstimator.hashCode;
}
