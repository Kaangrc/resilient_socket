import 'package:meta/meta.dart';

/// Configuration options for subscription replay and buffer flushing.
@immutable
class ReplayOptions {
  /// Creates replay configuration options.
  const ReplayOptions({
    this.pacing = const Duration(milliseconds: 100),
    this.batchSize = 5,
    this.flushAfterReplay = true,
  });

  /// Time delay between consecutive transmission bursts.
  final Duration pacing;

  /// Maximum number of messages sent per transmission burst.
  final int batchSize;

  /// Whether offline buffered messages must wait for subscription replay to complete before sending.
  final bool flushAfterReplay;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is ReplayOptions &&
          runtimeType == other.runtimeType &&
          pacing == other.pacing &&
          batchSize == other.batchSize &&
          flushAfterReplay == other.flushAfterReplay;

  @override
  int get hashCode =>
      pacing.hashCode ^ batchSize.hashCode ^ flushAfterReplay.hashCode;
}
