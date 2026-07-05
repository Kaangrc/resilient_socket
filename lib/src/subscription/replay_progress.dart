import 'package:meta/meta.dart';

/// Represents the current progress of an ongoing subscription replay operation.
@immutable
final class ReplayProgress {
  /// Creates a progress report with [total] expected items and [sent] items completed.
  const ReplayProgress({
    required this.total,
    required this.sent,
  });

  /// Total number of items to be transmitted in this replay run.
  final int total;

  /// Number of items transmitted so far.
  final int sent;

  /// Returns `true` if all items have been sent (`sent >= total`).
  bool get done => sent >= total;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is ReplayProgress &&
          runtimeType == other.runtimeType &&
          total == other.total &&
          sent == other.sent;

  @override
  int get hashCode => total.hashCode ^ sent.hashCode;

  @override
  String toString() =>
      'ReplayProgress(sent: $sent, total: $total, done: $done)';
}
