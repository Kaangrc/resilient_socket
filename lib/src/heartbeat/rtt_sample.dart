import 'package:meta/meta.dart';

/// Represents a snapshot of round-trip time measurements and estimations.
@immutable
final class RttSample {
  /// Creates an [RttSample] with the measured and calculated parameters.
  const RttSample({
    required this.raw,
    required this.smoothed,
    required this.variance,
    required this.rto,
  });

  /// The raw round-trip time measured for the most recent ping/pong exchange.
  final Duration raw;

  /// The TCP-style smoothed round-trip time (`srtt`).
  final Duration smoothed;

  /// The round-trip time variation (`rttvar`).
  final Duration variance;

  /// The calculated retransmission timeout (`RTO`), clamped between bounds.
  final Duration rto;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is RttSample &&
          runtimeType == other.runtimeType &&
          raw == other.raw &&
          smoothed == other.smoothed &&
          variance == other.variance &&
          rto == other.rto;

  @override
  int get hashCode => Object.hash(raw, smoothed, variance, rto);

  @override
  String toString() =>
      'RttSample(raw: $raw, smoothed: $smoothed, variance: $variance, rto: $rto)';
}
