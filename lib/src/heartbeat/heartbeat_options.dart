import 'package:meta/meta.dart';

/// Configuration options governing heartbeat monitoring and adaptive ping intervals.
@immutable
class HeartbeatOptions {
  /// Creates heartbeat monitoring options.
  const HeartbeatOptions({
    required this.pingBuilder,
    required this.pongMatcher,
    this.minInterval = const Duration(seconds: 5),
    this.maxInterval = const Duration(seconds: 30),
    this.initialInterval = const Duration(seconds: 15),
    this.staleFactor = 2.0,
    this.maxMisses = 2,
    this.adaptive = true,
  });

  /// Function that builds an outbound ping payload for a given sequence number.
  final Object Function(int seq) pingBuilder;

  /// Function that matches inbound messages against a pending sequence number.
  final bool Function(dynamic message, int seq) pongMatcher;

  /// Minimum allowed interval between consecutive pings when adaptive pacing is enabled.
  final Duration minInterval;

  /// Maximum allowed interval between consecutive pings when adaptive pacing is enabled.
  final Duration maxInterval;

  /// Initial interval between pings before RTT variance stabilizes.
  final Duration initialInterval;

  /// Multiplier applied to current RTO to schedule predictive stale detection timers.
  final double staleFactor;

  /// Consecutive missed pongs required before emitting `ConnectionDead` and terminating.
  final int maxMisses;

  /// Whether adaptive ping interval calculation based on RTT relative variance is enabled.
  final bool adaptive;
}
