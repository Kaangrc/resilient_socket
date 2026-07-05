import 'dart:math' as math;

import 'package:resilient_socket/src/backoff/reconnect_policy.dart';

/// Equal jitter backoff: half deterministic exponential, half random.
///
/// Computes `temp = min(cap, base * 2^attempt); temp ~/ 2 + uniform(0, temp ~/ 2)`.
/// Guarantees a minimum delay of half the exponential ceiling while still
/// providing jitter in the upper half.
class EqualJitterBackoff implements ReconnectPolicy {
  /// Creates an [EqualJitterBackoff] policy.
  ///
  /// [base] is the delay for attempt 0 ceiling. [cap] is the maximum delay.
  /// [random] is used for the uniform draw; defaults to `Random()`.
  EqualJitterBackoff({
    required Duration base,
    required Duration cap,
    math.Random? random,
  }) : assert(base > Duration.zero, 'base must be positive'),
       assert(cap >= base, 'cap must be >= base'),
       _baseUs = base.inMicroseconds,
       _capUs = cap.inMicroseconds,
       _random = random ?? math.Random();

  final int _baseUs;
  final int _capUs;
  final math.Random _random;

  @override
  Duration nextDelay(int attempt, Duration? previousDelay) {
    final exp = math.min(attempt, 40);
    final tempUs = math.min(_capUs, _baseUs * (1 << exp));
    final halfUs = tempUs ~/ 2;
    final delayUs = halfUs + _uniform(0, halfUs);
    return Duration(microseconds: delayUs);
  }

  @override
  void reset() {}

  int _uniform(int minUs, int maxUs) =>
      minUs + (_random.nextDouble() * (maxUs - minUs)).floor();
}
