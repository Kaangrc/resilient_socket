import 'dart:math' as math;

import 'package:resilient_socket/src/backoff/reconnect_policy.dart';

/// Classic exponential backoff without jitter.
///
/// Computes `min(cap, base * 2^attempt)`. All arithmetic uses microseconds
/// internally to avoid floating-point drift. The exponent is clamped at 40
/// to prevent integer overflow on bit-shift.
class ExponentialBackoff implements ReconnectPolicy {
  /// Creates an [ExponentialBackoff] policy.
  ///
  /// [base] is the delay for attempt 0. [cap] is the maximum delay.
  /// [random] is accepted for interface consistency (ADR-0002) but unused.
  ExponentialBackoff({
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
  // ignore: unused_field — kept for ADR-0002 interface uniformity.
  final math.Random _random;

  @override
  Duration nextDelay(int attempt, Duration? previousDelay) {
    final exp = math.min(attempt, 40);
    final delayUs = _baseUs * (1 << exp);
    final clampedUs = math.min(delayUs, _capUs);
    return Duration(microseconds: clampedUs);
  }

  @override
  void reset() {}
}
