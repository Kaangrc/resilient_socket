import 'dart:math' as math;

import 'package:resilient_socket/src/backoff/reconnect_policy.dart';

/// Full jitter backoff as described by the AWS architecture blog.
///
/// Computes `uniform(0, min(cap, base * 2^attempt))`. Produces a uniformly
/// distributed random delay between zero and the exponential ceiling,
/// providing maximum jitter spread to decorrelate competing clients.
class FullJitterBackoff implements ReconnectPolicy {
  /// Creates a [FullJitterBackoff] policy.
  ///
  /// [base] is the delay for attempt 0 ceiling. [cap] is the maximum delay.
  /// [random] is used for the uniform draw; defaults to `Random()`.
  FullJitterBackoff({
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
    final delayUs = _uniform(0, tempUs);
    return Duration(microseconds: delayUs);
  }

  @override
  void reset() {}

  int _uniform(int minUs, int maxUs) =>
      minUs + (_random.nextDouble() * (maxUs - minUs)).floor();
}
