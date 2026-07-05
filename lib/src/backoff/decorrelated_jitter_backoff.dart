import 'dart:math' as math;

import 'package:resilient_socket/src/backoff/reconnect_policy.dart';

/// Decorrelated jitter backoff (AWS / Marc Brooker formula).
///
/// Computes `min(cap, uniform(base, previousDelay * 3))` where
/// `previousDelay` defaults to `base` on the first attempt. Maintains
/// internal state via `_lastDelay` so that even if the caller passes
/// `null` for `previousDelay` on subsequent attempts, the correlation
/// chain is preserved. Call [reset] to clear the internal memory.
class DecorrelatedJitterBackoff implements ReconnectPolicy {
  /// Creates a [DecorrelatedJitterBackoff] policy.
  ///
  /// [base] is the minimum delay floor. [cap] is the maximum delay.
  /// [random] is used for the uniform draw; defaults to `Random()`.
  DecorrelatedJitterBackoff({
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
  Duration? _lastDelay;

  @override
  Duration nextDelay(int attempt, Duration? previousDelay) {
    final prevUs =
        previousDelay?.inMicroseconds ?? _lastDelay?.inMicroseconds ?? _baseUs;
    final upperUs = math.min(_capUs, prevUs * 3);
    final delayUs = _uniform(_baseUs, upperUs);
    final result = Duration(microseconds: delayUs);
    _lastDelay = result;
    return result;
  }

  @override
  void reset() {
    _lastDelay = null;
  }

  int _uniform(int minUs, int maxUs) {
    if (maxUs <= minUs) return minUs;
    return minUs + (_random.nextDouble() * (maxUs - minUs)).floor();
  }
}
