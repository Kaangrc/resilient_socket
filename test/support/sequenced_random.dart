import 'dart:math' as math;

/// A deterministic [math.Random] that returns pre-programmed [double] values.
///
/// Cycles through [_doubles] in order. Used in backoff tests to produce
/// exact, hand-computable delay values.
class SequencedRandom implements math.Random {
  /// Creates a [SequencedRandom] from a list of doubles in [0, 1).
  SequencedRandom(this._doubles);

  final List<double> _doubles;
  int _i = 0;

  @override
  double nextDouble() => _doubles[_i++ % _doubles.length];

  @override
  int nextInt(int max) => (nextDouble() * max).floor();

  @override
  bool nextBool() => nextDouble() >= 0.5;
}
