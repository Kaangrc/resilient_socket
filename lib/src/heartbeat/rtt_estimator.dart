import 'package:resilient_socket/src/heartbeat/rtt_sample.dart';

/// Calculates TCP-style smoothed round-trip times (SRTT), variance (RTTVAR),
/// and retransmission timeouts (RTO) using RFC 6298 integer arithmetic.
class RttEstimator {
  /// Creates an RTT estimator with configurable smoothing factors and RTO bounds.
  RttEstimator({
    this.alpha = 0.125,
    this.beta = 0.25,
    this.minRto = const Duration(milliseconds: 500),
    this.maxRto = const Duration(seconds: 30),
  });

  /// Smoothing factor for smoothed RTT (default 1/8 per RFC 6298).
  final double alpha;

  /// Smoothing factor for RTT variance (default 1/4 per RFC 6298).
  final double beta;

  /// Lower bound clamp for calculated RTO.
  final Duration minRto;

  /// Upper bound clamp for calculated RTO.
  final Duration maxRto;

  RttSample? _latest;

  /// Returns the most recently calculated [RttSample], or `null` if no samples
  /// have been recorded since construction or [reset].
  RttSample? get latest => _latest;

  /// Records a new measured [rawRtt] and returns the updated [RttSample].
  RttSample addSample(Duration rawRtt) {
    final rawUs = rawRtt.inMicroseconds;
    final int srttUs;
    final int rttvarUs;

    final latestSample = _latest;
    if (latestSample == null) {
      srttUs = rawUs;
      rttvarUs = rawUs ~/ 2;
    } else {
      final prevSrttUs = latestSample.smoothed.inMicroseconds;
      final prevRttvarUs = latestSample.variance.inMicroseconds;

      rttvarUs =
          ((1.0 - beta) * prevRttvarUs + beta * (prevSrttUs - rawUs).abs())
              .floor();
      srttUs = ((1.0 - alpha) * prevSrttUs + alpha * rawUs).floor();
    }

    final minRtoUs = minRto.inMicroseconds;
    final maxRtoUs = maxRto.inMicroseconds;
    var rtoUs = srttUs + 4 * rttvarUs;
    if (rtoUs < minRtoUs) {
      rtoUs = minRtoUs;
    } else if (rtoUs > maxRtoUs) {
      rtoUs = maxRtoUs;
    }

    final sample = RttSample(
      raw: Duration(microseconds: rawUs),
      smoothed: Duration(microseconds: srttUs),
      variance: Duration(microseconds: rttvarUs),
      rto: Duration(microseconds: rtoUs),
    );
    _latest = sample;
    return sample;
  }

  /// Resets estimator state. The next sample will be treated as the initial measurement.
  void reset() {
    _latest = null;
  }
}
