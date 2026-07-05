import 'package:resilient_socket/src/heartbeat/rtt_estimator.dart';
import 'package:test/test.dart';

void main() {
  group('RttEstimator', () {
    test(
      '1. First sample 100ms -> srtt 100ms, rttvar 50ms, rto clamped to 500ms',
      () {
        final estimator = RttEstimator();
        final sample1 = estimator.addSample(const Duration(milliseconds: 100));

        expect(sample1.raw.inMicroseconds, equals(100000));
        expect(sample1.smoothed.inMicroseconds, equals(100000));
        expect(sample1.variance.inMicroseconds, equals(50000));
        // raw RTO = 100000 + 4 * 50000 = 300000us -> clamped to minRto 500000us
        expect(sample1.rto.inMicroseconds, equals(500000));
        expect(estimator.latest, equals(sample1));
      },
    );

    test(
      '2. Second sample 200ms -> rttvar=62500us, srtt=112500us, rto clamped 500ms',
      () {
        final estimator = RttEstimator()
          ..addSample(const Duration(milliseconds: 100));
        final sample2 = estimator.addSample(const Duration(milliseconds: 200));

        expect(sample2.raw.inMicroseconds, equals(200000));
        expect(sample2.variance.inMicroseconds, equals(62500));
        expect(sample2.smoothed.inMicroseconds, equals(112500));
        // raw RTO = 112500 + 4 * 62500 = 362500us -> clamped to minRto 500000us
        expect(sample2.rto.inMicroseconds, equals(500000));
      },
    );

    test(
      '3. Third sample 1000ms -> verify rto equals exactly 1298437us after rounding',
      () {
        final estimator = RttEstimator()
          ..addSample(const Duration(milliseconds: 100))
          ..addSample(const Duration(milliseconds: 200));
        final sample3 = estimator.addSample(const Duration(milliseconds: 1000));

        expect(sample3.raw.inMicroseconds, equals(1000000));
        expect(sample3.variance.inMicroseconds, equals(268750));
        expect(sample3.smoothed.inMicroseconds, equals(223437));
        // srtt + 4 * rttvar = 223437 + 1075000 = 1298437
        expect(sample3.rto.inMicroseconds, equals(1298437));
      },
    );

    test('4. Clamp ceiling: samples of 60s drive rto to exactly maxRto', () {
      final estimator = RttEstimator();
      final sample = estimator.addSample(const Duration(seconds: 60));

      expect(sample.rto.inMicroseconds, equals(30000000)); // maxRto 30s
    });

    test('5. reset() -> next sample treated as first', () {
      final estimator = RttEstimator()
        ..addSample(const Duration(milliseconds: 100))
        ..addSample(const Duration(milliseconds: 200));
      expect(estimator.latest, isNotNull);

      estimator.reset();
      expect(estimator.latest, isNull);

      final sample1AfterReset = estimator.addSample(
        const Duration(milliseconds: 100),
      );
      expect(sample1AfterReset.smoothed.inMicroseconds, equals(100000));
      expect(sample1AfterReset.variance.inMicroseconds, equals(50000));
      expect(sample1AfterReset.rto.inMicroseconds, equals(500000));
    });
  });
}
