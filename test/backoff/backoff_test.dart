import 'dart:math' as math;

import 'package:resilient_socket/src/backoff/decorrelated_jitter_backoff.dart';
import 'package:resilient_socket/src/backoff/equal_jitter_backoff.dart';
import 'package:resilient_socket/src/backoff/exponential_backoff.dart';
import 'package:resilient_socket/src/backoff/full_jitter_backoff.dart';
import 'package:test/test.dart';

import '../support/sequenced_random.dart';

void main() {
  group('ExponentialBackoff', () {
    test('attempts 0..6 with base 100ms cap 3200ms', () {
      final policy = ExponentialBackoff(
        base: const Duration(milliseconds: 100),
        cap: const Duration(milliseconds: 3200),
      );
      const expectedMs = [100, 200, 400, 800, 1600, 3200, 3200];
      for (var i = 0; i < expectedMs.length; i++) {
        expect(
          policy.nextDelay(i, null),
          Duration(milliseconds: expectedMs[i]),
          reason: 'attempt $i',
        );
      }
    });

    test('overflow: attempt 64 returns cap, not negative or wrapped', () {
      final policy = ExponentialBackoff(
        base: const Duration(seconds: 1),
        cap: const Duration(seconds: 30),
      );
      expect(policy.nextDelay(64, null), const Duration(seconds: 30));
    });

    test('reset is a no-op (does not throw)', () {
      final policy = ExponentialBackoff(
        base: const Duration(milliseconds: 100),
        cap: const Duration(seconds: 1),
      )..reset();
      expect(policy.nextDelay(0, null), const Duration(milliseconds: 100));
    });
  });

  group('FullJitterBackoff', () {
    test('attempt 3 with r=0.5 yields exactly 400ms', () {
      final policy = FullJitterBackoff(
        base: const Duration(milliseconds: 100),
        cap: const Duration(seconds: 10),
        random: SequencedRandom([0.5]),
      );
      // attempt 3: temp = min(10_000_000, 100_000 * 8) = 800_000 µs
      // uniform(0, 800_000) with r=0.5 → 0 + (0.5 * 800_000).floor() = 400_000
      expect(policy.nextDelay(3, null), const Duration(milliseconds: 400));
    });

    test('attempt 3 with r=0.0 yields 0ms', () {
      final policy = FullJitterBackoff(
        base: const Duration(milliseconds: 100),
        cap: const Duration(seconds: 10),
        random: SequencedRandom([0.0]),
      );
      expect(policy.nextDelay(3, null), Duration.zero);
    });

    test('attempt 3 with r=0.999 yields less than 800ms', () {
      final policy = FullJitterBackoff(
        base: const Duration(milliseconds: 100),
        cap: const Duration(seconds: 10),
        random: SequencedRandom([0.999]),
      );
      final delay = policy.nextDelay(3, null);
      expect(delay.inMicroseconds, lessThan(800000));
      expect(delay.inMicroseconds, greaterThan(0));
    });

    test('reset is a no-op', () {
      final policy = FullJitterBackoff(
        base: const Duration(milliseconds: 100),
        cap: const Duration(seconds: 1),
        random: SequencedRandom([0.5]),
      )..reset();
      // attempt 0: temp = min(1_000_000, 100_000) = 100_000
      // uniform(0, 100_000) with r=0.5 → 50_000 µs = 50ms
      expect(policy.nextDelay(0, null), const Duration(milliseconds: 50));
    });
  });

  group('EqualJitterBackoff', () {
    test('attempt 3 with r=0.5 yields exactly 600ms', () {
      final policy = EqualJitterBackoff(
        base: const Duration(milliseconds: 100),
        cap: const Duration(seconds: 10),
        random: SequencedRandom([0.5]),
      );
      // attempt 3: temp = min(10_000_000, 100_000 * 8) = 800_000 µs
      // half = 400_000; uniform(0, 400_000) with r=0.5 → 200_000
      // result = 400_000 + 200_000 = 600_000 µs = 600ms
      expect(policy.nextDelay(3, null), const Duration(milliseconds: 600));
    });

    test('bounds property: result always in [temp/2, temp]', () {
      final rng = math.Random(42);
      final policy = EqualJitterBackoff(
        base: const Duration(milliseconds: 100),
        cap: const Duration(seconds: 10),
        random: rng,
      );
      for (var i = 0; i < 1000; i++) {
        for (var attempt = 0; attempt < 7; attempt++) {
          final delay = policy.nextDelay(attempt, null);
          final tempUs = math.min(
            10000000,
            100000 * (1 << math.min(attempt, 40)),
          );
          final halfUs = tempUs ~/ 2;
          expect(
            delay.inMicroseconds,
            greaterThanOrEqualTo(halfUs),
            reason: 'attempt $attempt iteration $i lower bound',
          );
          expect(
            delay.inMicroseconds,
            lessThanOrEqualTo(tempUs),
            reason: 'attempt $attempt iteration $i upper bound',
          );
        }
      }
    });

    test('reset is a no-op', () {
      final policy = EqualJitterBackoff(
        base: const Duration(milliseconds: 100),
        cap: const Duration(seconds: 1),
        random: SequencedRandom([0.5]),
      )..reset();
      expect(
        policy.nextDelay(0, null).inMicroseconds,
        greaterThanOrEqualTo(50000),
      );
    });
  });

  group('DecorrelatedJitterBackoff', () {
    test('attempt 0 prev=null with r=0.5 yields exactly 500ms', () {
      final policy = DecorrelatedJitterBackoff(
        base: const Duration(milliseconds: 250),
        cap: const Duration(seconds: 30),
        random: SequencedRandom([0.5]),
      );
      // prevUs = baseUs = 250_000; upperUs = min(30M, 750_000) = 750_000
      // uniform(250_000, 750_000) with r=0.5 → 250_000 + 250_000 = 500_000
      expect(policy.nextDelay(0, null), const Duration(milliseconds: 500));
    });

    test('attempt 1 prev=500ms with r=0.5 yields exactly 875ms', () {
      final policy = DecorrelatedJitterBackoff(
        base: const Duration(milliseconds: 250),
        cap: const Duration(seconds: 30),
        random: SequencedRandom([0.5, 0.5]),
      )..nextDelay(0, null);
      // Second call: prevUs = 500_000 (from _lastDelay)
      // upperUs = min(30M, 1_500_000) = 1_500_000
      // uniform(250_000, 1_500_000) with r=0.5
      //   → 250_000 + (0.5 * 1_250_000).floor() = 875_000
      expect(policy.nextDelay(1, null), const Duration(milliseconds: 875));
    });

    test('with prev fixed at cap, results never exceed cap', () {
      const cap = Duration(seconds: 30);
      final policy = DecorrelatedJitterBackoff(
        base: const Duration(milliseconds: 250),
        cap: cap,
        random: math.Random(42),
      );
      for (var i = 0; i < 1000; i++) {
        final delay = policy.nextDelay(i, cap);
        expect(
          delay.inMicroseconds,
          lessThanOrEqualTo(cap.inMicroseconds),
          reason: 'iteration $i',
        );
        expect(
          delay.inMicroseconds,
          greaterThanOrEqualTo(250000),
          reason: 'iteration $i lower bound',
        );
      }
    });

    test('reset clears _lastDelay; next call depends on base again', () {
      final policy =
          DecorrelatedJitterBackoff(
              base: const Duration(milliseconds: 250),
              cap: const Duration(seconds: 30),
              random: SequencedRandom([0.5]),
            )
            ..nextDelay(0, null)
            ..reset();
      // After reset, _lastDelay is null → prevUs = baseUs = 250_000
      expect(policy.nextDelay(0, null), const Duration(milliseconds: 500));
    });

    test('explicit previousDelay is used over _lastDelay', () {
      final policy = DecorrelatedJitterBackoff(
        base: const Duration(milliseconds: 250),
        cap: const Duration(seconds: 30),
        random: SequencedRandom([0.5]),
      );
      // prevUs = 1_000_000; upperUs = min(30M, 3_000_000) = 3_000_000
      // uniform(250_000, 3_000_000) with r=0.5
      //   → 250_000 + (0.5 * 2_750_000).floor() = 1_625_000
      expect(
        policy.nextDelay(5, const Duration(seconds: 1)),
        const Duration(microseconds: 1625000),
      );
    });
  });
}
