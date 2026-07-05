import 'package:resilient_socket/resilient_socket.dart';
import 'package:resilient_socket/src/heartbeat/heartbeat_monitor.dart';
import 'package:test/test.dart';

import 'support/sequenced_random.dart';

void main() {
  group('Value Types & Events Complete Coverage', () {
    test('RttSample equality, hashCode, toString, short-circuiting', () {
      const s1 = RttSample(
        raw: Duration(milliseconds: 100),
        smoothed: Duration(milliseconds: 110),
        variance: Duration(milliseconds: 10),
        rto: Duration(milliseconds: 500),
      );
      const s2 = RttSample(
        raw: Duration(milliseconds: 100),
        smoothed: Duration(milliseconds: 110),
        variance: Duration(milliseconds: 10),
        rto: Duration(milliseconds: 500),
      );
      expect(s1, equals(s2));
      expect(s1.hashCode, equals(s2.hashCode));
      expect(s1.toString(), contains('RttSample'));
      expect(s1, isNot(equals('other')));

      // Field short-circuiting
      expect(
        s1,
        isNot(
          equals(
            const RttSample(
              raw: Duration(milliseconds: 1),
              smoothed: Duration(milliseconds: 110),
              variance: Duration(milliseconds: 10),
              rto: Duration(milliseconds: 500),
            ),
          ),
        ),
      );
      expect(
        s1,
        isNot(
          equals(
            const RttSample(
              raw: Duration(milliseconds: 100),
              smoothed: Duration(milliseconds: 1),
              variance: Duration(milliseconds: 10),
              rto: Duration(milliseconds: 500),
            ),
          ),
        ),
      );
      expect(
        s1,
        isNot(
          equals(
            const RttSample(
              raw: Duration(milliseconds: 100),
              smoothed: Duration(milliseconds: 110),
              variance: Duration(milliseconds: 1),
              rto: Duration(milliseconds: 500),
            ),
          ),
        ),
      );
      expect(
        s1,
        isNot(
          equals(
            const RttSample(
              raw: Duration(milliseconds: 100),
              smoothed: Duration(milliseconds: 110),
              variance: Duration(milliseconds: 10),
              rto: Duration(milliseconds: 1),
            ),
          ),
        ),
      );
    });

    test(
      'BufferedMessage equality, hashCode, toString, isExpiredAt, short-circuiting',
      () {
        final now = DateTime(2026, 1, 1, 12);
        final m1 = BufferedMessage(
          payload: 'test',
          enqueuedAt: now,
          priority: 1,
          ttl: const Duration(seconds: 10),
        );
        final m2 = BufferedMessage(
          payload: 'test',
          enqueuedAt: now,
          priority: 1,
          ttl: const Duration(seconds: 10),
        );
        expect(m1, equals(m2));
        expect(m1.hashCode, equals(m2.hashCode));
        expect(m1.toString(), contains('BufferedMessage'));
        expect(m1, isNot(equals('other')));
        expect(m1.isExpiredAt(now.add(const Duration(seconds: 5))), isFalse);
        expect(m1.isExpiredAt(now.add(const Duration(seconds: 15))), isTrue);

        // Field short-circuiting
        expect(
          m1,
          isNot(
            equals(
              BufferedMessage(
                payload: 'diff',
                enqueuedAt: now,
                priority: 1,
                ttl: const Duration(seconds: 10),
              ),
            ),
          ),
        );
        expect(
          m1,
          isNot(
            equals(
              BufferedMessage(
                payload: 'test',
                enqueuedAt: now.add(const Duration(seconds: 1)),
                priority: 1,
                ttl: const Duration(seconds: 10),
              ),
            ),
          ),
        );
        expect(
          m1,
          isNot(
            equals(
              BufferedMessage(
                payload: 'test',
                enqueuedAt: now,
                priority: 1,
                ttl: const Duration(seconds: 1),
              ),
            ),
          ),
        );
        expect(
          m1,
          isNot(
            equals(
              BufferedMessage(
                payload: 'test',
                enqueuedAt: now,
                priority: 2,
                ttl: const Duration(seconds: 10),
              ),
            ),
          ),
        );
      },
    );

    test(
      'OutboundBufferOptions equality, hashCode, toString, short-circuiting',
      () {
        int fn1(Object p) => 1;
        int fn2(Object p) => 2;
        final o1 = OutboundBufferOptions(sizeEstimator: fn1);
        final o2 = OutboundBufferOptions(sizeEstimator: fn1);
        expect(o1, equals(o2));
        expect(o1.hashCode, equals(o2.hashCode));
        expect(o1.toString(), contains('OutboundBufferOptions'));
        expect(o1, isNot(equals('other')));

        // Field short-circuiting
        expect(
          const OutboundBufferOptions(maxMessages: 10),
          isNot(equals(const OutboundBufferOptions(maxMessages: 20))),
        );
        expect(
          const OutboundBufferOptions(maxBytes: 100),
          isNot(equals(const OutboundBufferOptions(maxBytes: 200))),
        );
        expect(
          const OutboundBufferOptions(defaultTtl: Duration(seconds: 1)),
          isNot(
            equals(
              const OutboundBufferOptions(defaultTtl: Duration(seconds: 2)),
            ),
          ),
        );
        expect(
          const OutboundBufferOptions(),
          isNot(
            equals(
              const OutboundBufferOptions(
                overflow: OverflowStrategy.dropNewest,
              ),
            ),
          ),
        );
        expect(
          OutboundBufferOptions(sizeEstimator: fn1),
          isNot(equals(OutboundBufferOptions(sizeEstimator: fn2))),
        );
      },
    );

    test('ReplayOptions equality, hashCode, toString, short-circuiting', () {
      const r1 = ReplayOptions();
      const r2 = ReplayOptions();
      expect(r1, equals(r2));
      expect(r1.hashCode, equals(r2.hashCode));
      expect(r1.toString(), contains('ReplayOptions'));
      expect(r1, isNot(equals('other')));

      expect(
        const ReplayOptions(pacing: Duration(seconds: 1)),
        isNot(equals(const ReplayOptions(pacing: Duration(seconds: 2)))),
      );
      expect(
        const ReplayOptions(batchSize: 1),
        isNot(equals(const ReplayOptions(batchSize: 2))),
      );
      expect(
        const ReplayOptions(),
        isNot(equals(const ReplayOptions(flushAfterReplay: false))),
      );
    });

    test('ReplayProgress equality, hashCode, toString, short-circuiting', () {
      const p1 = ReplayProgress(total: 10, sent: 5);
      const p2 = ReplayProgress(total: 10, sent: 5);
      expect(p1, equals(p2));
      expect(p1.hashCode, equals(p2.hashCode));
      expect(p1.toString(), contains('ReplayProgress'));
      expect(p1, isNot(equals('other')));

      expect(
        const ReplayProgress(total: 10, sent: 5),
        isNot(equals(const ReplayProgress(total: 20, sent: 5))),
      );
      expect(
        const ReplayProgress(total: 10, sent: 5),
        isNot(equals(const ReplayProgress(total: 10, sent: 6))),
      );
    });

    test('SubscriptionSpec equality, hashCode, toString, short-circuiting', () {
      Object sub1() => 'sub';
      Object sub2() => 'sub2';
      Object unsub1() => 'unsub';
      Object unsub2() => 'unsub2';

      final s1 = SubscriptionSpec(
        id: 'a',
        subscribeMessage: sub1,
        unsubscribeMessage: unsub1,
        priority: 1,
      );
      final s2 = SubscriptionSpec(
        id: 'a',
        subscribeMessage: sub1,
        unsubscribeMessage: unsub1,
        priority: 1,
      );
      expect(s1, equals(s2));
      expect(s1.hashCode, equals(s2.hashCode));
      expect(s1.toString(), contains('SubscriptionSpec'));
      expect(s1, isNot(equals('other')));

      expect(
        SubscriptionSpec(id: 'a', subscribeMessage: sub1),
        isNot(equals(SubscriptionSpec(id: 'b', subscribeMessage: sub1))),
      );
      expect(
        SubscriptionSpec(id: 'a', subscribeMessage: sub1),
        isNot(equals(SubscriptionSpec(id: 'a', subscribeMessage: sub2))),
      );
      expect(
        SubscriptionSpec(
          id: 'a',
          subscribeMessage: sub1,
          unsubscribeMessage: unsub1,
        ),
        isNot(
          equals(
            SubscriptionSpec(
              id: 'a',
              subscribeMessage: sub1,
              unsubscribeMessage: unsub2,
            ),
          ),
        ),
      );
      expect(
        SubscriptionSpec(id: 'a', subscribeMessage: sub1, priority: 1),
        isNot(
          equals(
            SubscriptionSpec(id: 'a', subscribeMessage: sub1, priority: 2),
          ),
        ),
      );
    });

    test('HeartbeatEvent classes equality, hashCode, toString', () {
      const sample1 = RttSample(
        raw: Duration(milliseconds: 1),
        smoothed: Duration(milliseconds: 1),
        variance: Duration(milliseconds: 1),
        rto: Duration(milliseconds: 1),
      );
      const sample2 = RttSample(
        raw: Duration(milliseconds: 2),
        smoothed: Duration(milliseconds: 2),
        variance: Duration(milliseconds: 2),
        rto: Duration(milliseconds: 2),
      );

      const pong1 = PongReceived(sample1);
      const pong2 = PongReceived(sample1);
      const pong3 = PongReceived(sample2);
      expect(pong1, equals(pong2));
      expect(pong1.hashCode, equals(pong2.hashCode));
      expect(pong1.toString(), contains('PongReceived'));
      expect(pong1, isNot(equals(pong3)));
      expect(pong1, isNot(equals('other')));

      const stale1 = StaleSuspected(Duration(seconds: 1));
      const stale2 = StaleSuspected(Duration(seconds: 1));
      const stale3 = StaleSuspected(Duration(seconds: 2));
      expect(stale1, equals(stale2));
      expect(stale1.hashCode, equals(stale2.hashCode));
      expect(stale1.toString(), contains('StaleSuspected'));
      expect(stale1, isNot(equals(stale3)));
      expect(stale1, isNot(equals('other')));

      const dead1 = ConnectionDead(2);
      const dead2 = ConnectionDead(2);
      const dead3 = ConnectionDead(3);
      expect(dead1, equals(dead2));
      expect(dead1.hashCode, equals(dead2.hashCode));
      expect(dead1.toString(), contains('ConnectionDead'));
      expect(dead1, isNot(equals(dead3)));
      expect(dead1, isNot(equals('other')));
    });

    test('BufferOverflowException toString', () {
      const exc = BufferOverflowException(capacity: 500);
      expect(exc.toString(), contains('BufferOverflowException'));
    });

    test('Backoff classes reset and general methods', () {
      final eq = EqualJitterBackoff(
        base: const Duration(milliseconds: 100),
        cap: const Duration(seconds: 1),
        random: SequencedRandom([0.5]),
      )..reset();
      expect(eq.nextDelay(0, null), isNotNull);

      final full = FullJitterBackoff(
        base: const Duration(milliseconds: 100),
        cap: const Duration(seconds: 1),
        random: SequencedRandom([0.5]),
      )..reset();
      expect(full.nextDelay(0, null), isNotNull);
      expect(full.nextDelay(10, null), isNotNull);

      final exp = ExponentialBackoff(
        base: const Duration(milliseconds: 100),
        cap: const Duration(seconds: 1),
      )..reset();
      expect(exp.nextDelay(0, null), const Duration(milliseconds: 100));
      expect(exp.nextDelay(10, null), const Duration(seconds: 1));
    });
  });
}
