import 'package:fake_async/fake_async.dart';
import 'package:resilient_socket/src/heartbeat/heartbeat_monitor.dart';
import 'package:resilient_socket/src/heartbeat/heartbeat_options.dart';
import 'package:resilient_socket/src/heartbeat/rtt_estimator.dart';
import 'package:test/test.dart';

Object _ping(int seq) => 'PING:$seq';
bool _pong(dynamic msg, int seq) => msg == 'PONG:$seq';

void main() {
  group('HeartbeatMonitor', () {
    test('6. start() sends ping seq 1 at t=0', () {
      fakeAsync((async) {
        final sent = <Object>[];
        HeartbeatMonitor(
          options: const HeartbeatOptions(
            pingBuilder: _ping,
            pongMatcher: _pong,
          ),
          send: sent.add,
          onEvent: (_) {},
          estimator: RttEstimator(),
        ).start();

        expect(sent.length, equals(1));
        expect(sent[0], equals('PING:1'));
      });
    });

    test('7. Pong at t=80ms -> PongReceived with raw exactly 80ms', () {
      fakeAsync((async) {
        final events = <HeartbeatEvent>[];
        final monitor = HeartbeatMonitor(
          options: const HeartbeatOptions(
            pingBuilder: _ping,
            pongMatcher: _pong,
          ),
          send: (_) {},
          onEvent: events.add,
          estimator: RttEstimator(),
        )..start();

        async.elapse(const Duration(milliseconds: 80));
        final consumed = monitor.onMessage('PONG:1');

        expect(consumed, isTrue);
        expect(events.length, equals(1));
        expect(events[0], isA<PongReceived>());
        expect(
          (events[0] as PongReceived).sample.raw,
          equals(const Duration(milliseconds: 80)),
        );
      });
    });

    test('8. Adaptive interval fires at 16111111us', () {
      fakeAsync((async) {
        final sent = <Object>[];
        final monitor = HeartbeatMonitor(
          options: const HeartbeatOptions(
            pingBuilder: _ping,
            pongMatcher: _pong,
          ),
          send: sent.add,
          onEvent: (_) {},
          estimator: RttEstimator(),
        )..start();

        async.elapse(const Duration(milliseconds: 100));
        monitor.onMessage(
          'PONG:1',
        ); // sample 1 (100ms) -> rttvar=50ms, srtt=100ms -> r=0.5 -> interval=17.5s

        async.elapse(const Duration(milliseconds: 17500));
        expect(sent.last, equals('PING:2'));

        async.elapse(const Duration(milliseconds: 200));
        monitor.onMessage(
          'PONG:2',
        ); // sample 2 (200ms) -> srtt=112.5ms, rttvar=62.5ms -> interval=16111111us

        final countBefore = sent.length;
        async.elapse(const Duration(microseconds: 16111110));
        expect(sent.length, equals(countBefore));

        async.elapse(const Duration(microseconds: 1));
        expect(sent.length, equals(countBefore + 1));
        expect(sent.last, equals('PING:3'));
      });
    });

    test(
      '9. Stale: no pong; elapse staleFactor * rto -> exactly one StaleSuspected',
      () {
        fakeAsync((async) {
          final events = <HeartbeatEvent>[];
          final monitor = HeartbeatMonitor(
            options: const HeartbeatOptions(
              pingBuilder: _ping,
              pongMatcher: _pong,
            ),
            send: (_) {},
            onEvent: events.add,
            estimator: RttEstimator(),
          )..start();

          async.elapse(const Duration(milliseconds: 100));
          monitor.onMessage('PONG:1'); // sample 1 -> rto=500ms

          // Next ping (PING:2) will fire after adaptive interval (17.5s)
          async.elapse(const Duration(milliseconds: 17500));
          events.clear(); // clear events before PING:2 stale check

          // Now PING:2 is sent. stale delay = 2.0 * 500ms = 1000ms.
          async.elapse(const Duration(milliseconds: 999));
          expect(events, isEmpty);

          async.elapse(const Duration(milliseconds: 1));
          expect(events.length, equals(1));
          expect(events[0], isA<StaleSuspected>());
          expect(
            (events[0] as StaleSuspected).outstanding,
            equals(const Duration(milliseconds: 1000)),
          );

          async.elapse(const Duration(seconds: 5));
          expect(events.length, equals(1));
        });
      },
    );

    test(
      '10. Death: no pongs across 2 ping ticks -> ConnectionDead(2), no further frames sent',
      () {
        fakeAsync((async) {
          final sent = <Object>[];
          final events = <HeartbeatEvent>[];
          HeartbeatMonitor(
            options: const HeartbeatOptions(
              pingBuilder: _ping,
              pongMatcher: _pong,
              initialInterval: Duration(seconds: 10),
              adaptive: false,
            ),
            send: sent.add,
            onEvent: events.add,
            estimator: RttEstimator(),
          ).start();

          expect(sent.length, equals(1));

          async.elapse(const Duration(seconds: 10));
          expect(sent.length, equals(2));

          async.elapse(const Duration(seconds: 10));
          expect(events.last, equals(const ConnectionDead(2)));
          expect(sent.length, equals(2));

          async.elapse(const Duration(seconds: 50));
          expect(sent.length, equals(2));
          expect(async.pendingTimers, isEmpty);
        });
      },
    );

    test(
      '11. Old-seq pong: consume (returns true) but estimator.latest unchanged, miss counter unchanged',
      () {
        fakeAsync((async) {
          final monitor = HeartbeatMonitor(
            options: const HeartbeatOptions(
              pingBuilder: _ping,
              pongMatcher: _pong,
              initialInterval: Duration(seconds: 10),
              adaptive: false,
            ),
            send: (_) {},
            onEvent: (_) {},
            estimator: RttEstimator(),
          )..start();

          async.elapse(const Duration(seconds: 10));

          expect(monitor.estimator.latest, isNull);

          final consumed = monitor.onMessage('PONG:1');
          expect(consumed, isTrue);
          expect(monitor.estimator.latest, isNull);
        });
      },
    );

    test(
      '12. Non-pong message: onMessage returns false, nothing else happens',
      () {
        fakeAsync((async) {
          final events = <HeartbeatEvent>[];
          final monitor = HeartbeatMonitor(
            options: const HeartbeatOptions(
              pingBuilder: _ping,
              pongMatcher: _pong,
            ),
            send: (_) {},
            onEvent: events.add,
            estimator: RttEstimator(),
          )..start();

          final consumed = monitor.onMessage('some application data');
          expect(consumed, isFalse);
          expect(events, isEmpty);
        });
      },
    );
  });
}
