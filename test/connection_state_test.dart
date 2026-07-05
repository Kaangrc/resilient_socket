import 'package:resilient_socket/resilient_socket.dart';
import 'package:test/test.dart';

void main() {
  group('SocketConnectionState', () {
    test('const constructible and equality / hashCode', () {
      const c1 = Connecting(0);
      const c2 = Connecting(0);
      const c3 = Connecting(1);
      expect(c1, equals(c2));
      expect(c1.hashCode, equals(c2.hashCode));
      expect(c1, isNot(equals(c3)));

      const conn1 = Connected(lastRtt: Duration(milliseconds: 10));
      const conn2 = Connected(lastRtt: Duration(milliseconds: 10));
      const conn3 = Connected();
      expect(conn1, equals(conn2));
      expect(conn1.hashCode, equals(conn2.hashCode));
      expect(conn1, isNot(equals(conn3)));

      const deg1 = Degraded(Duration(milliseconds: 500));
      const deg2 = Degraded(Duration(milliseconds: 500));
      const deg3 = Degraded(Duration(milliseconds: 600));
      expect(deg1, equals(deg2));
      expect(deg1.hashCode, equals(deg2.hashCode));
      expect(deg1, isNot(equals(deg3)));

      const rec1 = Reconnecting(
        attempt: 1,
        nextIn: Duration(milliseconds: 250),
      );
      const rec2 = Reconnecting(
        attempt: 1,
        nextIn: Duration(milliseconds: 250),
      );
      const rec3 = Reconnecting(
        attempt: 2,
        nextIn: Duration(milliseconds: 250),
      );
      expect(rec1, equals(rec2));
      expect(rec1.hashCode, equals(rec2.hashCode));
      expect(rec1, isNot(equals(rec3)));

      const sus1 = Suspended('max attempts');
      const sus2 = Suspended('max attempts');
      const sus3 = Suspended('fatal error');
      expect(sus1, equals(sus2));
      expect(sus1.hashCode, equals(sus2.hashCode));
      expect(sus1, isNot(equals(sus3)));

      const disp1 = Disposed();
      const disp2 = Disposed();
      expect(disp1, equals(disp2));
      expect(disp1.hashCode, equals(disp2.hashCode));
      expect(disp1, isNot(equals(c1)));
    });

    test('toString formatting', () {
      expect(const Connecting(0).toString(), equals('Connecting(0)'));
      expect(
        const Connected(lastRtt: Duration(milliseconds: 15)).toString(),
        equals('Connected(lastRtt: 0:00:00.015000)'),
      );
      expect(
        const Degraded(Duration(milliseconds: 800)).toString(),
        equals('Degraded(0:00:00.800000)'),
      );
      expect(
        const Reconnecting(
          attempt: 2,
          nextIn: Duration(milliseconds: 500),
        ).toString(),
        equals('Reconnecting(attempt: 2, nextIn: 0:00:00.500000)'),
      );
      expect(
        const Suspended('error').toString(),
        equals('Suspended(error)'),
      );
      expect(const Disposed().toString(), equals('Disposed()'));
    });

    test('exhaustive switch pattern matching', () {
      final states = <SocketConnectionState>[
        const Connecting(0),
        const Connected(),
        const Degraded(Duration(milliseconds: 100)),
        const Reconnecting(attempt: 0, nextIn: Duration(seconds: 1)),
        const Suspended('cause'),
        const Disposed(),
      ];

      final names = states
          .map(
            (s) => switch (s) {
              Connecting() => 'connecting',
              Connected() => 'connected',
              Degraded() => 'degraded',
              Reconnecting() => 'reconnecting',
              Suspended() => 'suspended',
              Disposed() => 'disposed',
            },
          )
          .toList();

      expect(
        names,
        equals([
          'connecting',
          'connected',
          'degraded',
          'reconnecting',
          'suspended',
          'disposed',
        ]),
      );
    });
  });
}
