import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:resilient_socket/resilient_socket.dart';
import 'package:test/test.dart';

import 'support/fake_transport.dart';
import 'support/sequenced_random.dart';

void main() {
  group('ResilientSocket', () {
    test('legal transition rows via transitionForTesting', () {
      final socket = ResilientSocket(Uri.parse('ws://test.local'));
      expect(socket.state, equals(const Suspended('Not connected')));

      socket
        ..transitionForTesting(const Connecting(0))
        ..transitionForTesting(const Connected())
        ..transitionForTesting(
          const Degraded(Duration(milliseconds: 100)),
        )
        ..transitionForTesting(const Connected())
        ..transitionForTesting(
          const Connected(lastRtt: Duration(milliseconds: 10)),
        )
        ..transitionForTesting(
          const Reconnecting(
            attempt: 0,
            nextIn: Duration(seconds: 1),
          ),
        );
      expect(socket.state, isA<Reconnecting>());

      socket
        ..transitionForTesting(const Connecting(1))
        ..transitionForTesting(
          const Reconnecting(
            attempt: 1,
            nextIn: Duration(seconds: 2),
          ),
        );
      expect(socket.state, isA<Reconnecting>());

      socket
        ..transitionForTesting(const Suspended('fatal'))
        ..transitionForTesting(const Disposed());
      expect(socket.state, equals(const Disposed()));
    });

    test('illegal transitions throw StateError', () {
      final socket = ResilientSocket(Uri.parse('ws://test.local'));

      expect(
        () => socket.transitionForTesting(const Connected()),
        throwsStateError,
      );

      socket
        ..transitionForTesting(const Connecting(0))
        ..transitionForTesting(
          const Reconnecting(
            attempt: 0,
            nextIn: Duration(seconds: 1),
          ),
        );

      expect(
        () => socket.transitionForTesting(const Connected()),
        throwsStateError,
      );

      socket.transitionForTesting(const Disposed());
      expect(
        () => socket.transitionForTesting(const Connecting(0)),
        throwsStateError,
      );
    });

    test('close() is idempotent and cleans up resources', () async {
      final factory = RecordingTransportFactory();
      final socket = ResilientSocket(
        Uri.parse('ws://test.local'),
        options: ResilientSocketOptions(transportFactory: factory.call),
      )..connect();

      expect(factory.created.length, equals(1));
      factory.created[0].completeReady();
      await Future<void>.microtask(() {});

      expect(socket.state, equals(const Connected()));

      await socket.close(1000, 'Normal closure');
      expect(socket.state, equals(const Disposed()));
      expect(factory.created[0].closeCode, equals(1000));
      expect(factory.created[0].closeReason, equals('Normal closure'));

      await socket.close(1001, 'Another');
      expect(socket.state, equals(const Disposed()));
    });

    test('connect() after Suspended resets attempt counter to 0', () {
      fakeAsync((async) {
        final factory = RecordingTransportFactory();
        final socket = ResilientSocket(
          Uri.parse('ws://test.local'),
          options: ResilientSocketOptions(
            maxAttempts: 0,
            transportFactory: factory.call,
          ),
        )..connect();

        factory.created[0].failReady('fatal 0');
        async.flushMicrotasks();

        expect(socket.state, isA<Suspended>());

        socket.connect();
        expect(socket.state, equals(const Connecting(0)));
        expect(factory.created.length, equals(2));
      });
    });

    test('maxAttempts: 2 -> exactly 3 transports created then Suspended', () {
      fakeAsync((async) {
        final factory = RecordingTransportFactory();
        final options = ResilientSocketOptions(
          maxAttempts: 2,
          reconnectPolicy: ExponentialBackoff(
            base: const Duration(milliseconds: 100),
            cap: const Duration(seconds: 1),
          ),
          transportFactory: factory.call,
        );
        final socket = ResilientSocket(
          Uri.parse('ws://test.local'),
          options: options,
        )..connect();

        expect(socket.state, equals(const Connecting(0)));
        expect(factory.created.length, equals(1));

        factory.created[0].failReady('error 0');
        async.flushMicrotasks();
        expect(socket.state, isA<Reconnecting>());
        expect((socket.state as Reconnecting).attempt, equals(0));

        async.elapse(const Duration(milliseconds: 100));
        expect(socket.state, equals(const Connecting(1)));
        expect(factory.created.length, equals(2));

        factory.created[1].failReady('error 1');
        async.flushMicrotasks();
        expect(socket.state, isA<Reconnecting>());
        expect((socket.state as Reconnecting).attempt, equals(1));

        async.elapse(const Duration(milliseconds: 200));
        expect(socket.state, equals(const Connecting(2)));
        expect(factory.created.length, equals(3));

        factory.created[2].failReady('error 2');
        async.flushMicrotasks();

        expect(socket.state, isA<Suspended>());
        expect(factory.created.length, equals(3));
      });
    });

    test('stability threshold resets attempt counters and backoff', () {
      fakeAsync((async) {
        final factory = RecordingTransportFactory();
        final options = ResilientSocketOptions(
          stabilityThreshold: const Duration(seconds: 10),
          reconnectPolicy: ExponentialBackoff(
            base: const Duration(milliseconds: 100),
            cap: const Duration(seconds: 1),
          ),
          transportFactory: factory.call,
        );
        final socket = ResilientSocket(
          Uri.parse('ws://test.local'),
          options: options,
        )..connect();

        factory.created[0].failReady('error 0');
        async.flushMicrotasks();
        expect((socket.state as Reconnecting).attempt, equals(0));

        async.elapse(const Duration(milliseconds: 100));
        expect(socket.state, equals(const Connecting(1)));

        factory.created[1].completeReady();
        async.flushMicrotasks();
        expect(socket.state, equals(const Connected()));

        async.elapse(const Duration(seconds: 10));

        factory.created[1].dropConnection();
        async.flushMicrotasks();
        expect(socket.state, isA<Reconnecting>());
        expect((socket.state as Reconnecting).attempt, equals(0));
      });
    });

    test('send and receive messages while connected', () {
      fakeAsync((async) {
        final factory = RecordingTransportFactory();
        final socket = ResilientSocket(
          Uri.parse('ws://test.local'),
          options: ResilientSocketOptions(transportFactory: factory.call),
        );

        final received = <dynamic>[];
        socket.messages.listen(received.add);

        socket
          ..send('buffered before connect')
          ..connect()
          ..send('buffered while connecting');

        factory.created[0].completeReady();
        async.flushMicrotasks();
        expect(socket.state, equals(const Connected()));

        socket.send('hello');
        expect(
          factory.created[0].sentData,
          equals([
            'buffered before connect',
            'buffered while connecting',
            'hello',
          ]),
        );

        factory.created[0].emit('world');
        async.flushMicrotasks();
        expect(received, equals(['world']));
      });
    });

    test('heartbeat: pong frames never appear on socket.messages', () {
      fakeAsync((async) {
        final factory = RecordingTransportFactory();
        final socket = ResilientSocket(
          Uri.parse('ws://test.local'),
          options: ResilientSocketOptions(
            transportFactory: factory.call,
            heartbeat: HeartbeatOptions(
              pingBuilder: (seq) => 'PING:$seq',
              pongMatcher: (msg, seq) => msg == 'PONG:$seq',
            ),
          ),
        );

        final received = <dynamic>[];
        socket.messages.listen(received.add);

        socket.connect();
        factory.created[0].completeReady();
        async.flushMicrotasks();

        expect(factory.created[0].sentData, equals(['PING:1']));

        factory.created[0].emit('PONG:1');
        async.flushMicrotasks();

        factory.created[0].emit('app data');
        async.flushMicrotasks();

        expect(received, equals(['app data']));
      });
    });

    test('send during Degraded hits the wire immediately', () {
      fakeAsync((async) {
        final factory = RecordingTransportFactory();
        final socket = ResilientSocket(
          Uri.parse('ws://test.local'),
          options: ResilientSocketOptions(transportFactory: factory.call),
        )..connect();

        factory.created[0].completeReady();
        async.flushMicrotasks();
        expect(socket.state, equals(const Connected()));

        final sentBefore = factory.created[0].sentData.length;
        socket.transitionForTesting(
          const Degraded(Duration(milliseconds: 500)),
        );
        expect(socket.state, isA<Degraded>());

        socket.send('degraded-wire-payload');
        async.flushMicrotasks();

        expect(
          factory.created[0].sentData.length,
          equals(sentBefore + 1),
        );
        expect(
          factory.created[0].sentData.last,
          equals('degraded-wire-payload'),
        );
      });
    });

    test('subscribe during Degraded sends the frame', () {
      fakeAsync((async) {
        final factory = RecordingTransportFactory();
        final socket = ResilientSocket(
          Uri.parse('ws://test.local'),
          options: ResilientSocketOptions(transportFactory: factory.call),
        )..connect();

        factory.created[0].completeReady();
        async.flushMicrotasks();
        expect(socket.state, equals(const Connected()));

        final sentBefore = factory.created[0].sentData.length;
        socket.transitionForTesting(
          const Degraded(Duration(milliseconds: 500)),
        );
        expect(socket.state, isA<Degraded>());

        socket.subscribe(
          SubscriptionSpec(
            id: 'degraded-channel',
            subscribeMessage: () => 'SUB:degraded-channel',
          ),
        );
        async.flushMicrotasks();

        expect(
          factory.created[0].sentData.length,
          equals(sentBefore + 1),
        );
        expect(
          factory.created[0].sentData.last,
          equals('SUB:degraded-channel'),
        );
      });
    });

    test(
      'heartbeat: StaleSuspected drives Degraded, PongReceived drives Degraded->Connected',
      () {
        fakeAsync((async) {
          final factory = RecordingTransportFactory();
          final socket = ResilientSocket(
            Uri.parse('ws://test.local'),
            options: ResilientSocketOptions(
              transportFactory: factory.call,
              heartbeat: HeartbeatOptions(
                pingBuilder: (seq) => 'PING:$seq',
                pongMatcher: (msg, seq) => msg == 'PONG:$seq',
              ),
            ),
          )..connect();

          factory.created[0].completeReady();
          async.flushMicrotasks();
          expect(socket.state, equals(const Connected()));

          // Emit PONG:1 at 100ms so RTO is clamped to minRto 500ms.
          async.elapse(const Duration(milliseconds: 100));
          factory.created[0].emit('PONG:1');
          async
            ..flushMicrotasks()
            ..elapse(const Duration(milliseconds: 17500));
          expect(factory.created[0].sentData.last, equals('PING:2'));

          // Stale delay is 2 * 500ms = 1000ms.
          async.elapse(const Duration(milliseconds: 1000));
          expect(socket.state, isA<Degraded>());
          expect(
            (socket.state as Degraded).rtt,
            equals(const Duration(milliseconds: 1000)),
          );

          // Emit PONG:2 -> PongReceived drives Degraded -> Connected
          factory.created[0].emit('PONG:2');
          async.flushMicrotasks();

          expect(socket.state, isA<Connected>());
          expect(
            (socket.state as Connected).lastRtt,
            equals(const Duration(milliseconds: 1000)),
          );
        });
      },
    );

    test('heartbeat: ConnectionDead drives Reconnecting', () {
      fakeAsync((async) {
        final factory = RecordingTransportFactory();
        final socket = ResilientSocket(
          Uri.parse('ws://test.local'),
          options: ResilientSocketOptions(
            transportFactory: factory.call,
            heartbeat: HeartbeatOptions(
              pingBuilder: (seq) => 'PING:$seq',
              pongMatcher: (msg, seq) => msg == 'PONG:$seq',
              initialInterval: const Duration(seconds: 10),
              adaptive: false,
            ),
          ),
        )..connect();

        factory.created[0].completeReady();
        async.flushMicrotasks();
        expect(socket.state, equals(const Connected()));

        async.elapse(const Duration(seconds: 20));
        expect(socket.state, isA<Reconnecting>());
      });
    });

    test('T8. Master Integration Test Suite: end-to-end lifecycle matrix', () {
      fakeAsync((async) {
        final factory = RecordingTransportFactory();
        final listener = _LifecycleListener();
        final socket = ResilientSocket(
          Uri.parse('ws://test.local'),
          options: ResilientSocketOptions(
            transportFactory: factory.call,
            metrics: listener,
            reconnectPolicy: DecorrelatedJitterBackoff(
              base: const Duration(milliseconds: 250),
              cap: const Duration(seconds: 30),
              random: SequencedRandom([0.5]),
            ),
            heartbeat: HeartbeatOptions(
              pingBuilder: (seq) => 'PING:$seq',
              pongMatcher: (msg, seq) => msg == 'PONG:$seq',
              initialInterval: const Duration(seconds: 10),
              adaptive: false,
            ),
          ),
        );

        // T0-T1: Invoke connect(), verify state shifts through [Connecting(0)], and track transport[0] creation.
        expect(socket.state, isA<Suspended>());
        socket.connect();
        expect(socket.state, equals(const Connecting(0)));
        expect(factory.created.length, equals(1));
        final transport0 = factory.created[0];

        // T2-T3: Fire subscribe() for specA and specB, and run send('early') while connecting.
        // Verify absolutely nothing hits the wire yet (buffering barrier check).
        final specA = SubscriptionSpec(
          id: 'specA',
          subscribeMessage: () => 'subA',
          unsubscribeMessage: () => 'unsubA',
          priority: 1,
        );
        final specB = SubscriptionSpec(
          id: 'specB',
          subscribeMessage: () => 'subB',
          unsubscribeMessage: () => 'unsubB',
          priority: 2,
        );
        socket
          ..subscribe(specA)
          ..subscribe(specB)
          ..send('early');
        expect(transport0.sentData, isEmpty);

        // T4-T5: Fire completeReady(). Enforce that the Connected-entry sequence executes in this absolute exact order on transport[0]: [ping1, subA, subB, 'early'].
        transport0.completeReady();
        async.flushMicrotasks();
        expect(socket.state, equals(const Connected()));
        expect(
          transport0.sentData,
          equals(['PING:1', 'subA', 'subB', 'early']),
        );

        // T6-T7: Simulate a network drop via dropConnection(code: 1006) at exactly t=1s.
        // Verify state falls back to Reconnecting with a deterministic Decorrelated Jitter offset of exactly 500ms.
        // Ensure onDisconnected records precise session uptime metrics.
        async.elapse(const Duration(seconds: 1));
        transport0.dropConnection(code: 1006);
        async.flushMicrotasks();
        expect(
          socket.state,
          equals(
            const Reconnecting(
              attempt: 0,
              nextIn: Duration(milliseconds: 500),
            ),
          ),
        );

        // Terminal Isolation: Invoke close(). Verify state hits Disposed, resources are completely torn down, and subsequent send() commands explicitly throw a StateError.
        unawaited(socket.close());
        async.flushMicrotasks();
        expect(socket.state, equals(const Disposed()));
        expect(() => socket.send('late'), throwsStateError);

        // Telemetry Sequence Validation: Assert chronological event sequence inside RecordingListener.
        expect(
          listener.events,
          equals([
            'connectAttempt(0)',
            'connected',
            'message(outbound, 6 bytes)',
            'message(outbound, 4 bytes)',
            'message(outbound, 4 bytes)',
            'replayCompleted(2)',
            'message(outbound, 5 bytes)',
            'disconnected(Connection closed, 1000ms)',
            'reconnectScheduled(0, 500ms)',
            'disconnected(Client closed connection, 0ms)',
          ]),
        );
      });
    });
  });
}

class _LifecycleListener implements SocketMetricsListener {
  final List<String> events = [];

  @override
  void onConnectAttempt(int attempt) => events.add('connectAttempt($attempt)');

  @override
  void onConnected(Duration handshakeTime) => events.add('connected');

  @override
  void onDisconnected(Object? cause, Duration sessionUptime) =>
      events.add('disconnected($cause, ${sessionUptime.inMilliseconds}ms)');

  @override
  void onReconnectScheduled(int attempt, Duration delay) =>
      events.add('reconnectScheduled($attempt, ${delay.inMilliseconds}ms)');

  @override
  void onRttSample(RttSample sample) => events.add('rttSample');

  @override
  void onHeartbeatMiss(int consecutiveMisses) =>
      events.add('heartbeatMiss($consecutiveMisses)');

  @override
  void onBufferDrop(BufferDropReason reason, int droppedCount) =>
      events.add('bufferDrop($reason, $droppedCount)');

  @override
  void onReplayCompleted(int subscriptions, Duration took) =>
      events.add('replayCompleted($subscriptions)');

  @override
  void onMessage({required bool inbound, required int sizeBytes}) => events.add(
    'message(${inbound ? "inbound" : "outbound"}, $sizeBytes bytes)',
  );
}
