import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:resilient_socket/resilient_socket.dart';
import 'package:resilient_socket/src/subscription/replay_coordinator.dart';
import 'package:resilient_socket/src/subscription/subscription_registry.dart';
import 'package:test/test.dart';

import '../support/fake_transport.dart';

void main() {
  group('T6. Subscriptions + replay (`lib/src/subscription/`)', () {
    test(
      '1. Ordering: priorities [5,0,0,3] with insertion order a,b,c,d -> b,c,d,a',
      () {
        final registry = SubscriptionRegistry()
          ..register(
            SubscriptionSpec(
              id: 'a',
              priority: 5,
              subscribeMessage: () => 'msg_a',
            ),
          )
          ..register(
            SubscriptionSpec(id: 'b', subscribeMessage: () => 'msg_b'),
          )
          ..register(
            SubscriptionSpec(id: 'c', subscribeMessage: () => 'msg_c'),
          )
          ..register(
            SubscriptionSpec(
              id: 'd',
              priority: 3,
              subscribeMessage: () => 'msg_d',
            ),
          );

        final active = registry.active;
        expect(active.map((s) => s.id).toList(), equals(['b', 'c', 'd', 'a']));
      },
    );

    test(
      '2. Pacing: 7 specs, batchSize 3, pacing 100ms -> cumulative [3, 6, 7] at t=0,100,200ms',
      () {
        fakeAsync((async) {
          final coordinator = ReplayCoordinator(
            const ReplayOptions(batchSize: 3),
          );

          final specs = List.generate(
            7,
            (i) => SubscriptionSpec(
              id: 's$i',
              subscribeMessage: () => 'frame_$i',
            ),
          );

          final sent = <Object>[];
          var completed = false;

          unawaited(
            coordinator
                .replay(specs: specs, send: sent.add, onProgress: (_) {})
                .then((res) => completed = res),
          );

          // At t=0: burst 0 sends first 3 frames synchronously
          expect(sent.length, equals(3));
          expect(completed, isFalse);

          async.elapse(const Duration(milliseconds: 50));
          expect(sent.length, equals(3));

          // At t=100ms: burst 1 sends next 3 frames (cumulative 6)
          async.elapse(const Duration(milliseconds: 50));
          expect(sent.length, equals(6));
          expect(completed, isFalse);

          // At t=200ms: burst 2 sends 7th frame (cumulative 7) and completes future
          async.elapse(const Duration(milliseconds: 100));
          expect(sent.length, equals(7));
          expect(completed, isTrue);

          // Verify no trailing wait or timer leaks
          expect(async.pendingTimers, isEmpty);
        });
      },
    );

    test('3. Progress: emitted 7 times, sent = 1..7, final done == true', () {
      fakeAsync((async) {
        final coordinator = ReplayCoordinator(
          const ReplayOptions(
            batchSize: 2,
            pacing: Duration(milliseconds: 50),
          ),
        );

        final specs = List.generate(
          7,
          (i) => SubscriptionSpec(id: 's$i', subscribeMessage: () => 'f$i'),
        );

        final progressEvents = <ReplayProgress>[];

        unawaited(
          coordinator.replay(
            specs: specs,
            send: (_) {},
            onProgress: progressEvents.add,
          ),
        );

        async.elapse(const Duration(milliseconds: 300));

        expect(progressEvents.length, equals(7));
        for (var i = 0; i < 7; i++) {
          expect(progressEvents[i].sent, equals(i + 1));
          expect(progressEvents[i].total, equals(7));
          expect(progressEvents[i].done, equals(i == 6));
        }
      });
    });

    test(
      '4. cancel() at t=150ms -> no 7th frame ever, resolves false, zero timer leak',
      () {
        fakeAsync((async) {
          final coordinator = ReplayCoordinator(
            const ReplayOptions(batchSize: 3),
          );

          final specs = List.generate(
            7,
            (i) => SubscriptionSpec(id: 's$i', subscribeMessage: () => 'f$i'),
          );

          final sent = <Object>[];
          bool? result;

          unawaited(
            coordinator
                .replay(specs: specs, send: sent.add, onProgress: (_) {})
                .then((res) => result = res),
          );

          expect(sent.length, equals(3));

          async.elapse(const Duration(milliseconds: 100));
          expect(sent.length, equals(6));
          expect(result, isNull);

          // At t=150ms (after 6 sent), call cancel()
          async.elapse(const Duration(milliseconds: 50));
          coordinator.cancel();
          async.flushMicrotasks();

          // Future must resolve with false immediately upon cancellation
          expect(result, isFalse);

          // Elapse far ahead into the future to verify no 7th frame is ever sent
          async.elapse(const Duration(seconds: 10));
          expect(sent.length, equals(6));
          expect(async.pendingTimers, isEmpty);
        });
      },
    );

    test(
      '5. Registry: duplicate id throws StateError; unregister unknown silent',
      () {
        final registry = SubscriptionRegistry();
        final spec = SubscriptionSpec(id: 'sub1', subscribeMessage: () => 'm1');

        registry.register(spec);
        expect(() => registry.register(spec), throwsA(isA<StateError>()));

        expect(() => registry.unregister('unknown_id'), returnsNormally);
      },
    );

    test(
      '6. Facade integration: subscribe connected sends/registers; reconnect resends both; mid-replay drop restarts clean',
      () {
        fakeAsync((async) {
          final factory = RecordingTransportFactory();
          final socket = ResilientSocket(
            Uri.parse('ws://test.local'),
            options: ResilientSocketOptions(
              transportFactory: factory.call,
              replay: const ReplayOptions(batchSize: 1),
            ),
          )..connect();

          factory.created[0].completeReady();
          async.flushMicrotasks();
          expect(socket.state, isA<Connected>());

          // subscribe() while Connected sends immediately AND registers
          socket.subscribe(
            SubscriptionSpec(id: 'a', subscribeMessage: () => 'msg_a'),
          );
          expect(factory.created[0].sentData, equals(['msg_a']));

          // drop connection -> transition out of Connected
          factory.created[0].dropConnection();
          async.flushMicrotasks();
          expect(socket.state, isNot(isA<Connected>()));

          // Subscribe while offline
          socket.subscribe(
            SubscriptionSpec(id: 'b', subscribeMessage: () => 'msg_b'),
          );

          // Reconnect -> both specs replayed
          async.elapse(const Duration(seconds: 5));
          factory.created[1].completeReady();
          async.flushMicrotasks();

          // At t=0 of replay, first spec 'a' is sent
          expect(factory.created[1].sentData, equals(['msg_a']));

          // Mid-replay drop before t=100ms when 'b' would be sent
          async
            ..elapse(const Duration(milliseconds: 50))
            ..flushMicrotasks();
          factory.created[1].dropConnection();

          // Reconnect again on transport 2
          async
            ..flushMicrotasks()
            ..elapse(const Duration(seconds: 5));
          factory.created[2].completeReady();
          async.flushMicrotasks();

          // New connection restarts replay from index 0 with NO duplicate within a single replay run
          expect(factory.created[2].sentData, equals(['msg_a']));
          async.elapse(const Duration(milliseconds: 100));
          expect(factory.created[2].sentData, equals(['msg_a', 'msg_b']));
        });
      },
    );

    test(
      '7. Flush barrier: flushAfterReplay=true -> [sub×4..., buffered×2]; false -> buffered precede',
      () {
        fakeAsync((async) {
          // Case A: flushAfterReplay = true
          final factoryA = RecordingTransportFactory();
          ResilientSocket(
              Uri.parse('ws://test.local'),
              options: ResilientSocketOptions(
                transportFactory: factoryA.call,
                replay: const ReplayOptions(
                  batchSize: 10,
                  pacing: Duration(milliseconds: 10),
                ),
              ),
            )
            ..send('buf1')
            ..send('buf2')
            ..subscribe(
              SubscriptionSpec(id: 's1', subscribeMessage: () => 'sub1'),
            )
            ..subscribe(
              SubscriptionSpec(id: 's2', subscribeMessage: () => 'sub2'),
            )
            ..subscribe(
              SubscriptionSpec(id: 's3', subscribeMessage: () => 'sub3'),
            )
            ..subscribe(
              SubscriptionSpec(id: 's4', subscribeMessage: () => 'sub4'),
            )
            ..connect();

          factoryA.created[0].completeReady();
          async.flushMicrotasks();

          // When flushAfterReplay is true, all 4 subscriptions are sent before buffered messages
          expect(
            factoryA.created[0].sentData,
            equals(['sub1', 'sub2', 'sub3', 'sub4', 'buf1', 'buf2']),
          );

          // Case B: flushAfterReplay = false
          final factoryB = RecordingTransportFactory();
          ResilientSocket(
              Uri.parse('ws://test.local'),
              options: ResilientSocketOptions(
                transportFactory: factoryB.call,
                replay: const ReplayOptions(
                  flushAfterReplay: false,
                  batchSize: 1,
                ),
              ),
            )
            ..send('buf1')
            ..send('buf2')
            ..subscribe(
              SubscriptionSpec(id: 's1', subscribeMessage: () => 'sub1'),
            )
            ..connect();

          factoryB.created[0].completeReady();
          async.flushMicrotasks();

          // When flushAfterReplay is false, buffered frames precede subscription replay frames
          expect(
            factoryB.created[0].sentData,
            equals(['buf1', 'buf2', 'sub1']),
          );
        });
      },
    );
  });
}
