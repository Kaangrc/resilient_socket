import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:resilient_socket/resilient_socket.dart';
import 'package:test/test.dart';

import 'support/fake_transport.dart';
import 'support/sequenced_random.dart';

void main() {
  group('Stream Protection Errors and Completions Coverage', () {
    test('throttleLatest forwards errors and flushes pending on done', () {
      fakeAsync((async) {
        final controller = StreamController<int>();
        final errors = <Object>[];
        final results = <int>[];
        var isDone = false;

        controller.stream
            .throttleLatest(const Duration(milliseconds: 100))
            .listen(
              results.add,
              onError: errors.add,
              onDone: () => isDone = true,
            );

        controller.add(1); // emitted at 0
        async.elapse(const Duration(milliseconds: 20));
        controller
          ..addError('err1')
          ..add(2); // pending
        unawaited(controller.close()); // should flush pending 2 and close
        async.flushMicrotasks();

        expect(errors, equals(['err1']));
        expect(results, equals([1, 2]));
        expect(isDone, isTrue);
      });
    });

    test('debounceQuiet forwards errors and flushes pending on done', () {
      fakeAsync((async) {
        final controller = StreamController<int>();
        final errors = <Object>[];
        final results = <int>[];
        var isDone = false;

        controller.stream
            .debounceQuiet(const Duration(milliseconds: 50))
            .listen(
              results.add,
              onError: errors.add,
              onDone: () => isDone = true,
            );

        controller.add(1);
        async.elapse(const Duration(milliseconds: 20));
        controller
          ..addError('err2')
          ..add(2);
        unawaited(controller.close());
        async.flushMicrotasks();

        expect(errors, equals(['err2']));
        expect(results, equals([2]));
        expect(isDone, isTrue);
      });
    });

    test('conflate forwards errors and handles completion', () {
      fakeAsync((async) {
        final controller = StreamController<int>();
        final errors = <Object>[];
        final results = <int>[];
        var isDone = false;

        controller.stream
            .conflate(
              const Duration(milliseconds: 100),
              merge: (a, b) => a + b,
            )
            .listen(
              results.add,
              onError: errors.add,
              onDone: () => isDone = true,
            );

        controller.add(10);
        async.elapse(const Duration(milliseconds: 20));
        controller
          ..addError('err3')
          ..add(20);
        unawaited(controller.close());
        async.flushMicrotasks();

        expect(errors, equals(['err3']));
        expect(results, equals([30]));
        expect(isDone, isTrue);
      });
    });

    test('sampleEvery forwards errors and handles completion', () {
      fakeAsync((async) {
        final controller = StreamController<int>();
        final errors = <Object>[];
        final results = <int>[];
        var isDone = false;

        controller.stream
            .sampleEvery(const Duration(milliseconds: 100))
            .listen(
              results.add,
              onError: errors.add,
              onDone: () => isDone = true,
            );

        controller.add(100);
        async.elapse(const Duration(milliseconds: 20));
        controller.addError('err4');
        unawaited(controller.close());
        async.flushMicrotasks();

        expect(errors, equals(['err4']));
        expect(isDone, isTrue);
      });
    });
  });

  group('ResilientSocket Edge Cases and Re-connect Guard Coverage', () {
    test('connect when already connecting/connected/reconnecting/disposed', () {
      fakeAsync((async) {
        final transports = <FakeTransport>[];
        final socket =
            ResilientSocket(
                Uri.parse('wss://example.com'),
                options: ResilientSocketOptions(
                  transportFactory: (uri) {
                    final t = FakeTransport();
                    transports.add(t);
                    return t;
                  },
                  reconnectPolicy: DecorrelatedJitterBackoff(
                    base: const Duration(milliseconds: 100),
                    cap: const Duration(seconds: 1),
                    random: SequencedRandom([0.5]),
                  ),
                ),
              )
              ..connect()
              ..connect(); // should be ignored
        expect(transports.length, equals(1));

        transports[0].completeReady();
        async.flushMicrotasks();
        expect(socket.state, isA<Connected>());
        socket.connect(); // ignored while connected
        expect(transports.length, equals(1));

        transports[0].dropConnection();
        async.flushMicrotasks();
        expect(socket.state, isA<Reconnecting>());
        socket.connect(); // ignored while reconnecting
        expect(transports.length, equals(1));

        unawaited(socket.close());
        async.flushMicrotasks();
        expect(socket.state, isA<Disposed>());
        expect(socket.connect, throwsStateError);
      });
    });

    test('unsubscribe non-existent spec and send/sub/unsub when disposed', () {
      fakeAsync((async) {
        final socket =
            ResilientSocket(
                Uri.parse('wss://example.com'),
                options: ResilientSocketOptions(
                  transportFactory: (uri) => FakeTransport(),
                ),
              )
              ..connect()
              ..unsubscribe('non-existent'); // should not error

        unawaited(socket.close());
        async.flushMicrotasks();

        expect(() => socket.send('data'), throwsStateError);
        expect(() => socket.unsubscribe('any'), throwsStateError);
        expect(
          () => socket.subscribe(
            SubscriptionSpec(id: 'id', subscribeMessage: () => 'sub'),
          ),
          throwsStateError,
        );
        unawaited(socket.close()); // idempotent close when already disposed
      });
    });
  });
}
