import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:resilient_socket/resilient_socket.dart';
import 'package:test/test.dart';

void main() {
  group('T7. StreamProtection operators (`lib/src/stream_ops/`)', () {
    test(
      '1. throttleLatest(100ms): events at t=0(a), 30(b), 60(c), 130(d) -> emissions a@0, c@100, d@200',
      () {
        fakeAsync((async) {
          final controller = StreamController<String>();
          final emitted = <String>[];
          final timestamps = <Duration>[];

          controller.stream
              .throttleLatest(const Duration(milliseconds: 100))
              .listen((event) {
                emitted.add(event);
                timestamps.add(async.elapsed);
              });

          // t = 0 (a)
          controller.add('a');
          async.flushMicrotasks();
          expect(emitted, equals(['a']));
          expect(timestamps, equals([Duration.zero]));

          // t = 30 (b)
          async.elapse(const Duration(milliseconds: 30));
          controller.add('b');
          async.flushMicrotasks();
          expect(emitted, equals(['a']));

          // t = 60 (c)
          async.elapse(const Duration(milliseconds: 30));
          controller.add('c');
          async.flushMicrotasks();
          expect(emitted, equals(['a']));

          // t = 100 (window end)
          async.elapse(const Duration(milliseconds: 40));
          expect(emitted, equals(['a', 'c']));
          expect(timestamps[1], equals(const Duration(milliseconds: 100)));

          // t = 130 (d)
          async.elapse(const Duration(milliseconds: 30));
          controller.add('d');
          async.flushMicrotasks();
          expect(emitted, equals(['a', 'c']));

          // t = 200 (second window end)
          async.elapse(const Duration(milliseconds: 70));
          expect(emitted, equals(['a', 'c', 'd']));
          expect(timestamps[2], equals(const Duration(milliseconds: 200)));

          unawaited(controller.close());
        });
      },
    );

    test(
      '2. debounceQuiet(50ms): events t=0(a), 30(b), 100(c) -> emissions b@80, c@150',
      () {
        fakeAsync((async) {
          final controller = StreamController<String>();
          final emitted = <String>[];
          final timestamps = <Duration>[];

          controller.stream
              .debounceQuiet(const Duration(milliseconds: 50))
              .listen((event) {
                emitted.add(event);
                timestamps.add(async.elapsed);
              });

          // t = 0 (a)
          controller.add('a');
          async.flushMicrotasks();
          expect(emitted, isEmpty);

          // t = 30 (b)
          async.elapse(const Duration(milliseconds: 30));
          controller.add('b');
          async.flushMicrotasks();
          expect(emitted, isEmpty);

          // t = 80 (timer fires for b)
          async.elapse(const Duration(milliseconds: 50));
          expect(emitted, equals(['b']));
          expect(timestamps[0], equals(const Duration(milliseconds: 80)));

          // t = 100 (c)
          async.elapse(const Duration(milliseconds: 20));
          controller.add('c');
          async.flushMicrotasks();
          expect(emitted, equals(['b']));

          // t = 150 (timer fires for c)
          async.elapse(const Duration(milliseconds: 50));
          expect(emitted, equals(['b', 'c']));
          expect(timestamps[1], equals(const Duration(milliseconds: 150)));

          unawaited(controller.close());
        });
      },
    );

    test(
      '3. conflate(100ms, merge: p+n): 1@0, 2@40, 3@90 -> 6@100; next 4@250 -> 4@350',
      () {
        fakeAsync((async) {
          final controller = StreamController<int>();
          final emitted = <int>[];
          final timestamps = <Duration>[];

          controller.stream
              .conflate(
                const Duration(milliseconds: 100),
                merge: (p, n) => p + n,
              )
              .listen((event) {
                emitted.add(event);
                timestamps.add(async.elapsed);
              });

          // t = 0 (1)
          controller.add(1);

          // t = 40 (2)
          async.elapse(const Duration(milliseconds: 40));
          controller.add(2);

          // t = 90 (3)
          async.elapse(const Duration(milliseconds: 50));
          controller.add(3);
          async.flushMicrotasks();
          expect(emitted, isEmpty);

          // t = 100 (timer fires)
          async.elapse(const Duration(milliseconds: 10));
          expect(emitted, equals([6]));
          expect(timestamps[0], equals(const Duration(milliseconds: 100)));

          // t = 250 (4)
          async.elapse(const Duration(milliseconds: 150));
          controller.add(4);

          // t = 350 (timer fires)
          async.elapse(const Duration(milliseconds: 100));
          expect(emitted, equals([6, 4]));
          expect(timestamps[1], equals(const Duration(milliseconds: 350)));

          unawaited(controller.close());
        });
      },
    );

    test(
      '4. sampleEvery(100ms): events 1@10, 2@50, 3@120 -> emissions 2@100, 3@200; nothing at 300',
      () {
        fakeAsync((async) {
          final controller = StreamController<int>();
          final emitted = <int>[];
          final timestamps = <Duration>[];

          controller.stream
              .sampleEvery(const Duration(milliseconds: 100))
              .listen((event) {
                emitted.add(event);
                timestamps.add(async.elapsed);
              });

          // t = 10 (1)
          async.elapse(const Duration(milliseconds: 10));
          controller.add(1);

          // t = 50 (2)
          async.elapse(const Duration(milliseconds: 40));
          controller.add(2);

          // t = 100 (first sample tick)
          async.elapse(const Duration(milliseconds: 50));
          expect(emitted, equals([2]));
          expect(timestamps[0], equals(const Duration(milliseconds: 100)));

          // t = 120 (3)
          async.elapse(const Duration(milliseconds: 20));
          controller.add(3);

          // t = 200 (second sample tick)
          async.elapse(const Duration(milliseconds: 80));
          expect(emitted, equals([2, 3]));
          expect(timestamps[1], equals(const Duration(milliseconds: 200)));

          // t = 300 (third sample tick - nothing new)
          async.elapse(const Duration(milliseconds: 100));
          expect(emitted, equals([2, 3]));

          unawaited(controller.close());
        });
      },
    );

    test(
      '5. Leak: cancel each subscription mid-window; elapse 10s -> zero emissions and zero pending timers',
      () {
        fakeAsync((async) {
          final c1 = StreamController<int>();
          final sub1 = c1.stream
              .throttleLatest(const Duration(seconds: 1))
              .listen((_) {});
          c1.add(1);
          async.elapse(const Duration(milliseconds: 500));
          c1.add(2);
          unawaited(sub1.cancel());

          final c2 = StreamController<int>();
          final sub2 = c2.stream
              .debounceQuiet(const Duration(seconds: 1))
              .listen((_) {});
          c2.add(1);
          async.elapse(const Duration(milliseconds: 500));
          unawaited(sub2.cancel());

          final c3 = StreamController<int>();
          final sub3 = c3.stream
              .conflate(const Duration(seconds: 1))
              .listen((_) {});
          c3.add(1);
          async.elapse(const Duration(milliseconds: 500));
          unawaited(sub3.cancel());

          final c4 = StreamController<int>();
          final sub4 = c4.stream
              .sampleEvery(const Duration(seconds: 1))
              .listen((_) {});
          c4.add(1);
          async.elapse(const Duration(milliseconds: 500));
          unawaited(sub4.cancel());

          async
            ..flushMicrotasks()
            ..elapse(const Duration(seconds: 10));
          expect(async.pendingTimers, isEmpty);

          unawaited(c1.close());
          unawaited(c2.close());
          unawaited(c3.close());
          unawaited(c4.close());
        });
      },
    );

    test(
      '6. done-flush: source closes at t=30 in case 1 with pending b -> b emitted, stream closed',
      () {
        fakeAsync((async) {
          final controller = StreamController<String>();
          final emitted = <String>[];
          var isDone = false;

          controller.stream
              .throttleLatest(const Duration(milliseconds: 100))
              .listen(emitted.add, onDone: () => isDone = true);

          // t = 0 (a)
          controller.add('a');
          async.flushMicrotasks();
          expect(emitted, equals(['a']));

          // t = 30 (b)
          async.elapse(const Duration(milliseconds: 30));
          controller.add('b');
          async.flushMicrotasks();
          expect(emitted, equals(['a']));

          // Close source at t=30
          unawaited(controller.close());
          async
            ..elapse(Duration.zero)
            ..flushMicrotasks();

          expect(emitted, equals(['a', 'b']));
          expect(isDone, isTrue);
          expect(async.pendingTimers, isEmpty);
        });
      },
    );
  });
}
