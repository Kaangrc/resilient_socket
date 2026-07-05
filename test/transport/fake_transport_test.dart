import 'package:fake_async/fake_async.dart';
import 'package:test/test.dart';

import '../support/fake_transport.dart';

void main() {
  group('FakeTransport', () {
    test('send before ready throws StateError', () {
      final transport = FakeTransport();
      expect(() => transport.send('hello'), throwsStateError);
    });

    test('send after close throws StateError', () async {
      final transport = FakeTransport()..completeReady();
      await transport.close();
      expect(() => transport.send('hello'), throwsStateError);
    });

    test(
      'dropConnection closes stream and sets closeCode/closeReason',
      () async {
        final transport = FakeTransport()..completeReady();
        var doneCalled = false;
        transport.incoming.listen(
          null,
          onDone: () => doneCalled = true,
        );

        transport.dropConnection(code: 1006, reason: 'abnormal');
        await Future<void>.delayed(Duration.zero);

        expect(doneCalled, isTrue);
        expect(transport.closeCode, equals(1006));
        expect(transport.closeReason, equals('abnormal'));
        expect(transport.closedByClient, isFalse);
      },
    );

    test('close sets closedByClient to true and is idempotent', () async {
      final transport = FakeTransport()..completeReady();
      await transport.close(1000, 'normal');
      expect(transport.closedByClient, isTrue);
      expect(transport.closeCode, equals(1000));
      expect(transport.closeReason, equals('normal'));

      // Second call should be no-op
      await transport.close(1001, 'other');
      expect(transport.closeCode, equals(1000));
    });

    test('frames recorded in order with fake-clock timestamps', () {
      fakeAsync((async) {
        final transport = FakeTransport()
          ..completeReady()
          ..send('frame1');
        async.elapse(const Duration(milliseconds: 100));
        transport.send('frame2');
        async.elapse(const Duration(seconds: 1));
        transport.send('frame3');

        expect(transport.sentData, equals(['frame1', 'frame2', 'frame3']));

        final f1 = transport.sentFrames[0] as FakeSentFrame;
        final f2 = transport.sentFrames[1] as FakeSentFrame;
        final f3 = transport.sentFrames[2] as FakeSentFrame;

        expect(
          f2.at.difference(f1.at),
          equals(const Duration(milliseconds: 100)),
        );
        expect(f3.at.difference(f2.at), equals(const Duration(seconds: 1)));
      });
    });
  });

  group('RecordingTransportFactory', () {
    test('creates and records fakes', () {
      final factory = RecordingTransportFactory();
      final t1 = factory(Uri.parse('ws://localhost'));
      final t2 = factory(Uri.parse('ws://localhost'));
      expect(factory.created, equals([t1, t2]));
    });

    test('returns scripted fakes in order', () {
      final s1 = FakeTransport();
      final s2 = FakeTransport();
      final factory = RecordingTransportFactory([s1, s2]);

      expect(factory(Uri.parse('ws://localhost')), equals(s1));
      expect(factory(Uri.parse('ws://localhost')), equals(s2));
      // Falls back to new fake when scripted runs out
      expect(factory(Uri.parse('ws://localhost')), isNot(equals(s1)));
    });
  });
}
