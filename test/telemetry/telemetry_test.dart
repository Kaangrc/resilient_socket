import 'package:resilient_socket/resilient_socket.dart';
import 'package:test/test.dart';

class _RecordingListener implements SocketMetricsListener {
  final List<String> events = [];
  final List<int> messageSizes = [];

  @override
  void onConnectAttempt(int attempt) => events.add('connectAttempt($attempt)');

  @override
  void onConnected(Duration handshakeTime) => events.add('connected');

  @override
  void onDisconnected(Object? cause, Duration sessionUptime) =>
      events.add('disconnected($cause)');

  @override
  void onReconnectScheduled(int attempt, Duration delay) =>
      events.add('reconnectScheduled($attempt)');

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
  void onMessage({required bool inbound, required int sizeBytes}) {
    events.add('message(inbound: $inbound, size: $sizeBytes)');
    messageSizes.add(sizeBytes);
  }
}

class _ThrowingListener implements SocketMetricsListener {
  @override
  void onConnectAttempt(int attempt) => throw Exception('fail');

  @override
  void onConnected(Duration handshakeTime) => throw Exception('fail');

  @override
  void onDisconnected(Object? cause, Duration sessionUptime) =>
      throw Exception('fail');

  @override
  void onReconnectScheduled(int attempt, Duration delay) =>
      throw Exception('fail');

  @override
  void onRttSample(RttSample sample) => throw Exception('fail');

  @override
  void onHeartbeatMiss(int consecutiveMisses) => throw Exception('fail');

  @override
  void onBufferDrop(BufferDropReason reason, int droppedCount) =>
      throw Exception('fail');

  @override
  void onReplayCompleted(int subscriptions, Duration took) =>
      throw Exception('fail');

  @override
  void onMessage({required bool inbound, required int sizeBytes}) =>
      throw Exception('fail');
}

void main() {
  group('T7. Telemetry Infrastructure (`lib/src/telemetry/`)', () {
    test('1. NoopMetricsListener executes silently without side effects', () {
      const noop = NoopMetricsListener();
      expect(() => noop.onConnectAttempt(0), returnsNormally);
      expect(() => noop.onConnected(Duration.zero), returnsNormally);
      expect(
        () => noop.onDisconnected('cause', Duration.zero),
        returnsNormally,
      );
      expect(
        () => noop.onReconnectScheduled(1, Duration.zero),
        returnsNormally,
      );
      expect(
        () => noop.onRttSample(
          const RttSample(
            raw: Duration(milliseconds: 50),
            smoothed: Duration(milliseconds: 50),
            variance: Duration(milliseconds: 25),
            rto: Duration(milliseconds: 500),
          ),
        ),
        returnsNormally,
      );
      expect(() => noop.onHeartbeatMiss(1), returnsNormally);
      expect(
        () => noop.onBufferDrop(BufferDropReason.overflow, 1),
        returnsNormally,
      );
      expect(() => noop.onReplayCompleted(0, Duration.zero), returnsNormally);
      expect(
        () => noop.onMessage(inbound: true, sizeBytes: 10),
        returnsNormally,
      );
    });

    test('2. CompositeMetricsListener fan-out order is preserved', () {
      final l1 = _RecordingListener();
      final l2 = _RecordingListener();
      CompositeMetricsListener([l1, l2]).onConnectAttempt(1);
      expect(l1.events, equals(['connectAttempt(1)']));
      expect(l2.events, equals(['connectAttempt(1)']));
    });

    test(
      '3. CompositeMetricsListener isolation: throwing listener does not disrupt others',
      () {
        final l1 = _RecordingListener();
        final throwing = _ThrowingListener();
        final l2 = _RecordingListener();
        final composite = CompositeMetricsListener([l1, throwing, l2]);

        expect(() => composite.onConnectAttempt(1), returnsNormally);
        expect(() => composite.onConnected(Duration.zero), returnsNormally);
        expect(
          () => composite.onDisconnected('err', Duration.zero),
          returnsNormally,
        );
        expect(
          () => composite.onReconnectScheduled(1, Duration.zero),
          returnsNormally,
        );
        expect(
          () => composite.onRttSample(
            const RttSample(
              raw: Duration(milliseconds: 10),
              smoothed: Duration(milliseconds: 10),
              variance: Duration(milliseconds: 5),
              rto: Duration(milliseconds: 500),
            ),
          ),
          returnsNormally,
        );
        expect(() => composite.onHeartbeatMiss(2), returnsNormally);
        expect(
          () => composite.onBufferDrop(BufferDropReason.ttlExpired, 3),
          returnsNormally,
        );
        expect(
          () => composite.onReplayCompleted(2, Duration.zero),
          returnsNormally,
        );
        expect(
          () => composite.onMessage(inbound: false, sizeBytes: 100),
          returnsNormally,
        );

        expect(l1.events.length, equals(9));
        expect(l2.events.length, equals(9));
        expect(l1.events, equals(l2.events));
      },
    );

    test('4. Lazy evaluation of sizeBytes and hasActiveListeners check', () {
      final noopComposite = CompositeMetricsListener([
        const NoopMetricsListener(),
      ]);
      expect(noopComposite.hasActiveListeners, isFalse);

      final l1 = _RecordingListener();
      final activeComposite = CompositeMetricsListener([
        const NoopMetricsListener(),
        l1,
      ]);
      expect(activeComposite.hasActiveListeners, isTrue);

      // String (utf8 length: 'hello' is 5 bytes, 'ü' is 2 bytes in utf-8 -> 'hello ü' is 8 bytes)
      activeComposite.recordFrame('hello ü', inbound: true);
      expect(l1.messageSizes.last, equals(8));

      // List<int>
      activeComposite.recordFrame([1, 2, 3, 4], inbound: false);
      expect(l1.messageSizes.last, equals(4));

      // Other objects fallback to 0
      activeComposite.recordFrame(12345, inbound: true);
      expect(l1.messageSizes.last, equals(0));
    });
  });
}
