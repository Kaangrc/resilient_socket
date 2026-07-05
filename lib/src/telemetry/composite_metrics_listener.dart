import 'dart:convert';

import 'package:resilient_socket/src/buffer/buffer_drop_reason.dart';
import 'package:resilient_socket/src/heartbeat/rtt_sample.dart';
import 'package:resilient_socket/src/telemetry/socket_metrics_listener.dart';

/// A fan-out composition wrapper that delegates metrics events to multiple
/// [SocketMetricsListener] instances.
///
/// Enforces isolation: every hook execution wraps individual listener calls in a
/// `try/catch` block. A throwing listener will never disrupt the remaining
/// listeners or the core socket lifecycle.
class CompositeMetricsListener implements SocketMetricsListener {
  /// Creates a composite listener wrapping [_listeners].
  CompositeMetricsListener(this._listeners);

  final List<SocketMetricsListener> _listeners;

  /// Returns `true` if at least one non-noop listener is present.
  bool get hasActiveListeners {
    for (final listener in _listeners) {
      if (listener is! NoopMetricsListener) return true;
    }
    return false;
  }

  void _notify(void Function(SocketMetricsListener listener) action) {
    for (final listener in _listeners) {
      try {
        action(listener);
      } on Object {
        // One throwing listener must not break others or disrupt socket operations.
      }
    }
  }

  @override
  void onConnectAttempt(int attempt) =>
      _notify((l) => l.onConnectAttempt(attempt));

  @override
  void onConnected(Duration handshakeTime) =>
      _notify((l) => l.onConnected(handshakeTime));

  @override
  void onDisconnected(Object? cause, Duration sessionUptime) =>
      _notify((l) => l.onDisconnected(cause, sessionUptime));

  @override
  void onReconnectScheduled(int attempt, Duration delay) =>
      _notify((l) => l.onReconnectScheduled(attempt, delay));

  @override
  void onRttSample(RttSample sample) => _notify((l) => l.onRttSample(sample));

  @override
  void onHeartbeatMiss(int consecutiveMisses) =>
      _notify((l) => l.onHeartbeatMiss(consecutiveMisses));

  @override
  void onBufferDrop(BufferDropReason reason, int droppedCount) =>
      _notify((l) => l.onBufferDrop(reason, droppedCount));

  @override
  void onReplayCompleted(int subscriptions, Duration took) =>
      _notify((l) => l.onReplayCompleted(subscriptions, took));

  @override
  void onMessage({required bool inbound, required int sizeBytes}) =>
      _notify((l) => l.onMessage(inbound: inbound, sizeBytes: sizeBytes));

  /// Helper method to record frame telemetry with lazy evaluation of `sizeBytes`.
  ///
  /// For String frames, computes `utf8.encode(s).length` lazily ONLY if a non-noop
  /// listener is present.
  void recordFrame(Object frame, {required bool inbound}) {
    if (!hasActiveListeners) return;

    final int sizeBytes;
    if (frame is String) {
      sizeBytes = utf8.encode(frame).length;
    } else if (frame is List<int>) {
      sizeBytes = frame.length;
    } else {
      sizeBytes = 0;
    }

    onMessage(inbound: inbound, sizeBytes: sizeBytes);
  }
}
