import 'package:resilient_socket/src/buffer/buffer_drop_reason.dart';
import 'package:resilient_socket/src/heartbeat/rtt_sample.dart';

/// Abstract interface for observing lifecycle and performance metrics
/// of a `ResilientSocket`.
abstract interface class SocketMetricsListener {
  /// Called when a connection attempt is initiated.
  void onConnectAttempt(int attempt);

  /// Called upon successful WebSocket connection handshake.
  void onConnected(Duration handshakeTime);

  /// Called when an active connection is disconnected or lost.
  void onDisconnected(Object? cause, Duration sessionUptime);

  /// Called when a backoff timer schedules a reconnection attempt.
  void onReconnectScheduled(int attempt, Duration delay);

  /// Called when a valid round-trip time sample is calculated from a pong frame.
  void onRttSample(RttSample sample);

  /// Called when consecutive heartbeat misses occur without pong response.
  void onHeartbeatMiss(int consecutiveMisses);

  /// Called when buffered messages are dropped due to overflow, TTL, or disposal.
  void onBufferDrop(BufferDropReason reason, int droppedCount);

  /// Called upon completion of the connected-entry subscription replay sequence.
  void onReplayCompleted(int subscriptions, Duration took);

  /// Called for every sent or received data frame over the active transport.
  void onMessage({required bool inbound, required int sizeBytes});
}

/// A no-op implementation of [SocketMetricsListener] with empty method bodies.
class NoopMetricsListener implements SocketMetricsListener {
  /// Creates a no-op metrics listener.
  const NoopMetricsListener();

  @override
  void onConnectAttempt(int attempt) {}

  @override
  void onConnected(Duration handshakeTime) {}

  @override
  void onDisconnected(Object? cause, Duration sessionUptime) {}

  @override
  void onReconnectScheduled(int attempt, Duration delay) {}

  @override
  void onRttSample(RttSample sample) {}

  @override
  void onHeartbeatMiss(int consecutiveMisses) {}

  @override
  void onBufferDrop(BufferDropReason reason, int droppedCount) {}

  @override
  void onReplayCompleted(int subscriptions, Duration took) {}

  @override
  void onMessage({required bool inbound, required int sizeBytes}) {}
}
