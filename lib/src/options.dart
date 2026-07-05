import 'package:resilient_socket/src/backoff/decorrelated_jitter_backoff.dart';
import 'package:resilient_socket/src/backoff/reconnect_policy.dart';
import 'package:resilient_socket/src/buffer/outbound_buffer_options.dart';
import 'package:resilient_socket/src/heartbeat/heartbeat_options.dart';
import 'package:resilient_socket/src/subscription/replay_options.dart';
import 'package:resilient_socket/src/telemetry/socket_metrics_listener.dart';
import 'package:resilient_socket/src/transport/socket_transport.dart';
import 'package:resilient_socket/src/transport/web_socket_channel_transport.dart';

/// Configuration options for `ResilientSocket`.
class ResilientSocketOptions {
  /// Creates configuration options for a resilient socket.
  ResilientSocketOptions({
    ReconnectPolicy? reconnectPolicy,
    this.maxAttempts,
    this.stabilityThreshold = const Duration(seconds: 30),
    TransportFactory? transportFactory,
    this.heartbeat,
    OutboundBufferOptions? buffer,
    ReplayOptions? replay,
    this.metrics = const NoopMetricsListener(),
  }) : reconnectPolicy =
           reconnectPolicy ??
           DecorrelatedJitterBackoff(
             base: const Duration(milliseconds: 250),
             cap: const Duration(seconds: 30),
           ),
       transportFactory = transportFactory ?? WebSocketChannelTransport.connect,
       buffer = buffer ?? const OutboundBufferOptions(),
       replay = replay ?? const ReplayOptions();

  /// Policy governing backoff delays between reconnection attempts.
  final ReconnectPolicy reconnectPolicy;

  /// Maximum number of consecutive reconnection attempts before transitioning
  /// to `Suspended`. If `null`, retries indefinitely.
  final int? maxAttempts;

  /// Duration of uninterrupted connection stability required before resetting
  /// backoff state and attempt counters.
  final Duration stabilityThreshold;

  /// Factory used to create underlying [SocketTransport] connections.
  final TransportFactory transportFactory;

  /// Optional configuration governing heartbeat monitoring and RTT estimation.
  final HeartbeatOptions? heartbeat;

  /// Configuration options governing outbound message buffering.
  final OutboundBufferOptions buffer;

  /// Configuration options governing subscription replay and buffer flushing.
  final ReplayOptions replay;

  /// Listener for observing lifecycle, performance, and traffic telemetry.
  final SocketMetricsListener metrics;
}
