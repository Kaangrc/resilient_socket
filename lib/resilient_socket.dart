/// Financial-grade WebSocket resilience for Dart.
///
/// Provides decorrelated-jitter reconnection, adaptive heartbeat with RTT
/// tracking, bounded offline buffering, and automatic subscription replay.
library;

export 'src/backoff/decorrelated_jitter_backoff.dart';
export 'src/backoff/equal_jitter_backoff.dart';
export 'src/backoff/exponential_backoff.dart';
export 'src/backoff/full_jitter_backoff.dart';
export 'src/backoff/reconnect_policy.dart';

export 'src/buffer/buffer_drop_reason.dart';
export 'src/buffer/buffer_overflow_exception.dart';
export 'src/buffer/buffered_message.dart';
export 'src/buffer/outbound_buffer_options.dart';
export 'src/buffer/overflow_strategy.dart';
export 'src/connection_state.dart';
export 'src/heartbeat/heartbeat_options.dart';
export 'src/heartbeat/rtt_sample.dart';
export 'src/options.dart';
export 'src/resilient_socket_base.dart';
export 'src/stream_ops/stream_protection.dart';
export 'src/subscription/replay_options.dart';
export 'src/subscription/replay_progress.dart';
export 'src/subscription/subscription_spec.dart';
export 'src/telemetry/composite_metrics_listener.dart';
export 'src/telemetry/socket_metrics_listener.dart';
export 'src/transport/socket_transport.dart';
export 'src/transport/web_socket_channel_transport.dart';
