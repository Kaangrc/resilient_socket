import 'dart:async';
import 'dart:convert';

import 'package:clock/clock.dart';
import 'package:meta/meta.dart';
import 'package:resilient_socket/src/buffer/buffer_drop_reason.dart';
import 'package:resilient_socket/src/buffer/outbound_buffer.dart';
import 'package:resilient_socket/src/connection_state.dart';
import 'package:resilient_socket/src/heartbeat/heartbeat_monitor.dart';
import 'package:resilient_socket/src/heartbeat/rtt_estimator.dart';
import 'package:resilient_socket/src/heartbeat/rtt_sample.dart';
import 'package:resilient_socket/src/options.dart';
import 'package:resilient_socket/src/subscription/replay_coordinator.dart';
import 'package:resilient_socket/src/subscription/replay_progress.dart';
import 'package:resilient_socket/src/subscription/subscription_registry.dart';
import 'package:resilient_socket/src/subscription/subscription_spec.dart';
import 'package:resilient_socket/src/telemetry/composite_metrics_listener.dart';
import 'package:resilient_socket/src/telemetry/socket_metrics_listener.dart';
import 'package:resilient_socket/src/transport/socket_transport.dart';

/// Financial-grade WebSocket client with automated resilience, backoff,
/// and connection state tracking.
class ResilientSocket {
  /// Creates a [ResilientSocket] targetting [uri] with optional [options].
  ///
  /// The constructor does NOT automatically initiate a connection. Call [connect]
  /// explicitly to start the connection lifecycle.
  ResilientSocket(this.uri, {ResilientSocketOptions? options})
    : options = options ?? ResilientSocketOptions();

  /// The target WebSocket URI.
  final Uri uri;

  /// Configuration options governing resilience behaviors.
  final ResilientSocketOptions options;

  final StreamController<SocketConnectionState> _stateController =
      StreamController<SocketConnectionState>.broadcast();
  final StreamController<dynamic> _messagesController =
      StreamController<dynamic>.broadcast();
  final StreamController<RttSample> _rttController =
      StreamController<RttSample>.broadcast();
  final StreamController<ReplayProgress> _replayProgressController =
      StreamController<ReplayProgress>.broadcast();

  late final SubscriptionRegistry _registry = SubscriptionRegistry();
  late final ReplayCoordinator _coordinator = ReplayCoordinator(options.replay);
  late final OutboundBuffer _buffer = OutboundBuffer(
    options.buffer,
    onDrop: _onBufferDrop,
  );
  bool _isFlushingBarrier = false;

  SocketConnectionState _state = const Suspended('Not connected');
  SocketTransport? _currentTransport;
  StreamSubscription<dynamic>? _incomingSubscription;
  Timer? _reconnectTimer;
  Timer? _stabilityTimer;
  final RttEstimator _rttEstimator = RttEstimator();
  HeartbeatMonitor? _heartbeatMonitor;
  int _attempt = 0;
  Duration? _lastDelay;

  DateTime? _attemptStart;
  DateTime? _connectedAt;

  /// Returns the current synchronous connection state.
  SocketConnectionState get state => _state;

  /// Exposes state transitions for deterministic unit testing of the state chart.
  @visibleForTesting
  void transitionForTesting(SocketConnectionState next) => _transition(next);

  /// A broadcast stream emitting the connection state on every transition.
  Stream<SocketConnectionState> get connectionState => _stateController.stream;

  /// A broadcast stream of inbound payloads ([String] or [List<int>]).
  Stream<dynamic> get messages => _messagesController.stream;

  /// A broadcast stream of round-trip time calculation samples.
  Stream<RttSample> get rtt => _rttController.stream;

  /// A broadcast stream emitting subscription replay progress updates.
  Stream<ReplayProgress> get replayProgress => _replayProgressController.stream;

  /// Initiates connection attempts.
  ///
  /// Resets backoff attempt counters and transitions to [Connecting].
  /// If the socket is already actively connecting, connected, or reconnecting,
  /// calling [connect] has no effect.
  void connect() {
    if (_state is Disposed) {
      throw StateError('Cannot connect on a disposed ResilientSocket.');
    }
    if (_state is! Suspended) return;
    options.reconnectPolicy.reset();
    _attempt = 0;
    _lastDelay = null;
    _startAttempt(_attempt);
  }

  /// Sends [payload] ([String] or [List<int>]) over the socket.
  ///
  /// Buffered when not [Connected]; sent immediately otherwise (subject to flush barriers).
  void send(Object payload, {Duration? ttl, int priority = 0}) {
    if (_state is Disposed) {
      throw StateError('Cannot send on a disposed ResilientSocket.');
    }
    if (_state is Connected && !_isFlushingBarrier) {
      _sendFrame(payload);
    } else {
      _buffer.enqueue(payload, ttl: ttl, priority: priority);
    }
  }

  /// Registers a persistent subscription [spec].
  ///
  /// Sends `subscribeMessage()` immediately if [Connected] and not blocked by a flush barrier.
  void subscribe(SubscriptionSpec spec) {
    if (_state is Disposed) {
      throw StateError('Cannot subscribe on a disposed ResilientSocket.');
    }
    _registry.register(spec);
    if (_state is Connected && !_isFlushingBarrier) {
      _sendFrame(spec.subscribeMessage());
    }
  }

  /// Unregisters the subscription identified by [id].
  ///
  /// Sends `unsubscribeMessage()` (if configured) when [Connected] and not blocked by a flush barrier.
  void unsubscribe(String id) {
    if (_state is Disposed) {
      throw StateError('Cannot unsubscribe on a disposed ResilientSocket.');
    }
    if (!_registry.contains(id)) return;
    final spec = _registry.get(id);
    _registry.unregister(id);
    if (_state is Connected &&
        !_isFlushingBarrier &&
        spec != null &&
        spec.unsubscribeMessage != null) {
      _sendFrame(spec.unsubscribeMessage!());
    }
  }

  /// Permanently closes the connection and transitions to [Disposed].
  ///
  /// Cancels all pending timers, closes the active transport with [code]
  /// and [reason], and closes all broadcast streams. This method is idempotent.
  Future<void> close([int code = 1000, String? reason]) async {
    if (_state is Disposed) return;
    _reconnectTimer?.cancel();
    _reconnectTimer = null;
    _stabilityTimer?.cancel();
    _stabilityTimer = null;
    _heartbeatMonitor?.stop();
    _heartbeatMonitor = null;
    _rttEstimator.reset();
    _isFlushingBarrier = false;
    _coordinator.cancel();
    _buffer.clear(BufferDropReason.disposed);
    _registry.clear();
    if (_incomingSubscription != null) {
      await _incomingSubscription!.cancel();
      _incomingSubscription = null;
    }

    if (_state is! Suspended) {
      final uptime = _connectedAt != null
          ? clock.now().difference(_connectedAt!)
          : Duration.zero;
      _connectedAt = null;
      options.metrics.onDisconnected('Client closed connection', uptime);
    }

    _transition(const Disposed());

    final transport = _currentTransport;
    _currentTransport = null;
    if (transport != null) {
      await transport.close(code, reason);
    }

    await _stateController.close();
    await _messagesController.close();
    await _rttController.close();
    await _replayProgressController.close();
  }

  void _onBufferDrop(BufferDropReason reason, int count) {
    options.metrics.onBufferDrop(reason, count);
  }

  void _sendFrame(Object frame) {
    final transport = _currentTransport;
    if (transport != null) {
      transport.send(frame);
      _recordMessage(frame, inbound: false);
    }
  }

  void _recordMessage(Object frame, {required bool inbound}) {
    final metrics = options.metrics;
    if (metrics is NoopMetricsListener) return;
    if (metrics is CompositeMetricsListener && !metrics.hasActiveListeners) {
      return;
    }
    final int sizeBytes;
    if (frame is String) {
      sizeBytes = utf8.encode(frame).length;
    } else if (frame is List<int>) {
      sizeBytes = frame.length;
    } else {
      sizeBytes = 0;
    }
    metrics.onMessage(inbound: inbound, sizeBytes: sizeBytes);
  }

  void _startAttempt(int attempt) {
    _attemptStart = clock.now();
    options.metrics.onConnectAttempt(attempt);
    _transition(Connecting(attempt));
    final transport = options.transportFactory(uri);
    _currentTransport = transport;

    try {
      unawaited(
        transport.ready.then(
          (_) {
            if (_state is Disposed || _currentTransport != transport) return;
            final now = clock.now();
            final handshakeTime = _attemptStart != null
                ? now.difference(_attemptStart!)
                : Duration.zero;
            _connectedAt = now;
            options.metrics.onConnected(handshakeTime);

            _transition(Connected(lastRtt: _rttEstimator.latest?.smoothed));
            if (options.heartbeat != null) {
              _heartbeatMonitor?.stop();
              _heartbeatMonitor = HeartbeatMonitor(
                options: options.heartbeat!,
                send: _sendFrame,
                onEvent: _onHeartbeatEvent,
                estimator: _rttEstimator,
              )..start();
            }
            _startStabilityTimer();
            _startListening(transport);
            unawaited(_runConnectedReplay(transport));
          },
          onError: (Object error) {
            if (_state is Disposed || _currentTransport != transport) return;
            _onConnectionLost(error);
          },
        ),
      );
    } on Object catch (error) {
      if (_state is Disposed || _currentTransport != transport) return;
      _onConnectionLost(error);
    }
  }

  Future<void> _runConnectedReplay(SocketTransport transport) async {
    if (_state is! Connected || _currentTransport != transport) return;

    if (options.replay.flushAfterReplay) {
      _isFlushingBarrier = true;
    } else {
      _isFlushingBarrier = false;
      final messages = _buffer.drain();
      for (final msg in messages) {
        if (_state is! Connected || _currentTransport != transport) return;
        _sendFrame(msg.payload);
      }
    }

    final replayStart = clock.now();
    final subscriptionsCount = _registry.active.length;
    final completed = await _coordinator.replay(
      specs: _registry.active,
      send: _sendFrame,
      onProgress: _replayProgressController.add,
    );
    if (_state is Disposed || _currentTransport != transport) return;
    options.metrics.onReplayCompleted(
      subscriptionsCount,
      clock.now().difference(replayStart),
    );

    if (completed &&
        options.replay.flushAfterReplay &&
        _state is Connected &&
        _currentTransport == transport) {
      while (_buffer.length > 0 &&
          _state is Connected &&
          _currentTransport == transport) {
        final messages = _buffer.drain();
        final flushed = await _coordinator.flushBuffer(
          messages: messages,
          send: _sendFrame,
        );
        if (!flushed) break;
      }
      if (_state is Connected && _currentTransport == transport) {
        _isFlushingBarrier = false;
        final trailing = _buffer.drain();
        for (final msg in trailing) {
          _sendFrame(msg.payload);
        }
      }
    }
  }

  void _startListening(SocketTransport transport) {
    _incomingSubscription = transport.incoming.listen(
      (data) {
        if (_state is Disposed || _currentTransport != transport) return;
        if (data != null) _recordMessage(data as Object, inbound: true);
        if (_heartbeatMonitor != null && _heartbeatMonitor!.onMessage(data)) {
          return;
        }
        _messagesController.add(data);
      },
      onError: (Object error) {
        if (_state is Disposed || _currentTransport != transport) return;
        _onConnectionLost(error);
      },
      onDone: () {
        if (_state is Disposed || _currentTransport != transport) return;
        _onConnectionLost(transport.closeReason ?? 'Connection closed');
      },
    );
  }

  void _startStabilityTimer() {
    _stabilityTimer?.cancel();
    _stabilityTimer = Timer(options.stabilityThreshold, () {
      if (_state is Connected || _state is Degraded) {
        options.reconnectPolicy.reset();
        _attempt = 0;
        _lastDelay = null;
      }
    });
  }

  void _onConnectionLost(Object cause) {
    if (_state is Disposed || _state is Suspended || _state is Reconnecting) {
      return;
    }

    final uptime = _connectedAt != null
        ? clock.now().difference(_connectedAt!)
        : Duration.zero;
    _connectedAt = null;
    options.metrics.onDisconnected(cause, uptime);

    _stabilityTimer?.cancel();
    _stabilityTimer = null;
    _heartbeatMonitor?.stop();
    _heartbeatMonitor = null;
    _rttEstimator.reset();
    _isFlushingBarrier = false;
    _coordinator.cancel();
    unawaited(_incomingSubscription?.cancel());
    _incomingSubscription = null;

    final transport = _currentTransport;
    _currentTransport = null;
    if (transport != null) {
      unawaited(transport.close());
    }

    if (options.maxAttempts != null && (_attempt + 1) > options.maxAttempts!) {
      _transition(Suspended(cause));
      return;
    }

    final delay = options.reconnectPolicy.nextDelay(_attempt, _lastDelay);
    _lastDelay = delay;
    _transition(Reconnecting(attempt: _attempt, nextIn: delay));
    options.metrics.onReconnectScheduled(_attempt, delay);

    _reconnectTimer?.cancel();
    _reconnectTimer = Timer(delay, () {
      if (_state is Disposed || _state is Suspended) return;
      _attempt++;
      _startAttempt(_attempt);
    });
  }

  void _onHeartbeatEvent(HeartbeatEvent event) {
    if (_state is Disposed || _state is Suspended || _state is Reconnecting) {
      return;
    }

    switch (event) {
      case PongReceived(:final sample):
        options.metrics.onRttSample(sample);
        _rttController.add(sample);
        if (_state is Degraded || _state is Connected) {
          _transition(Connected(lastRtt: sample.raw));
        }
      case StaleSuspected(:final outstanding):
        if (_state is Connected) {
          _transition(Degraded(outstanding));
        }
      case ConnectionDead(:final misses):
        options.metrics.onHeartbeatMiss(misses);
        _onConnectionLost('Heartbeat missed $misses times');
    }
  }

  void _transition(SocketConnectionState next) {
    if (_state is Disposed) {
      throw StateError('Cannot transition out of terminal Disposed state.');
    }
    if (!_isLegalTransition(_state, next)) {
      throw StateError('Illegal state transition from $_state to $next');
    }
    _state = next;
    _stateController.add(next);
  }

  bool _isLegalTransition(
    SocketConnectionState current,
    SocketConnectionState next,
  ) {
    if (current is Disposed) return false;
    if (next is Disposed) return true;

    return switch (current) {
      Suspended() => next is Connecting,
      Connecting() =>
        next is Connected || next is Reconnecting || next is Suspended,
      Connected() =>
        next is Degraded ||
            next is Connected ||
            next is Reconnecting ||
            next is Suspended,
      Degraded() =>
        next is Connected ||
            next is Degraded ||
            next is Reconnecting ||
            next is Suspended,
      Reconnecting() => next is Connecting || next is Suspended,
      Disposed() => false,
    };
  }
}
