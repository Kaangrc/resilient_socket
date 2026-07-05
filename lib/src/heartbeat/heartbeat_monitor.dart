import 'dart:async';
import 'dart:math' as math;

import 'package:clock/clock.dart';
import 'package:meta/meta.dart';
import 'package:resilient_socket/src/heartbeat/heartbeat_options.dart';
import 'package:resilient_socket/src/heartbeat/rtt_estimator.dart';
import 'package:resilient_socket/src/heartbeat/rtt_sample.dart';

/// Base sealed class for events emitted by a [HeartbeatMonitor].
@immutable
sealed class HeartbeatEvent {
  const HeartbeatEvent();
}

/// Emitted when a pong frame is received and matched to a pending ping.
@immutable
final class PongReceived extends HeartbeatEvent {
  /// Creates a [PongReceived] event with the resulting [sample].
  const PongReceived(this.sample);

  /// The RTT sample calculated from this ping/pong exchange.
  final RttSample sample;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is PongReceived &&
          runtimeType == other.runtimeType &&
          sample == other.sample;

  @override
  int get hashCode => sample.hashCode;

  @override
  String toString() => 'PongReceived($sample)';
}

/// Emitted when a predictive stale timer expires before a pong is received.
@immutable
final class StaleSuspected extends HeartbeatEvent {
  /// Creates a [StaleSuspected] event with the [outstanding] duration.
  const StaleSuspected(this.outstanding);

  /// How long the pending ping has been unanswered.
  final Duration outstanding;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is StaleSuspected &&
          runtimeType == other.runtimeType &&
          outstanding == other.outstanding;

  @override
  int get hashCode => outstanding.hashCode;

  @override
  String toString() => 'StaleSuspected(outstanding: $outstanding)';
}

/// Emitted when consecutive missed pongs exceed the threshold.
@immutable
final class ConnectionDead extends HeartbeatEvent {
  /// Creates a [ConnectionDead] event with the consecutive [misses] count.
  const ConnectionDead(this.misses);

  /// Number of consecutive unanswered ping intervals.
  final int misses;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is ConnectionDead &&
          runtimeType == other.runtimeType &&
          misses == other.misses;

  @override
  int get hashCode => misses.hashCode;

  @override
  String toString() => 'ConnectionDead(misses: $misses)';
}

/// Manages ping/pong cadence, predictive stale detection, and dead connection tracking.
class HeartbeatMonitor {
  /// Creates a heartbeat monitor with dependencies and configuration.
  HeartbeatMonitor({
    required this.options,
    required this.send,
    required this.onEvent,
    required this.estimator,
  });

  /// Configuration options for heartbeat pacing and thresholds.
  final HeartbeatOptions options;

  /// Function invoked to send an outbound ping payload over the socket.
  final void Function(Object frame) send;

  /// Callback invoked when a heartbeat lifecycle event occurs.
  final void Function(HeartbeatEvent event) onEvent;

  /// Estimator tracking RTT variance and smoothed averages.
  final RttEstimator estimator;

  bool _isRunning = false;
  int _seq = 0;
  int? _pendingSeq;
  int _misses = 0;
  DateTime? _pingSentAt;
  Timer? _intervalTimer;
  Timer? _staleTimer;

  /// Starts the heartbeat monitor. Sends the first ping immediately and schedules pacing timers.
  void start() {
    stop();
    _isRunning = true;
    _seq = 0;
    _misses = 0;
    _sendPing();
  }

  /// Stops the monitor and cancels all active timers. Safe to call repeatedly.
  void stop() {
    _isRunning = false;
    _intervalTimer?.cancel();
    _intervalTimer = null;
    _staleTimer?.cancel();
    _staleTimer = null;
    _pendingSeq = null;
    _pingSentAt = null;
  }

  /// Feeds an inbound [message] into the monitor.
  ///
  /// Returns `true` if consumed as a matching pong (current or old sequence),
  /// `false` otherwise.
  bool onMessage(dynamic message) {
    if (!_isRunning) return false;

    final pending = _pendingSeq;
    if (pending != null && options.pongMatcher(message, pending)) {
      final now = clock.now();
      final raw = now.difference(_pingSentAt!);
      final sample = estimator.addSample(raw);

      _pendingSeq = null;
      _misses = 0;
      _staleTimer?.cancel();
      _staleTimer = null;

      onEvent(PongReceived(sample));

      _intervalTimer?.cancel();
      _intervalTimer = Timer(_interval(), _sendPing);
      return true;
    }

    final minSeq = math.max(1, _seq - 20);
    for (var s = _seq - 1; s >= minSeq; s--) {
      if (options.pongMatcher(message, s)) {
        return true;
      }
    }

    return false;
  }

  void _sendPing() {
    if (!_isRunning) return;

    if (_pendingSeq != null) {
      _misses++;
      if (_misses >= options.maxMisses) {
        onEvent(ConnectionDead(_misses));
        stop();
        return;
      }
    }

    _seq++;
    _pendingSeq = _seq;
    _pingSentAt = clock.now();
    send(options.pingBuilder(_seq));

    final currentRto = estimator.latest?.rto ?? options.initialInterval;
    final staleUs = (options.staleFactor * currentRto.inMicroseconds).floor();
    _staleTimer?.cancel();
    _staleTimer = Timer(Duration(microseconds: staleUs), () {
      if (_pendingSeq != null && _isRunning) {
        final outstanding = clock.now().difference(_pingSentAt!);
        onEvent(StaleSuspected(outstanding));
      }
    });

    _intervalTimer?.cancel();
    _intervalTimer = Timer(_interval(), _sendPing);
  }

  /// High relative variance = unstable network = probe more often.
  Duration _interval() {
    if (!options.adaptive) {
      return options.initialInterval;
    }
    final latest = estimator.latest;
    if (latest == null) {
      return options.initialInterval;
    }

    final srttUs = latest.smoothed.inMicroseconds;
    final rttvarUs = latest.variance.inMicroseconds;
    if (srttUs <= 0) {
      return options.minInterval;
    }

    var r = rttvarUs / srttUs;
    if (r < 0.0) r = 0.0;
    if (r > 1.0) r = 1.0;

    final maxUs = options.maxInterval.inMicroseconds;
    final minUs = options.minInterval.inMicroseconds;
    final intervalUs = (maxUs - (maxUs - minUs) * r).floor();
    return Duration(microseconds: intervalUs);
  }
}
