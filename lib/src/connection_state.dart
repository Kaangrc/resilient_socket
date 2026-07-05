import 'package:meta/meta.dart';

/// The sealed connection state hierarchy for a resilient socket.
@immutable
sealed class SocketConnectionState {
  /// Creates a [SocketConnectionState].
  const SocketConnectionState();
}

/// The socket is actively attempting its initial connection or a retry.
@immutable
final class Connecting extends SocketConnectionState {
  /// Creates a [Connecting] state for the given [attempt] number (0-indexed).
  const Connecting(this.attempt);

  /// The current attempt number (0-indexed).
  final int attempt;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is Connecting &&
          runtimeType == other.runtimeType &&
          attempt == other.attempt;

  @override
  int get hashCode => attempt.hashCode;

  @override
  String toString() => 'Connecting($attempt)';
}

/// The socket is connected and open.
@immutable
final class Connected extends SocketConnectionState {
  /// Creates a [Connected] state, optionally with the [lastRtt] sample.
  const Connected({this.lastRtt});

  /// The last measured round-trip time, if available.
  final Duration? lastRtt;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is Connected &&
          runtimeType == other.runtimeType &&
          lastRtt == other.lastRtt;

  @override
  int get hashCode => lastRtt.hashCode;

  @override
  String toString() => 'Connected(lastRtt: $lastRtt)';
}

/// The socket connection is open but experiencing high latency or degraded performance.
@immutable
final class Degraded extends SocketConnectionState {
  /// Creates a [Degraded] state with the current measured [rtt].
  const Degraded(this.rtt);

  /// The current measured round-trip time indicating degradation.
  final Duration rtt;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is Degraded && runtimeType == other.runtimeType && rtt == other.rtt;

  @override
  int get hashCode => rtt.hashCode;

  @override
  String toString() => 'Degraded($rtt)';
}

/// The socket lost connection and is waiting to attempt a reconnection.
@immutable
final class Reconnecting extends SocketConnectionState {
  /// Creates a [Reconnecting] state with the [attempt] number and delay [nextIn].
  const Reconnecting({required this.attempt, required this.nextIn});

  /// The upcoming attempt number (0-indexed).
  final int attempt;

  /// The duration until the next connection attempt will begin.
  final Duration nextIn;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is Reconnecting &&
          runtimeType == other.runtimeType &&
          attempt == other.attempt &&
          nextIn == other.nextIn;

  @override
  int get hashCode => Object.hash(attempt, nextIn);

  @override
  String toString() => 'Reconnecting(attempt: $attempt, nextIn: $nextIn)';
}

/// Reconnection attempts have been suspended due to breaching max attempts or fatal error.
@immutable
final class Suspended extends SocketConnectionState {
  /// Creates a [Suspended] state with the underlying [cause] of suspension.
  const Suspended(this.cause);

  /// The error or cause that suspended reconnection attempts.
  final Object cause;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is Suspended &&
          runtimeType == other.runtimeType &&
          cause == other.cause;

  @override
  int get hashCode => cause.hashCode;

  @override
  String toString() => 'Suspended($cause)';
}

/// The socket has been permanently closed and disposed.
@immutable
final class Disposed extends SocketConnectionState {
  /// Creates a [Disposed] terminal state.
  const Disposed();

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is Disposed && runtimeType == other.runtimeType;

  @override
  int get hashCode => runtimeType.hashCode;

  @override
  String toString() => 'Disposed()';
}
