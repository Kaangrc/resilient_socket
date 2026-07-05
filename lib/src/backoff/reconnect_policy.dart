/// Contract for reconnection delay strategies.
///
/// All implementations compute delays based on attempt index and optional
/// knowledge of the previous delay. Attempt numbering is 0-indexed: the
/// first retry is attempt 0.
abstract interface class ReconnectPolicy {
  /// Returns the delay before reconnect attempt [attempt] (0-indexed).
  ///
  /// [previousDelay] is the delay actually used for the previous attempt,
  /// or `null` on attempt 0.
  Duration nextDelay(int attempt, Duration? previousDelay);

  /// Clears internal state; the next call behaves as attempt 0.
  void reset();
}
