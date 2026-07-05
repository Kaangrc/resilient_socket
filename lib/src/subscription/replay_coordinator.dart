import 'dart:async';
import 'dart:math' as math;

import 'package:resilient_socket/src/buffer/buffered_message.dart';
import 'package:resilient_socket/src/subscription/replay_options.dart';
import 'package:resilient_socket/src/subscription/replay_progress.dart';
import 'package:resilient_socket/src/subscription/subscription_spec.dart';

/// Coordinates paced transmission of subscription replay batches and buffered messages.
class ReplayCoordinator {
  /// Creates a replay coordinator configured with [options].
  ReplayCoordinator(this.options);

  /// Configuration options governing pacing and batch sizes.
  final ReplayOptions options;

  Timer? _activeTimer;
  Completer<bool>? _activeCompleter;
  bool _isCancelled = false;

  /// Sends `subscribeMessage()` for each spec in order, in bursts of [ReplayOptions.batchSize]
  /// with [ReplayOptions.pacing] between bursts. Emits progress after every individual send.
  ///
  /// Returns `true` if completed, `false` if [cancel] was called mid-flight.
  Future<bool> replay({
    required List<SubscriptionSpec> specs,
    required void Function(Object frame) send,
    required void Function(ReplayProgress progress) onProgress,
  }) {
    return _sendBatched(
      items: specs,
      sendItem: (spec) => send(spec.subscribeMessage()),
      onProgress: (sent, total) =>
          onProgress(ReplayProgress(total: total, sent: sent)),
    );
  }

  /// Flushes buffered messages using the same pacing and batch size implementation.
  ///
  /// Returns `true` if completed, `false` if [cancel] was called mid-flight.
  Future<bool> flushBuffer({
    required List<BufferedMessage> messages,
    required void Function(Object frame) send,
  }) {
    return _sendBatched(
      items: messages,
      sendItem: (msg) => send(msg.payload),
      onProgress: null,
    );
  }

  Future<bool> _sendBatched<T>({
    required List<T> items,
    required void Function(T item) sendItem,
    required void Function(int sent, int total)? onProgress,
  }) {
    cancel();

    final total = items.length;
    if (total == 0) {
      onProgress?.call(0, 0);
      return Future.value(true);
    }

    final completer = Completer<bool>();
    _activeCompleter = completer;
    _isCancelled = false;
    var sent = 0;
    final batchSize = math.max(1, options.batchSize);

    void sendNextBurst() {
      if (_isCancelled || completer.isCompleted) return;

      var burstSent = 0;
      while (sent < total && burstSent < batchSize) {
        if (_isCancelled || completer.isCompleted) break;
        final item = items[sent];
        sendItem(item);
        sent++;
        burstSent++;
        onProgress?.call(sent, total);
      }

      if (_isCancelled || completer.isCompleted) return;

      if (sent >= total) {
        _activeTimer?.cancel();
        _activeTimer = null;
        _activeCompleter = null;
        if (!completer.isCompleted) {
          completer.complete(true);
        }
      } else {
        _activeTimer?.cancel();
        _activeTimer = Timer(options.pacing, sendNextBurst);
      }
    }

    sendNextBurst();
    return completer.future;
  }

  /// Cancels any active replay or flush loop immediately and resolves pending futures with `false`.
  void cancel() {
    _isCancelled = true;
    _activeTimer?.cancel();
    _activeTimer = null;
    if (_activeCompleter != null && !_activeCompleter!.isCompleted) {
      _activeCompleter!.complete(false);
    }
    _activeCompleter = null;
  }
}
