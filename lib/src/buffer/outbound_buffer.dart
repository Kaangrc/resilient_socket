import 'package:clock/clock.dart';
import 'package:resilient_socket/src/buffer/buffer_drop_reason.dart';
import 'package:resilient_socket/src/buffer/buffer_overflow_exception.dart';
import 'package:resilient_socket/src/buffer/buffered_message.dart';
import 'package:resilient_socket/src/buffer/outbound_buffer_options.dart';
import 'package:resilient_socket/src/buffer/overflow_strategy.dart';

/// Manages an offline queue of outbound messages with TTL expiration, size limits,
/// and configurable overflow eviction strategies.
class OutboundBuffer {
  /// Creates an outbound buffer with configuration [options] and drop reporting callback [onDrop].
  OutboundBuffer(
    this.options, {
    required this.onDrop,
  });

  /// Configuration options governing limits and overflow strategy.
  final OutboundBufferOptions options;

  /// Callback invoked whenever messages are dropped from the buffer.
  final void Function(BufferDropReason reason, int count) onDrop;

  final List<BufferedMessage> _queue = [];
  int _totalBytes = 0;

  /// The current number of messages stored in the buffer.
  int get length => _queue.length;

  int _estimateSize(Object payload) {
    final estimator = options.sizeEstimator;
    final maxBytes = options.maxBytes;
    if (estimator != null && maxBytes != null) {
      return estimator(payload);
    }
    return 0;
  }

  bool _isOverflow(int incomingSize) {
    if (_queue.length >= options.maxMessages) {
      return true;
    }
    if (options.sizeEstimator != null && options.maxBytes != null) {
      if (_totalBytes + incomingSize > options.maxBytes!) {
        return true;
      }
    }
    return false;
  }

  /// Enqueues a message [payload] into the buffer with optional [ttl] override and [priority].
  void enqueue(Object payload, {Duration? ttl, int priority = 0}) {
    final now = clock.now();
    final message = BufferedMessage(
      payload: payload,
      enqueuedAt: now,
      ttl: ttl ?? options.defaultTtl,
      priority: priority,
    );

    final incomingSize = _estimateSize(payload);

    if (!_isOverflow(incomingSize)) {
      _queue.add(message);
      _totalBytes += incomingSize;
      return;
    }

    switch (options.overflow) {
      case OverflowStrategy.throwException:
        throw BufferOverflowException(capacity: options.maxMessages);

      case OverflowStrategy.dropNewest:
        onDrop(BufferDropReason.overflow, 1);
        return;

      case OverflowStrategy.dropOldest:
        var droppedCount = 0;
        while (_isOverflow(incomingSize)) {
          if (_queue.isEmpty) {
            droppedCount++;
            onDrop(BufferDropReason.overflow, droppedCount);
            return;
          }
          final removed = _queue.removeAt(0);
          _totalBytes -= _estimateSize(removed.payload);
          droppedCount++;
        }
        if (droppedCount > 0) {
          onDrop(BufferDropReason.overflow, droppedCount);
        }
        _queue.add(message);
        _totalBytes += incomingSize;
        return;

      case OverflowStrategy.dropByPriority:
        var droppedCount = 0;
        var incomingDiscarded = false;

        while (_isOverflow(incomingSize)) {
          if (_queue.isEmpty) {
            droppedCount++;
            incomingDiscarded = true;
            break;
          }

          var lowestIdx = 0;
          var lowestPriority = _queue[0].priority;
          for (var i = 1; i < _queue.length; i++) {
            if (_queue[i].priority < lowestPriority) {
              lowestPriority = _queue[i].priority;
              lowestIdx = i;
            }
          }

          if (message.priority > lowestPriority) {
            final removed = _queue.removeAt(lowestIdx);
            _totalBytes -= _estimateSize(removed.payload);
            droppedCount++;
          } else {
            // Equal priority does NOT displace — FIFO fairness.
            droppedCount++;
            incomingDiscarded = true;
            break;
          }
        }

        if (droppedCount > 0) {
          onDrop(BufferDropReason.overflow, droppedCount);
        }
        if (!incomingDiscarded) {
          _queue.add(message);
          _totalBytes += incomingSize;
        }
        return;
    }
  }

  /// Removes and returns all non-expired messages in strict FIFO order.
  ///
  /// Any expired messages are stripped out and reported via [onDrop] exactly once per call.
  List<BufferedMessage> drain() {
    final now = clock.now();
    final valid = <BufferedMessage>[];
    var expiredCount = 0;

    for (final msg in _queue) {
      if (msg.isExpiredAt(now)) {
        expiredCount++;
      } else {
        valid.add(msg);
      }
    }

    _queue.clear();
    _totalBytes = 0;

    if (expiredCount > 0) {
      onDrop(BufferDropReason.ttlExpired, expiredCount);
    }

    return valid;
  }

  /// Clears all messages from the buffer and reports the drop [reason] if non-empty.
  void clear(BufferDropReason reason) {
    if (_queue.isNotEmpty) {
      final count = _queue.length;
      _queue.clear();
      _totalBytes = 0;
      onDrop(reason, count);
    }
  }
}
