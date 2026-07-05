import 'package:resilient_socket/src/buffer/buffer_overflow_exception.dart';

/// Strategy for handling message buffer overflow when capacity is reached.
enum OverflowStrategy {
  /// Evict the oldest message in the buffer to make room for the new message.
  dropOldest,

  /// Discard the newly arriving message and leave the existing buffer intact.
  dropNewest,

  /// Evict the message with the lowest priority. If priorities tie, evict the oldest among them.
  dropByPriority,

  /// Throw a [BufferOverflowException] without modifying the buffer.
  throwException,
}
