/// Exception thrown when the outbound buffer overflows under `OverflowStrategy.throwException`.
class BufferOverflowException implements Exception {
  /// Creates a buffer overflow exception for a buffer with the given [capacity].
  const BufferOverflowException({required this.capacity});

  /// The message capacity of the buffer that overflowed.
  final int capacity;

  @override
  String toString() =>
      'BufferOverflowException: buffer overflowed at capacity $capacity';
}
