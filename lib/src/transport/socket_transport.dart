/// Abstract transport layer used by the library.
///
/// Yields inbound messages directly from the underlying socket without any
/// intermediate [String] conversion.
abstract interface class SocketTransport {
  /// Completes when the connection is established; completes with an error
  /// on handshake failure.
  Future<void> get ready;

  /// Inbound data exactly as received: [String] or [List<int>].
  ///
  /// Must be single-subscription. Emits done when the connection closes for
  /// any reason.
  Stream<dynamic> get incoming;

  /// Sends [data] ([String] or [List<int>]).
  ///
  /// Throws [StateError] if called before [ready] completes or after close.
  void send(Object data);

  /// Closes the connection. Idempotent.
  Future<void> close([int code = 1000, String? reason]);

  /// Populated after [incoming] is done, when the underlying channel
  /// provides them.
  int? get closeCode;

  /// Populated after [incoming] is done, when the underlying channel
  /// provides them.
  String? get closeReason;
}

/// Creates a fresh, unconnected-yet-connecting transport for [uri].
typedef TransportFactory = SocketTransport Function(Uri uri);
