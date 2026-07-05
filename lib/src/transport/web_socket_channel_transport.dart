import 'dart:async';

import 'package:resilient_socket/src/transport/socket_transport.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// A [SocketTransport] implementation backed by [WebSocketChannel].
///
/// This is the only file in the library that imports `package:web_socket_channel`.
class WebSocketChannelTransport implements SocketTransport {
  WebSocketChannelTransport._(this._channel) {
    unawaited(
      _channel.ready.then(
        (_) => _isReady = true,
        onError: (_) {
          // Handshake failed; ready future will emit error to callers.
        },
      ),
    );
  }

  /// Connects to [uri] and returns a new [WebSocketChannelTransport].
  ///
  /// Optionally accepts [channelFactory] for injecting a custom channel
  /// during unit testing without network IO.
  factory WebSocketChannelTransport.connect(
    Uri uri, {
    WebSocketChannel Function(Uri uri)? channelFactory,
  }) {
    final channel = channelFactory != null
        ? channelFactory(uri)
        : WebSocketChannel.connect(uri);
    return WebSocketChannelTransport._(channel);
  }

  final WebSocketChannel _channel;
  bool _isReady = false;
  bool _isClosed = false;

  @override
  Future<void> get ready => _channel.ready;

  @override
  late final Stream<dynamic> incoming = _channel.stream.transform(
    StreamTransformer<dynamic, dynamic>.fromHandlers(
      handleDone: (sink) {
        _isClosed = true;
        sink.close();
      },
    ),
  );

  @override
  void send(Object data) {
    if (!_isReady) {
      throw StateError('Cannot send before transport is ready.');
    }
    if (_isClosed) {
      throw StateError('Cannot send after transport is closed.');
    }
    _channel.sink.add(data);
  }

  @override
  Future<void> close([int code = 1000, String? reason]) async {
    if (_isClosed) return;
    _isClosed = true;
    await _channel.sink.close(code, reason);
  }

  @override
  int? get closeCode => _channel.closeCode;

  @override
  String? get closeReason => _channel.closeReason;
}
