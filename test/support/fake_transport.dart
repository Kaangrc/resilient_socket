import 'dart:async';

import 'package:clock/clock.dart';
import 'package:resilient_socket/src/transport/socket_transport.dart';

/// A record representing a sent frame captured by [FakeTransport].
final class FakeSentFrame {
  /// Creates a sent frame record.
  const FakeSentFrame({required this.at, required this.data});

  /// The virtual clock timestamp when the frame was sent.
  final DateTime at;

  /// The payload object sent.
  final Object data;

  @override
  String toString() => 'FakeSentFrame(at: $at, data: $data)';
}

/// A fake [SocketTransport] for deterministic unit and integration testing.
class FakeTransport implements SocketTransport {
  final Completer<void> _readyCompleter = Completer<void>();
  final StreamController<dynamic> _incomingController =
      StreamController<dynamic>();

  /// All sent frames recorded as [FakeSentFrame] instances.
  final sentFrames = <Object>[];

  bool _isReady = false;
  bool _isClosed = false;
  bool _closedByClient = false;
  int? _closeCode;
  String? _closeReason;

  @override
  Future<void> get ready => _readyCompleter.future;

  @override
  Stream<dynamic> get incoming => _incomingController.stream;

  /// Returns whether [close] was explicitly called by the client.
  bool get closedByClient => _closedByClient;

  @override
  int? get closeCode => _closeCode;

  @override
  String? get closeReason => _closeReason;

  /// Convenience getter returning the payloads of all sent frames.
  List<Object> get sentData =>
      sentFrames.map((e) => (e as FakeSentFrame).data).toList();

  /// Completes the [ready] future successfully.
  void completeReady() {
    if (!_readyCompleter.isCompleted) {
      _isReady = true;
      _readyCompleter.complete();
    }
  }

  /// Completes the [ready] future with an [error].
  void failReady(Object error) {
    if (!_readyCompleter.isCompleted) {
      _readyCompleter.completeError(error);
    }
  }

  /// Emits [data] on the [incoming] stream.
  void emit(Object data) {
    if (!_isClosed) {
      _incomingController.add(data);
    }
  }

  /// Closes the [incoming] stream and sets close codes without marking
  /// as closed by the client.
  void dropConnection({int? code, String? reason}) {
    if (_isClosed) return;
    _isClosed = true;
    _closeCode = code;
    _closeReason = reason;
    unawaited(_incomingController.close());
  }

  @override
  void send(Object data) {
    if (!_isReady) {
      throw StateError('Cannot send before transport ready completes.');
    }
    if (_isClosed) {
      throw StateError('Cannot send after transport is closed.');
    }
    sentFrames.add(FakeSentFrame(at: clock.now(), data: data));
  }

  @override
  Future<void> close([int code = 1000, String? reason]) async {
    if (_isClosed) return;
    _closedByClient = true;
    _isClosed = true;
    _closeCode = code;
    _closeReason = reason;
    if (_incomingController.hasListener) {
      await _incomingController.close();
    } else {
      unawaited(_incomingController.close());
    }
  }
}

/// A [TransportFactory] that returns [FakeTransport] instances and records them.
class RecordingTransportFactory {
  /// Creates a factory, optionally scripting a sequence of [scripted] transports.
  RecordingTransportFactory([this.scripted]);

  /// Optional pre-created transports to return in order.
  final List<FakeTransport>? scripted;

  /// All transports created or returned by this factory.
  final created = <FakeTransport>[];
  int _index = 0;

  /// Creates or returns the next [FakeTransport].
  SocketTransport call(Uri uri) {
    final FakeTransport transport;
    if (scripted != null && _index < scripted!.length) {
      transport = scripted![_index++];
    } else {
      transport = FakeTransport();
    }
    created.add(transport);
    return transport;
  }
}
