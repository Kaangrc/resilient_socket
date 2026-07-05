import 'dart:async';

import 'package:resilient_socket/src/transport/web_socket_channel_transport.dart';
// stream_channel is used to build a fake WebSocket channel in tests.
import 'package:stream_channel/stream_channel.dart';
import 'package:test/test.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

class _FakeWebSocketSink implements WebSocketSink {
  final added = <Object>[];
  bool isClosed = false;
  int? closeCode;
  String? closeReason;

  @override
  void add(dynamic data) {
    if (isClosed) throw StateError('sink closed');
    added.add(data as Object);
  }

  @override
  void addError(Object error, [StackTrace? stackTrace]) {}

  @override
  Future<void> addStream(Stream<dynamic> stream) async {}

  @override
  Future<void> close([int? closeCode, String? closeReason]) async {
    isClosed = true;
    this.closeCode = closeCode;
    this.closeReason = closeReason;
  }

  @override
  Future<void> get done => Future.value();
}

class _FakeWebSocketChannel extends StreamChannelMixin<dynamic>
    implements WebSocketChannel {
  final _readyCompleter = Completer<void>();
  final _streamController = StreamController<dynamic>();
  final _sink = _FakeWebSocketSink();
  int? _closeCode;
  String? _closeReason;

  @override
  Future<void> get ready => _readyCompleter.future;

  @override
  Stream<dynamic> get stream => _streamController.stream;

  @override
  WebSocketSink get sink => _sink;

  @override
  int? get closeCode => _closeCode;

  @override
  String? get closeReason => _closeReason;

  @override
  String? get protocol => null;
}

void main() {
  group('WebSocketChannelTransport', () {
    test(
      'maps ready, stream, send, close, and closeCode/closeReason',
      () async {
        final fakeChannel = _FakeWebSocketChannel();
        final transport = WebSocketChannelTransport.connect(
          Uri.parse('ws://localhost:8080'),
          channelFactory: (_) => fakeChannel,
        );

        // send before ready should throw
        expect(() => transport.send('test'), throwsStateError);

        // Complete ready
        fakeChannel._readyCompleter.complete();
        await transport.ready;

        // Now send should forward to channel sink
        transport.send('msg1');
        expect(fakeChannel._sink.added, equals(['msg1']));

        // Inbound messages forward from channel stream
        final received = <dynamic>[];
        transport.incoming.listen(received.add);
        fakeChannel._streamController.add('inbound1');
        await Future<void>.delayed(Duration.zero);
        expect(received, equals(['inbound1']));

        // Close transport
        fakeChannel
          .._closeCode = 1000
          .._closeReason = 'bye';
        await transport.close(1000, 'bye');

        expect(fakeChannel._sink.isClosed, isTrue);
        expect(fakeChannel._sink.closeCode, equals(1000));
        expect(fakeChannel._sink.closeReason, equals('bye'));
        expect(transport.closeCode, equals(1000));
        expect(transport.closeReason, equals('bye'));

        // Send after close should throw
        expect(() => transport.send('msg2'), throwsStateError);
      },
    );
  });
}
