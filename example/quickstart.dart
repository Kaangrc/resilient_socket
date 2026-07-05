// coverage:ignore-file
// ignore_for_file: avoid_print
import 'package:resilient_socket/resilient_socket.dart';

void main() {
  final socket = ResilientSocket(
    Uri.parse('wss://stream.binance.com:9443/ws'),
    options: ResilientSocketOptions(
      transportFactory: WebSocketChannelTransport.connect,
      reconnectPolicy: DecorrelatedJitterBackoff(
        base: const Duration(milliseconds: 250),
        cap: const Duration(seconds: 30),
      ),
      heartbeat: HeartbeatOptions(
        pingBuilder: (seq) => '{"method":"PING","id":$seq}',
        pongMatcher: (msg, seq) => msg is String && msg.contains('PONG'),
      ),
    ),
  );

  socket.connectionState.listen((state) => print('state: $state'));

  socket.messages
      .throttleLatest(const Duration(milliseconds: 500))
      .listen((msg) => print('tick: $msg'));

  socket
    ..connect()
    ..subscribe(
      SubscriptionSpec(
        id: 'btc-ticker',
        subscribeMessage: () =>
            '{"method":"SUBSCRIBE","params":["btcusdt@ticker"],"id":1}',
      ),
    );
}
