// coverage:ignore-file
// ignore_for_file: avoid_print
import 'dart:io';

import 'package:resilient_socket/resilient_socket.dart';

void main(List<String> args) {
  var exchange = 'binance';
  for (var i = 0; i < args.length; i++) {
    if (args[i] == '--exchange' && i + 1 < args.length) {
      exchange = args[i + 1].toLowerCase();
    }
  }

  late final Uri uri;
  late final SubscriptionSpec spec;

  if (exchange == 'binance') {
    uri = Uri.parse('wss://stream.binance.com:9443/ws');
    spec = SubscriptionSpec(
      id: 'binance-btc-ticker',
      subscribeMessage: () =>
          '{"method": "SUBSCRIBE", "params": ["btcusdt@ticker"], "id": 1}',
      unsubscribeMessage: () =>
          '{"method": "UNSUBSCRIBE", "params": ["btcusdt@ticker"], "id": 1}',
    );
  } else if (exchange == 'btcturk') {
    uri = Uri.parse('wss://ws-feed-pro.btcturk.com');
    spec = SubscriptionSpec(
      id: 'btcturk-btc-ticker',
      subscribeMessage: () =>
          '[151, {"type": 151, "channel": "ticker", "event": "BTCTRY", "join": true}]',
      unsubscribeMessage: () =>
          '[151, {"type": 151, "channel": "ticker", "event": "BTCTRY", "join": false}]',
    );
  } else {
    print(
      'Unsupported exchange "$exchange". Use --exchange binance or btcturk.',
    );
    exit(1);
  }

  final socket = ResilientSocket(
    uri,
    options: ResilientSocketOptions(
      transportFactory: WebSocketChannelTransport.connect,
      metrics: const _StdoutMetricsListener(),
    ),
  );

  final subscription = socket.messages
      .throttleLatest(const Duration(milliseconds: 500))
      .listen((event) {
        print('[Ticker] $event');
      });

  print('Connecting to $exchange at $uri...');
  socket
    ..connect()
    ..subscribe(spec);

  ProcessSignal.sigint.watch().listen((_) async {
    print('\n[CLI] Shutting down...');
    await subscription.cancel();
    await socket.close(1000, 'CLI shutdown');
    exit(0);
  });
}

class _StdoutMetricsListener implements SocketMetricsListener {
  const _StdoutMetricsListener();

  @override
  void onConnectAttempt(int attempt) {
    print('[Telemetry] Connecting... (attempt $attempt)');
  }

  @override
  void onConnected(Duration handshakeTime) {
    print(
      '[Telemetry] Connected! (handshake: ${handshakeTime.inMilliseconds}ms)',
    );
  }

  @override
  void onDisconnected(Object? cause, Duration sessionUptime) {
    print(
      '[Telemetry] Disconnected: $cause (uptime: ${sessionUptime.inSeconds}s)',
    );
  }

  @override
  void onReconnectScheduled(int attempt, Duration delay) {
    print(
      '[Telemetry] Reconnect scheduled in ${delay.inSeconds}s (attempt $attempt)',
    );
  }

  @override
  void onRttSample(RttSample sample) {
    // Silent to avoid flooding stdout
  }

  @override
  void onHeartbeatMiss(int consecutiveMisses) {
    print('[Telemetry] Heartbeat miss! ($consecutiveMisses consecutive)');
  }

  @override
  void onBufferDrop(BufferDropReason reason, int droppedCount) {
    print('[Telemetry] Buffer drop: $reason ($droppedCount dropped)');
  }

  @override
  void onReplayCompleted(int subscriptions, Duration took) {
    print(
      '[Telemetry] Replayed $subscriptions subscriptions in ${took.inMilliseconds}ms',
    );
  }

  @override
  void onMessage({required bool inbound, required int sizeBytes}) {
    // Silent for individual messages; ticker updates printed via stream
  }
}
