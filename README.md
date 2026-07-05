# resilient_socket

[![CI](https://github.com/Kaangrc/resilient_socket/actions/workflows/ci.yaml/badge.svg)](https://github.com/Kaangrc/resilient_socket/actions)

Pure-Dart WebSocket resilience: decorrelated-jitter reconnection, adaptive heartbeat with RTT tracking, bounded offline buffering, subscription replay, and stream protection operators.

---

## Competitive Gap Matrix (v0.1.0)

| Capability | web_socket_channel | web_socket_client | adapter_websocket | **resilient_socket** |
| :--- | :---: | :---: | :---: | :---: |
| Auto-reconnect + jitter matrix (Full / Equal / Decorrelated) | — | partial | partial | **yes** |
| Adaptive heartbeat + RTT/RTO stream | — | — | static interval | **yes** |
| Predictive stale detection (RTO-based) | — | — | missed-pong only | **yes** |
| Offline buffer (TTL, priority, byte caps) | — | — | — | **yes** |
| Subscription replay (paced, rate-limit aware) | — | — | — | **yes** |
| Telemetry hook (`SocketMetricsListener`) | — | — | log stream | **yes** |
| Virtual-time test kit (`FakeTransport`, `SequencedRandom`) | — | — | mock adapter | **yes** |

---

## Quickstart

```dart
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
```

---

## Architectural Guarantees & Verification Matrix

Every guarantee maps to an executable test in this repository:

| Architectural Guarantee | Specification Reference | Active Verification Test Suite |
| :--- | :--- | :--- |
| Pure Dart & zero framework coupling | ADR-0001 | `test/connection_state_test.dart`, `test/transport/fake_transport_test.dart` |
| Deterministic time & randomness discipline | ADR-0002 | `test/backoff/backoff_test.dart`, `test/transport/fake_transport_test.dart` |
| Decorrelated jitter thundering-herd mitigation | ADR-0003, §3.1 | `test/backoff/backoff_test.dart` |
| Sealed & exhaustive connection state machine | §3.1 | `test/connection_state_test.dart` |
| Mathematical RTO clamping & adaptive heartbeat | §3.2 | `test/heartbeat/rtt_estimator_test.dart`, `test/heartbeat/heartbeat_monitor_test.dart` |
| Bounded outbound buffer & dual-capacity eviction | ADR-0004, §3.3 | `test/buffer/outbound_buffer_test.dart` |
| Flush barrier & priority-ordered replay pacing | ADR-0005, §3.4 | `test/subscription/subscription_test.dart` |
| Single-subscription leak-free stream protection | ADR-0005, §3.5 | `test/stream_ops/stream_protection_test.dart`, `test/stream_ops/stream_protection_edge_cases_test.dart` |
| Non-disruptive telemetry fan-out isolation | §3.6 | `test/telemetry/telemetry_test.dart` |
| End-to-end master lifecycle matrix | §3.7 | `test/resilient_socket_test.dart` |

---

## Reconnection Strategy Comparison

All delay arithmetic uses integer microseconds (`Duration(microseconds: …)`). Configuration: `base = 250,000 µs`, `cap = 30,000,000 µs`. `exp = min(cap, base × 2^attempt)`.

### Formulas

| Strategy | `nextDelay(attempt, prev)` |
| :--- | :--- |
| `DecorrelatedJitterBackoff` | `uniform(base, min(cap, prev × 3))` — `prev` defaults to `base` on attempt 0 |
| `EqualJitterBackoff` | `(exp / 2) + uniform(0, exp / 2)` |
| `ExponentialBackoff` | `min(cap, base × 2^attempt)` |
| `FullJitterBackoff` | `uniform(0, exp)` |

### Attempt 0–7 delay table (microseconds)

| Attempt | `ExponentialBackoff` | `FullJitterBackoff` | `EqualJitterBackoff` | `DecorrelatedJitterBackoff` |
| :---: | :---: | :---: | :---: | :--- |
| 0 | 250,000 | 0 – 250,000 | 125,000 – 250,000 | 250,000 – 750,000 |
| 1 | 500,000 | 0 – 500,000 | 250,000 – 500,000 | 250,000 – 1,500,000 |
| 2 | 1,000,000 | 0 – 1,000,000 | 500,000 – 1,000,000 | 250,000 – 3,000,000 |
| 3 | 2,000,000 | 0 – 2,000,000 | 1,000,000 – 2,000,000 | 250,000 – 6,000,000 |
| 4 | 4,000,000 | 0 – 4,000,000 | 2,000,000 – 4,000,000 | 250,000 – 12,000,000 |
| 5 | 8,000,000 | 0 – 8,000,000 | 4,000,000 – 8,000,000 | 250,000 – 24,000,000 |
| 6 | 16,000,000 | 0 – 16,000,000 | 8,000,000 – 16,000,000 | 250,000 – 30,000,000 |
| 7 | 30,000,000 | 0 – 30,000,000 | 15,000,000 – 30,000,000 | 250,000 – 30,000,000 |

Decorrelated upper bounds assume `prev` equals the exponential ceiling at the prior attempt (`min(cap, base × 2^(attempt−1))`). Actual draws depend on the delay used on the previous reconnect.

---

## Architecture Decision Records

- [ADR-0001: Pure Dart Core & Transport Abstraction](doc/adr/0001_initial_scaffold.md)
- [ADR-0002: Injected Clock, Timers, and Random](doc/adr/0002_injected_time_and_random.md)
- [ADR-0003: Decorrelated Jitter as Default Reconnect Policy](doc/adr/0003_decorrelated_jitter_backoff.md)
- [ADR-0004: Bounded Outbound Buffer with Explicit Drop Strategies and TTL](doc/adr/0004_bounded_outbound_buffer.md)
- [ADR-0005: Separate Connection/Session State & Single-Subscription Stream Protection](doc/adr/0005_stream_protection.md)

---

## License

MIT — see [LICENSE](LICENSE).
