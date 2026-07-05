# Changelog

## 0.1.0

### Core Foundations & Architecture
- **Pure Dart Architecture**: Zero framework coupling (`package:flutter` is strictly prohibited). Runs cleanly across mobile, desktop, server, and web.
- **Injected Time & Randomness**: Strict time discipline via `package:clock` and injected `math.Random`, enabling 100% deterministic simulated time testing under `fake_async`.
- **Transport Abstraction**: Abstract `SocketTransport` contract with built-in `WebSocketChannelTransport` adapter wrapping `web_socket_channel`. Includes synchronous in-memory `FakeTransport` for testing.

### Reconnection Backoff Engine (`lib/src/backoff/`)
- **Decorrelated Jitter Backoff**: AWS-recommended formula (`nextDelay = uniform(base, min(cap, prev * 3))`) mitigating thundering herd problems during server outages.
- **Equal Jitter Backoff**: Guaranteed minimum sleep time (`(exp / 2) + uniform(0, exp / 2)`).
- **Full Jitter Backoff**: Maximum dispersion across zero-to-ceiling (`uniform(0, exp)`).
- **Exponential Backoff**: Standard deterministic doubling (`min(cap, base * 2^attempt)`).

### Connection State Machine (`lib/src/`)
- **Sealed State Hierarchy**: Exhaustive pattern-matching states inheriting from `SocketConnectionState`:
  - `Connecting(attempt)`
  - `Connected(lastRtt)`
  - `Degraded(rtt)`
  - `Reconnecting(attempt, nextIn)`
  - `Suspended(cause)`
  - `Disposed()`
- Hand-computed equality (`==`), `hashCode`, and descriptive `toString()` implementations.

### RTT Tracking & Heartbeat Monitor (`lib/src/heartbeat/`)
- **TCP RTT Smoothing Accumulator**: Microsecond-precision integer arithmetic tracking smoothed RTT (`srtt`) and RTT variance (`rttvar`) with RTO clamping between `minRto` (500ms) and `maxRto` (30s).
- **Adaptive Heartbeat**: Dynamic ping scheduling based on smoothed RTT (`interval = max(srtt * 4, minInterval)`), configurable static intervals, and stale connection detection (`StaleSuspected` -> `Degraded`, `ConnectionDead` -> `Reconnecting`).

### Bounded Outbound Buffer (`lib/src/buffer/`)
- **Dual-Capacity Constraints**: Enforces maximum limits by message count (`maxMessages`) and byte size (`maxBytes`, via custom `sizeEstimator`).
- **Overflow Eviction Strategies**: Configurable strategies for resolving queue pressure: `dropOldest`, `dropNewest`, `dropByPriority`, and `throwException`.
- **Per-Message TTL Expiration**: Automatic purging of expired real-time payloads before wire transmission.

### Subscription Registry & Replay Coordinator (`lib/src/subscription/`)
- **Session State Separation**: Decoupled physical connection lifecycle from persistent application subscriptions.
- **Flushing Barrier & Paced Replay**: Raises a flush barrier upon connection, replaying registered subscriptions in strict priority order pacted in configurable batches (`batchSize`, `pacing`) before buffered messages drain.

### Stream Protection Operators (`lib/src/stream_ops/`)
- **Reactive Backpressure Operators**: Extension on `Stream<T>` providing single-subscription, leak-free operators:
  - `throttleLatest(window)`
  - `debounceQuiet(quiet)`
  - `conflate(window, merge)`
  - `sampleEvery(period)`
- **Zero Lints**: Encapsulated subscription lifecycle management via `_ProtectionController` without `// ignore` comments.

### Telemetry Infrastructure (`lib/src/telemetry/`)
- **Comprehensive Lifecycle Observability**: `SocketMetricsListener` hooks for connection attempts, handshakes, RTT sampling, buffer drops, heartbeats, replay timing, and traffic sizing.
- **Isolated Composite Fan-Out**: `CompositeMetricsListener` guarantees that exceptions in child listeners never disrupt sibling observability or core socket execution.

### Documentation & Verification
- Comprehensive Architecture Decision Records (`doc/adr/0001` through `0005`).
- Standalone CLI prover (`example/ticker_cli.dart`) demonstrating live ticker updates from Binance or BtcTurk with `throttleLatest` and `stdout` telemetry.
- ≥95% line coverage enforced by `tool/check_coverage.dart` with zero static analysis lints under `very_good_analysis`.
