# `resilient_socket` — Milestone 0.1.0 "Core Foundations" Implementation Plan

**Audience:** an autonomous coding agent (Cursor / Claude Code / Antigravity).
**Authority:** this document overrides the agent's own preferences. Where this plan specifies a
signature, formula, constant, file path, or test expectation, the agent implements it VERBATIM.
Deviations are defects.

**Scope:** Milestone 0.1.0 only (Spec v2.0 §7, phases 1–9). NO circuit breaker, NO conflation,
NO auth rotation, NO memory guard, NO TransportFrame refactor — those are 0.2.0+. Building them
now is a scope violation, not initiative.

---

## G0. Global Rules (enforced on every task)

1. **Pure Dart package.** No `package:flutter` import anywhere. CI uses the Dart SDK, not Flutter.
2. **Runtime dependencies: exactly two.** `web_socket_channel`, `clock`. Adding any other runtime
   dependency is forbidden.
3. **Time discipline:** production code NEVER calls `DateTime.now()` or `Stopwatch` directly.
   All wall-clock reads go through `clock.now()` (`package:clock`). All delays/periodics use
   plain `Timer` / `Timer.periodic` / `Future.delayed` — `package:fake_async` fakes both Zone
   timers and `clock`, so tests get virtual time for free.
4. **Randomness discipline:** every random draw goes through an injected `math.Random` with a
   production default of `math.Random()`. Never a module-level `Random` singleton.
5. **Test discipline:** every test that involves time runs inside
   `fakeAsync((async) { ... async.elapse(...); ... })`. Real-time waits (`await Future.delayed`
   outside fakeAsync, `pumpEventQueue` with wall time, `sleep`) are forbidden. Full suite must
   complete in under 8 seconds wall-clock.
6. **Lint discipline:** `analysis_options.yaml` includes `very_good_analysis` with zero
   `// ignore:` and zero `// ignore_for_file:` comments in `lib/`. `public_member_api_docs`
   ENABLED for `lib/` (every public symbol gets real dartdoc, minimum one sentence stating
   behavior, not restating the name).
7. **No TODO/FIXME/HACK comments. No `print`. No `dynamic` in public API** except the two
   documented passthroughs: `Stream<dynamic> messages` and `void send(Object payload, ...)`
   payloads (String or List<int> at this milestone).
8. **Commit discipline:** one conventional commit minimum per task ID below
   (`feat(backoff): ...`, `test(heartbeat): ...`). Never squash the milestone into one commit.
9. **Equality:** value types (`RttSample`, `ReplayProgress`, all `SocketConnectionState`
   subclasses) override `==`, `hashCode`, and `toString` by hand. `package:equatable` is not
   permitted (runtime dep rule).
10. **Definition of done per task:** code + tests written, `dart format --set-exit-if-changed .`
    clean, `dart analyze` zero issues, `dart test` green, coverage not reduced.

---

## T0. Scaffold (paths + exact file contents)

Create:

```
resilient_socket/
├── pubspec.yaml
├── analysis_options.yaml
├── LICENSE                     # MIT, copyright holder: Yusuf Kaan Gürcüoğlu
├── CHANGELOG.md                # "## 0.1.0\n- Initial release: ..." (fill at T9)
├── README.md                   # placeholder header only at T0; written at T9
├── .gitignore                  # dart defaults + coverage/
├── .github/workflows/ci.yaml
├── tool/check_coverage.dart
├── docs/adr/                   # populated at T9
├── example/ticker_cli.dart     # populated at T8
├── lib/resilient_socket.dart
└── lib/src/ ... (files created per task)
```

**pubspec.yaml (verbatim, version numbers are pins of record):**

```yaml
name: resilient_socket
description: >-
  Financial-grade WebSocket resilience for Dart: decorrelated-jitter reconnection,
  adaptive heartbeat with RTT tracking, bounded offline buffering, and automatic
  subscription replay.
version: 0.1.0
repository: https://github.com/Kaangrc/resilient_socket
topics: [websocket, network, resilience, realtime]

environment:
  sdk: ^3.8.0

dependencies:
  clock: ^1.1.1
  web_socket_channel: ^3.0.0

dev_dependencies:
  coverage: ^1.9.0
  fake_async: ^1.3.1
  test: ^1.25.0
  very_good_analysis: ^10.0.0
```

**Resolution guard (T0 acceptance):** run `dart pub get` with the CI-pinned SDK (below). If
`very_good_analysis ^10.0.0` fails to resolve against it, bump the CI SDK pin upward to the
minimum satisfying version — never downgrade the analyzer package, never loosen `environment.sdk`.

**analysis_options.yaml (verbatim):**

```yaml
include: package:very_good_analysis/analysis_options.yaml

analyzer:
  language:
    strict-casts: true
    strict-inference: true
    strict-raw-types: true

linter:
  rules:
    lines_longer_than_80_chars: false
```

**.github/workflows/ci.yaml (verbatim):**

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dart-lang/setup-dart@v1
        with:
          sdk: "3.12.2"   # pin of record; must equal the version used to develop locally
      - run: dart pub get
      - run: dart format --output=none --set-exit-if-changed .
      - run: dart analyze --fatal-infos
      - run: dart test --coverage=coverage
      - run: dart pub global activate coverage
      - run: dart pub global run coverage:format_coverage
             --lcov --in=coverage --out=coverage/lcov.info
             --report-on=lib --check-ignore
      - run: dart tool/check_coverage.dart coverage/lcov.info 95
```

**tool/check_coverage.dart:** reads an lcov file (arg 1), computes total `LH/LF` percentage,
exits 1 with a per-file table if below threshold (arg 2, integer percent). ~40 lines, tested
manually, excluded from coverage reporting (it lives outside `lib/`).

**lib/resilient_socket.dart (barrel — final export list, keep sorted):**

```dart
export 'src/backoff/decorrelated_jitter_backoff.dart';
export 'src/backoff/equal_jitter_backoff.dart';
export 'src/backoff/exponential_backoff.dart';
export 'src/backoff/full_jitter_backoff.dart';
export 'src/backoff/reconnect_policy.dart';
export 'src/buffer/buffered_message.dart';
export 'src/buffer/outbound_buffer_options.dart';
export 'src/connection_state.dart';
export 'src/heartbeat/heartbeat_options.dart';
export 'src/heartbeat/rtt_sample.dart';
export 'src/options.dart';
export 'src/resilient_socket_base.dart';
export 'src/stream_ops/stream_protection.dart';
export 'src/subscription/replay_options.dart';
export 'src/subscription/replay_progress.dart';
export 'src/subscription/subscription_spec.dart';
export 'src/telemetry/composite_metrics_listener.dart';
export 'src/telemetry/socket_metrics_listener.dart';
export 'src/transport/socket_transport.dart';
export 'src/transport/web_socket_channel_transport.dart';
```

(Internal-only files — `outbound_buffer.dart`, `heartbeat_monitor.dart`, `rtt_estimator.dart`,
`subscription_registry.dart`, `replay_coordinator.dart` — are NOT exported. `RttEstimator` and
`RttSample` split so the sample type is public but the estimator is internal.)

---

## T1. Backoff module — `lib/src/backoff/`

### `reconnect_policy.dart`

```dart
abstract interface class ReconnectPolicy {
  /// Delay before reconnect attempt [attempt] (0-indexed: the first retry is attempt 0).
  /// [previousDelay] is the delay actually used for the previous attempt, or null on attempt 0.
  Duration nextDelay(int attempt, Duration? previousDelay);

  /// Clears internal state; the next call behaves as attempt 0.
  void reset();
}
```

### Shared math rules (all four implementations)

- All arithmetic in **microseconds** (`int`), converted once at the end:
  `Duration(microseconds: value)`.
- Exponent overflow guard: `exp = math.min(attempt, 40)` before any `1 << exp` / `pow`.
- `_uniform(int minUs, int maxUs)` helper: `minUs + (_random.nextDouble() * (maxUs - minUs)).floor()`.
  (`maxUs >= minUs` guaranteed by callers; if equal, returns `minUs`.)
- Every constructor: `({required Duration base, required Duration cap, math.Random? random})`
  storing `random ?? math.Random()`. Assert `base > Duration.zero && cap >= base`.

### Exact formulas

| File | Class | `nextDelay(attempt, prev)` returns |
|---|---|---|
| `exponential_backoff.dart` | `ExponentialBackoff` | `min(capUs, baseUs * 2^attempt)` |
| `full_jitter_backoff.dart` | `FullJitterBackoff` | `_uniform(0, min(capUs, baseUs * 2^attempt))` |
| `equal_jitter_backoff.dart` | `EqualJitterBackoff` | `temp = min(capUs, baseUs * 2^attempt); temp ~/ 2 + _uniform(0, temp ~/ 2)` |
| `decorrelated_jitter_backoff.dart` | `DecorrelatedJitterBackoff` | `prevUs = (previousDelay ?? base).inMicroseconds; min(capUs, _uniform(baseUs, prevUs * 3))` |

`DecorrelatedJitterBackoff` additionally keeps `_lastDelay` internally so that when the caller
passes `previousDelay == null` on attempt > 0 (facade always passes it, but the API tolerates
null) it uses its own memory; `reset()` clears `_lastDelay`.

### `test/backoff/` — required asserts (hand-computed, use `_SequencedRandom`)

`test/support/sequenced_random.dart`:

```dart
class SequencedRandom implements math.Random {
  SequencedRandom(this._doubles);
  final List<double> _doubles;
  int _i = 0;
  @override double nextDouble() => _doubles[_i++ % _doubles.length];
  @override int nextInt(int max) => (nextDouble() * max).floor();
  @override bool nextBool() => nextDouble() >= 0.5;
}
```

Mandatory cases:

1. `ExponentialBackoff(base: 100ms, cap: 3200ms)` → attempts 0..6 yield exactly
   `[100, 200, 400, 800, 1600, 3200, 3200]` ms.
2. Overflow: `ExponentialBackoff(base: 1s, cap: 30s).nextDelay(64, null) == 30s` (no wrap/negative).
3. `FullJitterBackoff(base: 100ms, cap: 10s, random: SequencedRandom([0.5]))`, attempt 3:
   temp = 800ms → expect exactly **400ms**. With `[0.0]` → 0ms. With `[0.999...]` → < 800ms.
4. `EqualJitterBackoff` same setup, attempt 3, r=0.5: `400 + 0.5*400` → exactly **600ms**.
   Bounds property (seeded loop, 1,000 draws): result ∈ [temp/2, temp].
5. `DecorrelatedJitterBackoff(base: 250ms, cap: 30s, random: SequencedRandom([0.5, 0.5]))`:
   - attempt 0, prev=null → uniform(250, 750) with r=0.5 → exactly **500ms**;
   - attempt 1, prev=500ms → uniform(250, 1500) with r=0.5 → exactly **875ms**.
6. Decorrelation property (seeded loop): with prev fixed at cap, results never exceed cap.
7. `reset()`: after reset, `DecorrelatedJitterBackoff.nextDelay(0, null)` distribution depends
   on `base` again (repeat case 5 step 1 and expect 500ms).

---

## T2. Transport — `lib/src/transport/` + FakeTransport

### `socket_transport.dart`

```dart
abstract interface class SocketTransport {
  /// Completes when the connection is established; completes with an error on handshake failure.
  Future<void> get ready;

  /// Inbound data exactly as received: String or List<int>. Single-subscription.
  /// Emits done when the connection closes for any reason.
  Stream<dynamic> get incoming;

  /// Sends [data] (String or List<int>). Throws StateError if called before [ready]
  /// completes or after close.
  void send(Object data);

  /// Closes the connection. Idempotent.
  Future<void> close([int code = 1000, String? reason]);

  /// Populated after [incoming] is done, when the underlying channel provides them.
  int? get closeCode;
  String? get closeReason;
}

/// Creates a fresh, unconnected-yet-connecting transport for [uri].
typedef TransportFactory = SocketTransport Function(Uri uri);
```

### `web_socket_channel_transport.dart`

`class WebSocketChannelTransport implements SocketTransport` with
`WebSocketChannelTransport.connect(Uri uri)` mapping: `ready` → `channel.ready`; `incoming` →
`channel.stream`; `send` → `channel.sink.add`; `close` → `channel.sink.close(code, reason)`;
`closeCode/closeReason` → channel getters. This is the ONLY file in `lib/` importing
`web_socket_channel`. Unit tests limited to construction/argument mapping via an injected
channel factory — the real network path is exercised only by `example/ticker_cli.dart`.

### `test/support/fake_transport.dart`

```dart
class FakeTransport implements SocketTransport {
  final sentFrames = <Object>[];            // with FakeSentFrame(at: clock.now(), data) records
  void completeReady();
  void failReady(Object error);
  void emit(Object data);                   // pushes into incoming
  void dropConnection({int? code, String? reason});  // closes incoming with done + sets codes
  bool get closedByClient;                  // close() was called
}

class RecordingTransportFactory {           // returns FakeTransports in scripted order,
  final created = <FakeTransport>[];        // exposes them for assertions
  SocketTransport call(Uri uri);
}
```

Asserts for the fake itself: send-before-ready throws `StateError`; drop closes stream and sets
`closeCode`; frames recorded in order with fake-clock timestamps.

---

## T3. Connection state + facade skeleton

### `connection_state.dart` (sealed, all `final class`, all const-constructible)

```dart
sealed class SocketConnectionState { const SocketConnectionState(); }
final class Connecting extends SocketConnectionState { const Connecting(this.attempt); final int attempt; }
final class Connected extends SocketConnectionState { const Connected({this.lastRtt}); final Duration? lastRtt; }
final class Degraded extends SocketConnectionState { const Degraded(this.rtt); final Duration rtt; }
final class Reconnecting extends SocketConnectionState {
  const Reconnecting({required this.attempt, required this.nextIn});
  final int attempt; final Duration nextIn;
}
final class Suspended extends SocketConnectionState { const Suspended(this.cause); final Object cause; }
final class Disposed extends SocketConnectionState { const Disposed(); }
```

### `options.dart`

```dart
class ResilientSocketOptions {
  ResilientSocketOptions({
    ReconnectPolicy? reconnectPolicy,       // default: DecorrelatedJitterBackoff(base: 250ms, cap: 30s)
    this.maxAttempts,                       // int?; null = retry forever
    this.stabilityThreshold = const Duration(seconds: 30),
    this.heartbeat,                         // HeartbeatOptions?; null = heartbeat disabled
    OutboundBufferOptions? buffer,          // default: OutboundBufferOptions()
    ReplayOptions? replay,                  // default: ReplayOptions()
    SocketMetricsListener? metrics,         // default: const NoopMetricsListener()
    TransportFactory? transportFactory,     // default: WebSocketChannelTransport.connect
  });
}
```

### `resilient_socket_base.dart` — the facade

```dart
class ResilientSocket {
  ResilientSocket(Uri uri, {ResilientSocketOptions? options});

  SocketConnectionState get state;                    // current, synchronous
  Stream<SocketConnectionState> get connectionState;  // broadcast; emits on every transition
  Stream<dynamic> get messages;                       // broadcast; heartbeat pongs filtered OUT
  Stream<RttSample> get rtt;                          // broadcast
  Stream<ReplayProgress> get replayProgress;          // broadcast

  /// Begins connecting. Constructor does NOT auto-connect (testability + explicit lifecycle).
  void connect();

  /// Buffered when not Connected; sent immediately otherwise (subject to flush barriers).
  void send(Object payload, {Duration? ttl, int priority = 0});

  void subscribe(SubscriptionSpec spec);   // registers; sends subscribeMessage now if Connected
  void unsubscribe(String id);             // sends unsubscribeMessage (if any) when Connected; deregisters

  /// Terminal. Transitions to Disposed, closes transport with [code], clears buffer
  /// (reporting BufferDropReason.disposed), cancels all timers. Idempotent.
  Future<void> close([int code = 1000, String? reason]);
}
```

**State machine — the ONLY writer is `void _transition(SocketConnectionState next)`.**
Legal transition table (anything else: `assert(false, 'illegal transition $state -> $next')`
and, in release, ignored + reported via `metrics` as a defect counter... no — keep it strict:
`StateError` in all modes; illegal transitions are bugs, not runtime conditions):

```
(initial)     -> Connecting(0)                    on connect()
Connecting    -> Connected                        transport.ready completes AND (no heartbeat OR first pong)*
Connecting    -> Reconnecting | Suspended         ready fails / drop before Connected
Connected     -> Degraded                         HeartbeatEvent.StaleSuspected
Degraded      -> Connected                        next PongReceived
Connected     -> Reconnecting                     transport incoming done / ConnectionDead
Degraded      -> Reconnecting                     same triggers as Connected
Reconnecting  -> Connecting(n+1)                  backoff timer fires
Reconnecting  -> Suspended                        attempt+1 > maxAttempts
any           -> Disposed                         close()
Suspended     -> Connecting(0)                    connect() called again (manual resume; resets policy)
```

\* For 0.1.0, `Connecting -> Connected` fires on `ready` alone; the pong-gated "probe success"
predicate is a 0.2.0 breaker concern. Do not implement it early.

**Connected-entry sequence (deterministic, in this order):**
1. `_transition(Connected(lastRtt: estimator.latest?.smoothed))`
2. heartbeat.start()
3. replay = coordinator.replay(registry.active, ...) → await
4. if `replay completed && options.replay.flushAfterReplay`: buffer.drain() → send each, paced
   by the same `pacing` interval as replay batches (one message per `pacing` tick is NOT
   required; drain sends in FIFO bursts of `batchSize` with `pacing` between bursts — identical
   pacer implementation, share it).
5. stability timer: after `stabilityThreshold` of uninterrupted Connected/Degraded,
   `policy.reset()` and internal attempt counter = 0. Timer cancelled on any exit from
   Connected/Degraded.

**Reconnect loop:** on entering Reconnecting: `attempt` = current counter;
`delay = policy.nextDelay(attempt, _lastDelay)`; `_lastDelay = delay`;
`metrics.onReconnectScheduled(attempt, delay)`; single-shot `Timer(delay)` → Connecting(attempt+1
counter). Mid-wait `close()` cancels the timer.

Facade tests at T3 (with FakeTransport, no heartbeat/replay yet): every legal transition row;
one illegal transition throws; `close()` idempotence; `connect()` after Suspended resets to
attempt 0; `maxAttempts: 2` → exactly 3 transports created (initial + 2 retries) then Suspended.

---

## T4. Heartbeat + RTT — `lib/src/heartbeat/`

### `rtt_sample.dart`

```dart
final class RttSample {
  const RttSample({required this.raw, required this.smoothed, required this.variance, required this.rto});
  final Duration raw; final Duration smoothed; final Duration variance; final Duration rto;
}
```

### `rtt_estimator.dart` (internal)

```dart
class RttEstimator {
  RttEstimator({
    this.alpha = 0.125,        // 1/8, RFC 6298
    this.beta = 0.25,          // 1/4
    this.minRto = const Duration(milliseconds: 500),
    this.maxRto = const Duration(seconds: 30),
  });
  RttSample addSample(Duration rawRtt);  // returns the new sample; also latest
  RttSample? get latest;
  void reset();                          // on every new connection
}
```

**Formulas (microsecond int math; compute rttvar BEFORE updating srtt):**

- First sample: `srtt = raw; rttvar = raw ~/ 2`.
- Subsequent: `rttvar = ((1-beta)*rttvar + beta*(srtt - raw).abs()).round();`
  `srtt = ((1-alpha)*srtt + alpha*raw).round();`
- Always: `rto = clamp(srtt + 4*rttvar, minRto, maxRto)`.

### `heartbeat_options.dart`

```dart
class HeartbeatOptions {
  const HeartbeatOptions({
    required this.pingBuilder,   // Object Function(int seq)
    required this.pongMatcher,   // bool Function(dynamic message, int seq)
    this.minInterval = const Duration(seconds: 5),
    this.maxInterval = const Duration(seconds: 30),
    this.initialInterval = const Duration(seconds: 15),
    this.staleFactor = 2.0,
    this.maxMisses = 2,
    this.adaptive = true,
  });
}
```

### `heartbeat_monitor.dart` (internal)

```dart
sealed class HeartbeatEvent {}
final class PongReceived extends HeartbeatEvent { final RttSample sample; }
final class StaleSuspected extends HeartbeatEvent { final Duration outstanding; }
final class ConnectionDead extends HeartbeatEvent { final int misses; }

class HeartbeatMonitor {
  HeartbeatMonitor({
    required HeartbeatOptions options,
    required void Function(Object frame) send,
    required void Function(HeartbeatEvent event) onEvent,
    required RttEstimator estimator,
  });
  void start();                    // sends first ping immediately, then schedules
  void stop();                     // cancels timers; safe to call repeatedly
  /// Feed EVERY inbound message. Returns true if consumed as a pong
  /// (facade then filters it from [messages]).
  bool onMessage(dynamic message);
}
```

**Behavior contract:**
- Ping tick: `seq++`; record `_pingSentAt = clock.now()`; `send(pingBuilder(seq))`; arm a
  one-shot stale timer at `staleFactor * currentRto` (currentRto = estimator.latest?.rto ??
  initialInterval). Stale timer firing while pong outstanding → `onEvent(StaleSuspected(...))`
  exactly once per ping.
- `onMessage` where `pongMatcher(msg, seq)` for the CURRENT seq: raw = now − _pingSentAt;
  sample = estimator.addSample(raw); `onEvent(PongReceived(sample))`; miss counter = 0; cancel
  stale timer; reschedule next ping at `_interval()`. Pong matching an OLD seq: return true
  (consume) but ignore for RTT/miss accounting.
- Next ping tick with pong still outstanding: `misses++`; if `misses >= maxMisses` →
  `onEvent(ConnectionDead(misses))` and stop(); else send next ping normally.
- **Adaptive interval formula** (recomputed after every pong; if `adaptive == false`, always
  `initialInterval`):

```
r        = clamp(rttvar_us / srtt_us, 0.0, 1.0)
interval = maxInterval − (maxInterval − minInterval) × r      // linear, in microseconds, floor
```

  Rationale one-liner for dartdoc: high relative variance = unstable network = probe more often.

### `test/heartbeat/` — required asserts

`rtt_estimator_test.dart` (pure math, hand-computed):
1. First sample 100ms → srtt 100ms, rttvar 50ms, rto raw 300ms → clamped to **500ms** (minRto).
2. Second sample 200ms → rttvar = 0.75·50 + 0.25·|100−200| = **62.5ms** (62500µs);
   srtt = 0.875·100 + 0.125·200 = **112.5ms**; rto = 112.5 + 250 = 362.5ms → clamped **500ms**.
3. Third sample 1s → recompute by formula; expect rto now > minRto and equal to the formula
   value (write the constant in the test, computed by hand: rttvar = 0.75·62.5 + 0.25·|112.5−1000|
   = 46.875 + 221.875 = 268.75ms; srtt = 0.875·112.5 + 0.125·1000 = 223.4375ms; rto = 223.4375
   + 1075 = 1298.4375ms → **1298437µs** after rounding rule).
4. Clamp ceiling: samples of 60s drive rto to exactly `maxRto`.
5. `reset()` → next sample treated as first.

`heartbeat_monitor_test.dart` (fakeAsync):
6. start() sends ping seq 1 at t=0 (assert `sentFrames.length == 1` immediately).
7. Pong at t=80ms → PongReceived with raw exactly 80ms.
8. Adaptive: with srtt 112.5ms / rttvar 62.5ms → r = 0.5555…; min 5s max 30s → interval =
   30s − 25s·r = **16111111µs** (floor). Assert next ping fires at that exact virtual offset.
9. Stale: no pong; elapse `staleFactor × rto` → exactly one StaleSuspected.
10. Death: no pongs across 2 ping ticks (maxMisses 2) → ConnectionDead(2), and no further
    frames sent after stop.
11. Old-seq pong: consume (returns true) but estimator.latest unchanged, miss counter unchanged.
12. Non-pong message: `onMessage` returns false, nothing else happens.

Facade integration (extend T3 tests): pong frames never appear on `socket.messages`;
StaleSuspected drives `Degraded`; PongReceived drives Degraded→Connected; ConnectionDead drives
Reconnecting.

---

## T5. Outbound buffer — `lib/src/buffer/`

### Public: `outbound_buffer_options.dart`, `buffered_message.dart`

```dart
enum OverflowStrategy { dropOldest, dropNewest, dropByPriority, throwException }
enum BufferDropReason { ttlExpired, overflow, disposed }

class OutboundBufferOptions {
  const OutboundBufferOptions({
    this.maxMessages = 500,
    this.maxBytes,                          // int?; enforced only when sizeEstimator != null
    this.defaultTtl = const Duration(seconds: 20),
    this.overflow = OverflowStrategy.dropOldest,
    this.sizeEstimator,                     // int Function(Object payload)?
  });
}

final class BufferedMessage {
  const BufferedMessage({required this.payload, required this.enqueuedAt,
                         required this.ttl, required this.priority});
  final Object payload; final DateTime enqueuedAt; final Duration ttl; final int priority;
  bool isExpiredAt(DateTime now) => now.difference(enqueuedAt) > ttl;
}

class BufferOverflowException implements Exception { final int capacity; }
```

### Internal: `outbound_buffer.dart`

```dart
class OutboundBuffer {
  OutboundBuffer(OutboundBufferOptions options,
      {required void Function(BufferDropReason reason, int count) onDrop});
  void enqueue(Object payload, {Duration? ttl, int priority = 0});
  List<BufferedMessage> drain();   // removes+returns all non-expired, FIFO;
                                   // expired ones removed + onDrop(ttlExpired, n) once per drain
  int get length;
  void clear(BufferDropReason reason);   // onDrop(reason, previousLength) if non-empty
}
```

**Overflow algorithm (exact):** on enqueue when `length == maxMessages` (or byte cap exceeded):
- `dropOldest`: remove index 0, `onDrop(overflow, 1)`, then append.
- `dropNewest`: `onDrop(overflow, 1)`, incoming discarded, buffer unchanged.
- `dropByPriority`: find the message with the LOWEST priority (earliest among ties).
  If `incoming.priority > lowest.priority` → evict it, report, append incoming.
  Else → discard incoming, report. (Equal priority does NOT displace — FIFO fairness.)
- `throwException`: throw `BufferOverflowException` (nothing reported — the caller knows).
- Byte cap: when `sizeEstimator != null && maxBytes != null`, evict per the same strategy until
  `bytes + incoming <= maxBytes`; report the aggregate count once.

### `test/buffer/outbound_buffer_test.dart` — required asserts (fakeAsync for TTL)

1. FIFO drain order preserved (enqueue a,b,c → drain [a,b,c]).
2. TTL: enqueue at t=0 with ttl 10s; elapse 10.001s; drain → `[]`, onDrop(ttlExpired, 1).
   Boundary: elapse exactly 10s → NOT expired (strict `>`).
3. Per-message ttl overrides defaultTtl.
4. dropOldest at cap 3: enqueue 1,2,3,4 → contents [2,3,4], onDrop(overflow,1).
5. dropNewest at cap 3: enqueue 1,2,3,4 → contents [1,2,3], onDrop(overflow,1).
6. dropByPriority: cap 3 with priorities [0,5,5]; enqueue p=3 → evicts the p=0; enqueue p=1 into
   [3,5,5] → incoming discarded; enqueue p=5 into [3,5,5] → incoming discarded (no tie
   displacement).
7. throwException: 4th enqueue throws BufferOverflowException; buffer intact.
8. Byte cap: sizeEstimator = String length; maxBytes 10; enqueue "aaaa","bbbb" then "ccccc"(5)
   with dropOldest → evicts "aaaa" AND "bbbb"? bytes 8 + 5 = 13 > 10 → evict "aaaa" (bytes 9 ≤ 10
   ✓) → contents ["bbbb","ccccc"], onDrop(overflow,1).
9. clear(disposed) on 3 messages → onDrop(disposed, 3); on empty → onDrop not called.

---

## T6. Subscriptions + replay — `lib/src/subscription/`

```dart
final class SubscriptionSpec {
  const SubscriptionSpec({required this.id, required this.subscribeMessage,
                          this.unsubscribeMessage, this.priority = 0});
  final String id;
  final Object Function() subscribeMessage;
  final Object Function()? unsubscribeMessage;
  final int priority;                       // lower value = replayed earlier
}

class ReplayOptions {
  const ReplayOptions({this.pacing = const Duration(milliseconds: 100),
                       this.batchSize = 5, this.flushAfterReplay = true});
}

final class ReplayProgress {
  const ReplayProgress({required this.total, required this.sent});
  final int total; final int sent;
  bool get done => sent >= total;
}
```

Internal `subscription_registry.dart`: `register(spec)` (duplicate id → `StateError`),
`unregister(id)` (unknown id → no-op), `List<SubscriptionSpec> get active` sorted by
`(priority ascending, insertion order ascending)` — sort must be stable; keep an insertion
counter, do not rely on `List.sort` stability assumptions.

Internal `replay_coordinator.dart`:

```dart
class ReplayCoordinator {
  ReplayCoordinator(ReplayOptions options);
  /// Sends subscribeMessage() for each spec in order, in bursts of batchSize with
  /// [options.pacing] between bursts. Emits progress after every individual send.
  /// Returns true if completed, false if cancel() was called mid-flight.
  Future<bool> replay({
    required List<SubscriptionSpec> specs,
    required void Function(Object frame) send,
    required void Function(ReplayProgress progress) onProgress,
  });
  void cancel();
}
```

Timing contract: burst 0 sends synchronously at invocation (t=0); burst k at `t = k * pacing`.

### `test/subscription/` — required asserts (fakeAsync)

1. Ordering: priorities [5,0,0,3] with insertion order a,b,c,d → send order **b,c,d,a**.
2. Pacing: 7 specs, batchSize 3, pacing 100ms → frames at t=0: 3, t=100ms: cumulative 6,
   t=200ms: cumulative 7. `replay` future completes true at t=200ms (no trailing wait).
3. Progress: emitted 7 times, `sent` = 1..7, final `done == true`.
4. cancel() at t=150ms (after 6 sent) → no 7th frame ever, future resolves **false**, no timer leak
   (`async.pendingTimers` empty — assert via elapsing far ahead and checking counts).
5. Registry: duplicate id register throws StateError; unregister unknown id silent.
6. Facade integration: `subscribe()` while Connected sends immediately AND registers (present in
   next replay); drop → reconnect → replay resends both; mid-replay drop → next Connected
   restarts replay from spec 0 with NO duplicate within a single replay run.
7. Flush barrier: 2 buffered sends + 4 subscriptions, flushAfterReplay=true → FakeTransport frame
   order is [sub×4 ..., buffered×2]; with false → buffered frames may precede (assert they are
   sent at Connected-entry step 3′ before replay completes).

---

## T7. Stream ops + telemetry

### `stream_ops/stream_protection.dart`

```dart
extension StreamProtection<T> on Stream<T> {
  Stream<T> throttleLatest(Duration window);
  Stream<T> debounceQuiet(Duration quiet);
  Stream<T> conflate(Duration window, {T Function(T previous, T next)? merge});
  Stream<T> sampleEvery(Duration period);
}
```

Exact semantics (single-subscription in/out; cancel propagates upstream; NO timers pending
after cancel; source `done` flushes any held value then closes):

- `throttleLatest`: first event emits immediately and opens a window; events inside the window
  overwrite a `pending` slot; window end: if pending → emit it and open a new window, else idle.
- `debounceQuiet`: (re)arm a timer per event; emit last value after `quiet` with no events.
- `conflate`: first event arms a window timer and seeds `acc`; subsequent events
  `acc = merge?.call(acc, next) ?? next`; timer fire → emit acc, disarm (next event re-arms).
- `sampleEvery`: periodic timer from first event; each tick emits latest unseen value, if any;
  ticker cancels when source done.

Required asserts (fakeAsync timing tables — one per operator):
1. throttleLatest(100ms), events at t=0(a), 30(b), 60(c), 130(d): emissions → a@0, c@100, d@200.
2. debounceQuiet(50ms), events t=0(a), 30(b), 100(c): emissions → b@80, c@150.
3. conflate(100ms, merge: (p,n)=>p+n) over ints 1@0, 2@40, 3@90 → **6@100**; next 4@250 → 4@350.
4. sampleEvery(100ms), events 1@10, 2@50, 3@120: emissions → 2@100, 3@200; nothing at 300.
5. Leak: cancel each subscription mid-window; `async.elapse(10s)`; assert no emissions and no
   pending timers.
6. done-flush: source closes at t=30 in case 1 with pending b → b emitted, stream closed.

### `telemetry/`

```dart
abstract interface class SocketMetricsListener {
  void onConnectAttempt(int attempt);
  void onConnected(Duration handshakeTime);
  void onDisconnected(Object? cause, Duration sessionUptime);
  void onReconnectScheduled(int attempt, Duration delay);
  void onRttSample(RttSample sample);
  void onHeartbeatMiss(int consecutiveMisses);
  void onBufferDrop(BufferDropReason reason, int droppedCount);
  void onReplayCompleted(int subscriptions, Duration took);
  void onMessage({required bool inbound, required int sizeBytes});
}
class NoopMetricsListener implements SocketMetricsListener { const NoopMetricsListener(); /* empty bodies */ }
class CompositeMetricsListener implements SocketMetricsListener {
  CompositeMetricsListener(List<SocketMetricsListener> listeners);  // fan-out, isolation:
}                                                                    // one throwing listener must not break others
```

`sizeBytes`: String → `utf8.encode(s).length` computed lazily ONLY if any non-noop listener is
attached; `List<int>` → `.length`; other → 0.

Asserts: RecordingListener captures the full lifecycle event sequence for the T8 integration
scenario (exact ordered list below); composite fan-out order; composite swallows a listener
throw (wrapped in try/catch per call) and still calls the next.

---

## T8. Facade integration test + CLI example

### `test/resilient_socket_test.dart` — the lifecycle scenario (single fakeAsync block)

Script (FakeTransport factory, DecorrelatedJitterBackoff with SequencedRandom([0.5]), heartbeat
min 5s / max 30s / initial 15s, buffer defaults, replay pacing 100ms batch 5, RecordingListener):

1. `connect()` → state sequence starts [Connecting(0)]; transport[0] created.
2. `subscribe(specA(priority 0))`, `subscribe(specB(priority 1))` while Connecting → nothing sent.
3. `send('early')` → buffered.
4. `completeReady()` → Connected; replay sends A,B at t=0 burst; then buffered 'early' flushes
   after replay. Assert transport[0].sentFrames order: [subA, subB, 'early', ping(1)... ] —
   heartbeat starts before replay per Connected-entry sequence, so ping(1) is actually FIRST:
   **exact expected order: [ping1, subA, subB, 'early']**. (This ordering is the documented
   contract; the test freezes it.)
5. `emit(pongFor(1))` at +80ms → RttSample raw 80ms on `socket.rtt`; `messages` did NOT emit it.
6. `emit('tick')` → `messages` emits 'tick'.
7. `dropConnection(code: 1006)` at t=1s → Reconnecting(attempt 0, nextIn 500ms) [decorrelated:
   uniform(250, 750)@r=0.5 = 500ms]; onDisconnected(cause, uptime 1s).
8. elapse 500ms → Connecting(1); transport[1]; completeReady() → replay replays A and B on
   transport[1]; no duplicate 'early' (buffer already drained).
9. `unsubscribe('A')` → unsubscribe frame sent; drop + reconnect again → replay sends only B.
10. `close()` → Disposed; transport[2].closedByClient true; further `send()` throws StateError;
    `connect()` after close throws StateError (Disposed is terminal — unlike Suspended).
11. RecordingListener sequence assert (exact, by event name):
    [connectAttempt(0), connected, rttSample, message(in), reconnectScheduled(0, 500ms),
     connectAttempt(1), connected, replayCompleted(2), ..., disconnected(..)] — write the full
    expected list in the test; any reorder is a failure.
12. maxAttempts scenario (separate test): maxAttempts 2, all transports failReady →
    states end [..., Reconnecting, Suspended]; exactly 3 transports created; `connect()` again →
    Connecting(0) and policy.reset() was invoked (verify via a spy policy).

### `example/ticker_cli.dart`

Pure Dart CLI (~80 lines): consts `binanceUri` (`wss://stream.binance.com:9443/ws/btcusdt@trade`)
and `btcturkUri` (`wss://ws-feed-pro.btcturk.com`); `--exchange`, `--verbose` flags; builds a
ResilientSocket with a `stdout` metrics listener, one SubscriptionSpec per exchange protocol,
prints state transitions and throttled (500ms `throttleLatest`) tickers. Exists to prove the
library against real endpoints; NOT covered by unit tests; excluded from coverage.

---

## T9. Docs, ADRs, README, release hygiene

- `docs/adr/0001..0005` — write per Spec v2.0 §1 texts (context/decision/consequences, ≤ 1 page each).
- README order: CI badge + pub badge placeholder → gap matrix (v1 row set) → 30-line quickstart
  (compiles verbatim against the real API — copy from a `dart analyze`-checked snippet) →
  "Guarantees" section where each bullet is a test name from T8 → strategy docs table (the four
  backoff formulas with the attempt 0–7 delay table) → ADR links. Forbidden in README: any claim
  without a symbol+test, the words "blazingly", "production release", emoji walls.
- CHANGELOG 0.1.0 entry listing modules factually.
- Run `dart pub publish --dry-run` → zero warnings. Run pana locally; record score in the PR
  description; target ≥ 150/160 (perfect score usually needs an example/ + docs, already present).

---

## tasks.md (place at repo root; the agent maintains checkboxes; VERBATIM content below)

```markdown
# Milestone 0.1.0 — Core Foundations

Rules: work top-to-bottom. A task is done only when its Acceptance line is true.
Never start a task while a previous task's Acceptance is false. One conventional
commit minimum per task. No scope from milestone 0.2+ (breaker/conflation/auth/
memory/frames).

- [x] T0 Scaffold
      Acceptance: `dart pub get` succeeds on CI-pinned SDK; CI workflow green on
      empty lib (format+analyze+test pass with a placeholder test); LICENSE=MIT;
      barrel file compiles with commented-out exports.
- [x] T1 Backoff (4 policies + SequencedRandom)
      Acceptance: test/backoff/* green incl. hand-computed cases
      [100,200,400,800,1600,3200,3200], FullJitter 400ms, EqualJitter 600ms,
      Decorrelated 500ms→875ms; overflow guard test green.
- [x] T2 Transport interface + WebSocketChannelTransport + FakeTransport
      Acceptance: fake_transport_test green (send-before-ready throws; drop sets
      closeCode; frame recording ordered).
- [x] T3 Connection state + facade skeleton + reconnect loop
      Acceptance: all legal-transition tests green; illegal transition throws
      StateError; maxAttempts=2 creates exactly 3 transports then Suspended;
      close() idempotent.
- [x] T4 RttEstimator + HeartbeatMonitor + facade wiring
      Acceptance: estimator hand-math tests green (500ms clamp; 62.5ms rttvar;
      1298437µs rto case); adaptive interval fires at 16111111µs; stale→Degraded;
      2 misses→Reconnecting; pongs filtered from messages.
- [x] T5 OutboundBuffer
      Acceptance: all 9 buffer asserts green incl. strict-> TTL boundary and
      byte-cap eviction case.
- [x] T6 Registry + ReplayCoordinator + flush barrier
      Acceptance: order test b,c,d,a; pacing 0/100/200ms timeline; cancel leaves
      zero pending timers; flush-after-replay frame order [subs..., buffered...].
- [x] T7 Stream ops + telemetry
      Acceptance: 4 timing-table tests + leak test + done-flush green; composite
      isolates a throwing listener.
- [x] T8 Full lifecycle integration test + ticker_cli example
      Acceptance: lifecycle test green incl. exact frame order
      [ping1, subA, subB, 'early'] and exact telemetry sequence; example compiles
      (`dart analyze example/` clean); manual run against one public feed logged
      in PR description.
- [x] T9 ADRs 0001–0005, README, CHANGELOG, coverage gate
      Acceptance: CI green with coverage ≥95% (tool/check_coverage.dart);
      `dart pub publish --dry-run` zero warnings; README quickstart snippet
      compiles; every README guarantee names an existing test.
```

---

## Appendix A — Forbidden-shortcut list (agent self-check before every commit)

- [ ] No `// ignore` anywhere in lib/.
- [ ] No `DateTime.now()` / `Stopwatch` in lib/ (grep must return zero).
- [ ] No `Future.delayed` in test/ outside fakeAsync bodies.
- [ ] No new dependencies beyond pubspec above.
- [ ] No skipped/`solo` tests committed.
- [ ] No public symbol without dartdoc.
- [ ] No weakening of a hand-computed expected value to "closeTo/greaterThan" —
      exact `equals` on microsecond integers is the contract; if the number is wrong,
      the code is wrong, not the test.
```