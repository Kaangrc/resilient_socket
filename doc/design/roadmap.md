> **Note**: This document represents the long-term architectural roadmap. Milestone 0.1.0 strictly implements the core protocol-agnostic features detailed in Section 1.

# `resilient_socket` — Package Specification v2.0

**Supersedes:** v1.0. All v1 modules (backoff matrix, adaptive heartbeat/RTT, outbound buffer,
subscription replay, stream ops, telemetry) remain in force; v2 adds five capabilities and
tightens the internal frame model. v2 features are **additive** — no v1 public API breaks.

**Positioning:** A pure-Dart, transport-agnostic WebSocket resilience layer engineered to the
standards of JVM Netty / resilience4j, RSocket resumption semantics, and exchange feed-handler
conflation queues — for fintech-grade mobile streaming where reconnect storms, UI flooding,
token expiry, and OOM-during-outage are the four primary production failure modes.

**Documentation integrity contract (unchanged from v1, strengthened in v2):** every README claim maps to a
public symbol; every symbol maps to a test file. Two claims are *explicitly renounced* in docs
because they are not achievable in Dart/WebSocket and must not be implied:
(1) "zero-GC binary path" → we promise **zero-copy discipline**, not zero allocation;
(2) "standard mid-session WS re-auth" → no such thing exists in RFC 6455; we promise
**protocol-hook re-auth OR make-before-break handover**, documented explicitly.

---

## 0. Ecosystem Research Digest (what the best teams do, and what we port)

| Source ecosystem | Pattern | What we port to Dart |
|---|---|---|
| resilience4j / Hystrix (Java) | CircuitBreaker with failure-rate window, Open→HalfOpen probes, slow-call detection | `CircuitBreaker` state machine gating the reconnect loop; failure *classification* so transient network errors never trip it |
| Netty (Java) | `IdleStateHandler` (read/write idle events), pooled `ByteBuf`, pipeline of handlers | Idle-detection folded into HeartbeatMonitor; frame pipeline via `FrameCodec`; pooling replaced with view-based zero-copy (Dart has no off-heap arenas — documented) |
| RSocket | Resumption (session survives transport swap), lease-based flow control | Make-Before-Break token rotation = resumption-style transport swap under a stable session; lease idea informs replay pacing |
| gRPC keepalive spec | Strict ping rate limits (servers punish aggressive pings: GOAWAY) | Heartbeat `minInterval` floor + doc warning; server-punishment close codes feed the breaker |
| LMAX / exchange feed handlers | Conflating queue: key-latest + delta folding, never unbounded fan-out to UI | `KeyedConflator` + `DepthDeltaFolder` (§3.2) |
| Go (gobwas/ws, nhooyr) | Explicit frame types, `[]byte` end-to-end, no implicit string decode | Sealed `TransportFrame` (Text/Binary), binary never touches String (§3.4) |
| Android/iOS lifecycle | `onTrimMemory` / memory pressure callbacks | `MemoryPressureSource` abstraction + escalation ladder (§3.5) |

---

## 1. Architectural Decision Records (full set; 0001–0005 carried from v1)

- **ADR-0001** Pure Dart core; transport behind `SocketTransport`; default adapter wraps
  `web_socket_channel` (sole runtime dep).
- **ADR-0002** Injected clock, timers, and `Random` everywhere. `fake_async`-compatible.
- **ADR-0003** Decorrelated jitter as default reconnect policy.
- **ADR-0004** Bounded buffer with explicit, reported drop policy.
- **ADR-0005** Connection state machine ≠ session state (SubscriptionRegistry) — separate machines.
- **ADR-0006 — Circuit breaker is a gate around the reconnect loop, not a replacement for backoff.**
  Backoff answers "how long until the next try"; the breaker answers "are tries allowed at all".
  They compose: `Breaker(allow?) → Backoff(delay) → connect()`. Rationale: merging them (as many
  naive libs do) either destroys jitter properties or turns every network blip into a lockout.
- **ADR-0007 — Failure classification is the breaker's input, not raw error count.**
  `FailureClassifier` maps `(closeCode, handshakeStatus, exception)` → `transient | hard | fatal`.
  Only `hard` feeds the failure window. `fatal` (e.g. HTTP 401 on handshake with a non-refreshable
  credential) trips immediately to `Open` and surfaces `Suspended(cause: AuthRejected)` — retrying
  a dead API key wastes client battery without recovery benefit.
- **ADR-0008 — Conflation folds, it does not drop.** Window-based conflation without a fold
  function silently loses financial data (volume deltas). The engine is keyed: latest-wins is
  just `fold = (prev, next) => next`, so "drop" becomes a degenerate case of "fold", not the design.
- **ADR-0009 — Token rotation = protocol hook first, make-before-break second, hard reconnect last.**
  Strategy chain tried in order of user configuration. Does not assume a standard WS re-auth frame.
- **ADR-0010 — Frames are sealed and binary-first internally.** All internal paths carry
  `TransportFrame`; `String` is a codec concern at the edges. Binary payloads are `Uint8List`
  views, never eagerly copied or decoded.
- **ADR-0011 — Memory pressure is an injected signal, not a polled heuristic.** Pure Dart cannot
  observe heap pressure portably. Core consumes an abstract `MemoryPressureSource`; a Flutter
  adapter (support library, not core) bridges `didHaveMemoryPressure`. A byte-accounting
  *soft guard* inside the buffer works even with no source attached.

---

## 2. Public API Surface v2 (delta on v1 — one screen)

```dart
final socket = ResilientSocket(
  Uri.parse('wss://stream.example.com/ws'),
  options: ResilientSocketOptions(
    // ── v1 (unchanged) ──
    reconnectPolicy: DecorrelatedJitterBackoff(base: ..., cap: ...),
    heartbeat: HeartbeatOptions.adaptive(...),
    buffer: OutboundBufferOptions(...),
    replay: ReplayOptions(...),
    metrics: myMetricsListener,

    // ── v2 additions ──
    circuitBreaker: CircuitBreakerOptions(
      failureWindow: 10,                       // sliding window of classified attempts
      hardFailureThreshold: 5,                 // hard failures in window → trip
      openCooldown: Duration(seconds: 30),
      halfOpenProbes: 1,
      classifier: const DefaultFailureClassifier(),
    ),
    auth: SessionAuthOptions(
      tokenProvider: myTokenProvider,          // Stream<AuthToken> + current
      strategy: RotationStrategy.chain([
        InBandRefresh(refreshFrameBuilder: ..., ackMatcher: ...),
        MakeBeforeBreak(overlap: Duration(seconds: 5)),
      ]),
      // connectHeaders / connectUri may derive from current token:
      handshake: (token) => HandshakeSpec(uri: ..., headers: {...}),
    ),
    memoryGuard: MemoryGuardOptions(
      source: myPressureSource,                // optional; Flutter adapter or custom
      ladder: EscalationLadder.standard(),
    ),
  ),
);

// Binary-first message access (v1 `messages` kept as convenience view):
socket.frames;                                  // Stream<TransportFrame> (sealed Text/Binary)
socket.typedMessages(myProtobufCodec);          // Stream<T> via FrameCodec<T>
socket.sendFrame(BinaryFrame(bytes), ttl: ...); // byte path, no string round-trip

// Conflation engine (replaces naive conflate for keyed data):
socket.frames
  .decode(depthCodec)                           // Stream<DepthDelta>
  .conflateKeyed(
    window: Duration(milliseconds: 50),
    key: (d) => d.priceLevel,
    fold: DepthDeltaFolder.sumVolumes,          // Stream<DepthDelta> folded
  );

socket.breakerState;                            // Stream<CircuitBreakerState> (sealed)
socket.rotateNow();                             // force a rotation cycle (returns Future<RotationReport>)
```

New sealed states:

```dart
sealed class CircuitBreakerState {}
class BreakerClosed   extends CircuitBreakerState { final int recentHardFailures; }
class BreakerOpen     extends CircuitBreakerState { final Duration remainingCooldown; final Object cause; }
class BreakerHalfOpen extends CircuitBreakerState { final int probesRemaining; }
```

`SocketConnectionState` (v1) gains one member: `class WaitingBreaker extends SocketConnectionState
{ final Duration remaining; }` — the UI can display "server unavailable, retrying in 24s"
instead of a lying spinner.

---

## 3. Capability Specifications (v2 modules in full depth)

### 3.1 ⚡ Circuit Breaker — Adaptive Fail-Fast (`breaker/`)

**Problem:** an infinite jittered reconnect loop against a dead or actively-rejecting server is
(a) battery drain, (b) radio wake-lock abuse, (c) load amplification on a struggling backend —
the client-side half of a self-inflicted DDoS. Backoff alone never says "stop".

**State machine (resilience4j semantics, mobile-tuned):**

```
Closed ──(hard failures ≥ threshold within window)──▶ Open
Open ──(cooldown elapsed)──▶ HalfOpen
HalfOpen ──(probe succeeds)──▶ Closed (window reset)
HalfOpen ──(probe hard-fails)──▶ Open (cooldown × openBackoffFactor, capped)
any ──(fatal classification)──▶ Open + surface Suspended(cause) to the app
```

**Failure classification (`FailureClassifier`)** — the design's core, per ADR-0007:

| Signal | Class | Rationale |
|---|---|---|
| Socket exception, DNS fail, timeout while radio state unknown | `transient` | Almost always the phone, not the server; backoff handles it; breaker ignores it |
| Handshake HTTP 500/502/503, WS close 1011 (internal error), 1013 (try again later) | `hard` | Server is up but broken/overloaded — exactly what breakers exist for |
| WS close 1008 (policy violation), 4000–4099 vendor auth codes, handshake 401/403 | `hard`, and `fatal` if the token provider reports no fresher credential | Retrying rejected credentials is noise; if a refresh is possible, rotation runs first |
| Server GOAWAY-style codes after ping abuse (per gRPC keepalive guidance) | `hard` + telemetry flag `serverPunishedKeepalive` | Signals misconfigured heartbeat floor |

**Composition with backoff (ADR-0006):** reconnect loop asks `breaker.allowAttempt()`. Denied →
transition to `WaitingBreaker(remaining)` and sleep the cooldown (fake-async timer), *not* the
backoff delay. Allowed → normal jittered path. Successful stable connection
(`resetAfterStable`) informs both: backoff attempt counter resets AND breaker window clears.

**Half-open discipline:** exactly `halfOpenProbes` connection attempts admitted; all other
triggers queue. Probe success = handshake + first heartbeat pong (a connect that dies in 2s is
not a recovery — this "success definition" is a documented, tested predicate).

**Telemetry:** `onBreakerTripped(cause, window)`, `onBreakerProbe(result)`, `onBreakerReset()`.

**Tests:** window arithmetic; classification table (each row is a test case); transient storms
never trip; fatal trips immediately; cooldown escalation factor + cap; half-open probe
accounting; probe "success = pong" predicate; `WaitingBreaker` emitted with correct countdown;
interplay test — breaker Open while user calls `send()` → messages buffer, nothing hits transport.

---

### 3.2 📊 Order-Book Conflation & Delta Folding (`conflation/`)

**Problem:** 1,000+ depth updates/sec during volatility; naive window-drop loses volume deltas
(financially wrong); no conflation melts the UI thread. Exchange feed handlers solve this with
a *conflating queue*: per-key latest/folded value, drained on a cadence.

**Core engine:**

```dart
class KeyedConflator<K, V> {
  KeyedConflator({
    required Duration window,               // drain cadence (e.g. 50ms ≈ 20fps data, UI at 60fps unharmed)
    required K Function(V) key,
    required V Function(V previous, V next) fold,
    int? maxPendingKeys,                    // guard: pathological key cardinality (see §3.5 tie-in)
    bool emitEmptyWindows = false,
  });
  Stream<List<V>> bind(Stream<V> source);   // drains as batches, insertion-ordered by first-touch
}
```

Semantics:
- Incoming `V` → `map[key] = contains(key) ? fold(map[key], v) : v`. O(1) per message.
- A single injected periodic timer drains the map as one batch. **One timer total**, not one per
  key — timer-per-key is the classic accidental O(n) that kills exactly the hot path this exists for.
- `fold` is where correctness lives. Shipped folders:

```dart
abstract final class DepthDeltaFolder {
  /// Sums volume deltas at a price level; keeps max(sequence).
  static DepthDelta sumVolumes(DepthDelta prev, DepthDelta next);
  /// Snapshot semantics: absolute quantity replace; keeps max(sequence).
  static DepthDelta lastQuantity(DepthDelta prev, DepthDelta next);
}
```

- **Sequence preservation:** folded output carries `firstSeq` and `lastSeq` of its constituents.
  Downstream gap detection (`lastSeq + 1 == next.firstSeq`) keeps working — folding must never
  mask a missed packet. This is the difference between a conflation engine and a data shredder;
  it gets its own doc section and test group.
- Zero-quantity levels: `sumVolumes` folding to qty 0 emits the zero (level removal is
  information), does not silently delete the key mid-window.
- `Stream<List<V>>` batch output: one UI rebuild per window, not one per key.
- v1 operators (`throttleLatest`, `conflate`) remain for unkeyed streams; docs position them as
  the scalar case (`key = const, fold = lastWins`).

**Tests (all fake-async):** fold arithmetic tables; per-window batch boundaries at exact tick
edges; sequence-range stitching; zero-qty emission; maxPendingKeys overflow behavior + telemetry;
1-timer invariant (no timer leak under 10k keys); cancel mid-window drains nothing and leaks nothing.

---

### 3.3 🔒 Dynamic Handshake & Mid-Session Token Rotation (`auth/`)

**Reality check (in docs, verbatim):** RFC 6455 has no re-authentication frame. Anyone claiming
universal "seamless mid-session re-auth" is describing either (a) an application-protocol feature
the server must support, or (b) a connection swap hidden well. Both are implemented, documented explicitly,
as a strategy chain.

**Building blocks:**

```dart
abstract interface class TokenProvider {
  AuthToken get current;                     // never blocks
  Stream<AuthToken> get rotations;           // emits when a fresh token exists
  Future<AuthToken?> refresh();              // active refresh; null = cannot refresh (feeds `fatal`)
}

class HandshakeSpec { final Uri uri; final Map<String, String> headers; }
typedef HandshakeBuilder = HandshakeSpec Function(AuthToken token);
```

**Strategy 1 — `InBandRefresh` (protocol hook):** for servers exposing an auth op (common on
exchange private streams):

```dart
InBandRefresh({
  required Object Function(AuthToken) refreshFrameBuilder,
  required bool Function(TransportFrame, AuthToken) ackMatcher,
  Duration ackTimeout = const Duration(seconds: 5),
});
```

Send refresh frame → await ack (timeout ⇒ strategy failure, chain falls through). Zero
interruption; stream never blinks. Rotation report: `RotationOutcome.inBand`.

**Strategy 2 — `MakeBeforeBreak` (RSocket-resumption-inspired transport swap):**

Sequence, fully specced because it is the package's signature move:
1. Rotation trigger (provider emits / expiry horizon < `rotateAhead` / `rotateNow()`).
2. Open **transport B** with `handshake(newToken)` while transport A stays live.
3. Run replay coordinator against B (paced, per v1 §3.4) — subscriptions established on B while
   A still delivers data. Duplicate-delivery window begins.
4. **Atomic swap** at a barrier: outbound sink flips to B; inbound merge enters dedupe mode —
   per-subscription `lastSeq` from A gates B's stream (`seq ≤ lastSeqA` dropped). Sequence
   numbers are how the overlap is de-duplicated; protocols without seq get a documented
   "overlap may duplicate; make handlers idempotent" caveat instead of a silent lie.
5. Drain A for `overlap` duration (late in-flight frames), then close A with 1000.
6. Failure anywhere before the swap ⇒ B is closed, A untouched, outcome
   `RotationOutcome.failedHarmless` — the golden property: **a failed rotation must never
   degrade the existing session.**

**Strategy 3 — `HardReconnect` (implicit last resort):** standard drop-and-reconnect through the
normal resilience stack (buffer catches sends, replay restores session). Outcome: `hardReconnect`.

**Expiry-driven scheduling:** `rotateAhead` (default 60s) triggers rotation *before* the server
kills the socket with an auth close — proactive beats reactive; the reactive path (auth close
code arrives anyway) routes through classifier → refresh → reconnect with new token.

**Tests:** in-band ack happy path / timeout fallback; MBB full sequence under fake time with
scripted dual FakeTransports; dedupe-gate arithmetic; failed-B-harmless invariant; rotation
during `Reconnecting` (defers, single flight); `rotateNow()` idempotence while one is in flight;
provider `refresh() == null` escalates to breaker `fatal`.

---

### 3.4 📦 Binary Transport Optimization (`transport/` + `codec/`)

**Reality check (in docs):** Dart has no Netty-style pooled off-heap buffers; `Uint8List` is
GC-managed. The achievable, measurable promise is **zero-copy discipline**: no
byte↔String round-trips, no defensive copies, view-based slicing, encode-once buffering.

**Frame model (ADR-0010):**

```dart
sealed class TransportFrame {}
final class TextFrame extends TransportFrame { final String text; }
final class BinaryFrame extends TransportFrame {
  final Uint8List bytes;                     // by contract a view, never a copy
  int get lengthInBytes => bytes.lengthInBytes;
}
```

Discipline rules (each is a lint-able code-review rule and a documented guarantee):
1. `SocketTransport` yields `TransportFrame` — the dart:io adapter wraps incoming
   `List<int>` as `Uint8List.view` when possible (it usually already is one), never `List.from`.
2. Binary frames are **never** UTF-8 decoded anywhere in core. `socket.messages` (the v1
   convenience `Stream<dynamic>`) passes `Uint8List` through untouched.
3. Codec boundary is the only transform point:

```dart
abstract interface class FrameCodec<T> {
  T decode(TransportFrame frame);            // protobuf/flatbuffers impls live in userland/examples
  TransportFrame encode(T message);
}
```
   FlatBuffers note in docs: FB decode is inherently zero-copy (accessor over bytes) — the codec
   returns a lazy view object; the pipeline never forces materialization.
4. **Encode-once buffering:** `OutboundBuffer` stores `TransportFrame` (already-encoded bytes),
   not domain objects — flush after a 5-minute outage re-sends bytes, it does not re-run 500
   protobuf encodes in one frame drop (§3.5 byte-accounting also becomes exact for free).
5. Heartbeat on binary protocols: `pingBuilder`/`pongMatcher` (v1) generalize to frame-typed
   variants; matcher must be allocation-light (prefix check on bytes, not full decode).
6. GC-spike guidance doc: batch drains (§3.2) amortize allocation; avoid per-message closures in
   hot paths; measurements section with a microbenchmark harness (`benchmark/` dir, not shipped
   in package score path).

**Tests:** view-not-copy assertions (`identical` / `buffer` identity where the platform allows);
no-decode invariant (a BinaryFrame with invalid UTF-8 must traverse the full pipeline without
throwing); codec round-trips; buffer stores frames encode-once (encoder call-count spy);
byte-length accounting exactness.

---

### 3.5 🧠 Memory-Pressure Guards (`memory/`)

**Problem:** a long outage + generous buffer caps + big frames = OOM kill. The OS gives Flutter
apps a pressure signal; pure Dart core can't see it portably (ADR-0011) → inject it.

```dart
abstract interface class MemoryPressureSource {
  Stream<MemoryPressureLevel> get levels;    // normal | elevated | critical
}
```

Adapters (support code, keeping core pure):
- `FlutterMemoryPressureSource` — bridges `WidgetsBindingObserver.didHaveMemoryPressure`
  (arrives as one-shot "critical-ish" events; adapter adds decay back to `normal` after a
  configurable quiet period). Ships in `example/flutter_ticker/` or a `resilient_socket_flutter`
  companion — **not** in core deps.
- `ManualMemoryPressureSource` — for tests and server-side Dart.

**Escalation ladder (`EscalationLadder`)** — declarative, applied atomically to the buffer:

| Level | maxMessages | maxBytes | Overflow strategy | TTL |
|---|---|---|---|---|
| normal | configured | configured | configured | configured |
| elevated | ×0.5 | ×0.5 | forced `dropByPriority` | ×0.5 |
| critical | keep only `priority ≥ high` | ×0.1 | `dropByPriority` | ×0.25 |

Rules:
- Escalation applies **immediately** to existing contents: entering `elevated` evicts (with
  telemetry `onBufferDrop(reason: memoryPressure, count)`) until within the tightened caps —
  a guard that only constrains *future* growth is theater.
- De-escalation restores caps but never resurrects evicted messages (documented).
- Soft guard with no source attached: `maxBytes` accounting (exact, thanks to §3.4 encode-once)
  still enforces the configured ceiling — the ladder is enhancement, not prerequisite.
- Conflation tie-in: `KeyedConflator.maxPendingKeys` subscribes to the same source; `critical`
  halves pending-key budget (a pathological symbol flood is also memory).
- Telemetry: `onMemoryPressure(level)`, drops tagged with reason.

**Tests:** ladder table (each cell); immediate-eviction on escalation; priority survival at
critical; de-escalation no-resurrection; decay timer in Flutter-adapter semantics (tested via
Manual source + fake time); soft-guard-only mode; conflator budget coupling.

---

## 4. Competitive Gap Matrix v2 (README material)

| Capability | web_socket_channel | web_socket_client | adapter_websocket | **resilient_socket v2** |
|---|---|---|---|---|
| Auto-reconnect + jitter matrix (Full/Equal/Decorrelated) | ❌ | partial (no jitter) | partial (no jitter) | ✅ |
| Adaptive heartbeat + RTT/RTO stream | ❌ | ❌ | static interval | ✅ |
| Predictive stale detection | ❌ | ❌ | missed-pong only | ✅ RTO-based |
| **Circuit breaker w/ failure classification** | ❌ | ❌ | ❌ | ✅ Closed/Open/HalfOpen, fatal fast-path |
| Offline buffer (TTL, priority, byte caps) | ❌ | ❌ | ❌ | ✅ encode-once frames |
| **Memory-pressure escalation ladder** | ❌ | ❌ | ❌ | ✅ injected source + soft guard |
| Subscription replay (paced, rate-limit aware) | ❌ | ❌ | ❌ | ✅ + replay progress stream |
| **Mid-session token rotation** | ❌ | ❌ | ❌ | ✅ in-band hook + make-before-break swap |
| **Keyed conflation with delta folding + seq preservation** | ❌ | ❌ | ❌ | ✅ |
| Binary-first sealed frame model, zero-copy discipline | ❌ (raw dynamic) | ❌ | ❌ | ✅ + FrameCodec<T> |
| Telemetry hook | ❌ | ❌ | log stream | ✅ 13-event listener |
| Virtual-time test kit | ❌ | ❌ | mock adapter | ✅ FakeTransport + SequencedRandom + ManualPressure |

---

## 5. Repository Blueprint v2 (delta on v1 tree)

```
lib/src/
├── (v1 dirs unchanged: backoff/ heartbeat/ buffer/ subscription/ stream_ops/ telemetry/ transport/)
├── frames/
│   └── transport_frame.dart          # sealed TransportFrame / TextFrame / BinaryFrame
├── codec/
│   └── frame_codec.dart              # FrameCodec<T> interface (+ JsonStringCodec convenience)
├── breaker/
│   ├── circuit_breaker.dart          # state machine, sliding window
│   ├── circuit_breaker_state.dart    # sealed states
│   ├── failure_classifier.dart       # interface + DefaultFailureClassifier (the table in §3.1)
│   └── circuit_breaker_options.dart
├── conflation/
│   ├── keyed_conflator.dart
│   ├── depth_delta.dart              # value type: priceLevel, qtyDelta/absQty, firstSeq, lastSeq
│   └── depth_delta_folder.dart
├── auth/
│   ├── token_provider.dart
│   ├── handshake_spec.dart
│   ├── rotation_strategy.dart        # interface + RotationStrategy.chain
│   ├── in_band_refresh.dart
│   ├── make_before_break.dart        # the transport-swap coordinator
│   └── rotation_report.dart
└── memory/
    ├── memory_pressure_source.dart   # interface + ManualMemoryPressureSource
    ├── escalation_ladder.dart
    └── memory_guard.dart             # binds source → buffer/conflator budgets

test/                                  # mirrors 1:1; support/ gains ManualMemoryPressureSource
docs/adr/0006..0011-*.md
benchmark/                             # conflator throughput + codec allocation harness (excluded from pana path)
example/
├── ticker_cli.dart                    # v1, now demonstrates breaker + rotation via flags
└── flutter_ticker/                    # phase-gated: order-book screen driven by KeyedConflator
```

Facade (`resilient_socket_base.dart`) remains the single `_transition()` writer; v2 wires two
new inputs into it (breaker gate, rotation coordinator) — the state chart in its header comment
is updated and kept exhaustive. That file is the primary integration reference for maintainers.

---

## 6. Test Blueprint v2 (coverage boundaries)

Everything on `fake_async`; zero wall-clock waits; suite budget stays < 8s.

- Coverage gate raised: **≥95% line, 100% of `lib/src/breaker`, `conflation`, `auth`, `memory`**
  (new modules ship at full coverage or don't ship — they are the selling point).
- New integration scenarios in `resilient_socket_test.dart` (each is a named README guarantee):
  1. *Breaker storm*: 6 scripted hard handshake failures → Open → `WaitingBreaker` surfaced →
     cooldown → HalfOpen probe succeeds (handshake+pong) → Closed, replay runs, buffer flushes.
  2. *Fatal auth*: 401 + `refresh() == null` → immediate Open + `Suspended(AuthRejected)`;
     no further transport attempts recorded by FakeTransport.
  3. *Volatility flood*: 5,000 scripted DepthDeltas across 200 keys in 300ms virtual time →
     assert ≤ 6 drained batches, fold sums exact, seq ranges contiguous.
  4. *Seamless rotation*: MBB across FakeTransport A/B; assert zero gap in delivered seqs,
     duplicates gated, A closed with 1000 after overlap, sink flipped atomically.
  5. *Rotation failure harmless*: B handshake fails → A stream statistically identical
     (frame-for-frame) to a no-rotation control run.
  6. *Pressure during outage*: disconnect → buffer fills to cap → `critical` → eviction to
     high-priority only + telemetry drops → reconnect → replay → surviving frames flush in order.
  7. *Binary integrity*: invalid-UTF8 BinaryFrame traverses buffer→flush→FakeTransport
     byte-identical (checksum), zero decode attempts.

- Property-style tests (seeded loops, not a fuzzing dep): decorrelated jitter bounds across
  10k draws; conflator fold-sum == naive-sum invariant across random delta streams; dedupe gate
  never passes `seq ≤ lastSeqA` across random overlap schedules.

---

## 7. Build Order (phase gates; each ends with green CI)

**Milestone 0.1.0 — v1 core (phases 1–9 from Spec v1.0, unchanged, MUST complete first):**
scaffold → backoff → transport+FakeTransport → facade state machine → heartbeat/RTT → buffer →
replay → stream ops+telemetry → CLI example, ADRs 1–5, pana ≥ 150, publish 0.1.0.

**Milestone 0.2.0 — frames + breaker:**
10. `frames/` + `codec/` refactor (internal paths to TransportFrame; `messages` kept as view;
    encode-once buffer). Gate: binary-integrity test #7 green.
11. `breaker/` module + classifier table tests + integration #1–#2. ADR-0006/0007. Publish 0.2.0.

**Milestone 0.3.0 — conflation:**
12. `conflation/` engine + folders + flood test #3 + benchmark harness. ADR-0008. Publish 0.3.0.

**Milestone 0.4.0 — auth rotation:**
13. `auth/` providers + InBandRefresh.
14. MakeBeforeBreak coordinator + dedupe gate + integration #4–#5. ADR-0009. Publish 0.4.0.

**Milestone 0.5.0 — memory + polish:**
15. `memory/` ladder + guard + integration #6, conflator budget coupling. ADR-0011.
16. README v2 (gap matrix, guarantees-from-test-names), dartdoc pass, pana 160 target,
    `flutter_ticker` order-book example. Publish 0.5.0 and open the "1.0 hardening" milestone
    (real-world soak against two production WebSocket endpoints before any 1.0 tag).

Commit discipline unchanged: conventional commits per module; history must remain auditable.
No milestone starts before the previous one's CI badge, coverage gate, and pana score are green.
