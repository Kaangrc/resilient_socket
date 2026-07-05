# ADR-0005: Separate Connection/Session State & Single-Subscription Stream Protection

## Status
Accepted

## Context
A robust WebSocket client must manage two distinct lifecycles:
1. **The physical transport lifecycle**: Socket opening, TCP/TLS handshaking, heartbeat monitoring, and reconnection backoff.
2. **The application session lifecycle**: Persistent channel subscriptions (`SUBSCRIBE` / `UNSUBSCRIBE` messages) and buffered payload transmissions.

If these state machines are conflated into a single monolithic controller, reconnecting requires re-executing complex application logic, and race conditions arise between sending data and re-establishing server handshakes. Additionally, high-frequency inbound WebSocket streams (such as financial tickers or multiplayer coordinates) can overwhelm UI rendering pipelines or downstream state managers if consumed without backpressure or rate-limiting.

## Decision
We enforce a **strict separation of concerns** between connection management and session replay, supplemented by built-in stream protection operators:
1. **Separation of State Machines**:
   - `SocketConnectionState` (`Connecting`, `Connected`, `Degraded`, `Reconnecting`, `Suspended`, `Disposed`) governs purely physical transport and heartbeat health.
   - `SubscriptionRegistry` and `ReplayCoordinator` maintain session state independently. When a transport transitions to `Connected`, a **flushing barrier** is raised (`_isFlushingBarrier = true`). During the barrier, user `send()` calls are buffered while the coordinator replays registered subscriptions in strict priority order, paced in batches (`batchSize`, `pacing`). The buffer drains only after replay completes.
2. **Single-Subscription Stream Protection Operators**:
   - We provide domain-specific stream operators on `Stream<T>` (`throttleLatest`, `debounceQuiet`, `conflate`, `sampleEvery`).
   - Every operator enforces **single-subscription semantics** (throwing `StateError` on secondary listen attempts) and guarantees zero timer leaks by cleaning up scheduled timers immediately upon subscription cancellation or stream completion.
   - To satisfy static analysis and linter rules (`cancel_subscriptions`) without using `// ignore` comments, subscription lifecycle management is encapsulated in a dedicated `_ProtectionController` helper.

## Consequences
### Positive
- **Race-Free Reconnection**: Raising a flush barrier during subscription replay guarantees that server-side channel state is fully restored before queued user messages hit the wire.
- **Predictable Telemetry & Health**: The physical transport state machine cleanly reports network degradation (`Degraded(rtt)`) without tangling with application subscription IDs.
- **Resource Safety**: Single-subscription stream operators prevent accidental fan-out bugs and memory leaks in reactive downstream consumers.

### Negative / Trade-offs
- Replaying large subscription tables with conservative pacing delays delays the resumption of standard buffered message traffic after a reconnect.
- Requiring single-subscription semantics means consumers desiring broadcast fan-out of throttled streams must explicitly apply `.asBroadcastStream()` downstream.
