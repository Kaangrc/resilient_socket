# ADR-0004: Bounded Outbound Buffer with Explicit Drop Strategies and TTL

## Status
Accepted

## Context
During network disconnections or reconnect loops, applications continue generating outbound messages (such as user actions, telemetry, or state synchronizations). An unbounded in-memory queue will eventually consume all available memory if the outage persists, causing application crashes (Out-Of-Memory errors). Conversely, discarding all messages immediately upon disconnection causes data loss for transient, sub-second network blips. Furthermore, stale messages (such as real-time player positions or high-frequency stock tickers) lose their value rapidly and should not be sent after an extended delay.

## Decision
We implement a **bounded `OutboundBuffer`** governed by strict capacity constraints and explicit overflow strategies:
1. **Dual Bounding**: The buffer enforces limits by both message count (`maxMessages`, default 500) and byte size (`maxBytes`, estimated via an injectable `sizeEstimator`).
2. **Explicit Overflow Strategies**: When capacity is exceeded, the buffer resolves pressure according to the configured `OverflowStrategy`:
   - `dropOldest` (default): Discards the oldest queued message to make room for new data.
   - `dropNewest`: Rejects incoming messages while retaining existing queued data.
   - `dropByPriority`: Discards the lowest-priority queued message (breaking ties via FIFO fairness: oldest among lowest priority is dropped first).
   - `throwException`: Throws a `BufferOverflowException` immediately, leaving the buffer intact.
3. **Time-To-Live (TTL) Expiration**: Messages carry an optional per-message `ttl` (overriding `defaultTtl`, default 20s). Expired messages are purged automatically during queue drainage before transmission.
4. **Telemetry Visibility**: Every drop event (whether `ttlExpired`, `overflow`, or `disposed`) triggers an explicit `onBufferDrop` telemetry notification.

## Consequences
### Positive
- **Guaranteed Memory Safety**: The buffer cannot grow indefinitely during prolonged network outages.
- **Priority Awareness**: Critical payloads (like payment confirmations or subscription handshakes) can survive overflow events over low-priority background events.
- **Freshness Guarantee**: TTL expiration ensures stale real-time data is never transmitted across newly restored connections.

### Negative / Trade-offs
- Calculating byte capacity requires invoking `sizeEstimator` during enqueueing, adding minor cpu overhead for large string or binary payloads.
- Developers must choose appropriate priorities and TTLs for their domain payloads to get the full benefit of intelligent eviction.
