# ADR-0002: Injected Clock, Timers, and Random

## Status
Accepted

## Context
Resilient network connections rely heavily on time-dependent operations: backoff delays, heartbeat ping/pong intervals, RTT calculation, stream debouncing/throttling, and TTL message expiration. In standard implementations, code often calls `DateTime.now()`, `Stopwatch()`, `Future.delayed()`, or `Timer()` directly. 
When testing timing-critical algorithms (such as ensuring backoff jitter is decorrelated over hours, or verifying reconnect timers do not leak after socket disposal), real-time delays make test suites slow, flaky, and non-deterministic.

## Decision
We enforce strict **time discipline** across the entire package:
1. **No direct time primitives**: Direct calls to `DateTime.now()`, `Stopwatch()`, and `Timer()` are strictly forbidden in `lib/`.
2. **Clock Injection**: All timestamp generation and duration calculations must use the injected `clock` object from `package:clock` (the second allowed runtime dependency).
3. **Randomness Injection**: All randomized backoff algorithms must accept an injectable `math.Random` instance (defaulting to `math.Random()`). In test environments, a deterministic `SequencedRandom` or fixed seed is injected.
4. **fake_async Compatibility**: All timers and asynchronous delays must be compatible with `package:fake_async`, allowing tests to simulate hours of network timeouts and reconnect loops instantly and deterministically.

## Consequences
### Positive
- **100% Deterministic Testing**: Complex multi-stage reconnect sequences, heartbeat timeouts, and TTL expirations can be verified with microsecond precision in zero wall-clock time.
- **Zero Flakiness**: No timing race conditions or CI failures caused by slow host machines.
- **Microsecond Precision**: Enforces strict mathematical verification of RTT accumulators and backoff formulas.

### Negative / Trade-offs
- Developers must maintain strict discipline never to use `Future.delayed` or `DateTime.now()` directly in domain logic.
- Requires custom helper abstractions (like `_ProtectionController`) to manage timer subscriptions without breaking static analysis or linting rules.
