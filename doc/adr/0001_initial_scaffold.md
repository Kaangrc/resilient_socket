# ADR-0001: Pure Dart Core & Transport Abstraction

## Status
Accepted

## Context
Many Dart/Flutter networking libraries tightly couple their domain logic and state machines to specific UI frameworks (such as `package:flutter`) or specific underlying transport protocols (such as `dart:io` `WebSocket` or browser `WebSocket` primitives). This coupling creates several critical problems:
- It prevents the library from running in backend Dart CI environments, pure server-side command-line tools, or web contexts without Flutter dependencies.
- It makes deterministic, simulated time testing impossible because network I/O cannot be easily replaced with an in-memory test double without opening real socket ports.
- It locks the codebase into a single protocol implementation, preventing future adaptations for alternative transports (such as custom framing, TCP sockets, or HTTP fallbacks).

## Decision
We mandate a **pure Dart package architecture** (`resilient_socket`) with zero imports of `package:flutter`. Furthermore:
1. All network interactions must pass through an abstract `SocketTransport` interface (`send()`, `completeReady()`, `dropConnection()`, `incoming`, `closeReason`).
2. The default live adapter (`WebSocketChannelTransport`) wraps the standard `web_socket_channel` package, which serves as one of the only two allowed runtime dependencies (alongside `clock`).
3. Domain logic (state transitions, buffering, heartbeat, subscription replay) is implemented entirely against `SocketTransport` and never accesses underlying OS or browser socket primitives directly.

## Consequences
### Positive
- **Universal Portability**: The library compiles and runs cleanly across mobile (iOS/Android), desktop, web, and server-side Dart VMs.
- **Testability**: The `SocketTransport` abstraction allows creating a fully synchronous, in-memory `FakeTransport` for testing without network latency, flakiness, or port binding.
- **Protocol Flexibility**: Additional transport layers can be implemented without modifying the core resilience engine.

### Negative / Trade-offs
- Requires implementing adapter boilerplate around external networking libraries (`web_socket_channel`).
- Transport-specific low-level socket options (like TCP keep-alive or custom SSL verification) must be abstracted or configured prior to transport injection.
