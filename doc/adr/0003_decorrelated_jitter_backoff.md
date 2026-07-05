# ADR-0003: Decorrelated Jitter as Default Reconnect Policy

## Status
Accepted

## Context
When a server experiences an outage or network blip, thousands of connected clients may simultaneously drop their connections. If clients attempt reconnection using fixed intervals or simple exponential backoff (`base * 2^attempt`), they generate synchronized waves of reconnect traffic—a phenomenon known as the **thundering herd problem**. Even adding simple randomized jitter often fails to disperse reconnect spikes evenly across time when multiple attempts occur in succession.

## Decision
We select **Decorrelated Jitter Backoff** (based on AWS architecture recommendations) as the default `reconnectPolicy` in `ResilientSocketOptions`. The algorithm computes each consecutive attempt's delay dynamically using the previous delay:
```
nextDelay = uniform(base, min(cap, prev * 3))
```
Where `uniform(min, max)` calculates a random integer microsecond delay between `min` and `max`.

To support varied application requirements, the library also provides three additional `ReconnectPolicy` implementations:
- **Exponential Backoff**: Power-of-two growth (`delay = min(cap, base * 2^attempt)`).
- **Equal Jitter Backoff**: Half fixed, half uniform jitter (`delay = (exp / 2) + uniform(0, exp / 2)`).
- **Full Jitter Backoff**: Uniform random up to exponential (`delay = uniform(0, exp)`).

## Consequences
### Positive
- **Thundering Herd Mitigation**: Decorrelated jitter provides the most optimal distribution of reconnect attempts across time, maximizing server recovery capacity during outages.
- **Adaptive Spacing**: Because the upper bound depends on `prev * 3`, delays naturally decorrelate without growing monotonically too quickly or locking clients out during brief transients.
- **Interoperability**: Users can swap policies via the `ReconnectPolicy` contract without changing socket lifecycle code.

### Negative / Trade-offs
- Delays are non-monotonic by design; a client might sleep 2 seconds on attempt 3 and then 1 second on attempt 4. Applications expecting strictly increasing delays must opt into `ExponentialBackoff` or `EqualJitterBackoff`.
