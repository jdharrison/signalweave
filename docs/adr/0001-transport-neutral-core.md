# ADR 0001: Keep the Core Transport-Neutral

## Status

Accepted

## Context

Woven must expose the same session, routing, ownership, sequencing, and backpressure semantics to browser, Unity, and native clients over transports with different capabilities. Coupling those rules to sockets or a transport library would duplicate behavior and make conformance difficult.

## Decision

The realtime core will consume authenticated, typed commands and emit typed delivery intents through transport-independent interfaces. Connection identity, capability negotiation, framing I/O, and mapping delivery intents onto streams or datagrams belong in transport adapters. Core domain code will not depend on Axum, Quinn, WebTransport, or socket types.

## Consequences

- Core behavior can be tested deterministically without network I/O.
- All adapters share authorization, routing, sequencing, and queue policy.
- Transport-specific optimization requires explicit capabilities rather than leaking into domain logic.
- Adapter conformance tests are required to prove equivalent application semantics.
