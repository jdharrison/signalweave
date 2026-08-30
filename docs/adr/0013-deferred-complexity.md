# ADR 0013: Deliberately Defer Unproven Complexity

## Status

Accepted

## Context

Signalweave has broad eventual requirements, but its initial load, topology, and inference usage are unknown. Premature distributed infrastructure or low-level optimization would increase risk without measured benefit.

## Decision

The first vertical slice will defer raw UDP, QUIC and WebTransport until the WebSocket path works, databases and brokers, distributed consensus and automatic sharding, GKE, persistent GPUs, unsafe code, lock-free structures, custom allocators, application ECS or physics, legacy wire compatibility, autonomous-agent frameworks, vector databases, and elaborate long-term memory. Concurrency claims and performance changes require configurable benchmarks and observed profiles. Deferred features must retain explicit interfaces or migration seams only where a near-term requirement justifies them.

## Consequences

- The initial system stays small, testable, and locally runnable.
- No deferred capability may be described as implemented or production-ready.
- Some later requirements may require migrations rather than activating prebuilt scaffolding.
- Complexity is introduced only with evidence, an owner, acceptance criteria, and cost impact.
