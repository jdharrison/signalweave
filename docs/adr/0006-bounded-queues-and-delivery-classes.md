# ADR 0006: Require Bounded Queues and Semantic Delivery Classes

## Status

Accepted

## Context

Unbounded fan-out and slow consumers can turn transient load into process-wide memory growth. Different messages also have different value under delay: a join event must arrive, while an old transform should be replaced.

## Decision

Every connection, worker, inference provider, and persistence seam will have an explicit capacity and overflow policy. The core supports `ReliableOrdered`, `ReliableUnordered`, `LatestValue`, `UnreliableSequenced`, and `BestEffortEvent`. Replaceable values coalesce by recipient and state key; stale sequences are rejected. Saturation replaces stale state first, then drops low-priority transient events, and disconnects persistently slow consumers before critical queues can grow without bound.

## Consequences

- Memory use and overload behavior are bounded and measurable.
- “Reliable” does not mean infinite retention; overload can terminate a connection.
- Capacity, replacement, drop, and disconnect metrics are required.
- Correctness depends on explicit sequencing and careful classification of every message family.
