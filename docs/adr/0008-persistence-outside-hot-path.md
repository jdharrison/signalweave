# ADR 0008: Keep Persistence Outside the Realtime Hot Path

## Status

Accepted

## Context

The first relay is ephemeral, but later deployments may need snapshots and durable mutations. Database or broker latency must not block room ownership, replication ticks, or fan-out.

## Decision

Information is classified as `Ephemeral`, `Stateful`, or `Durable`. The initial implementation uses in-memory state and a no-op journal behind a small asynchronous `JournalSink`. Realtime workers publish persistence work through a bounded seam and never await storage during fan-out. Future recovery may combine snapshots with journaled durable mutations. Semantic delivery delays use a separate scheduler seam rather than artificial network latency.

## Consequences

- Initial nodes can lose state on restart; durability is not implied.
- Storage engines, brokers, and distributed consensus are not required for the first vertical slice.
- Journal overload and failure need explicit policy, metrics, and eventual recovery semantics.
- The seam permits persistence later without placing storage clients in core routing loops.
