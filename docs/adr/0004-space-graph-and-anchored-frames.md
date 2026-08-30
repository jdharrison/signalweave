# ADR 0004: Model Spaces as a Graph with Anchored Local Frames

## Status

Accepted

## Context

Sessions may contain systems, planets, ships, interiors, compartments, and non-spatial topics. A flat room model or universe-scale floating-point frame would create precision, fan-out, and representation problems. Participants may also subscribe to several scopes simultaneously.

## Decision

Each session will own a graph of spatial and logical spaces. Spatial spaces use local coordinates and may be anchored to an entity in a parent space. Parent spaces receive coarse proxies rather than every child-space entity. Routing is selected per space (`BroadcastAll`, spatial grid, or `TopicOnly`). Cross-space transitions are explicit and sequenced; space epochs reject packets for destroyed or recreated spaces.

## Consequences

- Nested simulations remain localized and can use suitable precision and update rates.
- Interest management scales by scope instead of flattening all entities into one index.
- Coordinate transforms, proxy synchronization, transitions, and epoch handling need dedicated tests.
- Cross-space queries are explicit operations rather than implicit global-coordinate lookups.
