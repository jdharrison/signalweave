# Signalweave Status

Signalweave is feature-complete for its own scope: a transport-neutral realtime core, a
versioned FlatBuffers wire protocol, three interchangeable realtime transports, spatial
interest routing with a load runner, and an optional adjacent inference plane. It is
designed to be self-hosted standalone, the way you'd self-host Redis or Postgres, with no
dependency on any hosted control plane or console.

## What's implemented

**Core** (`signalweave-core`) — validated typed IDs, explicit namespace/session/space/channel
grants, server-provisioned bounded sessions, nested anchored spaces with epoch tombstones,
entities and ownership, subscriptions, server-controlled delivery/persistence policy,
monotonic sequencing, bounded in-memory state, rate and payload limits, priority-aware
bounded/coalescing queues, stale-queue purging, immediate slow-consumer cleanup, a bounded
journal outbox with a no-op sink, and a deterministic worker harness.

**Protocol** (`signalweave-protocol`) — the full v1 metadata envelope and typed control
messages, including inference/tool-call lifecycle messages, a pinned vendored FlatBuffers
compiler, verifier-backed bounded decoding, semantic validation, and checked-in golden
fixtures proving byte-for-byte cross-language stability.

**Realtime transports** — an Axum control plane (`signalweave-server`) exposing
`/healthz`, `/readyz`, `/metrics`, and `/v1/capabilities`; a bounded single-owner Tokio
worker and protocol bridge shared by every adapter (`signalweave-transport`); binary
WebSocket (`signalweave-transport-websocket`, the universal baseline); native QUIC
(`signalweave-transport-quic`) and browser WebTransport
(`signalweave-transport-webtransport`), both mapping unreliable/best-effort delivery to
datagrams under a conservative packet budget. Real-socket conformance coverage exists for
all three, and clients negotiate/observe available transports through `/v1/capabilities`.

**Interest management** (`signalweave-core` + `signalweave-loadtest`) — bounded 2D/3D
spatial grid routing for replaceable state, with owner-updated positions, cell indexes,
radius filtering, optional exact distance checks, and reliable-event bypass; a bounded
local load runner for broadcast, topic, 2D-grid, and 3D-grid scenarios reporting measured
publish latency percentiles, delivery counts, queue effects, and machine metadata.

**Inference plane** (`signalweave-inference-*`) — an optional, adjacent plane, disabled by
default, adding no dependency to the core or protocol crates beyond twelve additive wire
message kinds. A coordinator (`signalweave-inference-coordinator`) runs each AI identity as
an ordinary authenticated core connection, holding a bounded per-request provider queue. A
provider-neutral capability/request model and `Provider` trait live in
`signalweave-inference-core`. A deterministic tool-call gateway
(`signalweave-inference-tools`) lets model output propose state changes without ever
mutating state directly — the model proposes, the gateway decides. A deterministic scripted
provider (`signalweave-inference-test-provider`) exercises the full path — an AI
conversation, a read-only tool call, and rejection of a stale state-changing proposal —
with no paid service required.

**Reference clients** — a native Rust client (`signalweave-client-rust`) used as the
integration-test driver, and generated TypeScript FlatBuffers bindings
(`signalweave-client-ts`) with Node scripts validating decode against both a live server
frame and checked-in golden fixtures.

See [`docs/adr`](adr) for the architecture decisions behind these choices, and
[`AGENTS.md`](../AGENTS.md) for exact public APIs.

## Out of scope for this repo

Cloud deployment, orchestration, and any hosted console/control-panel UI are deliberately
kept out of this repository so the core stays agnostic and self-hostable on its own.
Signalweave exposes what an external orchestrator needs — health/readiness endpoints,
`/v1/capabilities`, and bounded, explicit resource configuration — without assuming one
exists. Domain-specific consumer examples (a game namespace, a portfolio site, etc.) are
likewise left to consuming projects: no game rules, physics, or domain logic belong in
`signalweave-core` or `signalweave-protocol`.
