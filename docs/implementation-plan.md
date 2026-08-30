# Signalweave Implementation Plan

## Scope and status

This plan tracks the staged implementation requested by the bootstrap prompt. Status terms are used precisely:

- **Completed**: work was performed and verified in the current repository.
- **Initialized**: a reviewed direction or artifact now exists, but the encompassing milestone is not complete.
- **Deferred**: no implementation is claimed; work remains for a later change.

**Milestones 0 and 1 are completed locally.** The repository now has a runnable Rust workspace, baseline CI and developer configuration, 13 accepted ADRs, a bounded transport-neutral core, and a verified FlatBuffers protocol crate. Network transports and runtime endpoints begin in Milestone 2.

## Discovered assumptions and constraints

- The repository starts nearly empty: one initial commit on `main`, with only `.gitignore`, `LICENSE`, `README.md`, and the supplied `docs/bootstrap.md` visible at discovery.
- `session-server-nodejs.zip` is absent, so no legacy implementation was inspected and no compatibility behavior is inferred beyond `docs/bootstrap.md`.
- Rust stable is `1.98.0`; Cargo `1.98.0`, rustfmt, and Clippy are available.
- Node.js `22.23.2`, npm `10.9.8`, and Docker `29.7.2` are available. Docker will not be required for ordinary Rust development.
- A system `flatc`, .NET, Terraform, and OpenTofu are absent. Rust protocol generation uses a pinned vendored FlatBuffers 25.12.19 compiler; C# execution and infrastructure validation remain deferred until their toolchains are available.
- No cloud resources, IAM, secrets, DNS, deployments, or other cloud state were inspected or mutated during initialization.
- Performance, scale, compatibility, deployment success, and cost figures remain unmeasured until their stated validation work is run.

## Staged milestones

### Milestone 0 — Discovery and initialization

**Status: Completed.**

Completed: repository and local-tool discovery, legacy ZIP check, 13 architecture decisions, this staged plan, the root Rust workspace, stable toolchain and formatting/lint policy, baseline GitHub Actions validation, non-secret environment examples, and the project README. Root formatting, lint, test, and documentation commands pass locally.

### Milestone 1 — Core vertical slice

**Status: Completed.**

Implemented the smallest useful transport-neutral core and protocol boundary. The core includes validated typed IDs, explicit namespace/session/space/channel grants, server-provisioned bounded sessions, nested anchored spaces with epoch tombstones, entities and ownership, subscriptions, server-controlled delivery/persistence policy, monotonic sequencing, bounded in-memory state, rate and payload limits, priority-aware bounded/coalescing queues, stale-queue purging, immediate slow-consumer cleanup for reliable saturation, a bounded journal outbox with no-op asynchronous sink, and a deterministic worker harness. The protocol includes the full v1 metadata envelope and typed controls, a pinned vendored FlatBuffers compiler, verifier-backed bounded decoding, semantic validation, and a checked-in Rust golden fixture. Tests cover authorization and isolation, sequencing, ownership, nested-space epoch reuse, state coalescing, capacity/rate saturation, malformed protocol input, and transport-loss cleanup.

### Milestone 2 — Universal realtime path

**Status: Completed.**

Implemented the Axum health/capability control plane, bounded single-owner Tokio core worker, binary WebSocket handshake, Rust reference client, and generated TypeScript FlatBuffers bindings. The core now provides bounded entity lifecycle details and atomic, epoch-validated entity transitions. Real in-process TCP tests cover public control-plane endpoints, authentication, namespace isolation, nested subscriptions, ownership rejection, malformed-frame rejection, reliable fan-out, latest-value coalescing, scoped snapshots, transition ordering, and disconnect presence cleanup. The TypeScript Node test starts the Rust server and decodes its live `ProtocolError` WebSocket frame. C# remains deferred because .NET is unavailable.

### Milestone 3 — Interest management and measurement

**Status: Completed.**

Implemented bounded per-session entity-to-cell and cell-to-entity indexes for uniform 2D and 3D grids. Entity owners can update validated local positions; position updates only mutate the index when a cell boundary is crossed. Replaceable traffic uses configured grid radius with optional exact squared-distance filtering, while critical reliable traffic bypasses spatial filtering. The `signalweave-loadtest` crate runs bounded local broadcast, topic, 2D-grid, and 3D-grid scenarios and reports measured publish latency percentiles, delivery counts, queue effects, and local machine metadata. Focused tests cover 2D/3D routing, dimensional validation, radius behavior, reliable bypass, and all load-runner policies. Parent-space coarse proxies and distance-based representation/update-frequency policy remain consumer-specific extensions: defining aggregate payloads or fidelity tiers in the generic core would introduce domain logic. A future protocol position-update control can expose the existing core API to live clients without changing routing semantics.

### Milestone 4 — QUIC and WebTransport

**Status: Completed.**

The Quinn adapter now maps `UnreliableSequenced` and `BestEffortEvent` delivery classes to QUIC unreliable datagrams when they fit within the connection's conservative datagram budget, falling back to silent drop when they do not. The same mapping is implemented for the WebTransport adapter. Both adapters read inbound datagrams after authentication and decode them through the shared protocol codec. Real in-process UDP socket conformance tests cover the full handshake, reliable event fan-out, disconnect presence cleanup, and datagram round-trip for both transports. The development server continues to start all three transports and advertise them in capabilities.

### Milestone 5 — Minimal inference plane

**Status: Completed.**

Added an optional, adjacent inference plane that never enters `signalweave-core` or `signalweave-protocol` internals beyond twelve additive wire message kinds (`InferenceRequested`…`InferenceExpired`, `ToolCallProposed`…`ToolCallCompleted`, MessageKind 21–32). The coordinator (`signalweave-inference-coordinator`) runs one AI identity as an ordinary authenticated core connection, holds a bounded per-request-queue (`tokio::sync::Semaphore`), and drives its provider (`signalweave-inference-core::Provider`) and tool gateway (`signalweave-inference-tools`, ADR 0010) without any core or protocol-crate changes; two small generic additions to `signalweave-transport` (`WorkerHandle::broadcast_to_space`, `WorkerHandle::send_to_connection`) reuse the existing entity-lifecycle fan-out plumbing to deliver these messages. The deterministic fake provider (`signalweave-inference-test-provider`) drives three demonstrations with no paid service: a full AI conversation round trip, a read-only diagnostic tool call, and rejection of a deliberately stale state-changing tool proposal (checked via a per-tool revision counter, not a new core `AuthorityPolicy`). The plane is disabled by default (`ServerConfig::inference_enabled = false`, `SIGNALWEAVE_INFERENCE_ENABLED`); the entire pre-existing WebSocket/QUIC/WebTransport relay test suite passes unmodified through that disabled path, proving the plane's absence changes nothing. Real hosted/local HTTP providers, a dedicated `signalweave-inference-memory` crate, and multi-identity config-driven provisioning remain deferred as explicitly out of scope for this minimal slice.

### Milestone 6 — Cloud-ready staging plan

**Status: Deferred; any cloud mutation is approval-gated.**

Create a portable container, reviewed Terraform or OpenTofu modules, CI validation, Workload Identity Federation instructions, Compute Engine/network-load-balancer staging design, rollback procedure, and idle/expected/stress cost estimate. First install or otherwise pin one infrastructure validator. Planning and local validation may complete without cloud changes; provisioning, public exposure, secrets, IAM changes, and deployment remain deferred until explicit approval.

### Milestone 7 — Project integrations

**Status: Deferred.**

Add generic consumer examples only after the platform slices they exercise are complete: a DARK FOREST namespace showing system, ship exterior, anchored interior, and crew subscriptions; and a portfolio namespace showing lightweight presence/events plus optional inference. Keep all domain rules outside the Signalweave core.

## Delivery discipline

Each milestone must remain runnable, use bounded resources, and distinguish scaffolding from working behavior. Validation starts with focused tests, then formatting, Clippy, workspace tests, protocol conformance, container build, load scenarios, and infrastructure validation as those artifacts appear. Results are reported only when actually measured. Deferred complexity remains governed by ADR 0013 and is introduced only when evidence and acceptance criteria justify it.
