# SIGNALWEAVE — Agent Reference

Read this file first and stop. Do not re-read `docs/bootstrap.md`, ADRs, or source files
unless you need to extend a specific area. Everything an agent needs to orient and act is here.
Source sections below provide exact public APIs so you can write code against them without reads.

---

## Identity and vocabulary

| Term | Meaning |
|---|---|
| SIGNALWEAVE | Umbrella project |
| Signalweave Node | Runtime process |
| Signalweave Protocol | Wire protocol (file identifier `SWP1`) |
| Signalweave Intelligence | Inference subsystem (Milestone 5, not started) |
| `Namespace` | Project/tenant isolation (e.g. `dark-forest`, `portfolio`) |
| `Session` | Shared realm inside a namespace |
| `Space` | Spatial or logical scope within a session, owns a coordinate frame and routing policy |
| `Entity` | Addressable participant; always server-assigned, always owned by one connection |
| `Channel` | Typed event/state family; delivery class and persistence class are server-controlled |
| `Subscription` | A connection's authorized view into a space |
| `Envelope` | Versioned routing metadata wrapping a typed payload |

IDs: all are `u64` newtypes. **0 is reserved for absent/unassigned on the wire and rejected at every core API boundary.** Assigned IDs start at 1.

---

## Repository layout

```
signalweave/
├── AGENTS.md                        ← you are here
├── Cargo.toml                       ← workspace root, shared deps/lints
├── Cargo.lock                       ← committed, use --locked in CI
├── rust-toolchain.toml              ← pinned to 1.98.0 stable
├── rustfmt.toml                     ← edition 2024, max_width 100
├── .cargo/config.toml               ← aliases: check-all lint test-all
├── .env.example                     ← SIGNALWEAVE_* env vars, no secrets
├── .github/workflows/ci.yml         ← format, lint, test, doc, audit
├── docs/
│   ├── bootstrap.md                 ← original prompt (read only when adding a new milestone)
│   ├── implementation-plan.md       ← milestone status (update when milestones complete)
│   └── adr/0001–0013-*.md          ← architecture decisions (read only when topic-relevant)
└── crates/
    ├── signalweave-core/            ← Milestone 1 DONE
    └── signalweave-protocol/        ← Milestone 1 DONE
```

Crates added in future milestones (do not create empty scaffolding):
- `signalweave-server` — Milestone 2
- `signalweave-transport-websocket` — Milestone 2
- `signalweave-client-rust` — Milestone 2
- `signalweave-client-ts` — Milestone 2
- `signalweave-transport-quic` — Milestone 4
- `signalweave-transport-webtransport` — Milestone 4
- `signalweave-inference-*` — Milestone 5
- `signalweave-loadtest` — Milestone 3 bounded local routing scenarios and measurements

---

## Milestone status

| Milestone | Status |
|---|---|
| 0 — Discovery, workspace, CI, ADRs | **Complete** |
| 1 — Core + Protocol vertical slice | **Complete** |
| 2 — WebSocket server + clients | **Complete** — WebSocket vertical slice, presence/transition lifecycle, Rust client, and TS live-frame decode validation exist |
| 3 — Interest management + load runner | **Complete** — bounded 2D/3D grid routing and a bounded local load runner exist; consumer-specific proxy/fidelity policy remains deferred |
| 4 — QUIC + WebTransport | **Complete** — native QUIC and WebTransport adapters with reliable streams and datagram delivery-class mapping; real-socket conformance coverage for both |
| 5 — Inference plane | **Complete** — adjacent optional coordinator, deterministic tool gateway, and deterministic fake provider exist; disabled by default, zero core/protocol dependency beyond 12 additive message kinds |
| 6 — Cloud staging plan | Deferred (approval-gated) |
| 7 — DARK FOREST + portfolio examples | Deferred |

---

## Validation commands (run before every commit)

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
RUSTDOCFLAGS=-Dwarnings cargo doc --locked --workspace --no-deps
cargo audit
```

Aliases defined in `.cargo/config.toml`: `cargo check-all`, `cargo lint`, `cargo test-all`.

Regenerate the protocol golden fixture after any schema change:
```sh
cargo run -p signalweave-protocol --example write_golden
```

---

## Hard rules (never violate)

- No handwritten `unsafe` code. `signalweave-core` has `#![forbid(unsafe_code)]`. The protocol crate uses `#![deny(unsafe_code)]` with a narrowly scoped `#[allow]` only inside the private `generated` module (FlatBuffers runtime).
- No unbounded channels, collections, or queues anywhere.
- No global locks. Hot state has a single owner.
- No game rules, physics, or domain logic in `signalweave-core` or `signalweave-protocol`.
- No cloud mutations, IAM changes, secret operations, DNS changes, or production deployments without explicit user approval. Local code, tests, containers, infra plans, and read-only inspection are always safe.
- No empty placeholder crates or modules. Add a crate only when it contains working behavior.
- No `ensure_session` / implicit session creation. Sessions are provisioned by the server via `core.provision_session()`.
- No comments that restate the code. Comments explain non-obvious intent only.

---

## `signalweave-core` public API

### IDs (`crate::ids`)

All are `struct Foo(u64)` with `::new(u64)`, `.get() -> u64`, `From<u64>`, `Display`.

```
NamespaceId  SessionId  SpaceId  EntityId
ConnectionId  PrincipalId  ChannelId  SpaceEpoch
SessionKey { namespace: NamespaceId, session: SessionId }
SpaceKey   { session: SessionKey, space: SpaceId }
```

### Authorization (`crate::auth`)

```rust
// Build grants for a principal
let mut grants = AuthorizationGrants::new();
grants.grant_namespace(ns, AccessGrant::ReadWrite);
grants.grant_session(session_key, AccessGrant::ReadWrite);
grants.grant_space(space_key, AccessGrant::ReadWrite);
grants.grant_channel(ChannelScope::new(session_key, channel_id), AccessGrant::ReadWrite);
// AccessGrant variants: Read, Write, ReadWrite

let principal = AuthenticatedPrincipal::new(PrincipalId::new(1), grants);

// DevAuthenticator — development only, bounded map of token → principal
let mut auth = DevAuthenticator::new();    // DEFAULT_MAX_IDENTITIES = 64
auth.insert("token", principal)?;          // returns Err(CapacityReached) when full
// Implements Authenticator trait
```

### Channel definition (`crate::authority`)

```rust
// Delivery class and persistence are SERVER-CONTROLLED per channel.
// Clients cannot override them; the core enforces ChannelPolicyMismatch.
ChannelDefinition::relay_owned(id, DeliveryClass::ReliableOrdered, PersistenceClass::Ephemeral, max_bytes)
ChannelDefinition::new(id, delivery, persistence, max_bytes)         // same as relay_owned
ChannelDefinition::with_authority(id, delivery, persistence, max_bytes, Arc<dyn AuthorityPolicy>)

// AuthorityPolicy trait — custom validation/transform/emit
// Built-in: RelayOwned (checks session member, space subscriber, entity owner)
// AuthorityOutcome: Accept | Reject(AuthorityRejection) | Transform(AuthorityTransform) | Emit(Box<[AuthorityEmission]>)
```

### Model types (`crate::model`)

```rust
DeliveryClass:    ReliableOrdered | ReliableUnordered | LatestValue | UnreliableSequenced | BestEffortEvent
PersistenceClass: Ephemeral | Stateful | Durable
RoutingPolicy:    BroadcastAll | SpatialGrid2D{cell_size,interest_radius,exact_distance} | SpatialGrid3D{...} | TopicOnly
CoordinateFrame:  Logical | Cartesian2D{meters_per_unit} | Cartesian3D{meters_per_unit}
EntityPosition:   Cartesian2D{x,y} | Cartesian3D{x,y,z}  // finite; must match a spatial space frame

SpaceDescriptor { id, local_frame, parent: Option<ParentAnchor>, epoch, routing }
ParentAnchor    { parent_space: SpaceId, anchor_entity: EntityId }

OutboundMessage { namespace, session, space, space_epoch, entity: Option<EntityId>,
                  channel, sequence, delivery, persistence, coalesce_key: Option<CoalesceKey>, payload: Vec<u8> }
// outbound_message.scoped_coalesce_key() → Option<ScopedCoalesceKey>  (namespace+session+space+epoch+application)

CoalesceKey { channel, entity: Option<EntityId>, component: u64 }
// LatestValue/UnreliableSequenced MUST supply a CoalesceKey; coalescing is fully-scoped by ScopedCoalesceKey

SessionSnapshot { key, member_count, subscription_count, state_bytes, spaces, entities, state }
```

### Queue (`crate::queue`)

```rust
OutboundQueueConfig { total_capacity: 512, critical_capacity: 256, latest_capacity: 512, best_effort_capacity: 128 }
// critical_capacity + latest_capacity + best_effort_capacity may exceed total_capacity (total is the hard ceiling)
// Overflow priority: evict oldest latest → evict best-effort → CriticalCapacityExhausted → disconnect

QueuePush: Queued | QueuedCriticalAfterEviction(QueueEviction) | ReplacedLatest |
           EvictedLatest{key} | EvictedBestEffortForLatest | DroppedLatest |
           DroppedBestEffort | CriticalCapacityExhausted
// CriticalCapacityExhausted → core immediately calls transport_lost on that connection
```

### Core (`crate::core`)

```rust
// Construction
let core = SignalweaveCore::new(authenticator, CoreConfig::default())?;

// CoreConfig defaults (all adjustable):
//   max_connections: 4_096        max_sessions: 1_024
//   max_channels: 1_024           max_memberships_per_connection: 32
//   max_subscriptions_per_connection: 128   max_owned_entities_per_connection: 256
//   max_payload_bytes: 64KB       max_spaces_per_session: 1_024
//   max_space_epoch_tombstones_per_session: 4_096
//   max_entities_per_session: 16_384      max_state_entries_per_session: 65_536
//   max_state_bytes_per_session: 64MB     max_sequence_keys_per_session: 131_072
//   max_authority_emissions: 16   journal_outbox_capacity: 1_024
//   publish_rate_limit: { max_publishes: 256, window: 1s }

// Server setup (before any connections)
core.register_channel(channel_definition)?;   // must be nonzero id, nonzero payload limit
core.provision_session(session_key)?;         // server provisions sessions; clients cannot create them
core.install_space(session_key, descriptor)?; // parent anchor must already exist

// Per-connection lifecycle (call in this order)
let conn_id = core.transport_connected()?;
let principal_id = core.authenticate(conn_id, &Credentials::new("token"))?;
core.join_session(conn_id, session_key)?;     // checks grants, session must already exist
core.subscribe(conn_id, space_key)?;          // checks grants, session membership, space existence
let entity_id = core.spawn_entity(conn_id, space_key, epoch)?;  // server-assigns ID
let outcome = core.publish(publish_request)?; // or core.publish_at(request, Instant)
let summary = core.unsubscribe(conn_id, space_key)?;            // purges queued msgs for that space
let summary = core.leave_session(conn_id, session_key)?;        // purges queued session msgs
core.remove_entity(conn_id, session_key, entity_id)?;
let messages = core.drain_outbound(conn_id)?;
let snapshot = core.snapshot(conn_id, session_key)?;            // scoped to subscribed spaces + grants
let summary = core.transport_lost(conn_id)?;                    // full cleanup, call on disconnect

// Space management
core.advance_space_epoch(session_key, space_id, new_epoch)?;   // evicts entities+state+sequences
core.update_entity_position(conn_id, session_key, entity_id, position)?; // owner-only; updates grid index on cell crossing
// Epoch tombstones prevent ID reuse. Recreation must advance the epoch.

// PublishRequest fields:
//   connection, session, space, space_epoch, entity: Option<EntityId>, channel,
//   sequence (must be strictly monotone per connection+space+epoch+entity+channel+component),
//   delivery (must match ChannelDefinition), persistence (must match ChannelDefinition),
//   coalesce_key (required for LatestValue/UnreliableSequenced), payload

// Introspection helpers
core.is_connected(conn_id) → bool
core.subscription_count(conn_id) → Option<usize>
core.owned_entity_count(conn_id) → Option<usize>
core.sequence_key_count(session_key) → Option<usize>
core.space_epoch_tombstone_count(session_key) → Option<usize>
core.session_count() → usize
core.connection_count() → usize
core.journal_outbox_len() → usize
core.pop_journal_record() → Option<JournalRecord>

// JournalSink trait — async, currently no-op
// NoopJournalSink implements it with a Ready<Ok> future (no runtime needed)
```

### Worker harness (`crate::worker`)

```rust
// Thin synchronous wrapper used by tests and will be used by the transport adapter
let worker = TransportIndependentWorker::new(core);
let mut harness = WorkerHarness::new(worker, capacity)?;  // capacity = max pending commands
harness.submit(Command::TransportConnected)?;
harness.step() → Option<Result<CommandResult, CoreError>>
harness.run_pending() → Vec<Result<CommandResult, CoreError>>

// Commands: TransportConnected | Authenticate{..} | JoinSession{..} | LeaveSession{..}
//           Subscribe{..} | Unsubscribe{..} | SpawnEntity{..} | RemoveEntity{..}
//           UpdateEntityPosition{..} | TransitionEntity(..) | Publish(PublishRequest)
//           Snapshot{..} | DrainOutbound{..} | TransportLost{..}
// CommandResult: Connected(ConnectionId) | Authenticated(PrincipalId) | Joined | Left(CleanupSummary)
//               Subscribed | Unsubscribed(CleanupSummary) | EntitySpawned(EntityId)
//               EntityRemoved(CleanupSummary) | Published(PublishOutcome) | Snapshot(SessionSnapshot)
//               Outbound(Vec<OutboundMessage>) | Disconnected(CleanupSummary)
```

---

## `signalweave-protocol` public API

```rust
// Constants
PROTOCOL_VERSION: u16 = 1
FILE_IDENTIFIER: &str  = "SWP1"

// Codec — size-prefixed FlatBuffers framing
let codec = Codec::default();                                      // default limits: 1MB frame, 256KB payload
let codec = Codec::new(CodecLimits::new(max_frame, max_payload)?)?;
codec.encode(&envelope) → Result<Vec<u8>, CodecError>
codec.decode(frame: &[u8]) → Result<Envelope, CodecError>
codec.expected_frame_len(prefix: &[u8]) → Result<Option<usize>, CodecError>  // for stream transports
// decode always runs FlatBuffers verifier + semantic validation before returning

// Envelope (owned)
struct Envelope {
    protocol_version: u16,          // must be 1
    delivery_class: DeliveryClass,  // never Unknown
    namespace_id: u64,
    session_id: u64,
    space_id: u64,
    channel_id: Option<u64>,        // required for channel-bearing messages
    entity_id: Option<u64>,
    space_epoch: u64,
    server_tick: u64,
    sender_sequence: u64,
    correlation_id: Option<u64>,
    message: MessagePayload,
}
// Constructors: Envelope::control(delivery, ControlPayload) | ::entity_state | ::reliable_event | ::snapshot

// MessagePayload variants
Control(ControlPayload)           // typed control messages
EntityState(OpaquePayload)        // requires channel_id + entity_id + nonzero type_id, delivery=LatestValue or UnreliableSequenced
ReliableEvent(OpaquePayload)      // requires channel_id + nonzero type_id, delivery=ReliableOrdered or ReliableUnordered
Snapshot(OpaquePayload)           // requires channel_id + nonzero type_id

// OpaquePayload { type_id: u64, bytes: Vec<u8> }  — domain bytes, opaque to routing

// ControlPayload variants (message kind numeric value in parens)
Hello(Hello)                    (1)   — unscoped, delivery=ReliableOrdered
Capabilities(Capabilities)      (2)   — unscoped, delivery=ReliableOrdered
Authenticate(Authenticate)      (3)   — unscoped, delivery=ReliableOrdered
Authenticated(Authenticated)    (4)   — unscoped, delivery=ReliableOrdered
JoinSession(JoinSession)        (5)   — session-scoped, delivery=ReliableOrdered
LeaveSession(LeaveSession)      (6)   — session-scoped, delivery=ReliableOrdered
SubscribeSpace(SubscribeSpace)  (7)   — space+channel-scoped, delivery=ReliableOrdered
UnsubscribeSpace(Unsubscribe)   (8)   — space+channel-scoped, delivery=ReliableOrdered
SubscriptionAccepted(..)        (9)   — space+channel-scoped, delivery=ReliableOrdered
SubscriptionRejected(..)        (10)  — space+channel-scoped, delivery=ReliableOrdered
EntityEntered(..)               (11)  — space+entity-scoped, delivery=ReliableOrdered
EntityLeft(..)                  (12)  — space+entity-scoped, delivery=ReliableOrdered
SnapshotRequest(..)             (15)  — space+channel-scoped, delivery=ReliableOrdered
Snapshot                        (16)  — opaque, space+channel-scoped
SpaceTransition(..)             (17)  — space+entity-scoped, delivery=ReliableOrdered
Ping(Ping)                      (18)  — unscoped, delivery=ReliableUnordered
Pong(Pong)                      (19)  — unscoped, delivery=ReliableUnordered
ProtocolError(..)               (20)  — optional scope, delivery=ReliableOrdered

// Semantic constraints enforced on both encode and decode:
// - Unscoped controls: namespace/session/space/channel/entity/epoch must all be 0/None
// - Session controls: namespace+session nonzero; space/channel/entity/epoch must be absent
// - Space controls: namespace+session+space+epoch nonzero
// - Channel-bearing controls: channel_id required (nonzero)
// - Entity-bearing controls: entity_id required (nonzero)
// - Hello: min_version ≤ max_version, range includes v1, frame/payload limits nonzero
// - Authenticate: scheme != Unknown, credentials nonempty
// - Authenticated: principal_id nonzero
// - DeliveryClass must be compatible with MessageKind (see semantics.rs)

// CodecError variants (for ProtocolError mapping):
// InvalidLimits | FrameTooLarge | PayloadTooLarge | TruncatedFrame | TrailingBytes
// InvalidSizePrefix | InvalidFileIdentifier | InvalidFlatbuffer | UnsupportedProtocolVersion
// UnknownMessageKind | UnknownDeliveryClass | UnsupportedEnumValue | MessageControlMismatch
// MissingPayloadType | UnexpectedDomainPayload | InvalidSemantics{message_kind, reason}

// Schema: crates/signalweave-protocol/schemas/signalweave_v1.fbs
// Golden fixture: crates/signalweave-protocol/tests/fixtures/reliable_event_v1.swp
// Companion: crates/signalweave-protocol/tests/fixtures/reliable_event_v1.expected.txt
// FlatBuffers generation: vendored via flatc-fork=0.6.0 + flatbuffers-build=0.2.4 (no system flatc needed)
```

---

## Architecture decisions (summaries — read full ADR only if extending that area)

| ADR | Decision |
|---|---|
| 0001 | Core is transport-independent. Networking, routing, authority, persistence, and inference are explicit separate layers |
| 0002 | QUIC/WebTransport instead of raw UDP. No application-level encryption, congestion control, or fragmentation |
| 0003 | Binary WebSocket is the universal baseline. Implement and test it before QUIC/WebTransport |
| 0004 | Session owns a graph of spaces. Every space has local coordinates anchored to a parent entity. Cross-space transitions are sequenced and epoch-protected |
| 0005 | FlatBuffers with pinned versioned schema. Additive-only evolution. Never reuse IDs. Golden fixtures prove cross-language equivalence |
| 0006 | Bounded queues everywhere. Priority: evict replaceable → drop best-effort → disconnect slow consumer |
| 0007 | Authority is per-channel, not global. Default RelayOwned. Custom policies via trait |
| 0008 | Persistence seam outside the realtime hot path. In-memory + no-op journal now; pluggable later |
| 0009 | Inference is an optional adjacent plane. Never inside room-worker hot loops. Disabling it must leave relay tests unchanged |
| 0010 | Model output passes through a deterministic tool gateway. Models never mutate state directly |
| 0011 | Compute Engine VM + external passthrough NLB for staging. Not Cloud Run (wrong lifecycle). Not GKE until scale justifies it |
| 0012 | Normal target $10–30/month. No GPU continuously. All cloud mutations approval-gated |
| 0013 | Deferred complexity list: distributed consensus, GKE, GPU inference, vector DBs, agent frameworks, UDP, app-level fragmentation |

---

## Approval gates (require explicit user approval before proceeding)

- Creating or modifying billable cloud resources
- Allocating or resizing GPUs
- Expanding IAM permissions
- Creating or rotating secrets
- Changing DNS or public exposure
- Deploying to staging or production
- Any action that materially increases expected monthly cost

---

## Environment and toolchain

```
Rust: 1.98.0 stable (rust-toolchain.toml pins this)
Cargo workspace resolver: 3, edition: 2024, rust-version: 1.88
Available: cargo, rustfmt, clippy, Node 22.23.2, npm 10.9.8, Docker 29.7.2, gh 2.98.0
Absent: system flatc (vendored via Cargo), .NET, Terraform, OpenTofu
GitHub: jdharrison/signalweave (public), SSH remote, gh authenticated
CI: .github/workflows/ci.yml — format + clippy + test + doc + rustsec/audit-check@v2.0.0
```

Workspace lints (inherited by all crates via `[lints] workspace = true`):
- `unsafe_code = "deny"` (core overrides to `forbid`)
- `clippy::all`, `clippy::pedantic` at warn; `missing_errors_doc` and `module_name_repetitions` allowed

---

## Milestone 2 — What to build next

**Goal:** end-to-end binary WebSocket vertical slice. A client connects, authenticates, joins a session, subscribes, publishes, and disconnects cleanly over a real socket. Every protocol message from Milestone 1 that has a corresponding core operation must work.

### New crates required

**`crates/signalweave-server`** — Tokio runtime + Axum HTTP
- Load config from `SIGNALWEAVE_*` env vars (see `.env.example`) with explicit defaults
- Expose only: `GET /healthz` (200), `GET /readyz` (200 when core ready), `GET /metrics` (placeholder), `GET /v1/capabilities` (JSON: protocol version, transport list, max frame/payload)
- Never expose session state unauthenticated
- Accept connections and hand off to the WebSocket transport
- `SignalweaveCore` runs in a single-threaded worker task; command/result passing via bounded `mpsc`

**`crates/signalweave-transport-websocket`** — Axum WebSocket adapter
- Accept binary WebSocket frames; reject text frames with `ProtocolError`
- Per-connection lifecycle driven by `TransportIndependentWorker` (already tested)
- Connection handshake order: receive `Hello` → send `Capabilities` → receive `Authenticate` → send `Authenticated` or `ProtocolError` → session/space commands → teardown
- Use `Codec` for all framing; pass decoded `Envelope` to core commands; encode `OutboundMessage` back to frames
- Separate read task and write task per connection; bounded channel between them
- On write-channel full or WebSocket error: call `transport_lost` on the core worker, drain any queued `OutboundMessage`s to discard
- Keep WebSocket upgrade on a configurable path (default `/ws`)

**`crates/signalweave-client-rust`** — reference native client (used as integration test driver)
- Connect, send `Hello`, receive `Capabilities`, send `Authenticate`, receive `Authenticated`
- `join_session`, `subscribe_space`, `publish`, `drain` (receive loop)
- Synchronous or minimal-async; no need to mirror the full server abstraction
- Used directly in integration tests — not a production client library yet

### Integration tests (in `signalweave-server` or a `tests/` crate)
Must demonstrate over a real in-process TCP socket (Tokio test listener or `tokio::test`):
1. Authentication enforced — unauthenticated publish rejected
2. Namespace/session/space isolation — wrong-namespace principal cannot join
3. Nested subscription — one client holds two space subscriptions simultaneously
4. Entity ownership — bob cannot publish on alice's entity
5. LatestValue coalescing — two publishes → recipient sees only the newest
6. Reliable event fan-out — both subscribers receive
7. Snapshot — returns only subscribed+authorized spaces/state
8. Disconnect cleanup — transport_lost removes entity, subscriber sees `EntityLeft`
9. SpaceTransition round-trip — client sends, server sends `EntityLeft` + `EntityEntered`
10. Malformed frame — server sends `ProtocolError` and closes

### TypeScript client (`crates/signalweave-client-ts`)
- Use vendored flatc to generate TS bindings: find the built flatc at `target/debug/build/signalweave-protocol-*/out/bin/flatc`, run `flatc --ts -o <outdir> schemas/signalweave_v1.fbs`
- Wrap in a minimal npm package using the generated code
- Demonstrate Hello/Capabilities/Authenticate/Authenticated decode in a Node.js test script
- C# deferred — .NET not available in this environment

### Exit criteria for Milestone 2
- `cargo test --workspace` passes including integration tests
- `GET /healthz` returns 200
- `GET /v1/capabilities` returns valid JSON
- A Rust client connects, authenticates, publishes, and disconnects without errors
- Disconnect always removes presence (no stale entities or subscribers)
- No unauthenticated endpoint exposes session state
- TS client can decode a frame produced by the Rust server

---

## Notes on cost and deferred work

- No container, Dockerfile, or cloud infrastructure in Milestone 2. Those are Milestone 6.
- No spatial routing in Milestone 2. `BroadcastAll` is sufficient for the integration tests.
- No inference in Milestone 2. The inference plane is Milestone 5.
- Do not introduce GKE, Cloud Run, Redis, vector DBs, or distributed consensus at any point without explicit ADR revision and user approval.
- Benchmark before introducing unsafe code, lock-free structures, or custom allocators.
- The $10–30/month target applies to cloud deployment only. Local dev has no cost constraint.
