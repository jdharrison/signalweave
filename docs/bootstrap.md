# WOVEN — Autonomous Bootstrap and Implementation Prompt

You are the lead engineer responsible for initializing and developing **WOVEN**, a reusable distributed realtime session, event, state, and inference network written primarily in Rust.

Work autonomously within the local repository. Inspect available source material, make reasonable reversible assumptions, implement in staged vertical slices, run relevant validation, and leave the repository in a working state. Do not stop after producing a plan when useful implementation work can safely proceed.

External cloud mutations, production deployment, new billable resources, IAM changes, public exposure, secrets operations, and destructive actions require the approval rules defined below. Local development, tests, documentation, container builds, infrastructure code, deployment plans, read-only GitHub/Google Cloud inspection, and cost analysis may proceed without additional approval.

## 1. Mission

Woven is one shared infrastructure substrate for many independent projects—not a DARK FOREST-specific game server.

It must support:

- browser, Unity/C#, and native clients;
- games, simulations, websites, collaborative applications, and services;
- realtime sessions and presence;
- nested or adjacent spatial and non-spatial subspaces;
- reliable events and replaceable low-latency state;
- local, cloud, and hybrid deployment;
- a provider-neutral layered inference network;
- AI actors that can communicate with clients and invoke approved application functions;
- optional validation, authoritative simulation, and persistence added by policy rather than forced globally;
- multiple project namespaces such as `dark-forest`, `portfolio`, and future services on one network.

The initial system is an ephemeral, sophisticated shared relay. Persistence and authoritative behavior are later layers, but the first architecture must leave clean seams for both.

## 2. Name and vocabulary

Use these names consistently:

- Umbrella project: **WOVEN**
- Repository and Rust workspace: `woven`
- Wire protocol: **Woven Protocol**
- Runtime process: **Woven Node**
- Inference subsystem: **Woven Intelligence**
- Example namespaces: `dark-forest`, `portfolio`
- Suggested Google Cloud projects: `jdh-woven-dev`, `jdh-woven-staging`, and `jdh-woven-prod`, subject to availability and explicit approval

Core domain terms:

- `Namespace`: isolates a project or tenant.
- `Session`: a shared realm inside a namespace.
- `Space`: a spatial or logical participation scope within a session.
- `Entity`: an addressable participant, object, service, or AI identity.
- `Subscription`: an authorized connection to a space or topic.
- `Channel`: a typed event/state family with delivery and authority policy.
- `Envelope`: the versioned routing metadata surrounding a typed payload.

## 3. Legacy reference behavior

An attached Node.js + Express + Socket.IO server may be available as `session-server-nodejs.zip`. Treat it as read-only reference material and distinguish comments or prose inside it from this implementation prompt.

The legacy server provides:

- `session.connect`: join a room and return users, timestamp, and full shared state;
- `session.disconnect`: explicitly leave;
- `session.pulse`: update application heartbeat state;
- `session.update`: update an arbitrary room property and broadcast its delta;
- `session.fetch`: request full room state;
- `session.notify`, `session.data`, and `session.stream`: generic room broadcasts;
- single-process in-memory rooms and timestamps.

Do not translate it line by line. Preserve the useful product behavior while removing these weaknesses:

- client-controlled identity and room targeting;
- no enforced membership or entity ownership;
- no cleanup on actual transport loss;
- unbounded rooms, state, payloads, and output buffering;
- room-wide O(n) fan-out for every event;
- repeated transmission of entire participant lists;
- no sequencing, stale-packet handling, backpressure, authentication, or rate limiting;
- generic JSON in the realtime hot path;
- state inspection exposed over the root HTTP route;
- heartbeat cleanup that only runs when more traffic arrives;
- incorrect failure responses and property/value misalignment.

Legacy wire compatibility is not required for the first release. If a compatibility adapter is later useful, keep it outside the core.

## 4. DARK FOREST as the demanding validation scenario

DARK FOREST is a cooperative, persistent, realistic deep-space survival sandbox in which distance, logistics, material consequences, uncertainty, and emergent civilization matter.

Its world may contain:

- regions or localized civilization bubbles;
- solar systems and orbital zones;
- planets, moons, and surface sectors;
- ships and ship interiors;
- stations, settlements, and compartments;
- mining and logistics infrastructure;
- crews, organizations, markets, communications, and AI services;
- long-term player-built history and eventually persistent state.

Use this to validate the general architecture without putting DARK FOREST gameplay rules into Woven.

A representative hierarchy is:

```text
Namespace: dark-forest
└── Session / Realm
    ├── Region or civilization bubble
    │   └── Solar system
    │       ├── Orbital zone
    │       ├── Planet
    │       │   └── Surface sectors
    │       ├── Station exterior
    │       │   └── Station interior
    │       │       └── Compartments
    │       └── Ship exterior
    │           └── Ship interior
    └── Non-spatial channels
        ├── Organization
        ├── Crew
        ├── Market
        └── Communications
```

A client may subscribe to multiple scopes at once. Someone aboard a ship might receive low-frequency system summaries, the ship’s exterior neighborhood, detailed interior state, crew events, and organization events.

Do not replicate every ship occupant into the solar-system spatial index. The parent space sees a coarse ship proxy; occupants exist in a local coordinate frame anchored to that ship. Apply the same pattern to stations, planets, surface sectors, vehicles, and other nested simulations.

Do not invent concurrency promises from the concept. Client counts, active spaces, entity density, update frequencies, payload sizes, and interest radii must be configurable benchmark dimensions.

## 5. Architectural principles

1. Keep the realtime core transport-independent.
2. Keep networking, application protocol, routing, authority, persistence, inference, and cloud operations in explicit layers.
3. Give hot mutable state a single owner wherever practical instead of sharing it behind global locks.
4. Use bounded queues everywhere.
5. Coalesce replaceable state instead of accumulating stale updates.
6. Authenticate once, assign identity server-side, and never trust repeated client identity claims.
7. Benchmark before introducing unsafe code, lock-free structures, custom allocators, or exotic serialization.
8. Prefer a small understandable vertical slice over speculative distributed infrastructure.
9. Treat model output and client input as untrusted proposals.
10. Make local and cloud execution use the same configuration model and provider interfaces.
11. Measure cost as carefully as latency and throughput.

## 6. Proposed workspace

Initialize a Rust workspace with focused crates. Adjust exact boundaries only when there is a concrete reason and record the decision.

- `woven-core`: sessions, spaces, entities, subscriptions, ownership, routing, delivery classes, sequencing, and backpressure policy.
- `woven-protocol`: FlatBuffers schemas, generated Rust bindings, framing, compatibility, and conformance fixtures.
- `woven-server`: configuration, lifecycle, worker composition, HTTP control plane, and enabled transports.
- `woven-transport-websocket`: universal binary WebSocket adapter.
- `woven-transport-quic`: native QUIC adapter using Quinn.
- `woven-transport-webtransport`: browser WebTransport adapter.
- `woven-client`: reference native client library.
- `@signalweave/woven-client`: browser reference client package.
- `woven-client-csharp`: generated protocol and Unity-compatible reference client.
- `woven-inference-core`: inference capabilities, requests, routing policy, deadlines, budgets, cancellation, and streaming results.
- `woven-inference-coordinator`: context assembly, provider selection, scheduling, and response delivery.
- `woven-inference-tools`: application-function registry and validated execution gateway.
- `woven-inference-memory`: scoped memory interfaces and initial in-memory implementation.
- `woven-inference-test-provider`: deterministic fake inference provider.
- `woven-loadtest`: synthetic clients, scenarios, metrics, and reports.

It is acceptable to place several early modules in fewer crates if that speeds the first vertical slice. Do not create empty architectural theater. Preserve boundaries in code first and extract crates when useful.

## 7. Runtime and HTTP control plane

Use stable Rust and the current stable Rust edition.

Preferred initial libraries:

- Tokio for async execution;
- Axum for HTTP and the initial WebSocket adapter;
- Quinn for native QUIC;
- FlatBuffers for the cross-language wire schema;
- `bytes` for packet buffers;
- `tracing` and OpenTelemetry-compatible instrumentation;
- Criterion only for focused microbenchmarks, supplemented by the real load generator.

Expose only:

- `/healthz`
- `/readyz`
- `/metrics`
- `/v1/capabilities`

Do not expose live session state over unauthenticated HTTP. Provide explicit development authentication through an interface; never silently disable authentication.

## 8. Transport strategy

Implement one application protocol over several adapters:

1. **Binary WebSocket** is the first universal baseline for browsers, Unity, and native clients.
2. **WebTransport** provides reliable streams and unreliable datagrams for supported modern browsers.
3. **QUIC** provides reliable streams and datagrams for native clients and supported Unity/.NET environments.
4. Clients negotiate capabilities and fall back to WebSocket when QUIC or WebTransport is unavailable or blocked.

Complete and test the core plus WebSocket transport before implementing QUIC and WebTransport. Do not build three incomplete transports simultaneously.

Raw UDP is not the primary transport. Do not recreate encryption, congestion control, reliability, fragmentation policy, NAT behavior, or connection migration at the application layer.

Keep unreliable datagrams under a conservative packet budget near 1200 bytes. Do not implement application-level fragmentation for realtime datagrams. Send large snapshots over reliable streams.

## 9. Protocol and cross-language compatibility

Use FlatBuffers so one schema can generate Rust, C#, and TypeScript bindings. Keep schemas language-neutral and versioned. Validate all untrusted buffers before access.

Define a compact transport envelope containing:

- protocol version;
- message kind;
- delivery class;
- namespace, session, and space identifiers;
- entity identifier when applicable;
- space epoch;
- server tick;
- sender sequence;
- correlation or causal event identifier;
- payload type identifier;
- payload bytes.

Core messages should include:

- `Hello`, `Capabilities`;
- `Authenticate`, `Authenticated`;
- `JoinSession`, `LeaveSession`;
- `SubscribeSpace`, `UnsubscribeSpace`;
- `SubscriptionAccepted`, `SubscriptionRejected`;
- `EntityEntered`, `EntityLeft`;
- `EntityState`, `ReliableEvent`;
- `SnapshotRequest`, `Snapshot`;
- `SpaceTransition`;
- `Ping`, `Pong`, `ProtocolError`.

Domain-specific payloads remain typed but opaque to the routing core. The relay understands envelope metadata without needing every gameplay or application schema.

Create golden binary fixtures and prove that Rust, C#, and TypeScript decode equivalent values. Define compatibility rules for adding fields, reserving IDs, deprecating messages, and negotiating versions.

## 10. Delivery classes and backpressure

Support these semantic delivery classes:

- `ReliableOrdered`: joins, leaves, ownership, critical events, snapshots, and durable mutations.
- `ReliableUnordered`: independent reliable operations where cross-operation ordering is unnecessary.
- `LatestValue`: replaceable state; retain only the newest unsent value per entity/component/recipient.
- `UnreliableSequenced`: datagram state where loss is acceptable and older arrivals must be discarded.
- `BestEffortEvent`: bounded, droppable transient events.

WebSocket transports every class reliably at the network layer, but `LatestValue` and best-effort data must still be replaced or dropped before entering its bounded writer queue. Once stale data has been written into a TCP stream it cannot be recalled, so queue discipline matters.

Each connection and worker must have explicit capacities and overflow behavior:

- replace stale realtime state first;
- drop low-priority transient events next;
- preserve bounded critical state;
- disconnect a persistently slow consumer before memory can grow without limit.

Every replaceable update includes a sequence or tick. Receivers discard stale or duplicated values.

## 11. Space graph, coordinate frames, and interest management

A session owns a graph of spaces rather than a flat list of Socket.IO rooms.

Routing policies:

- `BroadcastAll`: all authorized members receive the event.
- `SpatialGrid2D` and `SpatialGrid3D`: use a uniform hashed grid and configured interest radius.
- `TopicOnly`: route by explicit non-spatial subscription.

Spatial grids must maintain entity-to-cell and cell-to-entity indexes. Update membership only when an entity crosses a cell boundary. Select neighboring cells, then optionally perform exact squared-distance filtering. Permit critical events to bypass spatial filtering.

Support distance-based representation and update frequency. A distant ship may be a low-frequency aggregate; a nearby ship receives a precise exterior state; an occupant receives detailed interior state.

Do not use one universe-scale floating-point frame. Every spatial space owns local coordinates and may be anchored to an entity in a parent space. Cross-space transitions are explicit, sequenced, and protected by space epochs so packets from destroyed or recreated spaces cannot be accepted as current.

Rooms and spaces activate on demand. Empty ephemeral spaces are removed after a configurable grace period.

## 12. Ownership and optional authority

Authority is selected per channel, message family, entity, or component—not as one global server mode.

Define a narrow `AuthorityPolicy` capable of accepting, rejecting, transforming, or emitting messages.

Initial policies:

- `RelayOwned`: the authenticated owner may publish valid updates.
- `ServerValidated`: an installed deterministic rule module validates updates.
- `ServerSimulated`: an installed simulation module produces authoritative state.
- `ExternalOwned`: an authenticated external service owns the state.

Default to `RelayOwned`, while still enforcing authentication, membership, subscription, entity ownership, message state, sequence monotonicity, payload limits, and rate limits.

Do not embed a game ECS, physics engine, or DARK FOREST rules in the relay core. Optional authoritative modules consume and emit typed protocol events through explicit interfaces.

## 13. Persistence seam

Classify information as:

- `Ephemeral`: transforms and other replaceable state; never journaled.
- `Stateful`: appears in snapshots and may later be persisted.
- `Durable`: reliable mutation or event intended for journaling.

Begin with an in-memory state store and no-op journal. Define a small asynchronous `JournalSink` seam, but do not add Redis, a database, Kafka, Pub/Sub, or distributed consensus to the first vertical slice.

Persistence writes must not block realtime fan-out. Later implementations may snapshot state and journal durable mutations for recovery.

Gameplay-visible communication delay is semantic state, not artificial transport latency. Leave a scheduler seam for delayed radio or simulation messages while control and replication remain responsive.

## 14. Woven Intelligence: layered inference network

Inference is an optional plane adjacent to the relay. It must never execute inside room-worker hot loops, block replication ticks, or introduce provider SDKs into `woven-core`.

Inference layers:

1. deterministic application functions;
2. low-latency local inference;
3. high-capability local inference;
4. hosted cloud inference;
5. deliberative multi-model or agent workflows.

Providers register capabilities rather than being referenced by name in application code. Example capabilities:

- `language.dialogue`, `language.reasoning`, `language.summarize`, `language.translate`;
- `intent.classify`;
- `vision.inspect`;
- `speech.recognize`, `speech.synthesize`;
- `embedding.create`;
- `planning.navigation`, `planning.automation`.

Each provider advertises locality, privacy classification, modalities, streaming support, context and concurrency limits, health, latency class, cost class, and quality tier.

An inference request includes:

- request and causal event identifiers;
- authenticated principal and acting entity;
- namespace, session, and relevant spaces;
- capability, priority, and deadline;
- privacy policy and cost budget;
- quality and streaming preferences;
- context references and permitted tools;
- expected world-state revision.

Support immediate, streaming, background, cancellable, scheduled, and event-triggered jobs. Return acceptance immediately; never make a realtime handler synchronously wait for model completion.

Inference lifecycle messages include:

- `InferenceRequested`, `InferenceAccepted`, `InferenceProgress`;
- `InferenceStreamChunk`, `InferenceCompleted`, `InferenceFailed`;
- `InferenceCancelled`, `InferenceExpired`;
- `ToolCallProposed`, `ToolCallAccepted`, `ToolCallRejected`, `ToolCallCompleted`.

### AI identities

An AI identity may participate as a service-backed entity such as a personal companion, ship intelligence, NPC, station service, organization assistant, simulation director, or website assistant.

Each AI identity has:

- a stable entity ID;
- readable context and writable memory scopes;
- capability and tool grants;
- provider-routing and privacy policy;
- cost and concurrency budgets;
- audit policy.

Context is assembled from explicit scopes rather than dumping entire sessions into a model. Possible scopes include player-private, AI identity, ship, crew, organization, station, local environment, solar system, and public session knowledge. Context items carry provenance, visibility, timestamp, and revision.

Conversation and generated memory are derived information, not automatically authoritative world facts.

### Application-function gateway

Expose game and application functions through a provider-neutral tool registry. Each tool has a stable ID, version, input and result schema, required permissions, actor restrictions, timeout, side-effect classification, authority requirement, and idempotency behavior.

Models never mutate shared state directly. They produce a typed `ToolCallProposal`. The gateway:

1. validates identity and grants;
2. validates structured arguments;
3. checks current state revision and preconditions;
4. applies the relevant authority policy;
5. rejects stale or unsafe proposals;
6. executes the deterministic function;
7. records the outcome;
8. publishes resulting state changes through the relay.

Model output, retrieved text, and player input are untrusted. Tool authorization exists outside prompts. Never include credentials in model context.

Enforce deadlines, bounded provider queues, cancellation, circuit breaking, configured fallback, inference quotas, and idempotency. Expired realtime responses are discarded rather than delivered as current information.

The first inference demonstration uses a deterministic fake provider and one configurable local or hosted HTTP provider. Do not require a commercial account for tests. Do not begin with autonomous-agent frameworks, vector databases, or elaborate long-term memory.

## 15. Local execution

The entire first vertical slice must run locally without Google Cloud or paid inference.

Provide:

- reproducible toolchain setup;
- `.env.example` containing no secrets;
- clear configuration precedence;
- local development TLS/certificate instructions where required;
- a container image;
- a lightweight local composition file only if it materially improves development;
- commands for formatting, linting, tests, server startup, reference clients, load tests, and benchmarks;
- a fake inference provider and configurable local inference endpoint.

Do not make Docker mandatory for ordinary Rust development.

## 16. Google Cloud deployment

Google Cloud is a first-class deployment target, but keep it replaceable behind ordinary containers, configuration, and provider interfaces.

The initial serious realtime deployment should use a small Compute Engine VM or managed instance group capable of receiving both TCP and UDP through an external passthrough Network Load Balancer. The Woven Node terminates TLS/QUIC at the backend as required.

Do not use Cloud Run as the primary stateful realtime relay. It may be used later for suitable stateless HTTP or asynchronous helpers, but its WebSocket request lifetime and best-effort reconnect affinity do not match the desired room-owner lifecycle.

Start with one inexpensive non-GPU node in one region. Do not introduce GKE until multiple relay shards, specialized pools, or operational scale justify Kubernetes.

Preferred Google Cloud components when needed:

- Compute Engine for the initial Woven Node;
- regional external passthrough Network Load Balancer for TCP/UDP;
- Artifact Registry for immutable container images;
- Secret Manager for runtime secrets;
- Cloud Logging and Monitoring through OpenTelemetry-compatible export;
- Vertex AI as the initial hosted inference adapter;
- an explicitly approved Compute Engine GPU VM or GKE GPU pool only when cost/quality measurements justify self-hosted inference.

Infrastructure must be represented as reviewed Terraform or OpenTofu modules. Use `gcloud` for authentication, inspection, bootstrapping, troubleshooting, and approved operations—not as a substitute for reproducible infrastructure state.

Support `dev`, `staging`, and `prod` configurations without requiring all three environments to be provisioned.

## 17. GitHub and delivery workflow

Integrate naturally with GitHub CLI and GitHub Actions.

Pull-request validation should include:

- formatting and linting;
- unit and integration tests;
- dependency and security checks appropriate to Rust and generated clients;
- protocol conformance and golden vectors;
- container build;
- infrastructure validation and plan;
- selected local load scenarios when runtime permits.

An approved deployment flow should:

1. build an immutable image;
2. push it to Artifact Registry by digest;
3. deploy staging;
4. run health, smoke, compatibility, and bounded load tests;
5. present results for production approval;
6. retain a clear rollback path.

GitHub Actions must authenticate to Google Cloud using short-lived Workload Identity Federation rather than stored service-account keys.

The agent may create branches, commits, and PR material when the repository and user authorization permit. Do not push, merge, release, or deploy merely because the local implementation succeeds unless the relevant external action is already approved.

## 18. Cloud autonomy and approval boundaries

The agent has broad freedom to design and prepare whatever Woven needs, but human attention must be concentrated at meaningful risk boundaries.

Without new approval, the agent may:

- inspect approved GitHub repositories and Google Cloud projects read-only;
- write and test application code and infrastructure code;
- build containers locally;
- run local benchmarks and integration tests;
- generate Terraform/OpenTofu plans;
- estimate cost;
- prepare branches and PRs;
- recommend resources and configuration.

Explicit approval is required before:

- creating or changing billable cloud resources outside an already approved sandbox and spending envelope;
- allocating or resizing GPUs;
- expanding IAM permissions;
- creating or changing secrets;
- changing DNS or public exposure;
- deploying to production;
- destroying resources or durable data;
- materially increasing the expected monthly cost.

If an approved sandbox project and resource envelope are later defined, routine reversible operations inside that exact boundary may proceed autonomously. Label every resource with project, environment, owner, purpose, and cost attribution.

## 19. Cost contract

Treat cost as an acceptance criterion.

- Normal initial operating target: **USD $10–30 per month total**.
- Temporary initial-use and validation allowance: approximately **2–3× that range**, only with approval.
- Do not leave a GPU running continuously under the normal budget.
- Default to metered hosted inference with per-day and per-month application quotas.
- GPU experiments require explicit approval, automatic shutdown, and a written hosted-versus-self-hosted comparison.
- Keep the portfolio’s static assets on inexpensive static/CDN hosting; use Woven only for dynamic presence, events, inference, or interactive services.
- Configure billing budgets and alerts, but do not treat alerts as hard caps.
- Add application-level inference quotas, provider concurrency limits, maximum instance sizes/counts, and automation capable of shutting down approved experimental resources.
- Produce estimated monthly idle, expected, and stress costs before provisioning.

No architecture that silently implies thousands of dollars per month is acceptable for the prototype.

## 20. Security and observability

Enforce:

- authentication before session participation;
- server-assigned connection, principal, and entity identities;
- authorization on namespaces, sessions, spaces, channels, AI context, tools, and administration;
- strict payload and message-rate limits;
- malformed-message accounting and disconnect policy;
- no secrets in source, logs, prompts, traces, or generated artifacts;
- explicit CORS/origin and certificate policy;
- least-privilege cloud identities;
- auditable inference provider and tool selection;
- privacy-aware logging with content capture disabled by default.

Measure:

- active connections, sessions, spaces, entities, and subscriptions;
- input/output messages and bytes by delivery class;
- queue occupancy, replacement, drops, and slow-consumer disconnects;
- routing fan-out and spatial candidate counts;
- tick and handler latency;
- inference queue delay, time to first result, total latency, cancellation, expiration, provider fallback, token/compute usage, and configured cost;
- CPU, memory, network, and estimated monthly cloud cost.

Use correlation IDs across client events, relay routing, inference, tool calls, and resulting state changes.

## 21. Tests and benchmarks

Unit and integration coverage must include:

- authentication, join, leave, reconnect, and transport-loss cleanup;
- multiple simultaneous subscriptions;
- namespace/session/space isolation;
- nested anchored spaces and coarse parent proxies;
- cross-space transitions and epoch rejection;
- ownership and authority enforcement;
- uniform-grid boundary and radius behavior;
- state coalescing and stale-sequence rejection;
- bounded queue saturation and slow consumers;
- malformed, oversized, unknown-version, and unauthorized messages;
- FlatBuffers golden vectors across Rust, C#, and TypeScript;
- deterministic fake inference;
- inference cancellation, deadline, provider failure, and fallback;
- tool grants, stale-world rejection, and idempotent execution;
- equivalent semantics over each implemented transport.

Load scenarios:

- dense `BroadcastAll` room;
- sparse solar-system-style space;
- many small active spaces;
- nested ship/station interiors;
- frequent grid-cell movement;
- cross-space migration;
- mixed reliable and replaceable traffic;
- slow and lossy clients;
- mixed transports when available;
- inference bursts with strict quotas.

Make counts, frequencies, sizes, radii, and capacities configurable. Report throughput, p50/p95/p99 latency, replacements, drops, queue occupancy, CPU, memory, and test-machine configuration. Never invent performance results.

## 22. Required architecture records

Before or alongside implementation, record concise ADRs for:

1. transport-neutral core;
2. QUIC/WebTransport instead of raw UDP;
3. WebSocket as universal fallback;
4. space graph and anchored coordinate frames;
5. FlatBuffers and cross-language schema evolution;
6. bounded queues and delivery classes;
7. per-channel optional authority;
8. persistence outside the realtime hot path;
9. inference as an adjacent optional plane;
10. model proposals passing through a deterministic tool gateway;
11. initial Compute Engine deployment rather than Cloud Run or premature GKE;
12. cost and approval boundaries;
13. complexity deliberately deferred.

## 23. Delivery milestones

Proceed in this order. Keep every milestone runnable.

### Milestone 0 — Discovery and initialization

- Inspect the repository, legacy ZIP, installed toolchains, Git state, and local constraints.
- Write the initial ADRs and a concise implementation plan.
- Initialize the Rust workspace, formatting/lint configuration, baseline CI, and README.
- Record assumptions instead of blocking on non-critical unknowns.

### Milestone 1 — Core vertical slice

- Implement typed IDs, sessions, spaces, entities, subscriptions, delivery classes, bounded queues, and in-memory state.
- Implement FlatBuffers envelope/control messages and Rust conformance fixtures.
- Implement authentication interface and deterministic development provider.
- Implement a transport-independent worker/core test harness.

### Milestone 2 — Universal realtime path

- Implement binary WebSocket transport and Axum control plane.
- Implement reference Rust and TypeScript clients; generate C# bindings and a minimal compile/test example when the environment supports .NET.
- Demonstrate authentication, nested subscriptions, reliable events, latest-value state, snapshots, disconnect cleanup, and space transitions.

### Milestone 3 — Interest management and measurement

- Implement broadcast, topic, 2D grid, and 3D grid routing.
- Implement the load client and baseline scenarios.
- Profile before changing data structures or allocation strategies.

### Milestone 4 — QUIC and WebTransport

- Add native QUIC with reliable streams and datagrams.
- Add WebTransport when its server/client integration is proven in the selected environment.
- Keep automatic WebSocket fallback and the same protocol semantics.

### Milestone 5 — Minimal inference plane

- Add the capability registry, coordinator, deterministic test provider, scoped context, streaming lifecycle messages, and deterministic tool gateway.
- Demonstrate a ship/service AI conversation and a read-only diagnostic tool.
- Reject a deliberately stale state-changing proposal.
- Prove that disabling inference leaves relay tests and benchmarks unchanged.

### Milestone 6 — Cloud-ready staging plan

- Produce container, infrastructure modules, GitHub Actions, Workload Identity Federation instructions, deployment plan, rollback plan, and cost estimate.
- Do not provision billable resources without approval.
- After approval, deploy the smallest staging topology and run smoke/load validation.

### Milestone 7 — Project integrations

- Add a DARK FOREST example demonstrating solar-system, ship exterior, ship interior, and crew subscriptions.
- Add a portfolio example demonstrating a lightweight namespace, presence/events, and an optional inference-backed interactive feature.
- Keep both as consumers of the generic platform.

## 24. Initial definition of done

The foundation release is complete when:

- the workspace builds on a clean supported machine;
- formatting, linting, unit tests, and integration tests pass;
- no unbounded channel exists;
- disconnect always removes or expires presence;
- clients cannot publish outside authorized namespaces, sessions, spaces, entities, or channels;
- nested spaces and anchored frames work;
- replaceable state is coalesced and stale updates are rejected;
- Rust, TypeScript, and C# protocol fixtures agree;
- the WebSocket vertical slice works locally;
- inference can be disabled completely;
- the deterministic inference demonstration passes without paid services;
- the load runner produces reproducible measured results;
- the container builds;
- cloud infrastructure can be planned without being applied;
- the expected normal deployment is designed around the $10–30/month target;
- all deferred complexity is documented honestly.

## 25. Working and reporting style

- Lead with working outcomes, not speculative abstractions.
- Preserve user files and unrelated changes.
- Use small reviewable changes and keep the build green.
- Do not claim completion when only scaffolding exists.
- Do not fabricate benchmarks, costs, compatibility, or deployment success.
- When a dependency or API is current and consequential, verify its official documentation.
- When blocked by an approval boundary, finish all safe local preparation and present the exact proposed action, resources, permissions, estimated cost, rollback, and approval needed.
- At the end of each milestone, report what works, validation performed, measured results, remaining risks, cost implications, and the next smallest useful milestone.

Begin with Milestone 0, then continue into Milestone 1 when the local environment permits. Do not provision Google Cloud resources during initialization unless the user separately approves the exact plan.
