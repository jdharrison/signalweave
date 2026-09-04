# WOVEN

WOVEN (WVN) is a reusable distributed realtime session, event, state, and inference network. The implementation is Rust-first, transport-independent at its core, and designed for browser, Unity/C#, and native clients without embedding application-specific simulation rules.

## Standalone and self-hosted

WOVEN is a general-purpose realtime relay, not a product tied to any particular front end or control plane. Run it standalone and self-hosted, the way you'd self-host Redis or Postgres, with zero dependency on any hosted service. A managed instance is available at [woven.host](https://woven.host) for teams that don't want to operate their own, but that hosted offering, and any control-panel UI built on top of it, is a separate, optional consumer of this open-source core, not a requirement for using it. This repository is open-source under the [Apache License 2.0](LICENSE) and contains the complete server source.

## Current feature set

- A transport-neutral Rust core with typed IDs, authenticated namespace/session/space/channel grants, nested spaces, subscriptions, entity ownership, channel authority, sequencing, snapshots, bounded state, rate limits, and priority-aware bounded/coalescing outbound queues.
- Explicit entity lifecycle support: server-assigned entities, disconnect cleanup, subscriber `EntityLeft` notifications, and atomic epoch-validated transitions that emit ordered leave/enter events.
- A versioned, size-prefixed FlatBuffers protocol with verifier-backed bounded decoding, semantic validation, typed control payloads, and checked-in golden fixtures proving byte-for-byte cross-language stability.
- An Axum HTTP control plane with public `/healthz`, `/readyz`, `/metrics`, and `/v1/capabilities` endpoints.
- Two interchangeable realtime transports sharing one bounded single-owner Tokio worker and protocol bridge: native QUIC and browser WebTransport, which map unreliable/best-effort traffic to datagrams. Both have real-socket conformance coverage.
- Uniform 2D/3D spatial routing for replaceable state, with owner-updated local positions, cell indexes, radius filtering, optional exact distance checks, and reliable-event bypass.
- A bounded local load runner for broadcast, topic, 2D-grid, and 3D-grid scenarios with measured publish latency, delivery, queue, and machine metadata.
- An integrated inference plane: a bounded per-request provider queue, a provider-neutral capability/request model, and a deterministic tool-call gateway that lets model output propose state changes without ever mutating state directly.
- Reference clients in Rust, TypeScript, C#, and Python. The TypeScript, C#, and Python packages use generated FlatBuffers bindings and validate decoding both a live frame from the Rust server and checked-in golden fixtures, proving cross-language wire compatibility.

See [`docs/status.md`](docs/status.md) and [`docs/adr`](docs/adr) for what's implemented and the architecture decisions behind it.

## Prerequisites

- The pinned current-stable Rust 1.98.0 toolchain with rustfmt and Clippy. [`rust-toolchain.toml`](rust-toolchain.toml) installs these automatically through rustup.
- A C++ compiler and CMake for the pinned vendored FlatBuffers compiler used during protocol builds.
- Node.js, only if you're working on the TypeScript client bindings or running its decode smoke tests.
- A .NET SDK (10.0+), only if you're working on the C# client bindings or running its tests.
- Python 3.10+, only if you're working on the Python client bindings or running its tests.
- Docker is optional and is not needed for ordinary Rust development.

A system `flatc` installation is not required. Cargo builds the pinned FlatBuffers 25.12.19 compiler from the `flatc-fork` crate and generates Rust bindings into `OUT_DIR`.

## Quick start

```sh
cargo test --workspace --all-targets --all-features
cargo run -p woven-server
```

The server listens on `127.0.0.1:8080` (HTTP control plane), `127.0.0.1:8081` (QUIC), and
`127.0.0.1:8082` (WebTransport), using an ephemeral self-signed development certificate for
the two UDP-based transports.

Common commands:

```sh
cargo fmt --all -- --check
cargo check-all
cargo lint
cargo test-all
cargo doc --workspace --no-deps
cargo run -p woven-protocol --example write_golden
cargo run -p woven-protocol --example write_tool_call_completed_fixture
```

The two `write_*_fixture` commands regenerate the checked-in protocol golden fixtures and should only produce a diff when the protocol intentionally changes.

## Workspace

- [`crates/woven-core`](crates/woven-core): transport-neutral sessions, spaces, ownership, authority, state, queues, and worker harness.
- [`crates/woven-protocol`](crates/woven-protocol): FlatBuffers schema, generated Rust bindings, bounded framing, validation, and fixtures.
- [`crates/woven-transport`](crates/woven-transport): shared worker handle, entity-lifecycle fan-out, and protocol bridge used by every transport adapter.
- [`crates/woven-transport-websocket`](crates/woven-transport-websocket): binary WebSocket adapter, the universal baseline transport.
- [`crates/woven-transport-quic`](crates/woven-transport-quic): native QUIC adapter (Quinn) with reliable streams and unreliable datagrams.
- [`crates/woven-transport-webtransport`](crates/woven-transport-webtransport): browser WebTransport adapter with reliable streams and unreliable datagrams.
- [`crates/woven-server`](crates/woven-server): Axum control plane and development server composition.
- [`crates/woven-inference-core`](crates/woven-inference-core): capability/request/provider data model and the `Provider` trait for the optional inference plane.
- [`crates/woven-inference-tools`](crates/woven-inference-tools): bounded tool registry and deterministic tool-call gateway; models propose, the gateway decides.
- [`crates/woven-inference-test-provider`](crates/woven-inference-test-provider): deterministic, scripted provider used in tests and local development.
- [`crates/woven-inference-coordinator`](crates/woven-inference-coordinator): runs an AI identity as an ordinary core connection and drives providers/tools.
- [`crates/woven-client-rust`](crates/woven-client-rust): native reference client and integration-test driver.
- [`crates/woven-client-ts`](crates/woven-client-ts): generated TypeScript FlatBuffers bindings and Node decode-validation scripts.
- [`crates/woven-client-csharp`](crates/woven-client-csharp): generated C# FlatBuffers bindings and xunit decode-validation tests (Unity-compatible; not a Cargo workspace member).
- [`crates/woven-client-python`](crates/woven-client-python): generated Python FlatBuffers bindings and pytest decode-validation tests (not a Cargo workspace member).
- [`crates/woven-loadtest`](crates/woven-loadtest): bounded local routing scenarios and measurement output.
- [`docs/adr`](docs/adr): accepted architecture records.

## Development configuration

The development server composition explicitly provisions namespace/session `1`, logical spaces `1` and `2`, reliable and latest-value channels, and the development token `dev-token`. When the inference plane is enabled, it also provisions one demo AI identity with its own dev token, entity, and status channel; see [`crates/woven-inference-coordinator`](crates/woven-inference-coordinator).

[`.env.example`](.env.example) documents the non-secret `woven_*` configuration contract for deployment-oriented runtime configuration, including `woven_INFERENCE_ENABLED` (the inference plane is off by default). Development authentication must be selected explicitly; authentication is never silently disabled. TLS termination and production certificate configuration are deferred with deployment infrastructure.

## Security baseline

The core assigns connection and entity identities server-side, requires authentication before participation, enforces explicit namespace/session/space/channel grants and entity ownership, validates sequence and epoch freshness, rate-limits publication, bounds state and payload sizes, and bounds every implemented queue. Protocol buffers received from untrusted peers are verified before field access. The inference plane follows the same rules: an AI identity is just another authenticated connection, and model-proposed state changes only take effect after a deterministic tool gateway validates them, never directly.

## License

[Apache License 2.0](LICENSE)
