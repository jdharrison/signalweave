# SIGNALWEAVE

SIGNALWEAVE is a reusable distributed realtime session, event, state, and inference network. The implementation is Rust-first, transport-independent at its core, and designed for browser, Unity/C#, and native clients without embedding application-specific simulation rules.

## Hosted and self-hosted

A managed Signalweave server is available at [signalweave.host](https://signalweave.host). This repository is open-source under the [MIT License](LICENSE) and contains the complete server source, so you can run and self-host Signalweave yourself.

## Current feature set

- A transport-neutral Rust core with typed IDs, authenticated namespace/session/space/channel grants, nested spaces, subscriptions, entity ownership, channel authority, sequencing, snapshots, bounded state, rate limits, and priority-aware bounded/coalescing outbound queues.
- Explicit entity lifecycle support: server-assigned entities, disconnect cleanup, subscriber `EntityLeft` notifications, and atomic epoch-validated transitions that emit ordered leave/enter events.
- A versioned, size-prefixed FlatBuffers protocol with verifier-backed bounded decoding, semantic validation, typed control payloads, and checked-in golden fixtures.
- An Axum HTTP control plane with public `/healthz`, `/readyz`, `/metrics`, and `/v1/capabilities` endpoints, plus a binary WebSocket endpoint.
- A bounded single-owner Tokio worker and WebSocket adapter supporting handshake, authentication, join/leave, nested subscriptions, reliable fan-out, latest-value coalescing, snapshots, transitions, and clean disconnects.
- Uniform 2D/3D spatial routing for replaceable state, with owner-updated local positions, cell indexes, radius filtering, optional exact distance checks, and reliable-event bypass.
- A bounded local load runner for broadcast, topic, 2D-grid, and 3D-grid scenarios with measured publish latency, delivery, queue, and machine metadata.
- Reference Rust and TypeScript clients. The TypeScript package uses generated FlatBuffers bindings and validates decoding a live frame from the Rust server.

See [`docs/implementation-plan.md`](docs/implementation-plan.md) and [`docs/adr`](docs/adr) for delivery status and architecture decisions.

## Prerequisites

- The pinned current-stable Rust 1.98.0 toolchain with rustfmt and Clippy. [`rust-toolchain.toml`](rust-toolchain.toml) installs these automatically through rustup.
- A C++ compiler and CMake for the pinned vendored FlatBuffers compiler used during protocol builds.
- Docker is optional and is not needed for ordinary Rust development.

A system `flatc` installation is not required. Cargo builds the pinned FlatBuffers 25.12.19 compiler from the `flatc-fork` crate and generates Rust bindings into `OUT_DIR`.

## Quick start

```sh
cargo test --workspace --all-targets --all-features
```

Common commands:

```sh
cargo fmt --all -- --check
cargo check-all
cargo lint
cargo test-all
cargo doc --workspace --no-deps
cargo run -p signalweave-protocol --example write_golden
```

The final command regenerates the checked-in protocol fixture and should only produce a diff when the protocol intentionally changes.

## Workspace

- [`crates/signalweave-core`](crates/signalweave-core): transport-neutral sessions, spaces, ownership, authority, state, queues, and worker harness.
- [`crates/signalweave-protocol`](crates/signalweave-protocol): FlatBuffers schema, generated Rust bindings, bounded framing, validation, and fixtures.
- [`crates/signalweave-transport-websocket`](crates/signalweave-transport-websocket): bounded WebSocket adapter and single-owner core-worker bridge.
- [`crates/signalweave-server`](crates/signalweave-server): Axum control plane and development server composition.
- [`crates/signalweave-client-rust`](crates/signalweave-client-rust): native reference client and integration-test driver.
- [`crates/signalweave-client-ts`](crates/signalweave-client-ts): generated TypeScript FlatBuffers bindings and Node live-frame decoder validation.
- [`crates/signalweave-loadtest`](crates/signalweave-loadtest): bounded local routing scenarios and measurement output.
- [`docs/adr`](docs/adr): accepted architecture records.

## Development configuration

The development server listens on `127.0.0.1:8080` with WebSocket upgrades at `/ws`. Its built-in test composition explicitly provisions namespace/session `1`, logical spaces `1` and `2`, reliable and latest-value channels, and the development token `dev-token`.

[`.env.example`](.env.example) documents the non-secret `SIGNALWEAVE_*` configuration contract for deployment-oriented runtime configuration. Development authentication must be selected explicitly; authentication is never silently disabled. TLS termination and production certificate configuration are deferred with deployment infrastructure.

## Security baseline

The core assigns connection and entity identities server-side, requires authentication before participation, enforces explicit namespace/session/space/channel grants and entity ownership, validates sequence and epoch freshness, rate-limits publication, bounds state and payload sizes, and bounds every implemented queue. Protocol buffers received from untrusted peers are verified before field access.

## License

[MIT](LICENSE)
