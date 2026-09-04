# WOVEN

WOVEN is a reusable distributed realtime session, event, state, and inference network. The implementation is Rust-first, transport-independent at its core, and designed for browser and native clients without embedding application-specific simulation rules.

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
- Native Rust and browser TypeScript clients. Both use the generated FlatBuffers bindings and validate cross-language decoding against checked-in golden fixtures.

See [`docs/status.md`](docs/status.md) and [`docs/adr`](docs/adr) for what's implemented and the architecture decisions behind it.

## Prerequisites

- The pinned current-stable Rust 1.98.0 toolchain with rustfmt and Clippy. [`rust-toolchain.toml`](rust-toolchain.toml) installs these automatically through rustup.
- A C++ compiler and CMake for the pinned vendored FlatBuffers compiler used during protocol builds.
- Node.js 22+, only if you're working on the TypeScript browser client.

A system `flatc` installation is not required. Cargo builds the pinned FlatBuffers 25.12.19 compiler from the `flatc-fork` crate and generates Rust bindings into `OUT_DIR`.

## Installation

```sh
cargo install woven-server
cargo add woven-client
npm install @woven/client
```

`woven-server` is the self-hosted server executable. `woven-client` is the native Rust library, and `@woven/client` is the browser/WebTransport client.

## Local development

```sh
cargo test --workspace --all-targets --all-features
cargo run -p woven-server

cd crates/woven-client-ts
npm ci
npm run format:check
npm run lint
npm run typecheck
npm run test
npm run build
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
- [`crates/woven-transport-quic`](crates/woven-transport-quic): QUIC (Quinn) native and browser WebTransport adapter with reliable streams and unreliable datagrams.
- [`crates/woven-server`](crates/woven-server): Axum control plane and development server composition.
- [`crates/woven-inference-core`](crates/woven-inference-core): capability/request/provider data model and the `Provider` trait for the optional inference plane.
- [`crates/woven-inference-tools`](crates/woven-inference-tools): bounded tool registry and deterministic tool-call gateway; models propose, the gateway decides.
- [`crates/woven-inference-test-provider`](crates/woven-inference-test-provider): deterministic, scripted provider used in tests and local development.
- [`crates/woven-inference-coordinator`](crates/woven-inference-coordinator): runs an AI identity as an ordinary core connection and drives providers/tools.
- [`crates/woven-client-rust`](crates/woven-client-rust): `woven-client`, the native QUIC/WebTransport client library and integration-test driver.
- [`crates/woven-client-ts`](crates/woven-client-ts): `@woven/client`, the browser/WebTransport package and generated TypeScript FlatBuffers bindings.
- [`crates/woven-loadtest`](crates/woven-loadtest): bounded local routing scenarios and measurement output.
- [`docs/adr`](docs/adr): accepted architecture records.

## CI and releases

Pull requests and pushes to `main` run Rust formatting, Clippy, tests, and workspace builds, plus TypeScript formatting, static checks, tests, and package builds. CI never publishes packages, creates releases, or requires registry credentials.

Releases run only when a GitHub Release is published for a `vX.Y.Z` tag, or through **Actions → Release → Run workflow** with an existing tag and `confirm=publish`. The workflow checks that the tag version matches every published Rust package and `@woven/client`, validates the full workspace, publishes crates.io packages in dependency order, publishes npm with provenance, then attaches server archives and SHA-256 checksums to the GitHub Release.

Required GitHub Actions secrets:

- `CARGO_REGISTRY_TOKEN` — crates.io token authorized to publish the Woven crates.
- `NPM_TOKEN` — npm automation token when npm trusted publishing is not configured. Trusted publishing uses the workflow OIDC identity and provenance instead.

Supported `woven-server` binary platforms:

- Linux x86_64 (`x86_64-unknown-linux-musl`)
- macOS arm64 (`aarch64-apple-darwin`)
- macOS x86_64 (`x86_64-apple-darwin`)
- Windows x86_64 (`x86_64-pc-windows-msvc`)

### Maintainer release checklist

1. Update package versions consistently.
2. Confirm changelog/release notes.
3. Push the matching `vX.Y.Z` tag.
4. Create or publish the GitHub Release.
5. Monitor the release workflow.
6. Verify crates.io, npm, checksums, and downloaded binaries.

Cloud deployment and engine distributions are intentionally not part of this pipeline yet.

## Development configuration

The development server composition explicitly provisions namespace/session `1`, logical spaces `1` and `2`, reliable and latest-value channels, and the development token `dev-token`. When the inference plane is enabled, it also provisions one demo AI identity with its own dev token, entity, and status channel; see [`crates/woven-inference-coordinator`](crates/woven-inference-coordinator).

[`.env.example`](.env.example) documents the non-secret `WOVEN_*` configuration contract for deployment-oriented runtime configuration, including `WOVEN_INFERENCE_ENABLED` (the inference plane is off by default). Development authentication must be selected explicitly; authentication is never silently disabled. TLS termination and production certificate configuration are deferred with deployment infrastructure.

## Security baseline

The core assigns connection and entity identities server-side, requires authentication before participation, enforces explicit namespace/session/space/channel grants and entity ownership, validates sequence and epoch freshness, rate-limits publication, bounds state and payload sizes, and bounds every implemented queue. Protocol buffers received from untrusted peers are verified before field access. The inference plane follows the same rules: an AI identity is just another authenticated connection, and model-proposed state changes only take effect after a deterministic tool gateway validates them, never directly.

## License

[Apache License 2.0](LICENSE)
