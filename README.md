# SIGNALWEAVE

SIGNALWEAVE is a reusable distributed realtime session, event, state, and inference network. The implementation is Rust-first, transport-independent at its core, and designed for browser, Unity/C#, and native clients without embedding application-specific simulation rules.

## Current status

The repository is in its first runnable vertical slice:

- **Milestone 0:** workspace, toolchain policy, CI, architecture decisions, implementation plan, and developer setup are complete.
- **Milestone 1 core:** typed identities, authenticated membership, nested spaces, subscriptions, entity ownership, authority policies, sequencing, snapshots, bounded/coalescing outbound queues, transport-loss cleanup, and a transport-independent worker harness are implemented.
- **Milestone 1 protocol:** a versioned FlatBuffers envelope, typed control messages, verified bounded decoding, and a Rust golden fixture are implemented.
- **Deferred:** the HTTP/WebSocket server, TypeScript and C# clients, spatial indexes, QUIC/WebTransport, inference, persistence providers, load testing, containers, and cloud infrastructure.

See [`docs/implementation-plan.md`](docs/implementation-plan.md) for milestone status and [`docs/adr`](docs/adr) for architecture decisions.

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
- [`docs/adr`](docs/adr): accepted architecture records.

Crates for the server and transport adapters will be added when they contain working behavior; the project deliberately avoids empty architectural scaffolding.

## Configuration contract

The future runtime uses one configuration model locally and in deployment. Precedence, from highest to lowest, is:

1. command-line arguments;
2. `SIGNALWEAVE_*` environment variables;
3. an explicitly selected configuration file;
4. safe built-in defaults.

[`.env.example`](.env.example) contains non-secret local examples. Development authentication must be selected explicitly; authentication is never silently disabled. There is no network listener in the current milestone, so local TLS is not yet required. TLS and development-certificate instructions will arrive with the WebSocket transport.

## Security baseline

The core assigns connection and entity identities server-side, requires authentication before participation, enforces explicit namespace/session/space/channel grants and entity ownership, validates sequence and epoch freshness, rate-limits publication, bounds state and payload sizes, and bounds every implemented queue. Protocol buffers received from untrusted peers are verified before field access.

## License

[MIT](LICENSE)
