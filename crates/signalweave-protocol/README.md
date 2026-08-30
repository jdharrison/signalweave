# Signalweave Protocol

Milestone 1 defines the transport-neutral Signalweave Protocol v1 envelope, typed control messages, bounded size-prefixed framing, safe owned Rust representations, and conformance fixtures. A transport adapter supplies and consumes complete frames; this crate does not depend on WebSocket, QUIC, WebTransport, or `signalweave-core` internals.

## Wire format

The canonical schema is `schemas/signalweave_v1.fbs`. It uses the `SWP1` FlatBuffers file identifier and a four-byte little-endian FlatBuffers size prefix. The prefix is the byte count after the prefix; `CodecLimits::max_frame_len` counts the complete frame, including those four bytes.

`Envelope` contains protocol version, stable message kind and delivery class values, namespace/session/space/channel IDs, optional entity semantics, space epoch, server tick, sender sequence, correlation/causal ID, payload type ID, payload bytes, and a typed control union. `EntityState`, `ReliableEvent`, and `Snapshot` use a non-zero payload type ID plus opaque domain bytes. Routing can inspect the envelope without understanding those domain bytes. Every other v1 message has a typed control table.

Scalar ID value `0` means absent or unassigned. Assigned IDs and established epochs start at `1`. The owned Rust API represents optional entity, correlation, and channel values with `Option<u64>` where appropriate.

## Safe codec and limits

`Codec::decode` applies bounds before accessing a FlatBuffer, checks the exact size prefix and file identifier, and then calls the generated FlatBuffers verifier API. Only after successful verification does it copy values into owned Rust types. It rejects:

- incomplete frames and trailing bytes;
- frames, opaque payloads, or control strings/vectors above configured limits;
- malformed FlatBuffers and incorrect file identifiers;
- protocol versions other than v1;
- unknown message or delivery values;
- message-kind/control-union mismatches; and
- domain payloads on controls or missing domain payload type IDs; and
- invalid per-message scope, ID, enum, version-range, or delivery semantics.

`Codec::expected_frame_len` lets stream transports read exactly one bounded frame after receiving the four-byte prefix. This crate does not allocate queues; transport implementations remain responsible for bounded queue and backpressure policy.

FlatBuffers' generated Rust accessors necessarily contain the runtime's low-level `unsafe` implementations. Generated files remain private in `OUT_DIR` and receive a narrowly scoped lint allowance. All checked-in Rust and all public framing/codec logic are safe Rust, generated unchecked root functions are not exposed, and untrusted buffers always use verifier-backed access.

## Reproducible generation

Normal Cargo builds do not require a system `flatc` and do not download a compiler at build-script runtime. The crate pins:

- `flatbuffers = =25.12.19`
- `flatbuffers-build = =0.2.4+flatc-25.12.19`
- `flatc-fork = =0.6.0+25.12.19-2026-02-06-03fffb2`

`build.rs` passes the compiler returned by `flatc_fork::flatc()` to `flatbuffers_build::BuilderOptions::set_compiler`. Rust output is generated below `OUT_DIR/flatbuffers`; it is not checked into source control.

```sh
cargo build
```

For manual cross-language review, first run `cargo build`. The vendored executable can then be located under the crate-local target directory (the hash is Cargo-generated):

```sh
VENDORED_FLATC=$(find target/debug/build -path '*/out/bin/flatc' -type f -print -quit)
"$VENDORED_FLATC" --version
"$VENDORED_FLATC" --rust -o /tmp/signalweave-rust schemas/signalweave_v1.fbs
"$VENDORED_FLATC" --ts -o /tmp/signalweave-ts schemas/signalweave_v1.fbs
"$VENDORED_FLATC" --csharp -o /tmp/signalweave-csharp schemas/signalweave_v1.fbs
```

A separately installed compiler may be substituted only when `flatc --version` reports `25.12.19`. TypeScript and C# generation commands are documented for compatibility work, but executing their bindings and cross-language golden tests remains Milestone 2. A .NET SDK/runtime is not available in the current development environment.

## Compatibility rules

Schema changes are reviewed against these rules:

1. Add fields only at the end of an existing table. Never reorder fields or change an existing field's type or meaning.
2. Never renumber enum values, union variants, message kinds, or existing table fields. Numeric values are the wire contract.
3. Reserve and deprecate removed values; never reuse them for a new meaning. Prefer leaving deprecated fields in place.
4. Every added scalar field must have a backward-compatible default. Added table, string, and vector fields must tolerate absence. Do not change an existing default.
5. Keep `0` reserved for unknown/none/absent semantics. New assigned IDs begin at `1`.
6. Domain payload type IDs are stable identifiers for separately versioned application schemas. The routing core treats their bytes as opaque.
7. `Hello` advertises a supported version range and `Capabilities` selects a mutually supported version. Once selected, every envelope uses that exact version. Peers reject an unsupported version rather than silently reinterpreting it.
8. Breaking syntax or semantics require a new negotiated protocol version, a new versioned namespace/schema, and new golden fixtures.

## Golden fixture

- Binary: `tests/fixtures/reliable_event_v1.swp`
- Expected values: `tests/fixtures/reliable_event_v1.expected.txt`

The fixture exercises every envelope metadata field and an opaque reliable-event payload. Regenerate it deterministically with:

```sh
cargo run -p signalweave-protocol --example write_golden
cargo test -p signalweave-protocol --test golden
```

The tests require both byte-for-byte encoding stability and equivalent verified decoding.
