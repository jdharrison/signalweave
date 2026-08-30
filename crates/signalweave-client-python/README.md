# Signalweave Python client bindings

Generated FlatBuffers bindings for the Signalweave Protocol v1. Not yet a full reference
client (unlike `signalweave-client-rust`) — this currently proves decode correctness
cross-language, mirroring `crates/signalweave-client-ts` and
`crates/signalweave-client-csharp`.

## Layout

- `generated/` — flatc `--python` output. Regenerate after any schema change (see below).
- `test/` — pytest tests with two kinds of coverage:
  - decoding the checked-in golden fixtures (`crates/signalweave-protocol/tests/fixtures`),
    proving cross-language equivalence with Rust, TypeScript, and C#;
  - spawning the real `signalweave-server` binary and decoding a live malformed-frame
    `ProtocolError` response over a real WebSocket, mirroring
    `signalweave-client-ts/test/decode-live-server.ts`.

Unlike the C# client, this crate depends on the `flatbuffers` package directly from PyPI
rather than vendoring a matching runtime — PyPI publishes `flatbuffers==25.12.19`, which
exactly matches the pinned `flatc-fork` compiler version this workspace uses everywhere
else, so there's no version mismatch to work around.

## Regenerating bindings

```sh
cargo build -p signalweave-protocol
FLATC=$(find target/debug/build -path '*/out/bin/flatc' -type f -print -quit)
"$FLATC" --python -o crates/signalweave-client-python/generated crates/signalweave-protocol/schemas/signalweave_v1.fbs
```

Regenerating produces only a diff when the protocol schema changes; commit it alongside the
schema change like the Rust, TypeScript, and C# bindings.

## Running the tests

```sh
cd crates/signalweave-client-python/test
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest
```

The live-server test runs `cargo run -p signalweave-server` itself; a debug build already
existing in `target/` makes it faster but is not required.
