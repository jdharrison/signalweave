# Signalweave C# client bindings

Generated FlatBuffers bindings for the Signalweave Protocol v1, intended for Unity and
other .NET consumers. Not yet a full reference client (unlike `signalweave-client-rust`) —
this currently proves decode correctness cross-language, mirroring
`crates/signalweave-client-ts`.

## Layout

- `generated/` — flatc `--csharp` output. Regenerate after any schema change (see below).
- `vendor/FlatBuffers/` — the FlatBuffers C# runtime source vendored from `flatc-fork`,
  matching the pinned compiler version exactly. See `vendor/FlatBuffers/README.md` for why
  this isn't a NuGet package reference.
- `test/` — an xunit test project (`dotnet test`) with two kinds of coverage:
  - decoding the checked-in golden fixtures (`crates/signalweave-protocol/tests/fixtures`),
    proving cross-language equivalence with Rust and TypeScript;
  - spawning the real `signalweave-server` binary and decoding a live malformed-frame
    `ProtocolError` response over a real WebSocket, mirroring
    `signalweave-client-ts/test/decode-live-server.ts`.

## Regenerating bindings

```sh
cargo build -p signalweave-protocol
FLATC=$(find target/debug/build -path '*/out/bin/flatc' -type f -print -quit)
"$FLATC" --csharp -o crates/signalweave-client-csharp/generated crates/signalweave-protocol/schemas/signalweave_v1.fbs
```

Regenerating produces only a diff when the protocol schema changes; commit it alongside the
schema change like the Rust and TypeScript bindings.

## Running the tests

```sh
cd crates/signalweave-client-csharp/test
dotnet test
```

Requires a .NET SDK matching `test/SignalweaveClientCSharp.Tests.csproj`'s
`TargetFramework` (currently `net10.0`). The live-server test runs `cargo run -p
signalweave-server` itself; a debug build already existing in `target/` makes it faster but
is not required.
