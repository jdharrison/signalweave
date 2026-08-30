# Vendored FlatBuffers C# runtime

These files are the `net/FlatBuffers` runtime source bundled with the `flatc-fork` crate
(version `0.6.0+25.12.19-...`) that this workspace pins in `Cargo.toml` and uses to generate
every language's bindings, including `../../generated` here. Apache License 2.0, copyright
Google Inc.

They are vendored as source rather than referenced via the `Google.FlatBuffers` NuGet
package because NuGet's published releases top out at `25.2.10`, which does not define the
`FlatBufferConstants.FLATBUFFERS_25_12_19` version guard the generated code in `../generated`
compiles against — the runtime and generated code must be built from the exact same version.

Regenerate by copying from the same source `flatc-fork` uses to build the compiler:

```sh
FBSRC=$(find ~/.cargo/registry/src -maxdepth 2 -iname "flatc-fork-*" -type d -print -quit)
cp "$FBSRC"/flatbuffers/net/FlatBuffers/*.cs crates/signalweave-client-csharp/vendor/FlatBuffers/
```

Do not hand-edit these files.
