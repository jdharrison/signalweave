# ADR 0005: Use FlatBuffers with Explicit Schema Evolution

## Status

Accepted

## Context

Woven needs compact binary messages decoded consistently by Rust, TypeScript, and C#/Unity. Generic JSON in the realtime path is too weakly typed, while language-specific models would fragment the protocol.

## Decision

The Woven Protocol will use versioned, language-neutral FlatBuffers schemas and generated bindings for Rust, TypeScript, and C#. Untrusted buffers must be verified before field access. Evolution will be additive where possible: preserve existing field IDs and meanings, never reuse retired IDs, reserve removed identifiers, provide defaults for added fields, deprecate before removal, and negotiate incompatible protocol versions. Golden binary fixtures will prove equivalent decoding across supported languages. Domain payloads remain typed but opaque to the routing core.

## Consequences

- One schema defines cross-language wire behavior with efficient access.
- Schema review and compatibility fixtures become release gates.
- Generation requires a pinned `flatc` workflow and checked tool compatibility.
- Breaking semantic changes require a new negotiated version rather than silent reinterpretation.
