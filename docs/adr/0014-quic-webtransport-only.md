# ADR 0014: Standardize on QUIC and WebTransport, Drop WebSocket

## Status

Accepted

## Context

Maintaining three transports (WebSocket, QUIC, WebTransport) creates three parallel
implementation surfaces, three test matrices, and three client code paths. WebSocket cannot
provide true unreliable delivery and suffers from TCP head-of-line blocking, which forces
workarounds in the core queue discipline. QUIC provides secure, encrypted, fast UDP with
0-RTT resumption, datagrams for unreliable delivery, and streams for ordered delivery.
WebTransport exposes the same QUIC capabilities to browsers through a standard web API.

Unifying on QUIC (native) and WebTransport (browser) gives every platform the same wire
behavior, the same envelope codec, and a single client API that auto-selects the appropriate
backend from the URL and target environment.

## Decision

- Remove the WebSocket transport entirely.
- Merge `woven-transport-quic` and `woven-transport-webtransport` into a single
  QUIC/WebTransport crate (`woven-transport-quic`).
- Keep `woven-transport` as the shared, network-agnostic worker bridge and envelope
  router.
- Make the reference Rust client transport-agnostic: it connects via QUIC for native targets
  and WebTransport for browser/WASM targets, using the same public `Client` API.
- Serve QUIC on a UDP port and WebTransport over HTTPS/HTTP-3 on an HTTP upgrade path.
- Advertise only `quic` and `webtransport` in `/v1/capabilities`.
- Use a single standardized server URI, `quic://host:port`, for every client. A browser maps
  it to WebTransport via the **deterministic port convention**: WebTransport listens one port
  above the native QUIC port, on `/webtransport` (e.g. `quic://host:8081` →
  `https://host:8082/webtransport`). Explicit `wtransport://…` / `https://…/webtransport`
  URLs override the convention. This keeps one URI across runtimes without a discovery
  round-trip; native clients use `quic://` directly as QUIC.

## Consequences

- One central transport/codec path and one native client API across all platforms.
- Faster iteration: changes to framing, handshake, or delivery mapping apply once.
- True unreliable/best-effort delivery via QUIC datagrams without TCP head-of-line blocking.
- Native clients use QUIC directly; browser clients use WebTransport. The reference Rust
  client speaks QUIC and WebTransport, and the TypeScript client is a real WebTransport
  browser client. These two are the focused client-validation surface; other client
  languages (earlier codec-only C# and Python bindings) are deferred and added one at a
  time once the Rust/TypeScript pair are stable.
- WebSocket-only environments (old enterprise proxies, legacy runtimes) are no longer
  supported.
- ADR 0003 is superseded.

## Deferred

- Collapsing `woven-transport` into the unified QUIC/WebTransport crate is left as a
  future simplification if the shared bridge proves to have no other consumers.
- Unifying the control plane (`/v1/capabilities`) onto the same HTTP/3 QUIC port as
  WebTransport was evaluated (via `hyperium/`h3`), but deferred: `h3-webtransport` today
  takes exclusive ownership of the underlying HTTP/3 connection (`hyperium/h3` #327), which
  prevents serving plain HTTP/3 requests and WebTransport sessions concurrently on one
  connection. The deterministic port convention sidesteps this without that rewrite; revisit
  `h3` when #327, #348 (accept_bi head-of-line), and #349 (session close API) are resolved.
