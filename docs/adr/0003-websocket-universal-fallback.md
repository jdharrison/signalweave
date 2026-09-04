# ADR 0003: Use Binary WebSocket as the Universal Fallback

## Status

Superseded by [ADR 0014](0014-quic-webtransport-only.md)

## Context

QUIC and WebTransport are not uniformly available across browsers, enterprise networks, proxies, and Unity/.NET environments. Signalweave needs one broadly deployable path before adding specialized transports.

## Decision

Binary WebSocket will be the first implemented transport and the universal fallback selected through capability negotiation. It will carry the same versioned application envelope and preserve delivery-class semantics in the core and bounded writer queues. `LatestValue` updates may be replaced and best-effort events dropped before they enter the TCP stream.

## Consequences

- Browsers, native clients, and Unity have a common baseline.
- The core and WebSocket vertical slice can be completed before QUIC or WebTransport work begins.
- WebSocket cannot provide true unreliable delivery and may suffer TCP head-of-line blocking.
- Queue discipline remains necessary because stale data cannot be recalled after writing it to the stream.
