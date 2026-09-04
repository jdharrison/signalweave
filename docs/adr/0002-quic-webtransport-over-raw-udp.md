# ADR 0002: Prefer QUIC and WebTransport to Raw UDP

## Status

Accepted

## Context

Realtime state benefits from unreliable datagrams, while control messages, events, and snapshots require reliability. A raw UDP protocol would require Woven to recreate encryption, congestion control, reliability, fragmentation policy, NAT handling, and connection migration.

## Decision

Native clients will use QUIC where supported, and compatible browsers will use WebTransport. Both may carry reliable streams and unreliable datagrams under one application protocol. Raw UDP will not be a primary transport. Realtime datagrams will stay near a conservative 1200-byte budget; large snapshots use reliable streams, with no application-level datagram fragmentation.

## Consequences

- Mature transport security and congestion behavior replace custom networking machinery.
- Deployment must support TLS plus both TCP and UDP paths.
- Library and platform support constrain rollout, especially for browsers and Unity.
- Unsupported or blocked environments require the WebSocket fallback defined separately.
