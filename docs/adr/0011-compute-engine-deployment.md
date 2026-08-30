# ADR 0011: Start Deployment on Compute Engine

## Status

Accepted

## Context

The relay owns long-lived sessions and needs both TCP and UDP for WebSocket, QUIC, and WebTransport. Cloud Run's request lifetime and best-effort reconnect affinity do not fit that lifecycle, while GKE adds operational and cost overhead before multiple shards or specialized pools exist.

## Decision

The first serious Google Cloud topology will be one small, non-GPU Compute Engine instance in one region, or a small managed instance group when lifecycle management is needed, behind a regional external passthrough Network Load Balancer supporting TCP and UDP. The Signalweave Node terminates TLS/QUIC. Deployment remains containerized and configuration-driven. Infrastructure will be represented in Terraform or OpenTofu, but no resources are provisioned without approval.

## Consequences

- The topology supports required transport behavior with predictable low initial cost.
- The team owns VM patching, process supervision, health, and rollout mechanics.
- Cloud Run may still host suitable stateless helpers.
- GKE is deferred until measured scale or specialized compute justifies it.
