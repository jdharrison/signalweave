# ADR 0009: Run Inference as an Adjacent Optional Plane

## Status

Accepted

## Context

AI actors need relay context and must return events, but model latency, provider failure, cost, and SDK behavior are unsuitable for realtime worker loops. Deployments that do not use inference should not pay its runtime or dependency cost.

## Decision

Signalweave Intelligence will run adjacent to the relay behind asynchronous, provider-neutral interfaces. Realtime handlers submit bounded requests and return acceptance without waiting for completion. Providers advertise capabilities and operational attributes; the coordinator enforces deadlines, budgets, cancellation, fallback, privacy policy, and scoped context. Results return as correlated lifecycle messages. Inference dependencies will not enter `signalweave-core`, and the entire plane can be disabled.

## Consequences

- Relay latency and availability are isolated from model execution.
- Inference interactions are asynchronous and may expire or be cancelled.
- Context assembly and result publication require explicit authorization and revision tracking.
- Independent scaling and provider replacement are possible, with additional coordination and observability work.
