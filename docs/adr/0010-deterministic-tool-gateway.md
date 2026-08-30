# ADR 0010: Route Model Proposals Through a Deterministic Tool Gateway

## Status

Accepted

## Context

Model output, retrieved content, and player input are untrusted. Prompt instructions alone cannot authorize state changes or guarantee current preconditions, idempotency, or deterministic application behavior.

## Decision

Models may emit typed `ToolCallProposal` values but never mutate shared state directly. A provider-neutral registry defines each tool's stable ID and version, schemas, permissions, actor restrictions, timeout, side-effect class, authority requirement, and idempotency behavior. The gateway validates identity, grants, arguments, state revision, and preconditions; applies authority policy; rejects stale or unsafe calls; invokes deterministic application code; records the outcome; and publishes resulting changes through the relay. Credentials never enter model context.

## Consequences

- Authorization and world-state integrity remain outside probabilistic model behavior.
- Tool calls are testable, auditable, and retryable according to declared idempotency.
- State-changing calls add validation latency and may be rejected after inference completes.
- Applications must maintain versioned schemas and deterministic handlers for exposed tools.
