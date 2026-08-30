# ADR 0007: Select Authority Per Channel

## Status

Accepted

## Context

Some applications need only an authenticated relay, while particular channels or components may require validation, server simulation, or ownership by an external service. A single global authority mode would either overcomplicate simple traffic or under-protect critical state.

## Decision

Authority will be configurable per channel, message family, entity, or component through a narrow `AuthorityPolicy` that can accept, reject, transform, or emit messages. Initial policies are `RelayOwned`, `ServerValidated`, `ServerSimulated`, and `ExternalOwned`. `RelayOwned` is the default but still enforces authentication, membership, subscriptions, entity ownership, sequence monotonicity, payload limits, and rate limits. Application ECS, physics, and gameplay rules remain outside the relay core.

## Consequences

- Projects can add authority only where its value justifies the cost.
- Policy selection and outcomes must be observable and auditable.
- Mixed-authority sessions require clear configuration and test coverage.
- Authoritative modules integrate through typed interfaces rather than privileged access to core internals.
