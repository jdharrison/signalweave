# ADR 0012: Treat Cost and Approval Boundaries as Architecture Constraints

## Status

Accepted

## Context

Cloud, inference, public exposure, IAM, and GPU choices can create financial or security risk even when technically reversible. The prototype must remain operable within a normal target of USD $10–30 per month.

## Decision

Cost is an acceptance criterion. Local work, tests, plans, read-only inspection, and estimates may proceed without new approval. Explicit approval is required before billable resource changes outside an approved envelope, GPU allocation, IAM expansion, secret changes, DNS or public-exposure changes, production deployment, destructive operations, or material cost increases. Plans must estimate idle, expected, and stress cost. Hosted inference receives application quotas; GPU experiments require approval and automatic shutdown. Cloud resources must carry ownership, environment, purpose, project, and cost-attribution labels.

## Consequences

- Risky operations pause at clear human decision points.
- Architecture and tests must support local, unpaid execution.
- Cost estimates, quotas, labels, and rollback plans become delivery artifacts.
- Approval gates may slow deployment but prevent accidental spend and exposure.
