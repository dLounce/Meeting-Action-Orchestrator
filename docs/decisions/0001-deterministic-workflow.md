# ADR 0001: Deterministic Workflow Coordination

## Status

Accepted

## Context

The workflow has known stages and produces external side effects. A model-directed control
loop would make retry behavior, approval enforcement, persistence, and cost difficult to
reason about.

## Decision

Application code owns the workflow state machine. OpenAI specialists perform bounded
semantic work and return strict typed output. Specialists cannot call one another or invoke
calendar and task tools. Approved immutable write intents are executed through a dedicated
gateway.

## Consequences

The workflow is resumable, testable without network access, and auditable. Adding a new
semantic stage requires an explicit application change. This tradeoff is acceptable because
the workflow is transactional rather than conversational.
