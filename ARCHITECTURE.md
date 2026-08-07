# Architecture

## System boundary

Meeting Action Orchestrator is a local-first transactional workflow. OpenAI models
interpret meeting content, while application code controls execution.

```text
Browser
  -> FastAPI review application
    -> Meeting workflow
      -> Audio store
      -> OpenAI transcription
      -> Tool-less extraction agent
      -> Tool-less recap agent
      -> Tool-less verification agent
      -> SQLite state and outbox
    -> Approval service
      -> MCP write gateway
        -> Calendar
        -> Task manager
```

## Layers

`domain` contains immutable contracts, invariants, state transitions, canonical digests,
and idempotency rules.

`application` coordinates use cases through ports. It does not depend on FastAPI, the
OpenAI SDK, MCP transport details, or SQLite implementation details.

`agents` defines typed specialist contracts and the prompts used to produce them.

`infrastructure` implements storage, OpenAI calls, MCP calls, persistence, and external
resource reconciliation.

`api` maps HTTP requests to application services and renders review views.

## Meeting lifecycle

```text
ingested
  -> transcribing
  -> transcribed
  -> extracting
  -> awaiting_approval
  -> approved
  -> filing
  -> completed
```

Transcription and extraction have explicit failed states that can be retried. Filing can
finish as partially filed, failed, or completed. Cancellation is allowed only before
approval. Terminal writes are never silently rolled back.

## Review revisions

Extraction creates revision one. Every user edit creates a new immutable revision and
invalidates prior approval. A canonical JSON representation produces the review digest.
Approval records the revision, digest, actor, request key, and time.

The approval transaction creates the recap artifact and write intents together. A crash
cannot leave approved content without its durable outbox work.

## Write semantics

Each task or calendar event becomes one write intent. Its idempotency key is derived from
the schema version, target, meeting, approved review digest, write type, source action,
and canonical payload.

The gateway follows an ensure-and-reconcile contract:

1. Look up a prior receipt by idempotency key.
2. Return it when the payload digest matches.
3. Reject key reuse with a different payload.
4. Create the resource only when no prior result exists.
5. Mark uncertain outcomes for reconciliation before another create attempt.

Network calls never occur inside a database transaction.

## Concurrency and recovery

Meeting rows use optimistic versions. Background jobs and write intents use leases.
Expired processing leases become retryable. Expired write leases become uncertain because
the remote system may have accepted the request.

SQLite uses foreign keys, WAL mode, a busy timeout, and immediate transactions for claims.
The application database is the audit source of truth; model continuation state is not
business state.

## Privacy and observability

Logs contain run IDs, stage names, status, latency, request IDs, and token totals. They do
not contain recordings, transcript text, model payloads, OAuth tokens, or API keys.

OpenAI storage and sensitive tracing are disabled by default. Application events use safe
metadata and monotonically increasing sequence numbers.

## Decision records

- [ADR 0001: Deterministic workflow coordination](docs/decisions/0001-deterministic-workflow.md)
