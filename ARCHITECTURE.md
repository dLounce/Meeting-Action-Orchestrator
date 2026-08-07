# Architecture

## Scope

Meeting Action Orchestrator is a backend-only, review-first workflow service. Its FastAPI
surface is JSON and multipart HTTP; it does not render a frontend. OpenAI interprets
untrusted meeting content, while deterministic Python code controls persistence, review,
approval, idempotency, and side effects.

```text
Portfolio backend or API client
  -> FastAPI JSON transport
    -> Application use cases
      -> Domain invariants and state transitions
      -> SQLite unit of work
      -> Private local audio store
      -> OpenAI adapters
    -> In-process runtime supervisor
      -> Processing worker
      -> Recording cleanup and orphan-discovery workers
      -> Meeting-erasure worker
      -> Optional delivery worker
        -> Guarded MCP gateway
          -> Task or calendar provider
```

The HTTP server and workers run in one process. SQLite and the upload directory make the
current deployment model single-node.

## Module boundaries

`domain` contains immutable Pydantic models, lifecycle enums, canonical hashing,
validation rules, and pure projection logic. It has no framework, provider, transport, or
database dependency.

`agents` contains provider-neutral structured contracts, prompts, specialist definitions,
and execution budgets. Specialists are tool-less and do not call one another.

`application` contains ports and use cases for ingest, processing, review revision,
approval, delivery control, outbox execution, recording cleanup, and meeting erasure. It
depends on domain and typed agent contracts, not on FastAPI, SQLite, OpenAI, or MCP
implementations.

`infrastructure` implements the application ports for SQLite, local audio, OpenAI
transcription, OpenAI Agents SDK execution, Streamable HTTP MCP, and the guarded MCP
gateway.

`api` owns authentication, request and response schemas, RFC problem mapping, ETags,
middleware, and routes. Adapters move blocking application and SQLite calls off the event
loop.

`bootstrap.py` is the composition root. It constructs adapters, use cases, worker loops,
and API dependencies. `cli.py` exposes database migration, erasure-key verification, and
service startup commands.

Dependency direction points inward. Provider-specific exceptions are translated to
application-level failure categories before workflow policy evaluates them.

## Runtime lifecycle

The FastAPI lifespan owns the runtime supervisor:

1. Apply SQLite migrations in a worker thread.
2. Register and validate the complete erasure HMAC keyring.
3. Preflight the private recording store.
4. Start processing, recording-cleanup, orphan-discovery, and meeting-erasure loops.
5. Start the delivery loop when an MCP endpoint and at least one delivery target exist.
6. Close the MCP session and OpenAI clients during shutdown.

Ingest automatically creates the transcription job. The
`GET /v1/meetings/{meeting_id}/processing` endpoint reads persisted jobs; it does not
execute provider calls or create another job.

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

Transcription and extraction can enter `transcription_failed` or `extraction_failed`.
Delivery reduction can enter `partially_filed` or `filing_failed`. Cancellation exists in
the domain before approval but is not exposed by the current HTTP API.

### Durable processing

Processing state is stored separately from the meeting row. A job records its stage,
attempt count, next attempt time, lease owner, lease expiry, and safe failure summary.
Claims use short SQLite write transactions; OpenAI calls occur after the transaction
closes.

Transcription allows three attempts and extraction allows two. Retryable failures use
full-jitter exponential backoff. Expired processing leases are recovered as retryable
failures. A worker must still own a live lease before committing a stage result, which
prevents a stale worker from overwriting a recovered job.

Successful transcription stores the transcript and queues extraction. Extraction runs
three isolated semantic steps against typed contracts:

1. Extract decisions, actions, questions, risks, participants, and evidence.
2. Produce a recap from the canonical extracted record.
3. Verify the record and recap against the transcript.

Each specialist receives one turn and no tools. SDK retries are disabled. Per-call output
limits and a shared request/output budget bound one semantic run. Agent requests use
`store=False`, and sensitive trace data is disabled.

## Meeting erasure

Meeting erasure is an authenticated, idempotent workflow rather than a synchronous file
unlink. `DELETE /v1/meetings/{meeting_id}` requires the current quoted meeting version and
an idempotency key. A default immediate unit of work acquires a SQLite write reservation
before it validates and changes the graph.

The deletion transaction fails closed when it finds running processing, live delivery
control work, an in-flight or unknown write, an unrecognized work status, malformed
ownership, or an inconsistent relational graph. When validation succeeds, one transaction:

- deletes delivery-operation bindings and the meeting-owned relational graph;
- deletes the audio-asset row only when the recording has no other owner;
- creates recording cleanup work for the last owner, or records that the job is waiting on
  a shared asset;
- creates a durable erasure job, immutable tombstone, and operation binding.

Foreign-key cascades remove transcripts, review revisions, approvals, recap artifacts,
processing jobs, write intents, receipts, ingest bindings, and meeting-operation records.
The tombstone prevents the erased ingest identity from recreating the meeting. It stores
purpose-scoped HMAC-SHA-256 tokens instead of raw meeting, ingest, actor, or request
identities. The application checks candidates under every configured historical key so
rotation does not make old tombstones invisible.

Recording cleanup is asynchronous because SHA-based database ownership can make one
recording asset serve multiple meetings. The last owner schedules one identity-bound
cleanup job; earlier erasure jobs move from `waiting_shared` to the same cleanup group. The
filesystem adapter verifies the expected storage key, byte size, and SHA-256 before
quarantining and unlinking. A mismatch is a permanent failure, not permission to delete a
different file.

```text
active / waiting_shared
  -> active / cleanup_pending
  -> active / removed
  -> completed / removed

active / cleanup_pending
  -> active / failed
  -> failed / failed
  -> operator remediation -> active / cleanup_pending
```

The erasure worker uses leases and bounded retry scheduling. It requires a successful
`PRAGMA wal_checkpoint(TRUNCATE)` after relational deletion. Successful recording cleanup
removes its detail row and rearms the checkpoint before the job can become `completed`. A
busy or malformed checkpoint result is retryable. Failed cleanup can be remediated only
with the latest erasure ETag and a new idempotency key; every job linked to the shared
cleanup is reactivated atomically and each job has a bounded remediation counter.

The durable residue is intentionally narrow but not anonymous: the erasure job and its
safe status fields, HMAC tokens, key verifiers, tombstone, and idempotency bindings remain.
Anyone with an HMAC key and a candidate identity can test that candidate, so key material
is a high-value secret. The guarantee covers the database-known graph in the serialized
transaction snapshot and the managed local recording. It does not cover backups, copied
files, storage-device remanence, provider retention, or external MCP resources.

## Review and approval

The model creates revision one. Every action edit, issue resolution, or delivery selection
creates a new immutable revision. Canonical JSON over the review payload produces its
SHA-256 content digest and HTTP ETag.

Mutation requests must present that review ETag through `If-Match`. The application also
checks the meeting's optimistic version inside the use case. A concurrent edit therefore
fails instead of overwriting another revision.

Approval requires the current digest, no open blocking review issue, and valid transcript
evidence. Its transaction stores four related state changes together:

- the approval bound to the exact revision and digest;
- an immutable recap artifact;
- zero or more deterministic write intents;
- the meeting transition to `filing` or `completed`.

The approval idempotency key is persisted. A replay of the same meeting and digest returns
the existing result; incompatible reuse fails.

## Delivery boundary

MCP delivery is absent unless an endpoint and at least one task or calendar resource ID are
configured. Target identifiers are added to review directives before approval. A calendar
directive also requires a resolved action deadline.

Every approved task or event becomes a separate outbox intent. Its idempotency key derives
from stable workflow identity and canonical proposal content. The payload digest is stored
with the intent and must match before execution.

The executor follows this sequence:

1. Claim one due intent with a lease and mark it `in_flight`.
2. Reload the persisted approval, review digest, proposal, and live lease.
3. Reject any forged, stale, or unapproved intent before MCP.
4. Invoke only the configured task, calendar, or lookup tool.
5. Validate bounded `structuredContent` against the strict result contract.
6. Persist a receipt or a classified failure, then reduce the meeting status.

The MCP request carries the intent ID, approval ID, deterministic idempotency key, payload
digest, target, and approved proposal. The connector is responsible for honoring that
idempotency key at the remote provider.

### Unknown outcomes

A transport failure, malformed write response, timeout after dispatch, or expired write
lease may mean the provider accepted a create even though no receipt was stored. These
cases become `unknown`, not retryable creates.

An unknown intent can only use the lookup tool. A matching remote resource produces a
reconciled receipt. A confirmed absence moves the intent to a delayed retry. An unavailable
or ambiguous lookup leaves it unknown. This prevents automatic duplicate creation after
an uncertain result.

Retry and reconciliation API requests have their own durable binding to operation kind,
meeting, actor, and selected intent fingerprint. Reusing a key for a different operation
or selection is rejected.

## Persistence and concurrency

SQLite uses foreign keys, WAL mode, `secure_delete=ON`, `synchronous=FULL`, a busy timeout,
and explicit migrations. Write units of work use immediate transactions; read-only API
queries use deferred transactions. Meeting and write-intent updates use optimistic
versions. Processing jobs, cleanup jobs, write intents, and erasure jobs use leases so
abandoned work can be classified and recovered.

Each upload is streamed to a unique generated storage key with restrictive local
permissions. File type is detected from bytes and confirmed with `ffprobe`, and SHA-256 is
computed before the database chooses whether to create an audio asset or reuse an existing
asset with the same digest. A newly stored file that is not selected as the owned asset is
durably scheduled for identity-verified abandoned-ingest cleanup; it is not deleted inline
with ingest failure handling.

SQLite stores transcripts, immutable review revisions, approvals, recap artifacts,
processing jobs, write intents, receipts, delivery-operation bindings, erasure key
verifiers, HMAC tombstones, and erasure-operation bindings. It is the business source of
truth; OpenAI continuation state is not used as workflow state.

## HTTP boundary

Health and OpenAPI routes are public. Every `/v1/*` route uses one static bearer token and
maps it to the configured actor subject. Review mutations use strong digest ETags;
meeting-control and deletion mutations use meeting-version ETags; erasure remediation
uses erasure-version ETags. Side-effecting or replayable commands use explicit
`Idempotency-Key` headers.

Request middleware bounds declared and streamed bodies. Domain and application failures
are converted to `application/problem+json`; unknown exceptions receive a generic problem
without internal detail. Responses include a generated request ID, `no-store`, a restrictive
content security policy, frame denial, MIME sniffing protection, and related headers.

This boundary is designed for a trusted server-side caller. It does not provide CORS,
browser sessions, user-level authorization, or tenant isolation.

## Readiness and observability

Liveness confirms that the HTTP process can respond. Readiness checks:

- SQLite connectivity;
- recording-storage access;
- whether all registered and referenced erasure HMAC keys still validate;
- whether the runtime worker tasks and dedicated erasure worker are alive;
- MCP initialization when delivery is configured, or the explicit disabled mode.

Readiness does not call OpenAI and does not perform a connector write or lookup, so it
cannot prove provider credentials, quota, model availability, or downstream target access.

The CLI installs structured JSON logging. Known sensitive field names are redacted and long
strings are bounded. OpenAI tracing is disabled by default. The repository does not include
a metrics exporter, distributed tracing backend, log sink, or public audit-event stream.

## Current limitations

- No frontend is included.
- No event-stream endpoint is implemented.
- No automated retention scheduler or policy is implemented; erasure is requested per
  meeting.
- Erasure is bounded to the local transaction snapshot and managed recording store; it
  cannot sanitize backups, replicas, device remanence, provider systems, or MCP-created
  resources.
- Data is not encrypted at rest by the application.
- SQLite and local uploads are not a high-availability storage design.
- Provider health, backup, restore, metrics, alerting, and deployment hardening remain
  operator concerns.

## Decision records

- [ADR 0001: Deterministic workflow coordination](docs/decisions/0001-deterministic-workflow.md)
- [ADR 0002: Bounded meeting erasure](docs/decisions/0002-bounded-meeting-erasure.md)
