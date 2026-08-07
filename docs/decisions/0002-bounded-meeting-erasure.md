# ADR 0002: Bounded Meeting Erasure

## Status

Accepted

## Context

A meeting spans relational workflow state and a local recording stored under a unique
per-upload key. SHA-based database ownership can make one selected audio asset serve
identical uploads, SQLite may retain deleted content in its WAL, and external OpenAI or MCP
systems may hold copies outside the application's control. Deletion must also preserve
enough non-plaintext state to make request replay safe and to prevent the same ingest
identity from silently recreating an erased meeting.

A synchronous row delete cannot satisfy these constraints. Retaining raw identifiers in a
tombstone would weaken the privacy outcome, while deleting during active or uncertain work
could lose the evidence needed to reconcile a remote side effect.

## Decision

Meeting erasure is a durable workflow with a deliberately bounded guarantee.

The request uses `BEGIN IMMEDIATE`, the current meeting-version ETag, and a durable
idempotency key. It validates bidirectional ownership and fails closed around running,
unknown, malformed, or unrecognized work. One transaction removes the database-known
meeting graph and creates the erasure job, tombstone, operation binding, and any last-owner
recording cleanup job.

Tombstones and operation bindings store purpose-scoped HMAC-SHA-256 tokens for meeting,
ingest, actor, request, and erasure-resource identities. The active key signs new tokens;
all configured historical keys participate in lookups. Persisted non-secret verifiers bind
key IDs to their secret material. Startup and readiness reject missing referenced keys or
changed secrets.

Physical recording deletion is asynchronous and begins only after the final database
reference is removed. Cleanup verifies the generated storage key, expected size, and
SHA-256 before unlinking. Erasure jobs that shared the recording converge on one cleanup
group. Cleanup remediation is versioned, idempotent, atomic across the group, and bounded.

SQLite uses foreign keys, WAL, `secure_delete=ON`, and `synchronous=FULL`. A successful WAL
truncate checkpoint is required after relational purge. Successful recording cleanup
removes its detailed cleanup record, resets the checkpoint marker, and requires another
checkpoint before completion.

Completion covers only the database-known copies visible to the serialized transaction and
the managed recording in the configured upload directory. It does not claim deletion from
backups, replicas, exports, synchronized copies, logs outside the application contract,
filesystem or storage-device remnants, OpenAI systems, MCP systems, or downstream task and
calendar resources.

## Consequences

Deletion normally returns `202 Accepted` and clients poll a separate erasure resource.
Strong erasure ETags and durable idempotency bindings make retries explicit and replayable.
The relational graph disappears before recording cleanup finishes, so the erasure resource
is the only supported progress view.

Erasure HMAC configuration is mandatory for service startup. Rotation requires a no-write
maintenance window, historical keys must remain available while referenced, and key IDs
must never be rebound to different secrets. The current eight-key limit requires a future
token-migration design before it is exhausted.

The system retains a narrow pseudonymous residue for replay safety and resurrection
prevention. Operators remain responsible for automated retention scheduling and deletion
procedures in backups, providers, connectors, and other external systems.
