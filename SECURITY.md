# Security Policy

## Reporting

Report vulnerabilities privately through GitHub Security Advisories or private
vulnerability reporting for this repository. Do not include API keys, bearer tokens, MCP
credentials, recordings, transcripts, or participant data in a public issue.

Rotate any credential that may have been exposed before sharing a redacted report.

## Trust model

Recordings, filenames, transcript text, model output, HTTP input, review edits, and MCP
responses are untrusted. `OPENAI_API_KEY`, `API_BEARER_TOKEN`, `MCP_AUTH_TOKEN`, and every
secret in `ERASURE_HMAC_KEYS` are operator-managed secrets. Configured MCP endpoints,
tools, resource identifiers, and erasure key IDs are privileged deployment inputs and must
be reviewed before use.

The service assumes a trusted operator and a trusted server-side API caller. It is not a
multi-tenant identity or authorization system.

## Authentication boundary

Every `/v1/*` route requires the configured static bearer token. The token must contain at
least 32 UTF-8 bytes. The process stores only its SHA-256 digest in the authenticator and
uses constant-time digest comparison. `API_ACTOR_SUBJECT` is used as the actor when an
authenticated command emits workflow history or creates an operation binding.

Health routes and `/openapi.json` are public. The API does not implement user sessions,
token rotation, scopes, role-based access control, tenant isolation, or rate limiting.
Place it behind a trusted backend or gateway that supplies those controls when needed.

Generate a local token with:

```bash
openssl rand -hex 32
```

Do not expose the token to a browser. A portfolio or other frontend must call its own
server-side route, which authenticates the user and injects the orchestrator token from a
private environment variable. Never place the token in public JavaScript, HTML, browser
storage, source control, or a public environment-variable prefix.

The server-side route must authorize ownership independently for meeting and workflow-event
reads, deletion, erasure-status polling, and remediation. Possession of the orchestrator
bearer token grants access to all meetings, their event histories, and erasure jobs in this
single-operator deployment.

## Erasure key management

Meeting erasure uses purpose-scoped HMAC-SHA-256 tokens to preserve idempotency and prevent
an erased ingest identity from being recreated without retaining that raw identity. These
tokens are pseudonymous, not anonymous. Disclosure of a key permits offline testing of
candidate meeting IDs, ingest keys, actor IDs, request keys, and erasure job IDs.

`ERASURE_HMAC_ACTIVE_KEY_ID` and `ERASURE_HMAC_KEYS` are mandatory at service startup. The
keyring accepts one to eight unique base64url-encoded secrets, each decoding to 32 through
64 bytes. Keep the JSON value in a secret manager or protected environment file. Generate
a 32-byte base64url value with:

```bash
openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n'
```

Do not derive an erasure key from the API bearer token, an OpenAI key, a password, or
another deployment's key. Do not log, commit, transmit, or include key material in support
records. Database key-verifier rows are non-secret integrity checks; they are not a backup
of the keys.

Rotate keys only in a maintenance window with API writes and all workers stopped:

1. Generate a new secret and a never-before-used key ID.
2. Add the new entry while retaining every existing key unchanged.
3. Set the new key ID as active.
4. Run `uv run meeting-orchestrator erasure verify-keyring` against the target database.
5. Restart traffic only after verification succeeds.

The verifier command applies migrations and prints only the verified key count. Startup
and readiness fail closed if a configured secret changes, a referenced key is missing, or
a persisted verifier does not match. Never remove a historical key while a tombstone,
erasure job, or operation binding references it. The current service does not migrate
historical tokens, so the eight-key limit requires an explicit engineering change before
a ninth rotation.

## Input and prompt safety

- Request bodies are bounded whether or not `Content-Length` is present.
- Multipart recording size is bounded independently from its request envelope.
- Filenames are length-checked and cannot contain paths or null bytes.
- Recording type is detected from file bytes instead of trusting the extension or header.
- `ffprobe` must decode exactly one audio stream with a positive duration of at most two
  hours.
- Model inputs and outputs use strict typed contracts with size limits.
- Model-created decisions, actions, questions, and risks must reference transcript
  evidence that is checked against persisted segments.
- The extraction, recap, and verification specialists have no external tools and run for
  one turn.
- Request and output budgets bound semantic model execution; hidden SDK retries are
  disabled.

Prompt injection in a transcript can still influence semantic output. The protection is
containment and human approval, not a claim that arbitrary meeting content is trustworthy.
Review all extracted content and blocking issues before approval.

## External write controls

External delivery is disabled unless an MCP endpoint and a task or calendar resource ID
are both present. The gateway accepts only persisted write intents tied to the meeting's
approved review digest and a live worker lease.

Controls on the MCP path include:

- exact allowlisting of task, calendar, and lookup tool names;
- deterministic idempotency keys and canonical payload digests;
- no OpenAI or API credential in model context or MCP arguments;
- bounded JSON arguments and responses;
- strict validation of MCP `structuredContent` and receipt identity;
- rejection of response URLs containing credentials or non-HTTP schemes;
- reconciliation before any repeat create after an uncertain outcome;
- durable receipts and operation-level idempotency bindings.

The MCP server and downstream provider must enforce the supplied idempotency key. Review
the server's code, authentication, target resolution, logging, and update process before
granting access. Use the smallest available provider scopes and dedicated development
accounts during integration.

Authenticated non-loopback MCP connections require HTTPS. Production non-loopback MCP
connections require HTTPS even without `MCP_AUTH_TOKEN`. URL credentials are not accepted.

## Data handling

Recordings are stored outside the HTTP static surface under unique generated per-upload
names. The upload directory and files receive restrictive permissions where the operating
system supports them. SQLite stores transcript text, participant details, reviews,
approvals, intents, receipts, and meeting-scoped workflow events. Event rows can contain an
actor subject, content and identity digests, model identifiers, token counts, lifecycle
statuses, and bounded failure classifications. SHA-256-based database ownership may reuse
an existing audio asset; the unselected upload is durably scheduled for identity-verified
cleanup.

The application does not encrypt recordings or SQLite at rest. Use encrypted disks,
restricted service accounts, private backups, and filesystem access controls in deployment.
Do not treat `.gitignore` as an access-control mechanism.

Semantic OpenAI requests use `store=False`. OpenAI agent tracing is disabled by default and
sensitive trace payloads are excluded. The transcription provider and semantic models
still receive meeting content required for their work; the operator is responsible for
provider account policy, regional requirements, consent, and data-processing obligations.

Without an erasure request, recordings and derived records persist indefinitely. There is
no automated retention policy. The authenticated meeting-erasure API removes one meeting
on request, but operators must schedule those requests and define backup disposal
separately.

## Workflow audit boundary

`GET /v1/meetings/{meeting_id}/events` is an authenticated, `no-store`, meeting-scoped
history read. It returns events in ascending sequence order with a bounded page size and an
opaque cursor tied to the meeting and last sequence. The cursor checksum detects malformed
or cross-meeting use; it is not a secret, authorization token, digital signature, or proof
of an untampered history. The server-side caller must perform its own user-to-meeting
authorization before exposing an event page.

The API maps each event type to a strict public schema. It does not generically serialize
domain objects and does not expose transcripts, review prose, prompts, filenames,
idempotency keys, write-intent IDs, or raw provider request IDs. Raw client and server
request identifiers are represented only by one aggregate digest; a separate bounded
`request_count` reports model calls. `safe_metadata` means bounded and allowlisted, not
anonymous or non-sensitive: actor subjects, recording and review digests, workflow timing,
model identifiers, and usage counts can still support correlation or disclose operational
information.

Application code appends an event in the same immediate transaction as the state mutation
it describes. A rollback removes both, while replayed, stale, rejected, no-op, and lost
lease paths do not create duplicate history. Per-meeting sequences are contiguous. SQLite
triggers reject duplicate event IDs, sequence gaps, updates, and direct deletion while the
parent meeting exists.

This is an application audit trail, not a non-repudiation or compliance ledger. Events are
not hash-chained, signed, sent to immutable storage, or anchored outside SQLite. A
privileged database or filesystem operator can replace the file, change the schema, or
remove the triggers. History predating event emission is not reconstructed. Meeting
erasure intentionally cascades through the event rows, so the API cannot be used as a
retained erasure log.

## Meeting erasure boundary

`DELETE /v1/meetings/{meeting_id}` requires the current meeting ETag and a durable
idempotency key. The application refuses deletion while it observes running processing, a
live delivery operation, an in-flight or unknown write outcome, an unrecognized work
status, or an inconsistent ownership graph. This fail-closed behavior avoids deleting the
local evidence needed to reconcile an external side effect.

An accepted request serializes writes with `BEGIN IMMEDIATE` and atomically removes the
database-known meeting graph, creates HMAC tombstones and idempotency bindings, and
schedules recording cleanup when the meeting owns the last reference. The graph includes
the meeting, ingest binding, transcripts, review revisions, approvals, recap artifacts,
processing state, write intents and receipts, workflow events, delivery-operation bindings,
and meeting-control bindings. The event endpoint returns the generic meeting `404` after
this cascade. The retained erasure resource contains bounded status, failure, counter,
version, and timestamp fields; durable HMAC records do not contain the raw meeting, ingest,
actor, or request identities.

Recording deletion is identity-bound. Before unlinking, the local adapter verifies the
generated storage key, expected byte size, and full SHA-256. Shared content is retained
until its final database owner is erased. A mismatch fails cleanup instead of deleting an
unexpected file. Failed cleanup can be retried only through bounded, versioned,
idempotent remediation.

SQLite connections require foreign keys, WAL mode, `secure_delete=ON`, and
`synchronous=FULL`. The erasure worker requires WAL truncation after the relational purge;
successful recording cleanup rearms that checkpoint before the job becomes `completed`.
These controls reduce residual data in the live database files, but they are not a claim
of forensic sanitization.

The erasure guarantee is snapshot-bounded and covers only copies known to the serialized
SQLite transaction and the managed recording under `UPLOAD_DIRECTORY`. It does not erase:

- database backups, point-in-time recovery data, replicas, exports, or volume snapshots;
- copied recordings, desktop or cloud synchronization history, filesystem journals, swap,
  crash dumps, or application-external logs;
- flash translation layers, SSD wear-leveling remnants, discarded blocks, or other device
  remanence;
- data retained by OpenAI under the operator's provider agreement or account settings;
- MCP server logs or task and calendar resources already created in downstream systems;
- data manually exported, forwarded, or otherwise stored outside this service's managed
  database and upload directory.

An erasure completion must therefore trigger the operator's separate backup, provider,
connector, and incident-data procedures when those systems hold the same personal data.

## HTTP deployment

The application binds to loopback by default and does not configure CORS. Keep that default
for local use. For remote deployment, place the service behind a TLS-terminating reverse
proxy or private network boundary, restrict source networks, and set forwarded-header rules
explicitly.

Responses include `Cache-Control: no-store`, a restrictive content security policy,
same-origin opener and resource policies, frame denial, MIME-sniffing protection, a
no-referrer policy, and a restrictive permissions policy. HSTS is not enabled by the
application because TLS terminates outside it; configure HSTS at the TLS proxy.

The API returns a fresh `X-Request-ID` and includes it in problem responses. Do not place
credentials or personal data in URLs, since proxy and access logs commonly record paths.

## Logging and monitoring

The CLI emits structured JSON logs and redacts common sensitive keys. Application code does
not intentionally log request bodies, transcripts, prompts, recordings, or credentials.
Redaction is defense in depth, not a substitute for controlling what is sent to logging
APIs. Secure the log destination and review new fields before logging them.

An authenticated workflow-event read API is bundled, but no external audit sink, live
event stream, cross-meeting feed, metrics exporter, alerting integration, distributed
tracing backend, or intrusion detection is included. Readiness checks local database and
worker state but does not validate OpenAI credentials, quota, model availability, or
downstream target access.

## Operator checklist

- Store all credentials in a secret manager or protected environment file.
- Use a unique 32-byte-or-longer API bearer token per deployment.
- Use a separate randomly generated erasure HMAC keyring and verify it before startup.
- Retain every referenced historical erasure key and rotate only during no-write
  maintenance.
- Keep the API private and expose only a separately authenticated server-side proxy.
- Authorize meeting ownership before returning workflow-event history, and treat actor
  subjects and event digests as sensitive operational data.
- Restrict filesystem permissions and encrypt disks and backups.
- Use HTTPS and least-privilege credentials for remote MCP access.
- Verify MCP idempotency and lookup behavior before enabling writes.
- Keep OpenAI tracing disabled unless a reviewed operational need requires it.
- Review delivery receipts and reconcile every `unknown` intent.
- Define retention schedules and backup, provider, connector, and incident-data erasure
  procedures.
- Poll accepted erasure jobs to a terminal state and remediate failed recording cleanup.
- Rotate exposed credentials immediately.

## Supported versions

Security fixes apply to the latest revision on `main` until a stable release policy is
published.
