# Meeting Action Orchestrator

Meeting Action Orchestrator is a backend-only JSON API for turning meeting recordings
into evidence-backed recaps and approved follow-up actions. OpenAI handles transcription
and semantic analysis. Deterministic application code owns state transitions, review
revisions, approval, retries, and optional task or calendar delivery through MCP.

The repository does not include a browser UI. It is intended to sit behind a trusted
application backend, including a portfolio backend-for-frontend.

## Core guarantees

- Model-produced decisions and actions must reference transcript evidence.
- Ambiguous owners, deadlines, and unsupported claims remain review issues.
- Semantic agents receive no task or calendar tools.
- External writes are created only from a persisted, explicitly approved review digest.
- Ingest, approval, retry, and reconciliation requests have durable idempotency bindings.
- Processing jobs and write intents survive restarts and use leases for crash recovery.
- An uncertain remote write becomes `unknown` and must be looked up before another create.
- Meeting erasure atomically removes the database-known workflow graph before asynchronous
  recording cleanup and database checkpointing.
- Erasure persistence retains meeting, ingest, actor, and request identities only as
  scoped HMAC tokens needed for replay safety and resurrection prevention.
- OpenAI agent storage is disabled and sensitive tracing is off by default.

## Workflow

```mermaid
flowchart LR
    A[Multipart upload] --> B[Durable transcription job]
    B --> C[OpenAI transcription]
    C --> D[Durable extraction job]
    D --> E[Extract, recap, verify]
    E --> F[Review revision]
    F --> G[Explicit approval]
    G --> H[Approved write intents]
    H --> I[Optional MCP delivery]
    I --> J[Receipts or reconciliation]
```

Uploading a meeting queues processing automatically. The worker runs transcription first,
then extraction, recap writing, and verification. Approval atomically stores the approval,
the recap artifact, and any selected write intents. No external network call occurs inside
that transaction.

## Requirements

- Python 3.10 through 3.13
- [`uv`](https://docs.astral.sh/uv/)
- `ffprobe` on `PATH`
- An OpenAI API key
- A dedicated 32-to-64-byte HMAC secret for meeting-erasure identity tokens
- A write-capable Streamable HTTP MCP server only when external delivery is enabled

`ffprobe` is distributed with FFmpeg by most package managers. The service does not
normalize or transcode recordings.

## Setup

```bash
uv sync --group dev
cp .env.example .env
openssl rand -hex 32
openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n'
```

Use the first generated value for `API_BEARER_TOKEN`. Use the second, base64url-encoded
value as the erasure key, and set a non-secret key ID:

```dotenv
ERASURE_HMAC_ACTIVE_KEY_ID=local-1
ERASURE_HMAC_KEYS={"local-1":"<base64url-value-from-the-second-command>"}
```

Put an OpenAI project key in `OPENAI_API_KEY`. The bearer token must contain at least 32
UTF-8 bytes. Erasure HMAC configuration is mandatory at service startup even when the
deployment does not expect to issue deletion requests.

```bash
uv run meeting-orchestrator database migrate
uv run meeting-orchestrator erasure verify-keyring
uv run meeting-orchestrator serve
```

The service binds to `http://127.0.0.1:8000` by default. Startup also applies pending
database migrations, but running the migration command explicitly is recommended during
deployment.

```bash
curl -sS http://127.0.0.1:8000/health/live
curl -sS http://127.0.0.1:8000/health/ready
```

## Configuration

Settings come from environment variables or a local `.env` file. The complete set is in
[`.env.example`](.env.example).

| Variable | Default | Purpose |
|---|---:|---|
| `API_BEARER_TOKEN` | required | Static credential for every `/v1/*` request; 32 bytes minimum |
| `API_ACTOR_SUBJECT` | `portfolio-owner` | Actor recorded on human revisions and operations |
| `OPENAI_API_KEY` | required | OpenAI project credential |
| `DATABASE_PATH` | `runtime/orchestrator.sqlite3` | SQLite workflow database |
| `UPLOAD_DIRECTORY` | `uploads` | Private local recording store |
| `MAX_UPLOAD_BYTES` | `26214400` | Maximum recording size, 25 MiB by default |
| `OPENAI_TRANSCRIPTION_MODEL` | `gpt-4o-transcribe-diarize` | Transcription model |
| `OPENAI_WORKER_MODEL` | `gpt-5.4-mini` | Extraction and verification model |
| `OPENAI_RECAP_MODEL` | `gpt-5.6-terra` | Recap model |
| `OPENAI_MAX_REQUESTS_PER_RUN` | `5` | Semantic request budget for one extraction run |
| `OPENAI_MAX_OUTPUT_TOKENS_PER_RUN` | `12000` | Aggregate semantic output budget |
| `OPENAI_MAX_RETRIES` | `0` | Hidden SDK retries; only zero is accepted |
| `ERASURE_HMAC_ACTIVE_KEY_ID` | required | Key ID used for new erasure identity tokens |
| `ERASURE_HMAC_KEYS` | required | JSON object of key IDs to base64url-encoded HMAC secrets |
| `MEETING_ERASURE_BATCH_SIZE` | `20` | Erasure jobs handled per worker cycle |
| `MEETING_ERASURE_LEASE_SECONDS` | `300` | Erasure worker lease duration |
| `MEETING_ERASURE_MAX_REMEDIATIONS` | `3` | Maximum operator-requested cleanup remediations |
| `WORKER_POLL_INTERVAL_SECONDS` | `1` | Processing and delivery polling interval |
| `PROCESSING_BATCH_SIZE` | `1` | Jobs claimed per stage and cycle |
| `DELIVERY_BATCH_SIZE` | `20` | Write intents handled per delivery cycle |

The model aliases and per-specialist output limits are configurable. The service does not
silently substitute another model.

### Erasure key management

`ERASURE_HMAC_KEYS` must be a JSON object containing one to eight entries. Key IDs are 1
to 64 characters and may contain ASCII letters, digits, `.`, `_`, or `-`; the first
character must be alphanumeric. Every base64url value must decode to a unique 32-to-64-byte
secret. The active ID must be present in the object.

The service registers a non-secret verifier for every configured key and validates all
keys referenced by durable erasure records before workers start. It fails closed if a
configured secret changes, a referenced historical key is absent, or stored verifier data
does not match. The verification command applies migrations, registers new verifiers, and
prints only the number of verified keys:

```bash
uv run meeting-orchestrator erasure verify-keyring
```

Rotate keys during a maintenance window with API writes and workers stopped. Add the new
key without changing or removing existing entries, set its ID as
`ERASURE_HMAC_ACTIVE_KEY_ID`, run `erasure verify-keyring`, and restart the service only
after verification succeeds. Retain every historical key while any tombstone, erasure
job, or operation binding references it. Do not reuse a key ID with different secret
material. Because the keyring is capped at eight keys and tombstones are durable, plan a
reviewed token-migration design before a ninth rotation; the current service does not
rewrite historical tokens.

### Audio validation

Uploads must be actual MP3, M4A/MP4-audio, or WAV content, regardless of filename or
declared media type. Files are content-sniffed and inspected by `ffprobe`. A recording must
be non-empty, contain exactly one audio stream, stay within `MAX_UPLOAD_BYTES`, and be no
longer than two hours. The multipart request envelope is limited to the recording limit
plus 1 MiB.

### MCP delivery modes

MCP is optional. The delivery worker starts only when `MCP_SERVER_URL` and at least one
resource ID are configured.

| Mode | Required values |
|---|---|
| Disabled | Leave both resource IDs empty |
| Tasks only | `MCP_SERVER_URL`, `MCP_TASK_RESOURCE_ID` |
| Calendar only | `MCP_SERVER_URL`, `MCP_CALENDAR_RESOURCE_ID` |
| Tasks and calendar | `MCP_SERVER_URL`, both resource IDs |

`MCP_CONNECTOR_ID` identifies the configured workspace. `MCP_TASK_TOOL` and
`MCP_CALENDAR_TOOL` name the allowlisted create operations, while `MCP_LOOKUP_TOOL` must
find a prior result by idempotency key. `MCP_AUTH_TOKEN` is optional. Authenticated remote
connections require HTTPS; production remote connections require HTTPS even without a
token. Loopback HTTP is allowed for local development.

With delivery disabled, meetings still transcribe, produce reviews, and approve normally,
but approval creates no external write intents. Retry and reconcile commands return a
conflict response when connectors are not configured.

## HTTP API

Interactive documentation is disabled. The OpenAPI document is available at
`GET /openapi.json`.

`GET /health/live`, `GET /health/ready`, and `GET /openapi.json` are public. Every `/v1/*`
route requires:

```http
Authorization: Bearer <API_BEARER_TOKEN>
```

All HTTP responses include `X-Request-ID`, restrictive browser security headers, and
`Cache-Control: no-store`.

Meeting responses use an ETag such as `"meeting-3"`; transcript responses use the
transcript SHA-256; review responses and review mutations use the 64-character review
digest; erasure jobs use an ETag such as `"erasure-4"`; delivery responses use the current
meeting version. A mutation must use the validator documented for that route: meeting
version ETags protect processing retry, cancellation, and deletion; review digest ETags
protect review changes and approval; erasure version ETags protect cleanup remediation.

| Method | Path | Required concurrency or idempotency header | Behavior |
|---|---|---|---|
| `POST` | `/v1/meetings` | `Idempotency-Key` | Ingest multipart metadata and recording; queues transcription |
| `GET` | `/v1/meetings` | none | List meetings with an optional status filter and opaque cursor |
| `GET` | `/v1/meetings/{meeting_id}` | none | Read meeting state |
| `GET` | `/v1/meetings/{meeting_id}/processing` | none | Read durable processing jobs |
| `POST` | `/v1/meetings/{meeting_id}/processing/retry` | `If-Match`, `Idempotency-Key` | Retry eligible failed processing |
| `PUT` | `/v1/meetings/{meeting_id}/cancellation` | `If-Match`, `Idempotency-Key` | Cancel eligible processing |
| `GET` | `/v1/meetings/{meeting_id}/transcript` | none | Read the current transcript |
| `GET` | `/v1/meetings/{meeting_id}/review` | none | Read the current review and its strong ETag |
| `PATCH` | `/v1/meetings/{meeting_id}/review/actions/{action_id}` | `If-Match` | Create a human action revision |
| `PATCH` | `/v1/meetings/{meeting_id}/review/issues/{issue_id}` | `If-Match` | Resolve or accept a review issue |
| `PUT` | `/v1/meetings/{meeting_id}/review/actions/{action_id}/deliveries/{kind}` | `If-Match` | Enable or disable `task` or `calendar_event` delivery |
| `POST` | `/v1/meetings/{meeting_id}/approval` | `If-Match`, `Idempotency-Key` | Approve the exact current review |
| `GET` | `/v1/meetings/{meeting_id}/delivery` | none | Read intents and receipts |
| `POST` | `/v1/meetings/{meeting_id}/delivery/retry` | `Idempotency-Key` | Retry eligible selected writes |
| `POST` | `/v1/meetings/{meeting_id}/delivery/reconcile` | `Idempotency-Key` | Look up selected unknown writes |
| `DELETE` | `/v1/meetings/{meeting_id}` | `If-Match`, `Idempotency-Key` | Purge the meeting graph and create an erasure job |
| `GET` | `/v1/meeting-erasures/{erasure_job_id}` | none | Read erasure and recording-cleanup state |
| `POST` | `/v1/meeting-erasures/{erasure_job_id}/retry` | `If-Match`, `Idempotency-Key` | Retry eligible failed recording cleanup |

There are currently no event-stream or UI endpoints. Meeting retention is not automated.

### API workflow

Set local shell variables for the examples:

```bash
export API_URL=http://127.0.0.1:8000
export API_TOKEN='replace-with-the-private-api-token'
```

Create a meeting with a unique idempotency key. `metadata` is a JSON string inside the
multipart request, and `occurred_at` must include a UTC offset.

```bash
curl -i -X POST "$API_URL/v1/meetings" \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "Idempotency-Key: ingest-demo-001" \
  -F 'metadata={"title":"Release planning","occurred_at":"2026-08-07T09:00:00+05:30","timezone":"Asia/Calcutta","participants":[]}' \
  -F 'recording=@./meeting.m4a;type=audio/mp4'
```

The response is `201 Created` with `Location` and a meeting ETag such as
`"meeting-0"`. Save the returned `id`, then poll the meeting. Processing advances through
`ingested`, `transcribing`, `transcribed`, `extracting`, and `awaiting_approval`; retryable
and terminal failure states are returned in the same meeting resource.

```bash
export MEETING_ID='replace-with-meeting-id'
curl -sS "$API_URL/v1/meetings/$MEETING_ID" \
  -H "Authorization: Bearer $API_TOKEN"
curl -i "$API_URL/v1/meetings/$MEETING_ID/review" \
  -H "Authorization: Bearer $API_TOKEN"
```

Use the quoted 64-character ETag from the review response for every edit. Each successful
edit creates a new immutable review revision and returns a replacement ETag. An action
revision is a complete action edit: `title` and `timezone` are required, and omitted
optional owner, deadline, and notes fields are cleared.

```bash
export REVIEW_ETAG='"replace-with-review-content-digest"'
export ACTION_ID='replace-with-action-id'
curl -i -X PATCH "$API_URL/v1/meetings/$MEETING_ID/review/actions/$ACTION_ID" \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "If-Match: $REVIEW_ETAG" \
  -H 'Content-Type: application/json' \
  --data '{"title":"Publish the release brief","owner":"Shubham","due_date":"2026-08-10","due_time":"17:00:00","timezone":"Asia/Calcutta","notes":"Include rollout risks"}'
```

Resolve every blocking issue with `status` set to `resolved` or `accepted_risk`, and a
non-empty `resolution_note`. Configure delivery per action before approval. Calendar
delivery requires a resolved deadline and a configured calendar target.

```bash
curl -i -X PUT "$API_URL/v1/meetings/$MEETING_ID/review/actions/$ACTION_ID/deliveries/task" \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "If-Match: $REVIEW_ETAG" \
  -H 'Content-Type: application/json' \
  --data '{"enabled":true}'
```

Fetch the review again after the final edit, then approve its latest ETag:

```bash
export REVIEW_ETAG='"replace-with-latest-review-content-digest"'
curl -i -X POST "$API_URL/v1/meetings/$MEETING_ID/approval" \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "If-Match: $REVIEW_ETAG" \
  -H "Idempotency-Key: approval-demo-001"
```

The first approval returns `201`; a valid replay returns `200`. The response includes the
approved recap and initial write intents. Poll delivery for final receipts:

```bash
curl -sS "$API_URL/v1/meetings/$MEETING_ID/delivery" \
  -H "Authorization: Bearer $API_TOKEN"
```

Retry and reconciliation bodies contain up to 100 unique intent IDs. An empty list selects
all intents for that approval. Retry reconciles `unknown` intents before considering
another create and requeues eligible failed intents. Reconcile only looks up `unknown`
intents.

```bash
curl -sS -X POST "$API_URL/v1/meetings/$MEETING_ID/delivery/reconcile" \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "Idempotency-Key: reconcile-demo-001" \
  -H 'Content-Type: application/json' \
  --data '{"intent_ids":[]}'
```

Idempotency keys are 1 to 200 characters and use a restricted URL-safe character set.
Treat each key as permanently bound to one logical operation and selection.

### Meeting erasure

Fetch the meeting immediately before deletion and use its quoted meeting version ETag.
Deletion also requires a unique idempotency key:

```bash
export MEETING_ETAG='"meeting-3"'
curl -i -X DELETE "$API_URL/v1/meetings/$MEETING_ID" \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "If-Match: $MEETING_ETAG" \
  -H "Idempotency-Key: erase-demo-001"
```

A new request returns `202 Accepted`; a valid replay returns `200 OK`. Both return the
canonical erasure resource in `Location`, its strong `ETag`, and
`Idempotency-Replayed: true|false`. The representation is identical for a given erasure
ETag whether returned by creation, replay, or `GET`.

The public representation is limited to the erasure job ID, status and recording state,
reason, bounded retry and remediation counters, safe failure classification, version, and
timing fields. It omits the meeting ID, audio identity, storage key, hashes, HMAC key ID and
tokens, actor, and request key.

```bash
export ERASURE_JOB_ID='replace-with-erasure-job-id'
curl -i "$API_URL/v1/meeting-erasures/$ERASURE_JOB_ID" \
  -H "Authorization: Bearer $API_TOKEN"
```

The relational meeting graph is removed in the deletion transaction. The job remains
`active` while the service waits for the last reference to a shared recording, verifies
and removes the recording, or retries a busy WAL checkpoint. Terminal `completed` means
the database checkpoint and required recording cleanup have succeeded. Terminal `failed`
means recording cleanup needs operator remediation. A deletion can return `409 Conflict`
while processing is running, a delivery operation is live, a write is in flight, a remote
write outcome is unknown, or an integrity condition makes erasure unsafe.

Only a failed erasure with failed recording cleanup is retryable. Fetch the latest job
ETag, then submit a new idempotency key:

```bash
export ERASURE_ETAG='"erasure-4"'
curl -i -X POST "$API_URL/v1/meeting-erasures/$ERASURE_JOB_ID/retry" \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "If-Match: $ERASURE_ETAG" \
  -H "Idempotency-Key: erasure-remediation-001"
```

The retry returns `202 Accepted`, or `200 OK` for a valid replay. Remediation is bounded by
`MEETING_ERASURE_MAX_REMEDIATIONS`; jobs sharing the same physical recording are
reactivated as one atomic cleanup group.

The guarantee is deliberately bounded. It covers records known to the local SQLite
transaction snapshot and the managed recording in `UPLOAD_DIRECTORY`. It does not erase
database or filesystem backups, volume snapshots, SSD remanence, swap, filesystem
journals, copied or synchronized files, logs outside the application contract, OpenAI
provider-held data, or tasks and calendar events already created through MCP. See
[SECURITY.md](SECURITY.md#meeting-erasure-boundary) before handling production personal
data.

### Errors

Errors use `application/problem+json` with RFC 9457-style fields:

```json
{
  "type": "urn:meeting-action-orchestrator:problem:stale-review",
  "title": "Precondition Failed",
  "status": 412,
  "detail": "The review changed before the operation completed.",
  "instance": "/v1/meetings/{meeting_id}/approval",
  "request_id": "0123456789abcdef0123456789abcdef"
}
```

Validation errors can also include an `errors` array. A missing required precondition
returns `428`; a stale review, meeting, or erasure validator returns `412`; idempotency and
lifecycle conflicts return `409`.

## Portfolio integration

Do not call this API directly from public browser JavaScript. It has one static bearer
credential and does not implement browser sessions, per-user authorization, or CORS.

Use a server-side proxy:

```text
Browser -> portfolio backend or server route -> Meeting Action Orchestrator
```

Keep `API_BEARER_TOKEN` only in the portfolio server's secret environment. Authenticate
the portfolio user there, enforce ownership and CSRF controls, forward the multipart body,
and inject `Authorization`, `Idempotency-Key`, and `If-Match` on the server. Apply the same
server-side ownership check to erasure creation, status polling, and retry. Never expose
the orchestrator token in rendered HTML, public JavaScript, `NEXT_PUBLIC_*` variables, or
browser storage.

## Operational limits

- Readiness checks SQLite, recording storage, the registered erasure keyring, all runtime
  workers, the dedicated erasure worker, and MCP initialization when enabled. It does not
  call OpenAI or perform a side-effecting connector probe.
- Logs are structured JSON with common sensitive fields redacted. No metrics exporter,
  distributed tracing pipeline, or audit-event API is included.
- There is no automated retention policy. Operators must schedule erasure requests and
  separately govern backups and external provider data.
- SQLite and local recording storage target a single-node deployment. Backups, encryption
  at rest, and disaster recovery are operator responsibilities.

## Development

```bash
make format
make check
```

The test suite uses local fakes and does not make paid provider calls. See
[CONTRIBUTING.md](CONTRIBUTING.md), [ARCHITECTURE.md](ARCHITECTURE.md), and
[SECURITY.md](SECURITY.md).

## License

MIT
