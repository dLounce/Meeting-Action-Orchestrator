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
- A write-capable Streamable HTTP MCP server only when external delivery is enabled

`ffprobe` is distributed with FFmpeg by most package managers. The service does not
normalize or transcode recordings.

## Setup

```bash
uv sync --group dev
cp .env.example .env
openssl rand -hex 32
```

Put the generated value in `API_BEARER_TOKEN` and an OpenAI project key in
`OPENAI_API_KEY`. The bearer token must contain at least 32 UTF-8 bytes.

```bash
uv run meeting-orchestrator database migrate
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
| `WORKER_POLL_INTERVAL_SECONDS` | `1` | Processing and delivery polling interval |
| `PROCESSING_BATCH_SIZE` | `1` | Jobs claimed per stage and cycle |
| `DELIVERY_BATCH_SIZE` | `20` | Write intents handled per delivery cycle |

The model aliases and per-specialist output limits are configurable. The service does not
silently substitute another model.

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
digest; delivery responses use the current meeting version. Creation returns the meeting
path in `Location`, and approval returns the delivery path. Only the review digest ETag is
valid in `If-Match`.

| Method | Path | Required concurrency or idempotency header | Behavior |
|---|---|---|---|
| `POST` | `/v1/meetings` | `Idempotency-Key` | Ingest multipart metadata and recording; queues transcription |
| `GET` | `/v1/meetings/{meeting_id}` | none | Read meeting state |
| `POST` | `/v1/meetings/{meeting_id}/processing` | none | Return current state; does not run OpenAI inline |
| `GET` | `/v1/meetings/{meeting_id}/transcript` | none | Read the current transcript |
| `GET` | `/v1/meetings/{meeting_id}/review` | none | Read the current review and its strong ETag |
| `PATCH` | `/v1/meetings/{meeting_id}/review/actions/{action_id}` | `If-Match` | Create a human action revision |
| `PATCH` | `/v1/meetings/{meeting_id}/review/issues/{issue_id}` | `If-Match` | Resolve or accept a review issue |
| `PUT` | `/v1/meetings/{meeting_id}/review/actions/{action_id}/deliveries/{kind}` | `If-Match` | Enable or disable `task` or `calendar_event` delivery |
| `POST` | `/v1/meetings/{meeting_id}/approval` | `If-Match`, `Idempotency-Key` | Approve the exact current review |
| `GET` | `/v1/meetings/{meeting_id}/delivery` | none | Read intents and receipts |
| `POST` | `/v1/meetings/{meeting_id}/delivery/retry` | `Idempotency-Key` | Retry eligible selected writes |
| `POST` | `/v1/meetings/{meeting_id}/delivery/reconcile` | `Idempotency-Key` | Look up selected unknown writes |

There are currently no list, delete, purge, event-stream, or UI endpoints.

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

### Errors

Errors use `application/problem+json` with RFC 9457-style fields:

```json
{
  "type": "urn:meeting-action-orchestrator:problem:stale-review",
  "title": "Precondition Failed",
  "status": 412,
  "detail": "The review changed before the operation completed.",
  "instance": "/v1/meetings/00000000-0000-0000-0000-000000000000/approval",
  "request_id": "0123456789abcdef0123456789abcdef"
}
```

Validation errors can also include an `errors` array. A missing review precondition returns
`428`; a stale review returns `412`; idempotency and lifecycle conflicts return `409`.

## Portfolio integration

Do not call this API directly from public browser JavaScript. It has one static bearer
credential and does not implement browser sessions, per-user authorization, or CORS.

Use a server-side proxy:

```text
Browser -> portfolio backend or server route -> Meeting Action Orchestrator
```

Keep `API_BEARER_TOKEN` only in the portfolio server's secret environment. Authenticate
the portfolio user there, enforce ownership and CSRF controls, forward the multipart body,
and inject `Authorization`, `Idempotency-Key`, and `If-Match` on the server. Never expose
the orchestrator token in rendered HTML, public JavaScript, `NEXT_PUBLIC_*` variables, or
browser storage.

## Operational limits

- Readiness checks SQLite, the in-process workers, and MCP initialization when enabled. It
  does not call OpenAI or perform a side-effecting connector probe.
- Logs are structured JSON with common sensitive fields redacted. No metrics exporter,
  distributed tracing pipeline, or audit-event API is included.
- Recordings, transcripts, reviews, and delivery records persist until the operator removes
  them. There is no automated retention policy or per-meeting deletion workflow.
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
