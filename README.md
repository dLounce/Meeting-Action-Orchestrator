# Meeting Action Orchestrator

A review-first workflow that turns meeting recordings into evidence-backed recaps,
calendar events, and follow-up tasks.

The application uses isolated OpenAI specialists for transcription, extraction, recap
writing, and verification. A deterministic Python workflow owns state, validation,
approval, retries, and external writes. Models never receive calendar or task creation
tools.

## Why this architecture

- Every extracted decision and action item links back to transcript evidence.
- Ambiguous owners or deadlines remain unresolved instead of being guessed.
- External writes are created only after explicit approval.
- Approval is bound to an immutable payload digest.
- Every write has a deterministic idempotency key and a reconciliation path.
- Workflow state survives process restarts in SQLite.
- Sensitive model and tool payload tracing is disabled by default.

## Workflow

```mermaid
flowchart LR
    A[Upload] --> B[Transcribe]
    B --> C[Extract]
    C --> D[Write recap]
    D --> E[Verify]
    E --> F[Human review]
    F --> G[Approved outbox]
    G --> H[Calendar and tasks]
    H --> I[Receipts]
```

The specialists are hub-and-spoke workers. They do not call one another, and the write
path accepts only persisted approved intent IDs.

## Requirements

- Python 3.10 or newer
- `uv`
- An OpenAI API key
- A write-capable MCP server for calendar and task operations
- `ffmpeg` for production audio normalization

## Setup

```bash
uv sync --group dev
cp .env.example .env
uv run meeting-orchestrator database migrate
uv run meeting-orchestrator serve
```

Open `http://127.0.0.1:8000` after the server starts.

The application reads secrets only from the environment. The local `.env`, recordings,
transcripts, runtime database, logs, and credentials are excluded from Git.

## Configuration

The defaults in `.env.example` are suitable for local development. Required values:

| Variable | Purpose |
|---|---|
| `OPENAI_API_KEY` | OpenAI project credential |
| `MCP_SERVER_URL` | Streamable HTTP endpoint for the trusted MCP server |
| `MCP_AUTH_TOKEN` | Optional MCP bearer token |
| `MCP_CALENDAR_TOOL` | Allowlisted calendar creation tool |
| `MCP_TASK_TOOL` | Allowlisted task creation tool |

Model IDs, request limits, upload limits, tracing, and storage paths are configurable.
The service refuses to silently fall back to a different model.

## Development

```bash
make format
make check
```

The default test suite is offline. Paid API validation requires an explicit live-test
flag and uses a short fixture with a separate request ceiling.

## Safety boundary

Transcript content is untrusted input. Semantic workers have no external tools. The
application validates evidence, dates, targets, payload digests, and meeting revisions
before creating an outbox intent. A timeout after a possible remote write enters
reconciliation instead of issuing another create request.

See [ARCHITECTURE.md](ARCHITECTURE.md) and [SECURITY.md](SECURITY.md) for the full design.

## License

MIT
