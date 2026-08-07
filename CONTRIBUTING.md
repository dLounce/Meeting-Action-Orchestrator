# Contributing

## Local setup

Install Python 3.10 through 3.13, `uv`, and `ffprobe`, then install the development
environment:

```bash
uv sync --group dev
cp .env.example .env
```

The offline test suite does not need provider credentials. Running the service requires
`OPENAI_API_KEY` and an `API_BEARER_TOKEN` containing at least 32 UTF-8 bytes.

```bash
openssl rand -hex 32
uv run meeting-orchestrator database migrate
uv run meeting-orchestrator serve
```

MCP is optional. Leave `MCP_TASK_RESOURCE_ID` and `MCP_CALENDAR_RESOURCE_ID` empty unless a
reviewed Streamable HTTP MCP server is available. A resource ID requires
`MCP_SERVER_URL`.

## Module ownership

- `domain` owns immutable business models, invariants, canonical hashes, and pure state
  transitions. It must not import framework, persistence, or provider code.
- `agents` owns typed semantic contracts, prompts, specialist definitions, and model-run
  budgets. It must not own workflow state or side effects.
- `application` owns ports and use cases. It may depend on domain and typed agent contracts,
  but not FastAPI, SQLite, OpenAI, or MCP implementations.
- `infrastructure` owns provider, transport, filesystem, and SQLite adapters.
- `api` owns the JSON HTTP contract, authentication, ETags, middleware, and problem
  responses. It must not contain business rules.
- `bootstrap.py` and `cli.py` own composition and process lifecycle.

Keep new code within these boundaries. Add an abstraction only when it represents a real
port or removes repeated policy.

## Behavioral requirements

- Keep model workers tool-less and side effects outside model execution.
- Preserve evidence validation for model-derived content.
- Require explicit approval of the current immutable review digest before delivery.
- Keep OpenAI and MCP calls outside database transactions.
- Preserve idempotency bindings for ingest, approval, retry, reconciliation, and writes.
- Never move an `unknown` write directly back to create; reconcile it first.
- Keep blocking filesystem and SQLite work off the async event loop.
- Return safe public failures without provider payloads or internal exception detail.
- Do not add a frontend to this repository; the supported integration surface is JSON HTTP.

## Database changes

Append a new numbered migration in `infrastructure/database.py`. Do not edit an applied
migration. Migrations must be transactional, forward-only, and safe to run more than once
through the migration command.

Repository writes must participate in a unit of work. Avoid network calls while a SQLite
transaction is open. Add integration coverage for migrations, repository round trips,
claim behavior, and optimistic concurrency when those contracts change.

## API changes

Treat route paths, status codes, schemas, `ETag`, `If-Match`, `Idempotency-Key`,
`Location`, `X-Request-ID`, security headers, and `application/problem+json` as public
contracts.

New `/v1/*` routes must use bearer authentication and document every non-success response
as `application/problem+json`. Mutations of review content must use the strong review ETag,
and replayable commands must have a durable idempotency design before implementation.

Update `README.md`, OpenAPI assertions, and API tests in the same change. Do not expose the
static API bearer token through browser code or add permissive CORS as a substitute for a
server-side integration.

## Provider changes

Provider adapters must translate SDK-specific errors into application-level categories.
Use strict structured outputs, explicit timeouts, bounded payloads, and disabled hidden
retries. Never log provider request bodies, model prompts, transcript text, or credentials.

MCP tools must remain exactly allowlisted. Contract tests must cover malformed responses,
idempotency conflicts, timeouts, unknown outcomes, and lookup-based reconciliation. A fake
must prove behavior without contacting a real provider.

## Tests and checks

Run the complete local gate before opening a pull request:

```bash
make check
```

The individual commands are:

```bash
make lint
make typecheck
make test
```

Use `make format` to apply Ruff formatting and safe lint fixes.

The default suite must remain offline, deterministic, and free of paid OpenAI or MCP calls.
Use fakes at provider boundaries and real SQLite temporary databases for persistence
behavior. Scale coverage with risk: domain invariants need focused unit tests, while
transactions, leases, migrations, and HTTP contracts need integration or contract tests.

## Security and data

Never add credentials, `.env` files, recordings, transcripts, runtime databases, logs, or
provider response captures to Git. Use synthetic meeting text in tests and documentation.
Before committing, inspect staged changes for secrets and personal data.

Changes that introduce retention, deletion, authentication, external writes, or sensitive
logging need an explicit security review and corresponding updates to `SECURITY.md`.

## Commit style

Use short imperative subjects that describe the actual change. Keep unrelated refactors,
behavior changes, and dependency updates separate when practical. Do not include dates in
commit subjects.

## Pull requests

A pull request should explain the behavior changed, the invariant or failure mode it
protects, and the verification performed. Call out schema migrations, public API changes,
new credentials, and any residual operational risk.
