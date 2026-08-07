# Contributing

## Local setup

Install Python 3.10 through 3.13, `uv`, and `ffprobe`, then install the development
environment:

```bash
uv sync --group dev
cp .env.example .env
```

The offline test suite does not need provider credentials. Running the service requires
`OPENAI_API_KEY`, an `API_BEARER_TOKEN` containing at least 32 UTF-8 bytes, and a dedicated
erasure HMAC keyring.

```bash
openssl rand -hex 32
openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n'
uv run meeting-orchestrator database migrate
uv run meeting-orchestrator erasure verify-keyring
uv run meeting-orchestrator serve
```

Use the base64url value from the second command as a 32-byte secret in
`ERASURE_HMAC_KEYS`, and set its key ID in `ERASURE_HMAC_ACTIVE_KEY_ID`. Never reuse the
API token or a provider credential as an erasure key. See
[README.md](README.md#erasure-key-management) for the keyring format and rotation process.

MCP is optional. Leave `MCP_TASK_RESOURCE_ID` and `MCP_CALENDAR_RESOURCE_ID` empty unless a
reviewed Streamable HTTP MCP server is available. A resource ID requires
`MCP_SERVER_URL`.

## Module ownership

- `domain` owns immutable business models, invariants, canonical hashes, workflow-event
  contracts, and pure state transitions. It must not import framework, persistence, or
  provider code.
- `agents` owns typed semantic contracts, prompts, specialist definitions, and model-run
  budgets. It must not own workflow state or side effects.
- `application` owns ports and use cases. It may depend on domain and typed agent contracts,
  but not FastAPI, SQLite, OpenAI, or MCP implementations.
- `infrastructure` owns provider, transport, filesystem, and SQLite state and event-history
  adapters.
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
- Keep meeting erasure fail closed around active or unknown work and inconsistent ownership.
- Never persist raw meeting, ingest, actor, or request identities in erasure tombstones or
  operation bindings.
- Preserve last-owner recording cleanup, identity verification, bounded remediation, and
  the WAL-checkpoint gate on erasure completion.
- Append workflow events in the same immediate unit of work as the state mutation they
  describe. Rollback, replay, rejected or stale commands, no-op transitions, and lease loss
  must not create duplicate history.
- Keep event metadata strict, versioned, bounded, and explicitly projected. Do not add raw
  transcripts, review prose, prompts, filenames, idempotency keys, write-intent IDs, or
  provider request IDs. Treat actor subjects and digests as sensitive even though they are
  allowlisted.
- Keep autonomous events actorless and derive human-event actors only from the
  authenticated principal, never a request body.
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

Workflow-event appends require an immediate unit of work and must remain contiguous within
each meeting. Preserve the duplicate-ID, contiguous-insert, update-rejection, and
direct-delete triggers. Parent meeting deletion is the deliberate exception: its foreign
key cascade must remove event history as part of bounded erasure. Do not describe these
SQLite controls as cryptographic tamper evidence or protection from a privileged database
administrator.

An erasure migration must account for every meeting-owned table in graph validation and
deletion. New references that cannot cascade safely must be deleted explicitly in the same
immediate transaction. Preserve `secure_delete=ON`, `synchronous=FULL`, key-verifier
integrity, immutable tombstones, and terminal-state triggers.

## API changes

Treat route paths, status codes, schemas, `ETag`, `If-Match`, `Idempotency-Key`,
`Location`, `X-Request-ID`, security headers, and `application/problem+json` as public
contracts.

New `/v1/*` routes must use bearer authentication and document every non-success response
as `application/problem+json`. Mutations of review content must use the strong review ETag;
meeting control and deletion use a meeting-version ETag; erasure remediation uses an
erasure-version ETag. Replayable commands must have a durable idempotency design before
implementation.

Update `README.md`, OpenAPI assertions, and API tests in the same change. Do not expose the
static API bearer token through browser code or add permissive CORS as a substitute for a
server-side integration.

Changes to `GET /v1/meetings/{meeting_id}/events` must preserve ascending sequence
pagination, a limit from 1 through 100, generic cursor errors, and a cursor bound to the
requested meeting. Map domain events and every metadata variant to the public response
explicitly; do not rely on generic model serialization. The route must check meeting
existence and read the page in one deferred unit of work, return `Cache-Control: no-store`,
and remain behind bearer authentication.

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

Erasure changes require coverage for stale versions, idempotent replay, active-work
blocking, malformed ownership, shared recordings, exact-file preflight, cleanup failure
and group remediation, WAL checkpoint retries, historical key validation, tombstone ingest
conflicts, privacy-safe responses, and restart recovery.

Workflow-event changes require coverage for event order, same-transaction rollback,
idempotent replay and no-op suppression, actor attribution, lease loss, failure and recovery
paths, strict metadata validation, corrupt stored rows, append restrictions, erasure
cascade, authenticated pagination, cross-meeting cursors, safe response projection, and
generic post-erasure `404` behavior.

## Security and data

Never add credentials, `.env` files, recordings, transcripts, runtime databases, logs, or
provider response captures to Git. Use synthetic meeting text in tests and documentation.
Before committing, inspect staged changes for secrets and personal data.

Changes that introduce retention, deletion, authentication, external writes, key rotation,
or sensitive logging need an explicit security review and corresponding updates to
`SECURITY.md`. Do not broaden an erasure claim beyond the local SQLite snapshot and managed
recording store without an implementation and test for every additional copy.

## Commit style

Use short imperative subjects that describe the actual change. Keep unrelated refactors,
behavior changes, and dependency updates separate when practical. Do not include dates in
commit subjects.

## Pull requests

A pull request should explain the behavior changed, the invariant or failure mode it
protects, and the verification performed. Call out schema migrations, public API changes,
new credentials, and any residual operational risk.
