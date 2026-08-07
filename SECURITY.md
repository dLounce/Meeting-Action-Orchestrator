# Security Policy

## Reporting

Report vulnerabilities privately through GitHub Security Advisories or private
vulnerability reporting for this repository. Do not include API keys, bearer tokens, MCP
credentials, recordings, transcripts, or participant data in a public issue.

Rotate any credential that may have been exposed before sharing a redacted report.

## Trust model

Recordings, filenames, transcript text, model output, HTTP input, review edits, and MCP
responses are untrusted. `OPENAI_API_KEY`, `API_BEARER_TOKEN`, and `MCP_AUTH_TOKEN` are
operator-managed secrets. Configured MCP endpoints, tools, and resource identifiers are
privileged deployment inputs and must be reviewed before use.

The service assumes a trusted operator and a trusted server-side API caller. It is not a
multi-tenant identity or authorization system.

## Authentication boundary

Every `/v1/*` route requires the configured static bearer token. The token must contain at
least 32 UTF-8 bytes. The process stores only its SHA-256 digest in the authenticator and
uses constant-time digest comparison. `API_ACTOR_SUBJECT` is recorded as the actor for all
requests using that token.

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

Recordings are stored outside the HTTP static surface under generated digest-based names.
The upload directory and files receive restrictive permissions where the operating system
supports them. SQLite stores transcript text, participant details, reviews, approvals,
intents, and receipts.

The application does not encrypt recordings or SQLite at rest. Use encrypted disks,
restricted service accounts, private backups, and filesystem access controls in deployment.
Do not treat `.gitignore` as an access-control mechanism.

Semantic OpenAI requests use `store=False`. OpenAI agent tracing is disabled by default and
sensitive trace payloads are excluded. The transcription provider and semantic models
still receive meeting content required for their work; the operator is responsible for
provider account policy, regional requirements, consent, and data-processing obligations.

Recordings and derived records currently persist indefinitely. There is no automated
retention, per-meeting deletion, or purge API. Define deployment-level retention and backup
procedures before processing production personal data.

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

No metrics exporter, alerting integration, distributed tracing backend, audit-event API, or
intrusion detection is bundled. Readiness checks local database and worker state but does
not validate OpenAI credentials, quota, model availability, or downstream target access.

## Operator checklist

- Store all credentials in a secret manager or protected environment file.
- Use a unique 32-byte-or-longer API bearer token per deployment.
- Keep the API private and expose only a separately authenticated server-side proxy.
- Restrict filesystem permissions and encrypt disks and backups.
- Use HTTPS and least-privilege credentials for remote MCP access.
- Verify MCP idempotency and lookup behavior before enabling writes.
- Keep OpenAI tracing disabled unless a reviewed operational need requires it.
- Review delivery receipts and reconcile every `unknown` intent.
- Define retention, deletion, backup, and incident-response procedures.
- Rotate exposed credentials immediately.

## Supported versions

Security fixes apply to the latest revision on `main` until a stable release policy is
published.
