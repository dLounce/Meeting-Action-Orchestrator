# Security Policy

## Reporting

Report vulnerabilities through GitHub private vulnerability reporting. Do not include API
keys, OAuth tokens, recordings, transcripts, or other personal data in an issue.

## Trust model

Meeting recordings, transcript text, filenames, model outputs, browser requests, and MCP
responses are untrusted. OpenAI and MCP credentials are trusted secrets supplied by the
operator. MCP servers must be reviewed and explicitly configured.

## Controls

- The server binds to loopback by default.
- Upload size, extension, detected media type, and duration are bounded.
- Stored files use generated names outside the static file tree.
- Semantic agents have no side-effecting tools.
- All model outputs use strict typed contracts and application validation.
- External writes require a current review digest and explicit approval.
- MCP tools are allowlisted by configured exact names.
- OAuth and MCP tokens never enter model context.
- Write retries use idempotency keys and uncertain-result reconciliation.
- Model and tool payload tracing is disabled unless explicitly enabled.
- HTTP responses use restrictive security headers and no-store caching.
- Secrets, recordings, transcripts, runtime databases, and logs are ignored by Git.

## Operator responsibilities

Use a dedicated test account during development, grant the smallest available OAuth scopes,
rotate exposed credentials immediately, review MCP server updates before deployment, and
keep the service unavailable from untrusted networks.

## Supported versions

Security updates apply to the latest release on `main` until the first stable release.
