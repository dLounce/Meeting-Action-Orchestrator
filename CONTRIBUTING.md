# Contributing

## Local setup

```bash
uv sync --group dev
cp .env.example .env
```

## Change requirements

- Keep domain logic independent of framework and provider SDKs.
- Add or update tests for every behavior change.
- Preserve strict typed boundaries for model and tool output.
- Keep external writes approval-gated and idempotent.
- Never add recordings, transcripts, credentials, runtime databases, or generated logs.
- Run `make check` before opening a pull request.

## Commit style

Use short imperative subjects that describe the actual change. Keep refactors separate from
behavior changes when practical.
