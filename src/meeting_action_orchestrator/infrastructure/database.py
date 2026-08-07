from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path

SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS audio_assets (
    id TEXT PRIMARY KEY,
    storage_key TEXT NOT NULL UNIQUE,
    original_name TEXT NOT NULL,
    media_type TEXT NOT NULL,
    size_bytes INTEGER NOT NULL CHECK (size_bytes > 0),
    duration_ms INTEGER CHECK (duration_ms IS NULL OR duration_ms > 0),
    sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_audio_assets_sha256 ON audio_assets (sha256);
CREATE TABLE IF NOT EXISTS meetings (
    id TEXT PRIMARY KEY,
    ingest_key TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    audio_asset_id TEXT NOT NULL REFERENCES audio_assets (id),
    occurred_at TEXT,
    timezone TEXT NOT NULL,
    participants_json TEXT NOT NULL,
    status TEXT NOT NULL,
    current_transcript_id TEXT,
    current_review_id TEXT,
    approved_review_id TEXT,
    failure_json TEXT,
    version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_meetings_status_updated ON meetings (status, updated_at);
CREATE TABLE IF NOT EXISTS processing_jobs (
    id TEXT PRIMARY KEY,
    meeting_id TEXT NOT NULL REFERENCES meetings (id) ON DELETE CASCADE,
    stage TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
    next_attempt_at TEXT,
    lease_owner TEXT,
    lease_expires_at TEXT,
    last_failure_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (meeting_id, stage)
);
CREATE INDEX IF NOT EXISTS idx_processing_jobs_claim
ON processing_jobs (stage, status, next_attempt_at, lease_expires_at);
CREATE TABLE IF NOT EXISTS transcripts (
    id TEXT PRIMARY KEY,
    meeting_id TEXT NOT NULL REFERENCES meetings (id) ON DELETE CASCADE,
    audio_asset_id TEXT NOT NULL REFERENCES audio_assets (id),
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    language TEXT,
    segments_json TEXT NOT NULL,
    text TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    provider_request_id TEXT,
    usage_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_transcripts_meeting_created
ON transcripts (meeting_id, created_at DESC);
CREATE TABLE IF NOT EXISTS review_revisions (
    id TEXT PRIMARY KEY,
    meeting_id TEXT NOT NULL REFERENCES meetings (id) ON DELETE CASCADE,
    transcript_id TEXT NOT NULL REFERENCES transcripts (id),
    revision_number INTEGER NOT NULL CHECK (revision_number > 0),
    origin TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    content_digest TEXT NOT NULL,
    actor_id TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (meeting_id, revision_number)
);
CREATE INDEX IF NOT EXISTS idx_review_revisions_meeting
ON review_revisions (meeting_id, revision_number DESC);
CREATE TABLE IF NOT EXISTS approvals (
    id TEXT PRIMARY KEY,
    meeting_id TEXT NOT NULL UNIQUE REFERENCES meetings (id) ON DELETE CASCADE,
    review_revision_id TEXT NOT NULL REFERENCES review_revisions (id),
    review_digest TEXT NOT NULL,
    request_key TEXT NOT NULL UNIQUE,
    actor_id TEXT NOT NULL,
    approved_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recap_artifacts (
    id TEXT PRIMARY KEY,
    meeting_id TEXT NOT NULL REFERENCES meetings (id) ON DELETE CASCADE,
    approval_id TEXT NOT NULL UNIQUE REFERENCES approvals (id) ON DELETE CASCADE,
    format TEXT NOT NULL,
    content TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS write_intents (
    id TEXT PRIMARY KEY,
    meeting_id TEXT NOT NULL REFERENCES meetings (id) ON DELETE CASCADE,
    approval_id TEXT NOT NULL REFERENCES approvals (id) ON DELETE CASCADE,
    source_action_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    connector_id TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TEXT,
    lease_owner TEXT,
    lease_expires_at TEXT,
    external_id TEXT,
    external_url TEXT,
    last_failure_json TEXT,
    version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (approval_id, kind, source_action_id)
);
CREATE INDEX IF NOT EXISTS idx_write_intents_claim
ON write_intents (status, next_attempt_at, lease_expires_at);
CREATE TABLE IF NOT EXISTS write_attempts (
    id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL REFERENCES write_intents (id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    started_at TEXT NOT NULL,
    ended_at TEXT,
    outcome TEXT,
    provider_request_id TEXT,
    sanitized_failure_json TEXT,
    UNIQUE (intent_id, attempt_number)
);
CREATE TABLE IF NOT EXISTS write_receipts (
    id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL UNIQUE REFERENCES write_intents (id) ON DELETE CASCADE,
    idempotency_key TEXT NOT NULL UNIQUE,
    payload_digest TEXT NOT NULL,
    provider TEXT NOT NULL,
    external_id TEXT NOT NULL,
    external_url TEXT,
    reconciled INTEGER NOT NULL CHECK (reconciled IN (0, 1)),
    recorded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS workflow_events (
    id TEXT PRIMARY KEY,
    meeting_id TEXT NOT NULL REFERENCES meetings (id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    type TEXT NOT NULL,
    actor_id TEXT,
    safe_metadata_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    UNIQUE (meeting_id, sequence)
);
CREATE INDEX IF NOT EXISTS idx_workflow_events_meeting_sequence
ON workflow_events (meeting_id, sequence);
"""

SCHEMA_V2 = """
CREATE TABLE delivery_operation_bindings (
    request_key TEXT PRIMARY KEY,
    meeting_id TEXT NOT NULL REFERENCES meetings (id),
    operation TEXT NOT NULL CHECK (operation IN ('retry', 'reconcile')),
    actor_id TEXT NOT NULL CHECK (length(actor_id) BETWEEN 1 AND 200),
    selection_fingerprint TEXT NOT NULL CHECK (length(selection_fingerprint) = 64),
    created_at TEXT NOT NULL
);
CREATE INDEX idx_delivery_operation_bindings_meeting
ON delivery_operation_bindings (meeting_id, created_at);
"""

SCHEMA_V3 = """
CREATE INDEX idx_meetings_created_id
ON meetings (created_at DESC, id DESC);
CREATE INDEX idx_meetings_status_created_id
ON meetings (status, created_at DESC, id DESC);
"""

SCHEMA_V4 = """
CREATE TABLE meeting_operation_bindings (
    request_key TEXT PRIMARY KEY
        CHECK (length(request_key) BETWEEN 1 AND 200 AND request_key = trim(request_key)),
    meeting_id TEXT NOT NULL REFERENCES meetings (id) ON DELETE CASCADE,
    operation TEXT NOT NULL CHECK (operation IN ('processing_retry', 'cancellation')),
    actor_id TEXT NOT NULL
        CHECK (length(actor_id) BETWEEN 1 AND 200 AND actor_id = trim(actor_id)),
    stage TEXT CHECK (stage IN ('transcription', 'extraction')),
    expected_version INTEGER NOT NULL CHECK (expected_version >= 0),
    request_fingerprint TEXT NOT NULL
        CHECK (
            length(request_fingerprint) = 64
            AND request_fingerprint NOT GLOB '*[^0-9a-f]*'
        ),
    created_at TEXT NOT NULL,
    CHECK (
        (operation = 'processing_retry' AND stage IS NOT NULL)
        OR (operation = 'cancellation' AND stage IS NULL)
    )
);
CREATE INDEX idx_meeting_operation_bindings_meeting
ON meeting_operation_bindings (meeting_id, created_at);
"""

SCHEMA_V5 = """
UPDATE audio_assets
SET original_name = CASE media_type
    WHEN 'audio/mpeg' THEN 'recording.mp3'
    WHEN 'audio/mp4' THEN 'recording.m4a'
    WHEN 'audio/x-m4a' THEN 'recording.m4a'
    ELSE 'recording.wav'
END;
"""

SCHEMA_V6 = """
ALTER TABLE write_intents ADD COLUMN next_reconcile_at TEXT;
ALTER TABLE write_intents ADD COLUMN reconcile_attempt_count INTEGER NOT NULL DEFAULT 0
    CHECK (reconcile_attempt_count >= 0);
ALTER TABLE write_intents ADD COLUMN reconcile_lease_owner TEXT;
ALTER TABLE write_intents ADD COLUMN reconcile_lease_expires_at TEXT;
UPDATE write_intents
SET next_reconcile_at = updated_at
WHERE status = 'unknown';
CREATE INDEX idx_write_intents_reconcile
ON write_intents (
    status, next_reconcile_at, reconcile_lease_expires_at, created_at, id
);
ALTER TABLE delivery_operation_bindings ADD COLUMN status TEXT NOT NULL DEFAULT 'completed'
    CHECK (status IN ('pending', 'running', 'completed'));
ALTER TABLE delivery_operation_bindings ADD COLUMN lease_owner TEXT;
ALTER TABLE delivery_operation_bindings ADD COLUMN lease_expires_at TEXT;
ALTER TABLE delivery_operation_bindings ADD COLUMN completed_at TEXT;
ALTER TABLE delivery_operation_bindings ADD COLUMN version INTEGER NOT NULL DEFAULT 0
    CHECK (version >= 0);
ALTER TABLE delivery_operation_bindings ADD COLUMN updated_at TEXT;
UPDATE delivery_operation_bindings
SET completed_at = created_at, updated_at = created_at;
"""

SCHEMA_V7 = """
CREATE TABLE recording_cleanup_jobs (
    id TEXT PRIMARY KEY,
    storage_key TEXT NOT NULL UNIQUE
        CHECK (
            (
                length(storage_key) = 36
                AND substr(storage_key, 1, 32) NOT GLOB '*[^0-9a-f]*'
                AND substr(storage_key, 33, 4) IN ('.wav', '.mp3', '.m4a')
            )
            OR (
                length(storage_key) = 38
                AND substr(storage_key, 1, 1) = '.'
                AND substr(storage_key, 2, 32) NOT GLOB '*[^0-9a-f]*'
                AND substr(storage_key, 34, 5) = '.part'
            )
        ),
    expected_sha256 TEXT NOT NULL
        CHECK (
            length(expected_sha256) = 64
            AND expected_sha256 NOT GLOB '*[^0-9a-f]*'
        ),
    expected_size_bytes INTEGER NOT NULL CHECK (expected_size_bytes >= 0),
    reason TEXT NOT NULL
        CHECK (reason IN ('abandoned_ingest', 'orphan_reconciliation')),
    status TEXT NOT NULL
        CHECK (status IN ('ready', 'running', 'retry_wait', 'succeeded', 'failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
    next_attempt_at TEXT,
    lease_owner TEXT,
    lease_expires_at TEXT,
    last_failure_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    CHECK (attempt_count <= max_attempts),
    CHECK (
        (status = 'running' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR (status <> 'running' AND lease_owner IS NULL AND lease_expires_at IS NULL)
    ),
    CHECK (
        (status = 'retry_wait' AND next_attempt_at IS NOT NULL)
        OR (status <> 'retry_wait' AND next_attempt_at IS NULL)
    ),
    CHECK (
        (status IN ('retry_wait', 'failed') AND last_failure_json IS NOT NULL)
        OR (status NOT IN ('retry_wait', 'failed') AND last_failure_json IS NULL)
    ),
    CHECK (
        (status IN ('succeeded', 'failed') AND completed_at IS NOT NULL)
        OR (status NOT IN ('succeeded', 'failed') AND completed_at IS NULL)
    )
);
CREATE INDEX idx_recording_cleanup_jobs_claim
ON recording_cleanup_jobs (
    status, next_attempt_at, lease_expires_at, created_at, id
);
CREATE TRIGGER recording_cleanup_jobs_reject_owned_insert
BEFORE INSERT ON recording_cleanup_jobs
WHEN EXISTS (
    SELECT 1 FROM audio_assets WHERE storage_key = NEW.storage_key
)
BEGIN
    SELECT RAISE(ABORT, 'recording storage key is owned by an audio asset');
END;
CREATE TRIGGER recording_cleanup_jobs_reject_owned_update
BEFORE UPDATE OF storage_key ON recording_cleanup_jobs
WHEN EXISTS (
    SELECT 1 FROM audio_assets WHERE storage_key = NEW.storage_key
)
BEGIN
    SELECT RAISE(ABORT, 'recording storage key is owned by an audio asset');
END;
CREATE TRIGGER audio_assets_reject_cleanup_insert
BEFORE INSERT ON audio_assets
WHEN EXISTS (
    SELECT 1 FROM recording_cleanup_jobs WHERE storage_key = NEW.storage_key
)
BEGIN
    SELECT RAISE(ABORT, 'recording storage key is reserved for cleanup');
END;
CREATE TRIGGER audio_assets_reject_cleanup_update
BEFORE UPDATE OF storage_key ON audio_assets
WHEN EXISTS (
    SELECT 1 FROM recording_cleanup_jobs WHERE storage_key = NEW.storage_key
)
BEGIN
    SELECT RAISE(ABORT, 'recording storage key is reserved for cleanup');
END;
"""

SCHEMA_V8 = """
CREATE TABLE ingest_request_bindings (
    ingest_key TEXT PRIMARY KEY
        REFERENCES meetings (ingest_key) ON DELETE CASCADE
        CHECK (
            length(ingest_key) BETWEEN 1 AND 200
            AND ingest_key = trim(ingest_key)
        ),
    fingerprint_version INTEGER NOT NULL CHECK (fingerprint_version > 0),
    request_fingerprint TEXT NOT NULL
        CHECK (
            length(request_fingerprint) = 64
            AND request_fingerprint NOT GLOB '*[^0-9a-f]*'
        ),
    created_at TEXT NOT NULL
);
CREATE TRIGGER ingest_request_bindings_reject_update
BEFORE UPDATE ON ingest_request_bindings
BEGIN
    SELECT RAISE(ABORT, 'ingest request bindings are immutable');
END;
"""

MIGRATIONS = (
    (1, SCHEMA_V1),
    (2, SCHEMA_V2),
    (3, SCHEMA_V3),
    (4, SCHEMA_V4),
    (5, SCHEMA_V5),
    (6, SCHEMA_V6),
    (7, SCHEMA_V7),
    (8, SCHEMA_V8),
)


class Database:
    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def connect(self) -> sqlite3.Connection:
        self._path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        _restrict_permissions(self._path.parent, 0o700)
        connection = sqlite3.connect(self._path, timeout=5, isolation_level=None)
        _restrict_permissions(self._path, 0o600)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        _restrict_permissions(Path(f"{self._path}-wal"), 0o600)
        _restrict_permissions(Path(f"{self._path}-shm"), 0o600)
        connection.execute("PRAGMA synchronous = NORMAL")
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    @contextmanager
    def transaction(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
        try:
            yield connection
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()
        finally:
            connection.close()

    def migrate(self) -> int:
        with self.transaction(immediate=True) as connection:
            current = int(connection.execute("PRAGMA user_version").fetchone()[0])
            for version, sql in MIGRATIONS:
                if version <= current:
                    continue
                for statement in _migration_statements(sql):
                    connection.execute(statement)
                connection.execute(f"PRAGMA user_version = {version}")
                current = version
        return current

    def healthcheck(self) -> bool:
        with self.connect() as connection:
            result = connection.execute("SELECT 1").fetchone()
        return result is not None and result[0] == 1


def _restrict_permissions(path: Path, mode: int) -> None:
    with suppress(OSError):
        path.chmod(mode)


def _migration_statements(sql: str) -> Iterator[str]:
    pending: list[str] = []
    for character in sql:
        pending.append(character)
        if character != ";":
            continue
        statement = "".join(pending)
        if not sqlite3.complete_statement(statement):
            continue
        if statement.strip():
            yield statement
        pending.clear()
    trailing = "".join(pending).strip()
    if trailing:
        if not sqlite3.complete_statement(f"{trailing}\n;"):
            raise sqlite3.OperationalError("migration contains incomplete trailing SQL")
        yield trailing
