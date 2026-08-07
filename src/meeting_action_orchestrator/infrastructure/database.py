from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager, suppress
from pathlib import Path

from meeting_action_orchestrator.application.ports import WalCheckpointResult

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


def _utc_datetime_check(column: str) -> str:
    return f"""
        typeof({column}) = 'text'
        AND length({column}) = 32
        AND {column} GLOB
            '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9]+00:00'
        AND CAST(substr({column}, 1, 4) AS INTEGER) BETWEEN 1 AND 9999
        AND CAST(substr({column}, 6, 2) AS INTEGER) BETWEEN 1 AND 12
        AND CAST(substr({column}, 9, 2) AS INTEGER) BETWEEN 1 AND CASE
            WHEN CAST(substr({column}, 6, 2) AS INTEGER) = 2 THEN
                CASE
                    WHEN CAST(substr({column}, 1, 4) AS INTEGER) % 4 = 0
                        AND (
                            CAST(substr({column}, 1, 4) AS INTEGER) % 100 <> 0
                            OR CAST(substr({column}, 1, 4) AS INTEGER) % 400 = 0
                        )
                    THEN 29
                    ELSE 28
                END
            WHEN CAST(substr({column}, 6, 2) AS INTEGER) IN (4, 6, 9, 11) THEN 30
            ELSE 31
        END
        AND CAST(substr({column}, 12, 2) AS INTEGER) BETWEEN 0 AND 23
        AND CAST(substr({column}, 15, 2) AS INTEGER) BETWEEN 0 AND 59
        AND CAST(substr({column}, 18, 2) AS INTEGER) BETWEEN 0 AND 59
        AND julianday({column}) IS NOT NULL
    """


_SCHEMA_V9_TEMPLATE = """
DROP TRIGGER recording_cleanup_jobs_reject_owned_insert;
DROP TRIGGER recording_cleanup_jobs_reject_owned_update;
DROP TRIGGER audio_assets_reject_cleanup_insert;
DROP TRIGGER audio_assets_reject_cleanup_update;
DROP INDEX idx_recording_cleanup_jobs_claim;
ALTER TABLE recording_cleanup_jobs RENAME TO recording_cleanup_jobs_v7;
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
        CHECK (
            reason IN (
                'abandoned_ingest',
                'orphan_reconciliation',
                'meeting_erasure'
            )
        ),
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
INSERT INTO recording_cleanup_jobs (
    id, storage_key, expected_sha256, expected_size_bytes, reason, status,
    attempt_count, max_attempts, next_attempt_at, lease_owner, lease_expires_at,
    last_failure_json, created_at, updated_at, completed_at
)
SELECT
    id, storage_key, expected_sha256, expected_size_bytes, reason, status,
    attempt_count, max_attempts, next_attempt_at, lease_owner, lease_expires_at,
    last_failure_json, created_at, updated_at, completed_at
FROM recording_cleanup_jobs_v7;
DROP TABLE recording_cleanup_jobs_v7;
CREATE INDEX idx_recording_cleanup_jobs_claim
ON recording_cleanup_jobs (
    status, next_attempt_at, lease_expires_at, created_at, id
);
CREATE INDEX idx_recording_cleanup_jobs_sha_status
ON recording_cleanup_jobs (expected_sha256, status, id);
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
CREATE TRIGGER recording_cleanup_jobs_reject_identity_update
BEFORE UPDATE OF id, storage_key, expected_sha256, expected_size_bytes, reason,
    max_attempts, created_at
ON recording_cleanup_jobs
WHEN OLD.id IS NOT NEW.id
    OR OLD.storage_key IS NOT NEW.storage_key
    OR OLD.expected_sha256 IS NOT NEW.expected_sha256
    OR OLD.expected_size_bytes IS NOT NEW.expected_size_bytes
    OR OLD.reason IS NOT NEW.reason
    OR OLD.max_attempts IS NOT NEW.max_attempts
    OR OLD.created_at IS NOT NEW.created_at
BEGIN
    SELECT RAISE(ABORT, 'recording cleanup identity is immutable');
END;
CREATE TABLE erasure_key_verifiers (
    key_id TEXT PRIMARY KEY
        CHECK (
            typeof(key_id) = 'text'
            AND length(key_id) BETWEEN 1 AND 64
            AND key_id NOT GLOB '*[^A-Za-z0-9._-]*'
            AND substr(key_id, 1, 1) GLOB '[A-Za-z0-9]'
        ),
    verifier_version INTEGER NOT NULL
        CHECK (typeof(verifier_version) = 'integer' AND verifier_version > 0),
    verifier_digest TEXT NOT NULL
        CHECK (
            typeof(verifier_digest) = 'text'
            AND length(verifier_digest) = 64
            AND verifier_digest NOT GLOB '*[^0-9a-f]*'
        ),
    created_at TEXT NOT NULL
        CHECK (__UTC_CREATED_AT__)
);
CREATE TRIGGER erasure_key_verifiers_reject_update
BEFORE UPDATE ON erasure_key_verifiers
BEGIN
    SELECT RAISE(ABORT, 'erasure key verifiers are immutable');
END;
CREATE TABLE meeting_erasure_jobs (
    id TEXT PRIMARY KEY
        CHECK (
            typeof(id) = 'text'
            AND length(id) = 36
            AND substr(id, 9, 1) = '-'
            AND substr(id, 14, 1) = '-'
            AND substr(id, 19, 1) = '-'
            AND substr(id, 24, 1) = '-'
            AND length(replace(id, '-', '')) = 32
            AND replace(id, '-', '') NOT GLOB '*[^0-9a-f]*'
        ),
    token_version INTEGER NOT NULL
        CHECK (typeof(token_version) = 'integer' AND token_version > 0),
    token_key_id TEXT NOT NULL
        REFERENCES erasure_key_verifiers (key_id) ON DELETE RESTRICT ON UPDATE RESTRICT
        CHECK (typeof(token_key_id) = 'text'),
    meeting_token TEXT NOT NULL
        CHECK (
            typeof(meeting_token) = 'text'
            AND length(meeting_token) = 64
            AND meeting_token NOT GLOB '*[^0-9a-f]*'
        ),
    reason TEXT NOT NULL
        CHECK (typeof(reason) = 'text' AND reason IN ('user_request', 'retention')),
    erased_meeting_version INTEGER NOT NULL
        CHECK (
            typeof(erased_meeting_version) = 'integer'
            AND erased_meeting_version >= 0
        ),
    status TEXT NOT NULL
        CHECK (typeof(status) = 'text' AND status IN ('active', 'completed', 'failed')),
    recording_state TEXT NOT NULL
        CHECK (
            typeof(recording_state) = 'text'
            AND recording_state IN ('waiting_shared', 'cleanup_pending', 'removed', 'failed')
        ),
    pending_audio_asset_id TEXT
        CHECK (
            pending_audio_asset_id IS NULL
            OR (
                typeof(pending_audio_asset_id) = 'text'
                AND length(pending_audio_asset_id) = 36
                AND substr(pending_audio_asset_id, 9, 1) = '-'
                AND substr(pending_audio_asset_id, 14, 1) = '-'
                AND substr(pending_audio_asset_id, 19, 1) = '-'
                AND substr(pending_audio_asset_id, 24, 1) = '-'
                AND length(replace(pending_audio_asset_id, '-', '')) = 32
                AND replace(pending_audio_asset_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            )
        ),
    cleanup_job_id TEXT
        REFERENCES recording_cleanup_jobs (id) ON DELETE RESTRICT ON UPDATE RESTRICT
        CHECK (
            cleanup_job_id IS NULL
            OR (
                typeof(cleanup_job_id) = 'text'
                AND length(cleanup_job_id) = 36
                AND substr(cleanup_job_id, 9, 1) = '-'
                AND substr(cleanup_job_id, 14, 1) = '-'
                AND substr(cleanup_job_id, 19, 1) = '-'
                AND substr(cleanup_job_id, 24, 1) = '-'
                AND length(replace(cleanup_job_id, '-', '')) = 32
                AND replace(cleanup_job_id, '-', '') NOT GLOB '*[^0-9a-f]*'
            )
        ),
    database_checkpointed_at TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0
        CHECK (typeof(retry_count) = 'integer' AND retry_count >= 0),
    next_attempt_at TEXT,
    lease_owner TEXT
        CHECK (
            lease_owner IS NULL
            OR (
                typeof(lease_owner) = 'text'
                AND length(lease_owner) BETWEEN 1 AND 200
                AND lease_owner = trim(lease_owner)
            )
        ),
    lease_expires_at TEXT,
    last_failure_code TEXT,
    last_failure_disposition TEXT,
    last_failure_occurred_at TEXT,
    remediation_count INTEGER NOT NULL DEFAULT 0
        CHECK (typeof(remediation_count) = 'integer' AND remediation_count >= 0),
    max_remediations INTEGER NOT NULL
        CHECK (typeof(max_remediations) = 'integer' AND max_remediations BETWEEN 1 AND 10),
    version INTEGER NOT NULL DEFAULT 0
        CHECK (typeof(version) = 'integer' AND version >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE (token_version, token_key_id, meeting_token),
    UNIQUE (id, token_version, token_key_id, meeting_token),
    CHECK (remediation_count <= max_remediations),
    CHECK (
        (recording_state = 'waiting_shared'
            AND pending_audio_asset_id IS NOT NULL
            AND cleanup_job_id IS NULL)
        OR (
            recording_state IN ('cleanup_pending', 'failed')
            AND pending_audio_asset_id IS NULL
            AND cleanup_job_id IS NOT NULL
        )
        OR (
            recording_state = 'removed'
            AND pending_audio_asset_id IS NULL
            AND cleanup_job_id IS NULL
        )
    ),
    CHECK (
        (lease_owner IS NULL AND lease_expires_at IS NULL)
        OR (status = 'active' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    ),
    CHECK (
        next_attempt_at IS NULL
        OR (
            status = 'active'
            AND lease_owner IS NULL
            AND lease_expires_at IS NULL
            AND database_checkpointed_at IS NULL
        )
    ),
    CHECK (
        (recording_state = 'failed' AND last_failure_disposition IS 'permanent')
        OR (recording_state <> 'failed' AND status = 'active'
            AND database_checkpointed_at IS NULL
            AND (last_failure_disposition IS NULL
                OR last_failure_disposition IS 'retryable'))
        OR (recording_state <> 'failed'
            AND (status <> 'active' OR database_checkpointed_at IS NOT NULL)
            AND last_failure_disposition IS NULL)
    ),
    CHECK (
        (status = 'active' AND completed_at IS NULL
            AND NOT (
                database_checkpointed_at IS NOT NULL
                AND recording_state IN ('removed', 'failed')
            ))
        OR (status = 'completed' AND recording_state = 'removed'
            AND database_checkpointed_at IS NOT NULL
            AND completed_at IS NOT NULL
            AND last_failure_disposition IS NULL)
        OR (status = 'failed' AND recording_state = 'failed'
            AND database_checkpointed_at IS NOT NULL
            AND completed_at IS NOT NULL)
    ),
    CHECK (
        status = 'active'
        OR (
            lease_owner IS NULL
            AND lease_expires_at IS NULL
            AND next_attempt_at IS NULL
        )
    ),
    CHECK (
        __UTC_CREATED_AT__
        AND __UTC_UPDATED_AT__
        AND updated_at >= created_at
    ),
    CHECK (
        database_checkpointed_at IS NULL
        OR (
            __UTC_DATABASE_CHECKPOINTED_AT__
            AND database_checkpointed_at >= created_at
            AND database_checkpointed_at <= updated_at
        )
    ),
    CHECK (
        completed_at IS NULL
        OR (
            __UTC_COMPLETED_AT__
            AND completed_at >= updated_at
        )
    ),
    CHECK (
        lease_expires_at IS NULL
        OR (
            __UTC_LEASE_EXPIRES_AT__
            AND lease_expires_at > updated_at
        )
    ),
    CHECK (
        next_attempt_at IS NULL
        OR (
            __UTC_NEXT_ATTEMPT_AT__
            AND next_attempt_at >= updated_at
        )
    ),
    CHECK (
        (
            last_failure_code IS NULL
            AND last_failure_disposition IS NULL
            AND last_failure_occurred_at IS NULL
        )
        OR (
            last_failure_code IS NOT NULL
            AND last_failure_disposition IS NOT NULL
            AND last_failure_occurred_at IS NOT NULL
            AND typeof(last_failure_code) = 'text'
            AND typeof(last_failure_disposition) = 'text'
            AND typeof(last_failure_occurred_at) = 'text'
            AND last_failure_code IN (
                'database_sanitation_deferred',
                'recording_cleanup_rejected',
                'erasure_integrity_failed'
            )
            AND last_failure_disposition IN ('retryable', 'permanent')
            AND (
                (last_failure_code = 'database_sanitation_deferred'
                    AND last_failure_disposition = 'retryable')
                OR (
                    last_failure_code IN (
                        'recording_cleanup_rejected',
                        'erasure_integrity_failed'
                    )
                    AND last_failure_disposition = 'permanent'
                )
            )
            AND __UTC_LAST_FAILURE_OCCURRED_AT__
        )
    )
);
CREATE TRIGGER meeting_erasure_jobs_reject_identity_update
BEFORE UPDATE OF id, token_version, token_key_id, meeting_token, reason,
    erased_meeting_version, max_remediations, created_at
ON meeting_erasure_jobs
WHEN OLD.id IS NOT NEW.id
    OR OLD.token_version IS NOT NEW.token_version
    OR OLD.token_key_id IS NOT NEW.token_key_id
    OR OLD.meeting_token IS NOT NEW.meeting_token
    OR OLD.reason IS NOT NEW.reason
    OR OLD.erased_meeting_version IS NOT NEW.erased_meeting_version
    OR OLD.max_remediations IS NOT NEW.max_remediations
    OR OLD.created_at IS NOT NEW.created_at
BEGIN
    SELECT RAISE(ABORT, 'meeting erasure identity is immutable');
END;
CREATE TRIGGER meeting_erasure_jobs_reject_recording_rebind
BEFORE UPDATE OF pending_audio_asset_id, cleanup_job_id, database_checkpointed_at
ON meeting_erasure_jobs
WHEN (OLD.pending_audio_asset_id IS NULL AND NEW.pending_audio_asset_id IS NOT NULL)
    OR (
        OLD.pending_audio_asset_id IS NOT NULL
        AND NEW.pending_audio_asset_id IS NOT NULL
        AND OLD.pending_audio_asset_id IS NOT NEW.pending_audio_asset_id
    )
    OR (
        OLD.cleanup_job_id IS NULL
        AND NEW.cleanup_job_id IS NOT NULL
        AND OLD.recording_state <> 'waiting_shared'
    )
    OR (
        OLD.cleanup_job_id IS NOT NULL
        AND NEW.cleanup_job_id IS NOT NULL
        AND OLD.cleanup_job_id IS NOT NEW.cleanup_job_id
    )
    OR (
        OLD.cleanup_job_id IS NOT NULL
        AND NEW.cleanup_job_id IS NULL
        AND NEW.recording_state <> 'removed'
    )
    OR (
        OLD.database_checkpointed_at IS NOT NULL
        AND OLD.database_checkpointed_at IS NOT NEW.database_checkpointed_at
        AND NOT (
            NEW.database_checkpointed_at IS NULL
            AND (
                (
                    OLD.recording_state = 'waiting_shared'
                    AND NEW.recording_state = 'cleanup_pending'
                    AND OLD.pending_audio_asset_id IS NOT NULL
                    AND NEW.pending_audio_asset_id IS NULL
                    AND OLD.cleanup_job_id IS NULL
                    AND NEW.cleanup_job_id IS NOT NULL
                )
                OR (
                    OLD.recording_state IN ('cleanup_pending', 'failed')
                    AND NEW.recording_state = 'removed'
                    AND OLD.pending_audio_asset_id IS NULL
                    AND NEW.pending_audio_asset_id IS NULL
                    AND OLD.cleanup_job_id IS NOT NULL
                    AND NEW.cleanup_job_id IS NULL
                )
            )
        )
    )
    OR (
        OLD.recording_state = 'waiting_shared'
        AND NEW.recording_state <> 'waiting_shared'
        AND NOT (
            NEW.recording_state = 'cleanup_pending'
            AND OLD.pending_audio_asset_id IS NOT NULL
            AND NEW.pending_audio_asset_id IS NULL
            AND OLD.cleanup_job_id IS NULL
            AND NEW.cleanup_job_id IS NOT NULL
            AND NEW.database_checkpointed_at IS NULL
        )
    )
    OR (
        OLD.recording_state IN ('cleanup_pending', 'failed')
        AND NEW.recording_state = 'removed'
        AND NOT (
            OLD.pending_audio_asset_id IS NULL
            AND NEW.pending_audio_asset_id IS NULL
            AND OLD.cleanup_job_id IS NOT NULL
            AND NEW.cleanup_job_id IS NULL
            AND NEW.database_checkpointed_at IS NULL
        )
    )
    OR (
        OLD.recording_state = 'failed'
        AND NEW.recording_state NOT IN ('failed', 'removed')
        AND NOT (
            OLD.status = 'failed'
            AND NEW.status = 'active'
            AND NEW.recording_state = 'cleanup_pending'
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'meeting erasure recording identity cannot be rebound');
END;
CREATE TRIGGER meeting_erasure_jobs_require_cleanup_evidence
BEFORE INSERT ON meeting_erasure_jobs
WHEN NEW.cleanup_job_id IS NOT NULL
    AND (
        NOT EXISTS (
            SELECT 1 FROM recording_cleanup_jobs cleanup
            WHERE cleanup.id = NEW.cleanup_job_id
              AND cleanup.reason = 'meeting_erasure'
        )
        OR (
            NEW.recording_state = 'failed'
            AND NOT EXISTS (
                SELECT 1 FROM recording_cleanup_jobs cleanup
                WHERE cleanup.id = NEW.cleanup_job_id
                  AND cleanup.reason = 'meeting_erasure'
                  AND cleanup.status = 'failed'
            )
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'meeting erasure cleanup evidence is invalid');
END;
CREATE TRIGGER meeting_erasure_jobs_require_cleanup_transition
BEFORE UPDATE OF recording_state, cleanup_job_id ON meeting_erasure_jobs
WHEN (
        NEW.cleanup_job_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM recording_cleanup_jobs cleanup
            WHERE cleanup.id = NEW.cleanup_job_id
              AND cleanup.reason = 'meeting_erasure'
        )
    )
    OR (
        OLD.recording_state = 'cleanup_pending'
        AND NEW.recording_state = 'failed'
        AND NOT EXISTS (
            SELECT 1 FROM recording_cleanup_jobs cleanup
            WHERE cleanup.id = OLD.cleanup_job_id
              AND cleanup.reason = 'meeting_erasure'
              AND cleanup.status = 'failed'
        )
    )
    OR (
        OLD.recording_state IN ('cleanup_pending', 'failed')
        AND NEW.recording_state = 'removed'
        AND NOT EXISTS (
            SELECT 1 FROM recording_cleanup_jobs cleanup
            WHERE cleanup.id = OLD.cleanup_job_id
              AND cleanup.reason = 'meeting_erasure'
              AND cleanup.status = 'succeeded'
        )
    )
    OR (
        OLD.recording_state <> NEW.recording_state
        AND NOT (
            OLD.recording_state = 'waiting_shared'
            AND NEW.recording_state = 'cleanup_pending'
        )
        AND NOT (
            OLD.recording_state = 'cleanup_pending'
            AND NEW.recording_state IN ('removed', 'failed')
        )
        AND NOT (
            OLD.recording_state = 'failed'
            AND NEW.recording_state = 'removed'
        )
        AND NOT (
            OLD.status = 'failed'
            AND OLD.recording_state = 'failed'
            AND NEW.status = 'active'
            AND NEW.recording_state = 'cleanup_pending'
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'meeting erasure recording transition is invalid');
END;
CREATE TRIGGER meeting_erasure_jobs_reject_counter_regression
BEFORE UPDATE ON meeting_erasure_jobs
WHEN NEW.version <> OLD.version + 1
    OR NEW.retry_count < OLD.retry_count
    OR NEW.remediation_count < OLD.remediation_count
    OR NEW.updated_at < OLD.updated_at
BEGIN
    SELECT RAISE(ABORT, 'meeting erasure counters cannot regress');
END;
CREATE TRIGGER meeting_erasure_jobs_reject_invalid_remediation
BEFORE UPDATE ON meeting_erasure_jobs
WHEN OLD.status = 'completed'
    OR (
        OLD.status = 'failed'
        AND NOT (
            NEW.status = 'active'
            AND NEW.recording_state = 'cleanup_pending'
            AND NEW.pending_audio_asset_id IS NULL
            AND NEW.cleanup_job_id IS OLD.cleanup_job_id
            AND NEW.database_checkpointed_at IS OLD.database_checkpointed_at
            AND NEW.retry_count = OLD.retry_count
            AND NEW.next_attempt_at IS NULL
            AND NEW.lease_owner IS NULL
            AND NEW.lease_expires_at IS NULL
            AND NEW.last_failure_code IS NULL
            AND NEW.last_failure_disposition IS NULL
            AND NEW.last_failure_occurred_at IS NULL
            AND NEW.remediation_count = OLD.remediation_count + 1
            AND NEW.completed_at IS NULL
        )
    )
    OR (
        OLD.status <> 'failed'
        AND NEW.remediation_count <> OLD.remediation_count
    )
BEGIN
    SELECT RAISE(ABORT, 'meeting erasure remediation transition is invalid');
END;
CREATE TRIGGER recording_cleanup_jobs_reject_invalid_erasure_terminal_reset
BEFORE UPDATE OF status ON recording_cleanup_jobs
WHEN OLD.reason = 'meeting_erasure'
    AND OLD.status IN ('succeeded', 'failed')
    AND NOT (
        OLD.status = 'failed'
        AND NEW.status = 'ready'
        AND NEW.attempt_count = 0
        AND NEW.next_attempt_at IS NULL
        AND NEW.lease_owner IS NULL
        AND NEW.lease_expires_at IS NULL
        AND NEW.last_failure_json IS NULL
        AND NEW.completed_at IS NULL
        AND EXISTS (
            SELECT 1 FROM meeting_erasure_jobs erasure
            WHERE erasure.cleanup_job_id = OLD.id
        )
        AND NOT EXISTS (
            SELECT 1 FROM meeting_erasure_jobs erasure
            WHERE erasure.cleanup_job_id = OLD.id
              AND (
                  erasure.status <> 'active'
                  OR erasure.recording_state <> 'cleanup_pending'
              )
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'meeting erasure cleanup terminal transition is invalid');
END;
CREATE INDEX idx_meeting_erasure_jobs_claim
ON meeting_erasure_jobs (
    status, database_checkpointed_at, next_attempt_at, lease_expires_at, created_at, id
);
CREATE INDEX idx_meeting_erasure_jobs_audio_asset
ON meeting_erasure_jobs (pending_audio_asset_id, recording_state, id);
CREATE INDEX idx_meeting_erasure_jobs_cleanup
ON meeting_erasure_jobs (cleanup_job_id, status, id);
CREATE INDEX idx_meeting_erasure_jobs_token_key
ON meeting_erasure_jobs (token_key_id, token_version);
CREATE TABLE meeting_erasure_tombstones (
    erasure_job_id TEXT PRIMARY KEY
        CHECK (
            typeof(erasure_job_id) = 'text'
            AND length(erasure_job_id) = 36
            AND substr(erasure_job_id, 9, 1) = '-'
            AND substr(erasure_job_id, 14, 1) = '-'
            AND substr(erasure_job_id, 19, 1) = '-'
            AND substr(erasure_job_id, 24, 1) = '-'
            AND length(replace(erasure_job_id, '-', '')) = 32
            AND replace(erasure_job_id, '-', '') NOT GLOB '*[^0-9a-f]*'
        ),
    token_version INTEGER NOT NULL
        CHECK (typeof(token_version) = 'integer' AND token_version > 0),
    token_key_id TEXT NOT NULL
        REFERENCES erasure_key_verifiers (key_id) ON DELETE RESTRICT ON UPDATE RESTRICT
        CHECK (typeof(token_key_id) = 'text'),
    meeting_token TEXT NOT NULL
        CHECK (
            typeof(meeting_token) = 'text'
            AND length(meeting_token) = 64
            AND meeting_token NOT GLOB '*[^0-9a-f]*'
        ),
    ingest_key_token TEXT NOT NULL
        CHECK (
            typeof(ingest_key_token) = 'text'
            AND length(ingest_key_token) = 64
            AND ingest_key_token NOT GLOB '*[^0-9a-f]*'
        ),
    erased_at TEXT NOT NULL
        CHECK (__UTC_ERASED_AT__),
    UNIQUE (token_version, token_key_id, meeting_token),
    UNIQUE (token_version, token_key_id, ingest_key_token),
    FOREIGN KEY (erasure_job_id, token_version, token_key_id, meeting_token)
        REFERENCES meeting_erasure_jobs (
            id, token_version, token_key_id, meeting_token
        ) ON DELETE RESTRICT ON UPDATE RESTRICT
);
CREATE TRIGGER meeting_erasure_tombstones_reject_update
BEFORE UPDATE ON meeting_erasure_tombstones
BEGIN
    SELECT RAISE(ABORT, 'meeting erasure tombstones are immutable');
END;
CREATE INDEX idx_meeting_erasure_tombstones_token_key
ON meeting_erasure_tombstones (token_key_id, token_version);
CREATE TABLE meeting_erasure_operation_bindings (
    token_version INTEGER NOT NULL
        CHECK (typeof(token_version) = 'integer' AND token_version > 0),
    token_key_id TEXT NOT NULL
        REFERENCES erasure_key_verifiers (key_id) ON DELETE RESTRICT ON UPDATE RESTRICT
        CHECK (typeof(token_key_id) = 'text'),
    request_token TEXT NOT NULL
        CHECK (
            typeof(request_token) = 'text'
            AND length(request_token) = 64
            AND request_token NOT GLOB '*[^0-9a-f]*'
        ),
    actor_token TEXT NOT NULL
        CHECK (
            typeof(actor_token) = 'text'
            AND length(actor_token) = 64
            AND actor_token NOT GLOB '*[^0-9a-f]*'
        ),
    resource_token TEXT NOT NULL
        CHECK (
            typeof(resource_token) = 'text'
            AND length(resource_token) = 64
            AND resource_token NOT GLOB '*[^0-9a-f]*'
        ),
    erasure_job_id TEXT NOT NULL
        REFERENCES meeting_erasure_jobs (id) ON DELETE RESTRICT ON UPDATE RESTRICT
        CHECK (
            typeof(erasure_job_id) = 'text'
            AND length(erasure_job_id) = 36
            AND substr(erasure_job_id, 9, 1) = '-'
            AND substr(erasure_job_id, 14, 1) = '-'
            AND substr(erasure_job_id, 19, 1) = '-'
            AND substr(erasure_job_id, 24, 1) = '-'
            AND length(replace(erasure_job_id, '-', '')) = 32
            AND replace(erasure_job_id, '-', '') NOT GLOB '*[^0-9a-f]*'
        ),
    operation TEXT NOT NULL
        CHECK (typeof(operation) = 'text' AND operation IN ('request', 'retry')),
    expected_version INTEGER NOT NULL
        CHECK (typeof(expected_version) = 'integer' AND expected_version >= 0),
    request_fingerprint TEXT NOT NULL
        CHECK (
            typeof(request_fingerprint) = 'text'
            AND length(request_fingerprint) = 64
            AND request_fingerprint NOT GLOB '*[^0-9a-f]*'
        ),
    created_at TEXT NOT NULL
        CHECK (__UTC_CREATED_AT__),
    PRIMARY KEY (token_version, token_key_id, request_token)
);
CREATE INDEX idx_meeting_erasure_operations_job
ON meeting_erasure_operation_bindings (erasure_job_id, created_at);
CREATE INDEX idx_meeting_erasure_operations_token_key
ON meeting_erasure_operation_bindings (token_key_id, token_version);
CREATE TRIGGER meeting_erasure_operation_bindings_reject_update
BEFORE UPDATE ON meeting_erasure_operation_bindings
BEGIN
    SELECT RAISE(ABORT, 'meeting erasure operation bindings are immutable');
END;
CREATE INDEX idx_write_intents_meeting_status
ON write_intents (meeting_id, status);
CREATE INDEX idx_meetings_audio_asset_id
ON meetings (audio_asset_id, id);
CREATE INDEX idx_transcripts_audio_asset_id
ON transcripts (audio_asset_id, id);
CREATE INDEX idx_review_revisions_transcript_id
ON review_revisions (transcript_id, id);
CREATE INDEX idx_approvals_review_revision_id
ON approvals (review_revision_id, id);
CREATE INDEX idx_recap_artifacts_meeting_id
ON recap_artifacts (meeting_id, id);
"""


def _expand_utc_checks(template: str) -> str:
    result = template
    columns = (
        "created_at",
        "updated_at",
        "database_checkpointed_at",
        "completed_at",
        "lease_expires_at",
        "next_attempt_at",
        "last_failure_occurred_at",
        "erased_at",
    )
    for column in columns:
        marker = "__UTC_" + column.upper() + "__"
        result = result.replace(marker, _utc_datetime_check(column))
    return result


SCHEMA_V9 = _expand_utc_checks(_SCHEMA_V9_TEMPLATE)

MIGRATIONS = (
    (1, SCHEMA_V1),
    (2, SCHEMA_V2),
    (3, SCHEMA_V3),
    (4, SCHEMA_V4),
    (5, SCHEMA_V5),
    (6, SCHEMA_V6),
    (7, SCHEMA_V7),
    (8, SCHEMA_V8),
    (9, SCHEMA_V9),
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
        try:
            _restrict_permissions(self._path, 0o600)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA journal_mode = WAL")
            _restrict_permissions(Path(f"{self._path}-wal"), 0o600)
            _restrict_permissions(Path(f"{self._path}-shm"), 0o600)
            connection.execute("PRAGMA secure_delete = ON")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("PRAGMA busy_timeout = 5000")
            _verify_connection_pragmas(connection)
        except BaseException:
            connection.close()
            raise
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
        with closing(self.connect()) as connection:
            _verify_connection_pragmas(connection)
            result = connection.execute("SELECT 1").fetchone()
        return result is not None and result[0] == 1

    def truncate_wal(self) -> WalCheckpointResult:
        with closing(self.connect()) as connection:
            row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if row is None or len(row) != 3:
            raise RuntimeError("SQLite WAL checkpoint returned an invalid result")
        values = tuple(row)
        if any(isinstance(value, bool) or not isinstance(value, int) for value in values):
            raise RuntimeError("SQLite WAL checkpoint returned an invalid result")
        return WalCheckpointResult(
            busy=values[0],
            log_frames=values[1],
            checkpointed_frames=values[2],
        )


def _verify_connection_pragmas(connection: sqlite3.Connection) -> None:
    expected = {
        "foreign_keys": 1,
        "secure_delete": 1,
        "synchronous": 2,
    }
    for pragma, required in expected.items():
        row = connection.execute(f"PRAGMA {pragma}").fetchone()
        if row is None or int(row[0]) != required:
            raise RuntimeError("SQLite connection security settings could not be enforced")
    journal_mode = connection.execute("PRAGMA journal_mode").fetchone()
    if journal_mode is None or str(journal_mode[0]).casefold() != "wal":
        raise RuntimeError("SQLite connection security settings could not be enforced")


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
