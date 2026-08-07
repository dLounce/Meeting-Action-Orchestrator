from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from datetime import datetime, timezone
from types import TracebackType
from uuid import UUID

from meeting_action_orchestrator.application.ports import MeetingListCursor
from meeting_action_orchestrator.domain.enums import (
    FailureDisposition,
    MeetingErasureRecordingState,
    MeetingErasureStatus,
    MeetingStatus,
    ProcessingJobStatus,
    ProcessingStage,
    RecordingCleanupReason,
    RecordingCleanupStatus,
    WriteStatus,
)
from meeting_action_orchestrator.domain.hashing import canonical_json
from meeting_action_orchestrator.domain.models import (
    Approval,
    AudioAsset,
    DeliveryOperationBinding,
    ErasureKeyVerifier,
    ErasureToken,
    ErasureTokenIdentity,
    IngestRequestBinding,
    Meeting,
    MeetingErasureFailure,
    MeetingErasureJob,
    MeetingErasureOperationBinding,
    MeetingErasureTombstone,
    MeetingOperationBinding,
    ProcessingJob,
    RecapArtifact,
    RecordingCleanupJob,
    ReviewRevision,
    Transcript,
    WorkflowFailure,
    WriteIntent,
    WriteReceipt,
)
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.workflow_events import (
    SqliteWorkflowEventRepository,
)


class PersistenceConflictError(RuntimeError):
    pass


class PersistenceIntegrityError(RuntimeError):
    pass


def _as_text(value: object | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _as_erasure_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Meeting erasure timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _as_json(value: object) -> str:
    return canonical_json(value)


def _load_json(value: str | None, default: object = None) -> object:
    if value is None:
        return default
    return json.loads(value)


def _token_candidates(tokens: Sequence[ErasureToken]) -> tuple[ErasureToken, ...]:
    if len(tokens) > 8:
        raise ValueError("Erasure token candidate limit exceeded")
    unique: dict[tuple[int, str, str], ErasureToken] = {}
    for token in tokens:
        unique[(token.token_version, token.key_id, token.digest)] = token
    return tuple(unique.values())


def _erasure_failure_values(
    failure: MeetingErasureFailure | None,
) -> tuple[str | None, str | None, str | None]:
    if failure is None:
        return (None, None, None)
    return (
        failure.code.value,
        failure.disposition.value,
        _as_erasure_datetime(failure.occurred_at),
    )


def _erasure_failure_from_row(row: sqlite3.Row) -> MeetingErasureFailure | None:
    if row["last_failure_code"] is None:
        return None
    return MeetingErasureFailure.model_validate(
        {
            "code": row["last_failure_code"],
            "disposition": row["last_failure_disposition"],
            "occurred_at": row["last_failure_occurred_at"],
        }
    )


class SqliteMeetingRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def add(self, meeting: Meeting) -> None:
        self._connection.execute(
            """
            INSERT INTO meetings (
                id, ingest_key, title, audio_asset_id, occurred_at, timezone,
                participants_json, status, current_transcript_id, current_review_id,
                approved_review_id, failure_json, version, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            self._values(meeting),
        )

    def get(self, meeting_id: UUID) -> Meeting | None:
        row = self._connection.execute(
            "SELECT * FROM meetings WHERE id = ?", (str(meeting_id),)
        ).fetchone()
        return self._from_row(row) if row is not None else None

    def find_by_ingest_key(self, ingest_key: str) -> Meeting | None:
        row = self._connection.execute(
            "SELECT * FROM meetings WHERE ingest_key = ?", (ingest_key,)
        ).fetchone()
        return self._from_row(row) if row is not None else None

    def list_page(
        self,
        *,
        status: MeetingStatus | None,
        cursor: MeetingListCursor | None,
        limit: int,
    ) -> Sequence[Meeting]:
        if limit <= 0:
            return ()
        if status is not None and cursor is not None:
            cursor_time = str(cursor.created_at)
            rows = self._connection.execute(
                """
                SELECT * FROM meetings
                WHERE status = ?
                  AND (created_at, id) < (?, ?)
                ORDER BY created_at DESC, id DESC LIMIT ?
                """,
                (status.value, cursor_time, str(cursor.id), limit),
            ).fetchall()
        elif status is not None:
            rows = self._connection.execute(
                """
                SELECT * FROM meetings WHERE status = ?
                ORDER BY created_at DESC, id DESC LIMIT ?
                """,
                (status.value, limit),
            ).fetchall()
        elif cursor is not None:
            cursor_time = str(cursor.created_at)
            rows = self._connection.execute(
                """
                SELECT * FROM meetings
                WHERE (created_at, id) < (?, ?)
                ORDER BY created_at DESC, id DESC LIMIT ?
                """,
                (cursor_time, str(cursor.id), limit),
            ).fetchall()
        else:
            rows = self._connection.execute(
                """
                SELECT * FROM meetings
                ORDER BY created_at DESC, id DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return tuple(self._from_row(row) for row in rows)

    def save(self, meeting: Meeting, expected_version: int) -> None:
        values = self._values(meeting)
        cursor = self._connection.execute(
            """
            UPDATE meetings SET
                ingest_key = ?, title = ?, audio_asset_id = ?, occurred_at = ?, timezone = ?,
                participants_json = ?, status = ?, current_transcript_id = ?,
                current_review_id = ?, approved_review_id = ?, failure_json = ?, version = ?,
                updated_at = ?
            WHERE id = ? AND version = ? AND created_at = ?
            """,
            (*values[1:13], values[14], values[0], expected_version, values[13]),
        )
        if cursor.rowcount != 1:
            raise PersistenceConflictError("The meeting was changed by another operation")

    @staticmethod
    def _values(meeting: Meeting) -> tuple[object, ...]:
        return (
            str(meeting.id),
            meeting.ingest_key,
            meeting.title,
            str(meeting.audio_asset_id),
            _as_text(meeting.occurred_at),
            meeting.timezone,
            _as_json(meeting.participants),
            meeting.status.value,
            _as_text(meeting.current_transcript_id),
            _as_text(meeting.current_review_id),
            _as_text(meeting.approved_review_id),
            _as_json(meeting.failure) if meeting.failure is not None else None,
            meeting.version,
            str(meeting.created_at),
            str(meeting.updated_at),
        )

    @staticmethod
    def _from_row(row: sqlite3.Row) -> Meeting:
        return Meeting.model_validate(
            {
                "id": row["id"],
                "ingest_key": row["ingest_key"],
                "title": row["title"],
                "audio_asset_id": row["audio_asset_id"],
                "occurred_at": row["occurred_at"],
                "timezone": row["timezone"],
                "participants": _load_json(row["participants_json"], []),
                "status": row["status"],
                "current_transcript_id": row["current_transcript_id"],
                "current_review_id": row["current_review_id"],
                "approved_review_id": row["approved_review_id"],
                "failure": _load_json(row["failure_json"]),
                "version": row["version"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        )


class SqliteIngestRequestBindingRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def add(self, binding: IngestRequestBinding) -> None:
        self._connection.execute(
            """
            INSERT INTO ingest_request_bindings (
                ingest_key, fingerprint_version, request_fingerprint, created_at
            ) VALUES (?, ?, ?, ?)
            """,
            (
                binding.ingest_key,
                binding.fingerprint_version,
                binding.request_fingerprint,
                str(binding.created_at),
            ),
        )

    def get(self, ingest_key: str) -> IngestRequestBinding | None:
        row = self._connection.execute(
            "SELECT * FROM ingest_request_bindings WHERE ingest_key = ?",
            (ingest_key,),
        ).fetchone()
        if row is None:
            return None
        return IngestRequestBinding.model_validate(dict(row))


class SqliteAudioAssetRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def add(self, asset: AudioAsset) -> None:
        self._connection.execute(
            """
            INSERT INTO audio_assets (
                id, storage_key, original_name, media_type, size_bytes,
                duration_ms, sha256, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(asset.id),
                asset.storage_key,
                asset.original_name,
                asset.detected_media_type.value,
                asset.size_bytes,
                asset.duration_ms,
                asset.sha256,
                str(asset.created_at),
            ),
        )

    def get(self, asset_id: UUID) -> AudioAsset | None:
        row = self._connection.execute(
            "SELECT * FROM audio_assets WHERE id = ?", (str(asset_id),)
        ).fetchone()
        return self._from_row(row) if row is not None else None

    def find_by_sha256(self, digest: str) -> AudioAsset | None:
        row = self._connection.execute(
            "SELECT * FROM audio_assets WHERE sha256 = ?", (digest,)
        ).fetchone()
        return self._from_row(row) if row is not None else None

    def find_by_storage_key(self, storage_key: str) -> AudioAsset | None:
        row = self._connection.execute(
            "SELECT * FROM audio_assets WHERE storage_key = ?", (storage_key,)
        ).fetchone()
        return self._from_row(row) if row is not None else None

    @staticmethod
    def _from_row(row: sqlite3.Row) -> AudioAsset:
        return AudioAsset.model_validate(
            {
                "id": row["id"],
                "storage_key": row["storage_key"],
                "original_name": row["original_name"],
                "detected_media_type": row["media_type"],
                "size_bytes": row["size_bytes"],
                "duration_ms": row["duration_ms"],
                "sha256": row["sha256"],
                "created_at": row["created_at"],
            }
        )


class SqliteRecordingCleanupRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def add(self, job: RecordingCleanupJob) -> None:
        self._connection.execute(
            """
            INSERT INTO recording_cleanup_jobs (
                id, storage_key, expected_sha256, expected_size_bytes, reason,
                status, attempt_count, max_attempts, next_attempt_at, lease_owner,
                lease_expires_at, last_failure_json, created_at, updated_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            self._values(job),
        )

    def get(self, job_id: UUID) -> RecordingCleanupJob | None:
        row = self._connection.execute(
            "SELECT * FROM recording_cleanup_jobs WHERE id = ?", (str(job_id),)
        ).fetchone()
        return self._from_row(row) if row is not None else None

    def find_by_storage_key(self, storage_key: str) -> RecordingCleanupJob | None:
        row = self._connection.execute(
            "SELECT * FROM recording_cleanup_jobs WHERE storage_key = ?", (storage_key,)
        ).fetchone()
        return self._from_row(row) if row is not None else None

    def list_by_expected_sha256(self, digest: str) -> Sequence[RecordingCleanupJob]:
        rows = self._connection.execute(
            """
            SELECT * FROM recording_cleanup_jobs
            WHERE expected_sha256 = ? ORDER BY status, id
            """,
            (digest,),
        ).fetchall()
        return tuple(self._from_row(row) for row in rows)

    def delete_succeeded(self, job: RecordingCleanupJob) -> bool:
        if job.status is not RecordingCleanupStatus.SUCCEEDED:
            raise ValueError("Only successful recording cleanup can be removed")
        values = self._values(job)
        cursor = self._connection.execute(
            """
            DELETE FROM recording_cleanup_jobs
            WHERE id = ? AND storage_key = ? AND expected_sha256 = ?
              AND expected_size_bytes = ? AND reason = ? AND status = ?
              AND attempt_count = ? AND max_attempts = ?
              AND next_attempt_at IS ? AND lease_owner IS ? AND lease_expires_at IS ?
              AND last_failure_json IS ? AND created_at = ? AND updated_at = ?
              AND completed_at IS ?
              AND NOT EXISTS (
                  SELECT 1 FROM meeting_erasure_jobs erasure
                  WHERE erasure.cleanup_job_id = recording_cleanup_jobs.id
              )
            """,
            values,
        )
        return cursor.rowcount == 1

    def claim_due(
        self,
        worker_id: str,
        now: datetime,
        lease_until: datetime,
        limit: int,
    ) -> Sequence[RecordingCleanupJob]:
        normalized_worker_id = worker_id.strip()
        if not normalized_worker_id:
            raise ValueError("Worker ID cannot be empty")
        if len(normalized_worker_id) > 200:
            raise ValueError("Worker ID exceeds the supported length")
        if lease_until <= now:
            raise ValueError("Lease expiry must follow the claim time")
        if limit <= 0:
            return ()
        rows = self._connection.execute(
            """
            SELECT id, status, lease_owner, lease_expires_at
            FROM recording_cleanup_jobs
            WHERE (
                attempt_count < max_attempts AND (
                    status = ?
                    OR (status = ? AND next_attempt_at <= ?)
                )
                OR (status = ? AND lease_expires_at <= ?)
            )
            ORDER BY
                CASE status
                    WHEN 'ready' THEN created_at
                    WHEN 'retry_wait' THEN next_attempt_at
                    ELSE lease_expires_at
                END,
                created_at,
                id
            LIMIT ?
            """,
            (
                RecordingCleanupStatus.READY.value,
                RecordingCleanupStatus.RETRY_WAIT.value,
                str(now),
                RecordingCleanupStatus.RUNNING.value,
                str(now),
                limit,
            ),
        ).fetchall()
        claimed: list[RecordingCleanupJob] = []
        for row in rows:
            cursor = self._connection.execute(
                """
                UPDATE recording_cleanup_jobs
                SET status = ?,
                    attempt_count = CASE
                        WHEN status = 'running' THEN attempt_count
                        ELSE attempt_count + 1
                    END,
                    next_attempt_at = NULL, lease_owner = ?, lease_expires_at = ?,
                    last_failure_json = NULL, updated_at = ?, completed_at = NULL
                WHERE id = ? AND status = ?
                    AND lease_owner IS ? AND lease_expires_at IS ?
                    AND (
                        (status IN ('ready', 'retry_wait') AND attempt_count < max_attempts)
                        OR status = 'running'
                    )
                """,
                (
                    RecordingCleanupStatus.RUNNING.value,
                    normalized_worker_id,
                    str(lease_until),
                    str(now),
                    row["id"],
                    row["status"],
                    row["lease_owner"],
                    row["lease_expires_at"],
                ),
            )
            if cursor.rowcount != 1:
                continue
            job = self.get(UUID(row["id"]))
            if job is not None:
                claimed.append(job)
        return tuple(claimed)

    def save(
        self,
        job: RecordingCleanupJob,
        expected_status: RecordingCleanupStatus,
        expected_lease_owner: str | None,
        expected_lease_expires_at: datetime | None,
    ) -> None:
        current = self.get(job.id)
        if (
            current is not None
            and current.reason is RecordingCleanupReason.MEETING_ERASURE
            and current.status in {RecordingCleanupStatus.SUCCEEDED, RecordingCleanupStatus.FAILED}
        ):
            raise PersistenceConflictError("The recording cleanup terminal state cannot be reset")
        cursor = self._connection.execute(
            """
            UPDATE recording_cleanup_jobs
            SET status = ?, attempt_count = ?, next_attempt_at = ?,
                lease_owner = ?, lease_expires_at = ?, last_failure_json = ?,
                updated_at = ?, completed_at = ?
            WHERE id = ? AND storage_key = ? AND expected_sha256 = ?
                AND expected_size_bytes = ? AND reason = ? AND max_attempts = ?
                AND created_at = ? AND status = ?
                AND lease_owner IS ? AND lease_expires_at IS ?
            """,
            (
                job.status.value,
                job.attempt_count,
                _as_text(job.next_attempt_at),
                job.lease_owner,
                _as_text(job.lease_expires_at),
                _as_json(job.last_failure) if job.last_failure is not None else None,
                str(job.updated_at),
                _as_text(job.completed_at),
                str(job.id),
                job.storage_key,
                job.expected_sha256,
                job.expected_size_bytes,
                job.reason.value,
                job.max_attempts,
                str(job.created_at),
                expected_status.value,
                expected_lease_owner,
                _as_text(expected_lease_expires_at),
            ),
        )
        if cursor.rowcount != 1:
            raise PersistenceConflictError("The recording cleanup lease is no longer current")

    @staticmethod
    def _values(job: RecordingCleanupJob) -> tuple[object, ...]:
        return (
            str(job.id),
            job.storage_key,
            job.expected_sha256,
            job.expected_size_bytes,
            job.reason.value,
            job.status.value,
            job.attempt_count,
            job.max_attempts,
            _as_text(job.next_attempt_at),
            job.lease_owner,
            _as_text(job.lease_expires_at),
            _as_json(job.last_failure) if job.last_failure is not None else None,
            str(job.created_at),
            str(job.updated_at),
            _as_text(job.completed_at),
        )

    @staticmethod
    def _from_row(row: sqlite3.Row) -> RecordingCleanupJob:
        return RecordingCleanupJob.model_validate(
            {
                "id": row["id"],
                "storage_key": row["storage_key"],
                "expected_sha256": row["expected_sha256"],
                "expected_size_bytes": row["expected_size_bytes"],
                "reason": row["reason"],
                "status": row["status"],
                "attempt_count": row["attempt_count"],
                "max_attempts": row["max_attempts"],
                "next_attempt_at": row["next_attempt_at"],
                "lease_owner": row["lease_owner"],
                "lease_expires_at": row["lease_expires_at"],
                "last_failure": _load_json(row["last_failure_json"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "completed_at": row["completed_at"],
            }
        )


class SqliteErasureKeyVerifierRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def add(self, verifier: ErasureKeyVerifier) -> None:
        self._connection.execute(
            """
            INSERT INTO erasure_key_verifiers (
                key_id, verifier_version, verifier_digest, created_at
            ) VALUES (?, ?, ?, ?)
            """,
            (
                verifier.key_id,
                verifier.verifier_version,
                verifier.verifier_digest,
                _as_erasure_datetime(verifier.created_at),
            ),
        )

    def get(self, key_id: str) -> ErasureKeyVerifier | None:
        row = self._connection.execute(
            "SELECT * FROM erasure_key_verifiers WHERE key_id = ?",
            (key_id,),
        ).fetchone()
        return self._from_row(row) if row is not None else None

    def list_all(self) -> Sequence[ErasureKeyVerifier]:
        rows = self._connection.execute(
            "SELECT * FROM erasure_key_verifiers ORDER BY key_id"
        ).fetchall()
        return tuple(self._from_row(row) for row in rows)

    def list_referenced_tokens(self) -> Sequence[ErasureTokenIdentity]:
        rows = self._connection.execute(
            """
            SELECT token_version, token_key_id
            FROM meeting_erasure_jobs
            UNION
            SELECT token_version, token_key_id
            FROM meeting_erasure_tombstones
            UNION
            SELECT token_version, token_key_id
            FROM meeting_erasure_operation_bindings
            ORDER BY token_version, token_key_id
            """
        ).fetchall()
        return tuple(
            ErasureTokenIdentity(
                token_version=row["token_version"],
                key_id=row["token_key_id"],
            )
            for row in rows
        )

    @staticmethod
    def _from_row(row: sqlite3.Row) -> ErasureKeyVerifier:
        return ErasureKeyVerifier.model_validate(dict(row))


class SqliteMeetingErasureRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def add(self, job: MeetingErasureJob) -> None:
        self._connection.execute(
            """
            INSERT INTO meeting_erasure_jobs (
                id, token_version, token_key_id, meeting_token, reason,
                erased_meeting_version, status, recording_state,
                pending_audio_asset_id, cleanup_job_id, database_checkpointed_at,
                retry_count, next_attempt_at, lease_owner, lease_expires_at,
                last_failure_code, last_failure_disposition, last_failure_occurred_at,
                remediation_count, max_remediations, version, created_at,
                updated_at, completed_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            self._values(job),
        )

    def get(self, job_id: UUID) -> MeetingErasureJob | None:
        row = self._connection.execute(
            "SELECT * FROM meeting_erasure_jobs WHERE id = ?",
            (str(job_id),),
        ).fetchone()
        return self._from_row(row) if row is not None else None

    def find_by_meeting_tokens(
        self,
        tokens: Sequence[ErasureToken],
    ) -> MeetingErasureJob | None:
        matches: dict[str, sqlite3.Row] = {}
        for token in _token_candidates(tokens):
            row = self._connection.execute(
                """
                SELECT * FROM meeting_erasure_jobs
                WHERE token_version = ? AND token_key_id = ? AND meeting_token = ?
                """,
                (token.token_version, token.key_id, token.digest),
            ).fetchone()
            if row is not None:
                matches[row["id"]] = row
        if len(matches) > 1:
            raise PersistenceIntegrityError("Erasure token candidates matched multiple jobs")
        row = next(iter(matches.values()), None)
        return self._from_row(row) if row is not None else None

    def list_by_pending_audio_asset_id(
        self,
        audio_asset_id: UUID,
    ) -> Sequence[MeetingErasureJob]:
        rows = self._connection.execute(
            """
            SELECT * FROM meeting_erasure_jobs
            WHERE pending_audio_asset_id = ?
            ORDER BY created_at, id
            """,
            (str(audio_asset_id),),
        ).fetchall()
        return tuple(self._from_row(row) for row in rows)

    def list_by_cleanup_job_id(self, cleanup_job_id: UUID) -> Sequence[MeetingErasureJob]:
        rows = self._connection.execute(
            """
            SELECT * FROM meeting_erasure_jobs
            WHERE cleanup_job_id = ?
            ORDER BY created_at, id
            """,
            (str(cleanup_job_id),),
        ).fetchall()
        return tuple(self._from_row(row) for row in rows)

    def reactivate_failed_cleanup_group(
        self,
        cleanup_job_id: UUID,
        now: datetime,
    ) -> Sequence[MeetingErasureJob]:
        normalized_now = _as_erasure_datetime(now)
        self._connection.execute("SAVEPOINT meeting_erasure_group_remediation")
        try:
            cleanup_row = self._connection.execute(
                "SELECT * FROM recording_cleanup_jobs WHERE id = ?",
                (str(cleanup_job_id),),
            ).fetchone()
            cleanup = (
                SqliteRecordingCleanupRepository._from_row(cleanup_row)
                if cleanup_row is not None
                else None
            )
            rows = self._connection.execute(
                """
                SELECT * FROM meeting_erasure_jobs
                WHERE cleanup_job_id = ? ORDER BY created_at, id
                """,
                (str(cleanup_job_id),),
            ).fetchall()
            jobs = tuple(self._from_row(row) for row in rows)
            if (
                cleanup is None
                or cleanup.reason is not RecordingCleanupReason.MEETING_ERASURE
                or cleanup.status is not RecordingCleanupStatus.FAILED
                or cleanup.updated_at > now
                or not jobs
                or any(
                    job.status is not MeetingErasureStatus.FAILED
                    or job.recording_state is not MeetingErasureRecordingState.FAILED
                    or job.database_checkpointed_at is None
                    or job.remediation_count >= job.max_remediations
                    or job.updated_at > now
                    for job in jobs
                )
            ):
                raise PersistenceConflictError("The cleanup remediation group is not eligible")
            cursor = self._connection.execute(
                """
                UPDATE meeting_erasure_jobs
                SET status = 'active', recording_state = 'cleanup_pending',
                    last_failure_code = NULL, last_failure_disposition = NULL,
                    last_failure_occurred_at = NULL,
                    remediation_count = remediation_count + 1,
                    version = version + 1, updated_at = ?, completed_at = NULL
                WHERE cleanup_job_id = ? AND status = 'failed'
                  AND recording_state = 'failed'
                  AND remediation_count < max_remediations
                """,
                (normalized_now, str(cleanup_job_id)),
            )
            if cursor.rowcount != len(jobs):
                raise PersistenceConflictError("The cleanup remediation group changed")
            reset = self._connection.execute(
                """
                UPDATE recording_cleanup_jobs
                SET status = 'ready', attempt_count = 0, next_attempt_at = NULL,
                    lease_owner = NULL, lease_expires_at = NULL,
                    last_failure_json = NULL, updated_at = ?, completed_at = NULL
                WHERE id = ? AND storage_key = ? AND expected_sha256 = ?
                  AND expected_size_bytes = ? AND reason = 'meeting_erasure'
                  AND status = 'failed' AND attempt_count = ? AND max_attempts = ?
                  AND next_attempt_at IS NULL AND lease_owner IS NULL
                  AND lease_expires_at IS NULL AND last_failure_json IS NOT NULL
                  AND created_at = ? AND updated_at = ? AND completed_at IS ?
                """,
                (
                    str(now),
                    str(cleanup.id),
                    cleanup.storage_key,
                    cleanup.expected_sha256,
                    cleanup.expected_size_bytes,
                    cleanup.attempt_count,
                    cleanup.max_attempts,
                    str(cleanup.created_at),
                    str(cleanup.updated_at),
                    _as_text(cleanup.completed_at),
                ),
            )
            if reset.rowcount != 1:
                raise PersistenceConflictError("The cleanup remediation group changed")
            updated = tuple(
                self._from_row(row)
                for row in self._connection.execute(
                    """
                    SELECT * FROM meeting_erasure_jobs
                    WHERE cleanup_job_id = ? ORDER BY created_at, id
                    """,
                    (str(cleanup_job_id),),
                ).fetchall()
            )
        except BaseException:
            self._connection.execute("ROLLBACK TO meeting_erasure_group_remediation")
            self._connection.execute("RELEASE meeting_erasure_group_remediation")
            raise
        self._connection.execute("RELEASE meeting_erasure_group_remediation")
        return updated

    def claim_actionable(
        self,
        worker_id: str,
        now: datetime,
        lease_until: datetime,
        limit: int,
    ) -> Sequence[MeetingErasureJob]:
        normalized_worker_id = worker_id.strip()
        if not normalized_worker_id:
            raise ValueError("Worker ID cannot be empty")
        if len(normalized_worker_id) > 200:
            raise ValueError("Worker ID exceeds the supported length")
        if lease_until <= now:
            raise ValueError("Lease expiry must follow the claim time")
        if limit <= 0:
            return ()
        normalized_now = _as_erasure_datetime(now)
        normalized_lease_until = _as_erasure_datetime(lease_until)
        rows = self._connection.execute(
            """
            SELECT id, version, lease_owner, lease_expires_at
            FROM meeting_erasure_jobs
            WHERE status = 'active'
              AND ? >= updated_at
              AND (lease_owner IS NULL OR lease_expires_at <= ?)
              AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
              AND (
                  database_checkpointed_at IS NULL
                  OR (
                      recording_state = 'cleanup_pending'
                      AND EXISTS (
                          SELECT 1 FROM recording_cleanup_jobs cleanup
                          WHERE cleanup.id = meeting_erasure_jobs.cleanup_job_id
                            AND cleanup.status IN ('succeeded', 'failed')
                      )
                  )
              )
            ORDER BY
                CASE WHEN database_checkpointed_at IS NULL THEN 0 ELSE 1 END,
                COALESCE(next_attempt_at, created_at),
                created_at,
                id
            LIMIT ?
            """,
            (normalized_now, normalized_now, normalized_now, limit),
        ).fetchall()
        claimed: list[MeetingErasureJob] = []
        for row in rows:
            cursor = self._connection.execute(
                """
                UPDATE meeting_erasure_jobs
                SET next_attempt_at = NULL, lease_owner = ?, lease_expires_at = ?,
                    version = version + 1, updated_at = ?
                WHERE id = ? AND status = 'active' AND version = ?
                  AND ? >= updated_at
                  AND lease_owner IS ? AND lease_expires_at IS ?
                  AND (
                      lease_owner IS NULL
                      OR lease_expires_at <= ?
                  )
                  AND (
                      next_attempt_at IS NULL
                      OR next_attempt_at <= ?
                  )
                  AND (
                      database_checkpointed_at IS NULL
                      OR (
                          recording_state = 'cleanup_pending'
                          AND EXISTS (
                              SELECT 1 FROM recording_cleanup_jobs cleanup
                              WHERE cleanup.id = meeting_erasure_jobs.cleanup_job_id
                                AND cleanup.status IN ('succeeded', 'failed')
                          )
                      )
                  )
                """,
                (
                    normalized_worker_id,
                    normalized_lease_until,
                    normalized_now,
                    row["id"],
                    row["version"],
                    normalized_now,
                    row["lease_owner"],
                    row["lease_expires_at"],
                    normalized_now,
                    normalized_now,
                ),
            )
            if cursor.rowcount != 1:
                continue
            job = self.get(UUID(row["id"]))
            if job is not None:
                claimed.append(job)
        return tuple(claimed)

    def save(
        self,
        job: MeetingErasureJob,
        expected_version: int,
        expected_lease_owner: str | None,
        expected_lease_expires_at: datetime | None,
    ) -> None:
        row = self._connection.execute(
            "SELECT * FROM meeting_erasure_jobs WHERE id = ?",
            (str(job.id),),
        ).fetchone()
        current = self._from_row(row) if row is not None else None
        cleanup_row = self._connection.execute(
            """
            SELECT reason, status FROM recording_cleanup_jobs
            WHERE id = ?
            """,
            (_as_text(job.cleanup_job_id or (current.cleanup_job_id if current else None)),),
        ).fetchone()
        cleanup_reason = cleanup_row["reason"] if cleanup_row is not None else None
        cleanup_status = cleanup_row["status"] if cleanup_row is not None else None
        waiting_to_cleanup = (
            current is not None
            and current.recording_state is MeetingErasureRecordingState.WAITING_SHARED
            and job.recording_state is MeetingErasureRecordingState.CLEANUP_PENDING
            and current.pending_audio_asset_id is not None
            and job.pending_audio_asset_id is None
            and current.cleanup_job_id is None
            and job.cleanup_job_id is not None
            and job.database_checkpointed_at is None
            and cleanup_reason == RecordingCleanupReason.MEETING_ERASURE.value
        )
        cleanup_to_removed = (
            current is not None
            and current.recording_state
            in {
                MeetingErasureRecordingState.CLEANUP_PENDING,
                MeetingErasureRecordingState.FAILED,
            }
            and job.recording_state is MeetingErasureRecordingState.REMOVED
            and current.pending_audio_asset_id is None
            and job.pending_audio_asset_id is None
            and current.cleanup_job_id is not None
            and job.cleanup_job_id is None
            and job.database_checkpointed_at is None
            and cleanup_reason == RecordingCleanupReason.MEETING_ERASURE.value
            and cleanup_status == RecordingCleanupStatus.SUCCEEDED.value
        )
        cleanup_to_failed = (
            current is not None
            and current.recording_state is MeetingErasureRecordingState.CLEANUP_PENDING
            and job.recording_state is MeetingErasureRecordingState.FAILED
            and current.cleanup_job_id is not None
            and job.cleanup_job_id == current.cleanup_job_id
            and cleanup_reason == RecordingCleanupReason.MEETING_ERASURE.value
            and cleanup_status == RecordingCleanupStatus.FAILED.value
        )
        same_recording_state = (
            current is not None and job.recording_state is current.recording_state
        )
        if (
            current is None
            or current.version != expected_version
            or job.version != expected_version + 1
            or not current.retry_count <= job.retry_count <= current.retry_count + 1
            or current.status is MeetingErasureStatus.COMPLETED
            or current.status is MeetingErasureStatus.FAILED
            or job.remediation_count != current.remediation_count
            or not (
                same_recording_state
                or waiting_to_cleanup
                or cleanup_to_removed
                or cleanup_to_failed
            )
            or (
                current.database_checkpointed_at is not None
                and job.database_checkpointed_at != current.database_checkpointed_at
                and not waiting_to_cleanup
                and not cleanup_to_removed
            )
            or job.token_version != current.token_version
            or job.token_key_id != current.token_key_id
            or job.meeting_token != current.meeting_token
            or job.reason is not current.reason
            or job.erased_meeting_version != current.erased_meeting_version
            or job.max_remediations != current.max_remediations
            or job.created_at != current.created_at
            or job.updated_at < current.updated_at
        ):
            raise PersistenceConflictError("The meeting erasure job is no longer current")
        failure = _erasure_failure_values(job.last_failure)
        cursor = self._connection.execute(
            """
            UPDATE meeting_erasure_jobs SET
                status = ?, recording_state = ?, pending_audio_asset_id = ?,
                cleanup_job_id = ?, database_checkpointed_at = ?, retry_count = ?,
                next_attempt_at = ?, lease_owner = ?, lease_expires_at = ?,
                last_failure_code = ?, last_failure_disposition = ?,
                last_failure_occurred_at = ?, remediation_count = ?, version = ?,
                updated_at = ?, completed_at = ?
            WHERE id = ? AND version = ?
              AND lease_owner IS ? AND lease_expires_at IS ?
            """,
            (
                job.status.value,
                job.recording_state.value,
                _as_text(job.pending_audio_asset_id),
                _as_text(job.cleanup_job_id),
                _as_erasure_datetime(job.database_checkpointed_at),
                job.retry_count,
                _as_erasure_datetime(job.next_attempt_at),
                job.lease_owner,
                _as_erasure_datetime(job.lease_expires_at),
                *failure,
                job.remediation_count,
                job.version,
                _as_erasure_datetime(job.updated_at),
                _as_erasure_datetime(job.completed_at),
                str(job.id),
                expected_version,
                expected_lease_owner,
                _as_erasure_datetime(expected_lease_expires_at),
            ),
        )
        if cursor.rowcount != 1:
            raise PersistenceConflictError("The meeting erasure lease is no longer current")

    @staticmethod
    def _values(job: MeetingErasureJob) -> tuple[object, ...]:
        failure = _erasure_failure_values(job.last_failure)
        return (
            str(job.id),
            job.token_version,
            job.token_key_id,
            job.meeting_token,
            job.reason.value,
            job.erased_meeting_version,
            job.status.value,
            job.recording_state.value,
            _as_text(job.pending_audio_asset_id),
            _as_text(job.cleanup_job_id),
            _as_erasure_datetime(job.database_checkpointed_at),
            job.retry_count,
            _as_erasure_datetime(job.next_attempt_at),
            job.lease_owner,
            _as_erasure_datetime(job.lease_expires_at),
            *failure,
            job.remediation_count,
            job.max_remediations,
            job.version,
            _as_erasure_datetime(job.created_at),
            _as_erasure_datetime(job.updated_at),
            _as_erasure_datetime(job.completed_at),
        )

    @staticmethod
    def _from_row(row: sqlite3.Row) -> MeetingErasureJob:
        return MeetingErasureJob.model_validate(
            {
                "id": row["id"],
                "token_version": row["token_version"],
                "token_key_id": row["token_key_id"],
                "meeting_token": row["meeting_token"],
                "reason": row["reason"],
                "erased_meeting_version": row["erased_meeting_version"],
                "status": row["status"],
                "recording_state": row["recording_state"],
                "pending_audio_asset_id": row["pending_audio_asset_id"],
                "cleanup_job_id": row["cleanup_job_id"],
                "database_checkpointed_at": row["database_checkpointed_at"],
                "retry_count": row["retry_count"],
                "next_attempt_at": row["next_attempt_at"],
                "lease_owner": row["lease_owner"],
                "lease_expires_at": row["lease_expires_at"],
                "last_failure": _erasure_failure_from_row(row),
                "remediation_count": row["remediation_count"],
                "max_remediations": row["max_remediations"],
                "version": row["version"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "completed_at": row["completed_at"],
            }
        )


class SqliteMeetingErasureOperationRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def add(self, binding: MeetingErasureOperationBinding) -> None:
        self._connection.execute(
            """
            INSERT INTO meeting_erasure_operation_bindings (
                token_version, token_key_id, request_token, actor_token,
                resource_token, erasure_job_id, operation, expected_version,
                request_fingerprint, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                binding.token_version,
                binding.token_key_id,
                binding.request_token,
                binding.actor_token,
                binding.resource_token,
                str(binding.erasure_job_id),
                binding.operation.value,
                binding.expected_version,
                binding.request_fingerprint,
                _as_erasure_datetime(binding.created_at),
            ),
        )

    def find_by_request_tokens(
        self,
        tokens: Sequence[ErasureToken],
    ) -> MeetingErasureOperationBinding | None:
        matches: dict[tuple[int, str, str], sqlite3.Row] = {}
        for token in _token_candidates(tokens):
            row = self._connection.execute(
                """
                SELECT * FROM meeting_erasure_operation_bindings
                WHERE token_version = ? AND token_key_id = ? AND request_token = ?
                """,
                (token.token_version, token.key_id, token.digest),
            ).fetchone()
            if row is not None:
                identity = (row["token_version"], row["token_key_id"], row["request_token"])
                matches[identity] = row
        if len(matches) > 1:
            raise PersistenceIntegrityError("Erasure request candidates matched multiple bindings")
        row = next(iter(matches.values()), None)
        return self._from_row(row) if row is not None else None

    def list_for_job(self, job_id: UUID) -> Sequence[MeetingErasureOperationBinding]:
        rows = self._connection.execute(
            """
            SELECT * FROM meeting_erasure_operation_bindings
            WHERE erasure_job_id = ?
            ORDER BY created_at, token_version, token_key_id, request_token
            """,
            (str(job_id),),
        ).fetchall()
        return tuple(self._from_row(row) for row in rows)

    @staticmethod
    def _from_row(row: sqlite3.Row) -> MeetingErasureOperationBinding:
        return MeetingErasureOperationBinding.model_validate(dict(row))


class SqliteMeetingErasureTombstoneRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def add(self, tombstone: MeetingErasureTombstone) -> None:
        self._connection.execute(
            """
            INSERT INTO meeting_erasure_tombstones (
                erasure_job_id, token_version, token_key_id, meeting_token,
                ingest_key_token, erased_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(tombstone.erasure_job_id),
                tombstone.token_version,
                tombstone.token_key_id,
                tombstone.meeting_token,
                tombstone.ingest_key_token,
                _as_erasure_datetime(tombstone.erased_at),
            ),
        )

    def get_for_job(self, job_id: UUID) -> MeetingErasureTombstone | None:
        row = self._connection.execute(
            "SELECT * FROM meeting_erasure_tombstones WHERE erasure_job_id = ?",
            (str(job_id),),
        ).fetchone()
        return self._from_row(row) if row is not None else None

    def find_by_meeting_tokens(
        self,
        tokens: Sequence[ErasureToken],
    ) -> MeetingErasureTombstone | None:
        return self._find_by_tokens("meeting_token", tokens)

    def find_by_ingest_key_tokens(
        self,
        tokens: Sequence[ErasureToken],
    ) -> MeetingErasureTombstone | None:
        return self._find_by_tokens("ingest_key_token", tokens)

    def _find_by_tokens(
        self,
        column: str,
        tokens: Sequence[ErasureToken],
    ) -> MeetingErasureTombstone | None:
        if column == "meeting_token":
            query = """
                SELECT * FROM meeting_erasure_tombstones
                WHERE token_version = ? AND token_key_id = ? AND meeting_token = ?
            """
        else:
            query = """
                SELECT * FROM meeting_erasure_tombstones
                WHERE token_version = ? AND token_key_id = ? AND ingest_key_token = ?
            """
        matches: dict[str, sqlite3.Row] = {}
        for token in _token_candidates(tokens):
            row = self._connection.execute(
                query,
                (token.token_version, token.key_id, token.digest),
            ).fetchone()
            if row is not None:
                matches[row["erasure_job_id"]] = row
        if len(matches) > 1:
            raise PersistenceIntegrityError("Erasure token candidates matched multiple tombstones")
        row = next(iter(matches.values()), None)
        return self._from_row(row) if row is not None else None

    @staticmethod
    def _from_row(row: sqlite3.Row) -> MeetingErasureTombstone:
        return MeetingErasureTombstone.model_validate(dict(row))


class SqliteMeetingErasurePurgeRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def has_active_work(self, meeting_id: UUID, now: datetime) -> bool:
        row = self._connection.execute(
            """
            SELECT
                EXISTS (
                    SELECT 1 FROM processing_jobs
                    WHERE meeting_id = ?
                      AND (
                          status = 'running'
                          OR status NOT IN (
                              'ready', 'running', 'retry_wait',
                              'succeeded', 'failed', 'cancelled'
                          )
                      )
                )
                OR EXISTS (
                    SELECT 1 FROM delivery_operation_bindings
                    WHERE meeting_id = ? AND status = 'running'
                      AND (
                          lease_expires_at IS NULL
                          OR julianday(lease_expires_at) IS NULL
                          OR julianday(lease_expires_at) > julianday(?)
                      )
                )
                OR EXISTS (
                    SELECT 1 FROM write_intents
                    WHERE meeting_id = ?
                      AND (
                          status IN ('in_flight', 'unknown')
                          OR status NOT IN (
                              'pending', 'in_flight', 'retry_wait',
                              'unknown', 'succeeded', 'permanent_failed'
                          )
                      )
                )
            """,
            (str(meeting_id), str(meeting_id), str(now), str(meeting_id)),
        ).fetchone()
        return row is not None and bool(row[0])

    def meeting_graph_is_consistent(
        self,
        meeting_id: UUID,
        audio_asset_id: UUID,
    ) -> bool:
        row = self._connection.execute(
            """
            SELECT NOT (
                EXISTS (
                    SELECT 1 FROM meetings meeting
                    WHERE meeting.id = ?
                      AND meeting.current_transcript_id IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM transcripts transcript
                          WHERE transcript.id = meeting.current_transcript_id
                            AND transcript.meeting_id = meeting.id
                            AND transcript.audio_asset_id = ?
                      )
                )
                OR EXISTS (
                    SELECT 1 FROM meetings meeting
                    WHERE meeting.id = ?
                      AND meeting.current_review_id IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM review_revisions review
                          WHERE review.id = meeting.current_review_id
                            AND review.meeting_id = meeting.id
                      )
                )
                OR EXISTS (
                    SELECT 1 FROM meetings meeting
                    WHERE meeting.id = ?
                      AND meeting.approved_review_id IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM review_revisions review
                          WHERE review.id = meeting.approved_review_id
                            AND review.meeting_id = meeting.id
                      )
                )
                OR EXISTS (
                    SELECT 1 FROM meetings other
                    JOIN transcripts transcript
                      ON transcript.id = other.current_transcript_id
                    WHERE other.id <> ? AND transcript.meeting_id = ?
                )
                OR EXISTS (
                    SELECT 1 FROM meetings other
                    JOIN review_revisions review
                      ON review.id IN (other.current_review_id, other.approved_review_id)
                    WHERE other.id <> ? AND review.meeting_id = ?
                )
                OR EXISTS (
                    SELECT 1 FROM transcripts transcript
                    WHERE transcript.meeting_id = ? AND transcript.audio_asset_id <> ?
                )
                OR EXISTS (
                    SELECT 1 FROM review_revisions review
                    LEFT JOIN transcripts transcript ON transcript.id = review.transcript_id
                    WHERE (review.meeting_id = ? OR transcript.meeting_id = ?)
                      AND (
                          transcript.id IS NULL
                          OR review.meeting_id <> transcript.meeting_id
                      )
                )
                OR EXISTS (
                    SELECT 1 FROM approvals approval
                    LEFT JOIN review_revisions review
                      ON review.id = approval.review_revision_id
                    WHERE (approval.meeting_id = ? OR review.meeting_id = ?)
                      AND (
                          review.id IS NULL
                          OR approval.meeting_id <> review.meeting_id
                      )
                )
                OR EXISTS (
                    SELECT 1 FROM recap_artifacts recap
                    LEFT JOIN approvals approval ON approval.id = recap.approval_id
                    WHERE (recap.meeting_id = ? OR approval.meeting_id = ?)
                      AND (
                          approval.id IS NULL
                          OR recap.meeting_id <> approval.meeting_id
                      )
                )
                OR EXISTS (
                    SELECT 1 FROM write_intents intent
                    LEFT JOIN approvals approval ON approval.id = intent.approval_id
                    WHERE (intent.meeting_id = ? OR approval.meeting_id = ?)
                      AND (
                          approval.id IS NULL
                          OR intent.meeting_id <> approval.meeting_id
                      )
                )
            )
            """,
            (
                str(meeting_id),
                str(audio_asset_id),
                str(meeting_id),
                str(meeting_id),
                str(meeting_id),
                str(meeting_id),
                str(meeting_id),
                str(meeting_id),
                str(meeting_id),
                str(audio_asset_id),
                str(meeting_id),
                str(meeting_id),
                str(meeting_id),
                str(meeting_id),
                str(meeting_id),
                str(meeting_id),
                str(meeting_id),
                str(meeting_id),
            ),
        ).fetchone()
        return row is not None and bool(row[0])

    def audio_has_other_references(
        self,
        audio_asset_id: UUID,
        meeting_id: UUID,
    ) -> bool:
        row = self._connection.execute(
            """
            SELECT
                EXISTS (
                    SELECT 1 FROM meetings
                    WHERE audio_asset_id = ? AND id <> ?
                )
                OR EXISTS (
                    SELECT 1 FROM transcripts
                    WHERE audio_asset_id = ? AND meeting_id <> ?
                )
            """,
            (
                str(audio_asset_id),
                str(meeting_id),
                str(audio_asset_id),
                str(meeting_id),
            ),
        ).fetchone()
        return row is not None and bool(row[0])

    def delete_meeting_graph(self, meeting_id: UUID) -> bool:
        self._connection.execute("SAVEPOINT meeting_erasure_graph_delete")
        try:
            self._connection.execute(
                "DELETE FROM delivery_operation_bindings WHERE meeting_id = ?",
                (str(meeting_id),),
            )
            cursor = self._connection.execute(
                "DELETE FROM meetings WHERE id = ?",
                (str(meeting_id),),
            )
        except sqlite3.IntegrityError:
            self._connection.execute("ROLLBACK TO meeting_erasure_graph_delete")
            self._connection.execute("RELEASE meeting_erasure_graph_delete")
            return False
        if cursor.rowcount != 1:
            self._connection.execute("ROLLBACK TO meeting_erasure_graph_delete")
            self._connection.execute("RELEASE meeting_erasure_graph_delete")
            return False
        self._connection.execute("RELEASE meeting_erasure_graph_delete")
        return True

    def delete_audio_asset(self, audio_asset_id: UUID) -> bool:
        try:
            cursor = self._connection.execute(
                "DELETE FROM audio_assets WHERE id = ?",
                (str(audio_asset_id),),
            )
        except sqlite3.IntegrityError:
            return False
        return cursor.rowcount == 1


class SqliteTranscriptRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def add(self, transcript: Transcript) -> None:
        self._connection.execute(
            """
            INSERT INTO transcripts (
                id, meeting_id, audio_asset_id, provider, model, language,
                segments_json, text, sha256, provider_request_id, usage_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(transcript.id),
                str(transcript.meeting_id),
                str(transcript.audio_asset_id),
                transcript.provider,
                transcript.model,
                transcript.language,
                _as_json(transcript.segments),
                transcript.text,
                transcript.sha256,
                transcript.provider_request_id,
                "{}",
                str(transcript.created_at),
            ),
        )

    def get(self, transcript_id: UUID) -> Transcript | None:
        row = self._connection.execute(
            "SELECT * FROM transcripts WHERE id = ?", (str(transcript_id),)
        ).fetchone()
        return self._from_row(row) if row is not None else None

    def latest_for_meeting(self, meeting_id: UUID) -> Transcript | None:
        row = self._connection.execute(
            """
            SELECT * FROM transcripts
            WHERE meeting_id = ? ORDER BY created_at DESC, id DESC LIMIT 1
            """,
            (str(meeting_id),),
        ).fetchone()
        return self._from_row(row) if row is not None else None

    @staticmethod
    def _from_row(row: sqlite3.Row) -> Transcript:
        return Transcript.model_validate(
            {
                "id": row["id"],
                "meeting_id": row["meeting_id"],
                "audio_asset_id": row["audio_asset_id"],
                "provider": row["provider"],
                "model": row["model"],
                "language": row["language"],
                "segments": _load_json(row["segments_json"], []),
                "text": row["text"],
                "sha256": row["sha256"],
                "provider_request_id": row["provider_request_id"],
                "created_at": row["created_at"],
            }
        )


class SqliteReviewRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def add(self, review: ReviewRevision) -> None:
        self._connection.execute(
            """
            INSERT INTO review_revisions (
                id, meeting_id, transcript_id, revision_number, origin,
                payload_json, content_digest, actor_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(review.id),
                str(review.meeting_id),
                str(review.transcript_id),
                review.revision_number,
                review.origin.value,
                _as_json(review),
                review.content_digest,
                review.actor_id,
                str(review.created_at),
            ),
        )

    def get(self, review_id: UUID) -> ReviewRevision | None:
        row = self._connection.execute(
            "SELECT payload_json FROM review_revisions WHERE id = ?", (str(review_id),)
        ).fetchone()
        return self._from_row(row) if row is not None else None

    def latest_for_meeting(self, meeting_id: UUID) -> ReviewRevision | None:
        row = self._connection.execute(
            """
            SELECT payload_json FROM review_revisions
            WHERE meeting_id = ? ORDER BY revision_number DESC LIMIT 1
            """,
            (str(meeting_id),),
        ).fetchone()
        return self._from_row(row) if row is not None else None

    def list_for_meeting(self, meeting_id: UUID) -> Sequence[ReviewRevision]:
        rows = self._connection.execute(
            """
            SELECT payload_json FROM review_revisions
            WHERE meeting_id = ? ORDER BY revision_number
            """,
            (str(meeting_id),),
        ).fetchall()
        return tuple(self._from_row(row) for row in rows)

    @staticmethod
    def _from_row(row: sqlite3.Row) -> ReviewRevision:
        return ReviewRevision.model_validate_json(row["payload_json"])


class SqliteApprovalRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def add(self, approval: Approval) -> None:
        self._connection.execute(
            """
            INSERT INTO approvals (
                id, meeting_id, review_revision_id, review_digest,
                request_key, actor_id, approved_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(approval.id),
                str(approval.meeting_id),
                str(approval.review_revision_id),
                approval.review_digest,
                approval.request_key,
                approval.actor_id,
                str(approval.approved_at),
            ),
        )

    def get(self, approval_id: UUID) -> Approval | None:
        row = self._connection.execute(
            "SELECT * FROM approvals WHERE id = ?", (str(approval_id),)
        ).fetchone()
        return self._from_row(row)

    def for_meeting(self, meeting_id: UUID) -> Approval | None:
        row = self._connection.execute(
            "SELECT * FROM approvals WHERE meeting_id = ?", (str(meeting_id),)
        ).fetchone()
        return self._from_row(row)

    def find_by_request_key(self, request_key: str) -> Approval | None:
        row = self._connection.execute(
            "SELECT * FROM approvals WHERE request_key = ?", (request_key,)
        ).fetchone()
        return self._from_row(row)

    @staticmethod
    def _from_row(row: sqlite3.Row | None) -> Approval | None:
        if row is None:
            return None
        return Approval.model_validate(dict(row))


class SqliteRecapRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def add(self, recap: RecapArtifact) -> None:
        self._connection.execute(
            """
            INSERT INTO recap_artifacts (
                id, meeting_id, approval_id, format, content, sha256, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(recap.id),
                str(recap.meeting_id),
                str(recap.approval_id),
                "markdown",
                recap.content,
                recap.sha256,
                str(recap.created_at),
            ),
        )

    def for_approval(self, approval_id: UUID) -> RecapArtifact | None:
        row = self._connection.execute(
            "SELECT * FROM recap_artifacts WHERE approval_id = ?", (str(approval_id),)
        ).fetchone()
        if row is None:
            return None
        return RecapArtifact.model_validate(
            {
                "id": row["id"],
                "meeting_id": row["meeting_id"],
                "approval_id": row["approval_id"],
                "content": row["content"],
                "sha256": row["sha256"],
                "created_at": row["created_at"],
            }
        )


class SqliteDeliveryOperationRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def add(self, binding: DeliveryOperationBinding) -> None:
        self._connection.execute(
            """
            INSERT INTO delivery_operation_bindings (
                request_key, meeting_id, operation, actor_id,
                selection_fingerprint, status, lease_owner, lease_expires_at,
                completed_at, version, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                binding.request_key,
                str(binding.meeting_id),
                binding.operation.value,
                binding.actor_id,
                binding.selection_fingerprint,
                binding.status.value,
                binding.lease_owner,
                _as_text(binding.lease_expires_at),
                _as_text(binding.completed_at),
                binding.version,
                str(binding.created_at),
                str(binding.updated_at),
            ),
        )

    def get(self, request_key: str) -> DeliveryOperationBinding | None:
        row = self._connection.execute(
            "SELECT * FROM delivery_operation_bindings WHERE request_key = ?",
            (request_key,),
        ).fetchone()
        if row is None:
            return None
        return DeliveryOperationBinding.model_validate(dict(row))

    def claim(
        self,
        request_key: str,
        owner: str,
        now: datetime,
        lease_until: datetime,
    ) -> DeliveryOperationBinding | None:
        cursor = self._connection.execute(
            """
            UPDATE delivery_operation_bindings
            SET status = 'running', lease_owner = ?, lease_expires_at = ?,
                completed_at = NULL, version = version + 1, updated_at = ?
            WHERE request_key = ?
              AND (
                status = 'pending'
                OR (status = 'running' AND lease_expires_at <= ?)
              )
            """,
            (owner, str(lease_until), str(now), request_key, str(now)),
        )
        return self.get(request_key) if cursor.rowcount == 1 else None

    def release(
        self,
        request_key: str,
        owner: str,
        expected_version: int,
        now: datetime,
    ) -> bool:
        cursor = self._connection.execute(
            """
            UPDATE delivery_operation_bindings
            SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL,
                completed_at = NULL, version = version + 1, updated_at = ?
            WHERE request_key = ? AND status = 'running'
              AND lease_owner = ? AND version = ?
            """,
            (str(now), request_key, owner, expected_version),
        )
        return cursor.rowcount == 1

    def renew(
        self,
        request_key: str,
        owner: str,
        expected_version: int,
        now: datetime,
        lease_until: datetime,
    ) -> DeliveryOperationBinding | None:
        cursor = self._connection.execute(
            """
            UPDATE delivery_operation_bindings
            SET lease_expires_at = ?, version = version + 1, updated_at = ?
            WHERE request_key = ? AND status = 'running'
              AND lease_owner = ? AND version = ? AND lease_expires_at > ?
            """,
            (
                str(lease_until),
                str(now),
                request_key,
                owner,
                expected_version,
                str(now),
            ),
        )
        return self.get(request_key) if cursor.rowcount == 1 else None

    def complete(
        self,
        request_key: str,
        owner: str,
        expected_version: int,
        now: datetime,
    ) -> bool:
        cursor = self._connection.execute(
            """
            UPDATE delivery_operation_bindings
            SET status = 'completed', lease_owner = NULL, lease_expires_at = NULL,
                completed_at = ?, version = version + 1, updated_at = ?
            WHERE request_key = ? AND status = 'running'
              AND lease_owner = ? AND version = ? AND lease_expires_at > ?
            """,
            (str(now), str(now), request_key, owner, expected_version, str(now)),
        )
        return cursor.rowcount == 1


class SqliteMeetingOperationRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def add(self, binding: MeetingOperationBinding) -> None:
        self._connection.execute(
            """
            INSERT INTO meeting_operation_bindings (
                request_key, meeting_id, operation, actor_id, stage,
                expected_version, request_fingerprint, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                binding.request_key,
                str(binding.meeting_id),
                binding.operation.value,
                binding.actor_id,
                binding.stage.value if binding.stage is not None else None,
                binding.expected_version,
                binding.request_fingerprint,
                str(binding.created_at),
            ),
        )

    def get(self, request_key: str) -> MeetingOperationBinding | None:
        row = self._connection.execute(
            "SELECT * FROM meeting_operation_bindings WHERE request_key = ?",
            (request_key,),
        ).fetchone()
        if row is None:
            return None
        return MeetingOperationBinding.model_validate(dict(row))


class SqliteProcessingJobRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def add(self, job: ProcessingJob) -> None:
        self._connection.execute(
            """
            INSERT INTO processing_jobs (
                id, meeting_id, stage, status, attempt_count, max_attempts,
                next_attempt_at, lease_owner, lease_expires_at, last_failure_json,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            self._values(job),
        )

    def get(self, job_id: UUID) -> ProcessingJob | None:
        row = self._connection.execute(
            "SELECT * FROM processing_jobs WHERE id = ?", (str(job_id),)
        ).fetchone()
        return self._from_row(row) if row is not None else None

    def find_for_stage(
        self,
        meeting_id: UUID,
        stage: ProcessingStage,
    ) -> ProcessingJob | None:
        row = self._connection.execute(
            "SELECT * FROM processing_jobs WHERE meeting_id = ? AND stage = ?",
            (str(meeting_id), stage.value),
        ).fetchone()
        return self._from_row(row) if row is not None else None

    def list_for_meeting(self, meeting_id: UUID) -> Sequence[ProcessingJob]:
        rows = self._connection.execute(
            """
            SELECT * FROM processing_jobs WHERE meeting_id = ?
            ORDER BY CASE stage WHEN 'transcription' THEN 0 ELSE 1 END, id
            """,
            (str(meeting_id),),
        ).fetchall()
        return tuple(self._from_row(row) for row in rows)

    def list_expired_exhausted(
        self,
        stage: ProcessingStage,
        now: datetime,
        limit: int,
    ) -> Sequence[ProcessingJob]:
        if limit <= 0:
            return ()
        rows = self._connection.execute(
            """
            SELECT * FROM processing_jobs
            WHERE stage = ? AND status = ? AND lease_expires_at <= ?
                AND attempt_count >= max_attempts
            ORDER BY lease_expires_at, created_at, id
            LIMIT ?
            """,
            (
                stage.value,
                ProcessingJobStatus.RUNNING.value,
                str(now),
                limit,
            ),
        ).fetchall()
        return tuple(self._from_row(row) for row in rows)

    def claim_due(
        self,
        stage: ProcessingStage,
        worker_id: str,
        now: datetime,
        lease_until: datetime,
        limit: int,
    ) -> Sequence[ProcessingJob]:
        if limit <= 0:
            return ()
        rows = self._connection.execute(
            """
            SELECT id FROM processing_jobs
            WHERE stage = ? AND attempt_count < max_attempts AND (
                status = ? OR
                (status = ? AND next_attempt_at <= ?) OR
                (status = ? AND lease_expires_at <= ?)
            )
            ORDER BY COALESCE(next_attempt_at, created_at), created_at, id
            LIMIT ?
            """,
            (
                stage.value,
                ProcessingJobStatus.READY.value,
                ProcessingJobStatus.RETRY_WAIT.value,
                str(now),
                ProcessingJobStatus.RUNNING.value,
                str(now),
                limit,
            ),
        ).fetchall()
        claimed: list[ProcessingJob] = []
        for row in rows:
            self._connection.execute(
                """
                UPDATE processing_jobs SET status = ?, attempt_count = attempt_count + 1,
                    next_attempt_at = NULL, lease_owner = ?, lease_expires_at = ?,
                    last_failure_json = NULL, updated_at = ?
                WHERE id = ?
                """,
                (
                    ProcessingJobStatus.RUNNING.value,
                    worker_id,
                    str(lease_until),
                    str(now),
                    row["id"],
                ),
            )
            item = self.get(UUID(row["id"]))
            if item is not None:
                claimed.append(item)
        return tuple(claimed)

    def save(
        self,
        job: ProcessingJob,
        expected_status: ProcessingJobStatus,
        expected_lease_owner: str | None,
        expected_lease_expires_at: datetime | None,
    ) -> None:
        values = self._values(job)
        cursor = self._connection.execute(
            """
            UPDATE processing_jobs SET meeting_id = ?, stage = ?, status = ?,
                attempt_count = ?, max_attempts = ?, next_attempt_at = ?,
                lease_owner = ?, lease_expires_at = ?, last_failure_json = ?,
                created_at = ?, updated_at = ?
            WHERE id = ? AND status = ? AND lease_owner IS ? AND lease_expires_at IS ?
            """,
            (
                *values[1:],
                values[0],
                expected_status.value,
                expected_lease_owner,
                _as_text(expected_lease_expires_at),
            ),
        )
        if cursor.rowcount != 1:
            raise PersistenceConflictError("The processing job lease is no longer current")

    @staticmethod
    def _values(job: ProcessingJob) -> tuple[object, ...]:
        return (
            str(job.id),
            str(job.meeting_id),
            job.stage.value,
            job.status.value,
            job.attempt_count,
            job.max_attempts,
            _as_text(job.next_attempt_at),
            job.lease_owner,
            _as_text(job.lease_expires_at),
            _as_json(job.last_failure) if job.last_failure is not None else None,
            str(job.created_at),
            str(job.updated_at),
        )

    @staticmethod
    def _from_row(row: sqlite3.Row) -> ProcessingJob:
        return ProcessingJob.model_validate(
            {
                "id": row["id"],
                "meeting_id": row["meeting_id"],
                "stage": row["stage"],
                "status": row["status"],
                "attempt_count": row["attempt_count"],
                "max_attempts": row["max_attempts"],
                "next_attempt_at": row["next_attempt_at"],
                "lease_owner": row["lease_owner"],
                "lease_expires_at": row["lease_expires_at"],
                "last_failure": _load_json(row["last_failure_json"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        )


class SqliteWriteIntentRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def add_many(self, intents: Sequence[WriteIntent]) -> None:
        self._connection.executemany(
            """
            INSERT INTO write_intents (
                id, meeting_id, approval_id, source_action_id, kind, connector_id,
                resource_id, idempotency_key, payload_json, payload_sha256, status,
                attempt_count, next_attempt_at, next_reconcile_at,
                reconcile_attempt_count, reconcile_lease_owner,
                reconcile_lease_expires_at, lease_owner, lease_expires_at,
                last_failure_json, version, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [self._values(intent) for intent in intents],
        )

    def get(self, intent_id: UUID) -> WriteIntent | None:
        row = self._connection.execute(
            "SELECT * FROM write_intents WHERE id = ?", (str(intent_id),)
        ).fetchone()
        return self._from_row(row) if row is not None else None

    def list_for_approval(self, approval_id: UUID) -> Sequence[WriteIntent]:
        rows = self._connection.execute(
            """
            SELECT * FROM write_intents WHERE approval_id = ?
            ORDER BY source_action_id,
                CASE kind WHEN 'task' THEN 0 ELSE 1 END,
                id
            """,
            (str(approval_id),),
        ).fetchall()
        return tuple(self._from_row(row) for row in rows)

    def claim_due(
        self,
        worker_id: str,
        now: datetime,
        lease_until: datetime,
        limit: int,
    ) -> Sequence[WriteIntent]:
        claimed = self.claim_due_with_previous_statuses(
            worker_id,
            now,
            lease_until,
            limit,
        )
        return tuple(item for intent_id, _ in claimed if (item := self.get(intent_id)) is not None)

    def claim_due_with_previous_statuses(
        self,
        worker_id: str,
        now: datetime,
        lease_until: datetime,
        limit: int,
    ) -> Sequence[tuple[UUID, WriteStatus]]:
        if limit <= 0:
            return ()
        rows = self._connection.execute(
            """
            SELECT id, status FROM write_intents
            WHERE status IN (?, ?)
              AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
              AND lease_owner IS NULL
            ORDER BY created_at, id LIMIT ?
            """,
            (WriteStatus.PENDING.value, WriteStatus.RETRY_WAIT.value, str(now), limit),
        ).fetchall()
        for row in rows:
            self._connection.execute(
                """
                UPDATE write_intents SET status = ?, attempt_count = attempt_count + 1,
                    next_attempt_at = NULL, next_reconcile_at = NULL,
                    reconcile_attempt_count = 0, reconcile_lease_owner = NULL,
                    reconcile_lease_expires_at = NULL, lease_owner = ?,
                    lease_expires_at = ?, last_failure_json = NULL,
                    version = version + 1, updated_at = ?
                WHERE id = ?
                """,
                (
                    WriteStatus.IN_FLIGHT.value,
                    worker_id,
                    str(lease_until),
                    str(now),
                    row["id"],
                ),
            )
        return tuple((UUID(row["id"]), WriteStatus(row["status"])) for row in rows)

    def claim_due_ids(
        self,
        worker_id: str,
        now: datetime,
        lease_until: datetime,
        limit: int,
    ) -> Sequence[UUID]:
        return tuple(intent.id for intent in self.claim_due(worker_id, now, lease_until, limit))

    def recover_expired_ids(
        self,
        now: datetime,
        failure: WorkflowFailure,
        limit: int,
    ) -> Sequence[UUID]:
        if limit <= 0:
            return ()
        if failure.disposition is not FailureDisposition.UNKNOWN_OUTCOME:
            raise ValueError("Expired writes require an unknown-outcome failure")
        rows = self._connection.execute(
            """
            SELECT id FROM write_intents
            WHERE status = ? AND lease_expires_at <= ?
            ORDER BY lease_expires_at, created_at, id LIMIT ?
            """,
            (WriteStatus.IN_FLIGHT.value, str(now), limit),
        ).fetchall()
        recovered = tuple(UUID(row["id"]) for row in rows)
        if not recovered:
            return ()
        self._connection.executemany(
            """
            UPDATE write_intents SET status = ?, next_attempt_at = NULL,
                next_reconcile_at = ?, reconcile_attempt_count = 0,
                reconcile_lease_owner = NULL, reconcile_lease_expires_at = NULL,
                lease_owner = NULL, lease_expires_at = NULL, last_failure_json = ?,
                version = version + 1, updated_at = ?
            WHERE id = ? AND status = ? AND lease_expires_at <= ?
            """,
            [
                (
                    WriteStatus.UNKNOWN.value,
                    str(now),
                    _as_json(failure),
                    str(now),
                    str(intent_id),
                    WriteStatus.IN_FLIGHT.value,
                    str(now),
                )
                for intent_id in recovered
            ],
        )
        return recovered

    def list_unknown_ids(self, now: datetime, limit: int) -> Sequence[UUID]:
        if limit <= 0:
            return ()
        rows = self._connection.execute(
            """
            SELECT id FROM write_intents
            WHERE status = ? AND next_reconcile_at <= ?
              AND (
                reconcile_lease_owner IS NULL
                OR reconcile_lease_expires_at <= ?
              )
            ORDER BY next_reconcile_at, created_at, id LIMIT ?
            """,
            (WriteStatus.UNKNOWN.value, str(now), str(now), limit),
        ).fetchall()
        return tuple(UUID(row["id"]) for row in rows)

    def claim_due_unknown_ids(
        self,
        owner: str,
        now: datetime,
        lease_until: datetime,
        limit: int,
    ) -> Sequence[UUID]:
        if limit <= 0:
            return ()
        rows = self._connection.execute(
            """
            SELECT id FROM write_intents
            WHERE status = ? AND next_reconcile_at <= ?
              AND (
                reconcile_lease_owner IS NULL
                OR reconcile_lease_expires_at <= ?
              )
            ORDER BY next_reconcile_at, created_at, id LIMIT ?
            """,
            (WriteStatus.UNKNOWN.value, str(now), str(now), limit),
        ).fetchall()
        claimed: list[UUID] = []
        for row in rows:
            cursor = self._connection.execute(
                """
                UPDATE write_intents
                SET reconcile_lease_owner = ?, reconcile_lease_expires_at = ?,
                    next_reconcile_at = ?, version = version + 1, updated_at = ?
                WHERE id = ? AND status = ? AND next_reconcile_at <= ?
                  AND (
                    reconcile_lease_owner IS NULL
                    OR reconcile_lease_expires_at <= ?
                  )
                """,
                (
                    owner,
                    str(lease_until),
                    str(now),
                    str(now),
                    row["id"],
                    WriteStatus.UNKNOWN.value,
                    str(now),
                    str(now),
                ),
            )
            if cursor.rowcount == 1:
                claimed.append(UUID(row["id"]))
        return tuple(claimed)

    def claim_unknown(
        self,
        intent_id: UUID,
        owner: str,
        now: datetime,
        lease_until: datetime,
        *,
        force: bool,
    ) -> WriteIntent | None:
        cursor = self._connection.execute(
            """
            UPDATE write_intents
            SET reconcile_lease_owner = ?, reconcile_lease_expires_at = ?,
                next_reconcile_at = ?, version = version + 1, updated_at = ?
            WHERE id = ? AND status = ?
              AND (
                reconcile_lease_owner IS NULL
                OR reconcile_lease_expires_at <= ?
              )
              AND (? = 1 OR next_reconcile_at <= ?)
            """,
            (
                owner,
                str(lease_until),
                str(now),
                str(now),
                str(intent_id),
                WriteStatus.UNKNOWN.value,
                str(now),
                int(force),
                str(now),
            ),
        )
        return self.get(intent_id) if cursor.rowcount == 1 else None

    def save(self, intent: WriteIntent, expected_version: int) -> None:
        values = self._values(intent)
        cursor = self._connection.execute(
            """
            UPDATE write_intents SET
                meeting_id = ?, approval_id = ?, source_action_id = ?, kind = ?,
                connector_id = ?, resource_id = ?, idempotency_key = ?, payload_json = ?,
                payload_sha256 = ?, status = ?, attempt_count = ?, next_attempt_at = ?,
                next_reconcile_at = ?, reconcile_attempt_count = ?,
                reconcile_lease_owner = ?, reconcile_lease_expires_at = ?,
                lease_owner = ?, lease_expires_at = ?, last_failure_json = ?, version = ?,
                created_at = ?, updated_at = ?
            WHERE id = ? AND version = ?
            """,
            (*values[1:], values[0], expected_version),
        )
        if cursor.rowcount != 1:
            raise PersistenceConflictError("The write intent was changed by another operation")

    @staticmethod
    def _values(intent: WriteIntent) -> tuple[object, ...]:
        proposal = intent.proposal
        return (
            str(intent.id),
            str(intent.meeting_id),
            str(intent.approval_id),
            str(proposal.source_action_id),
            proposal.kind.value,
            proposal.target.connector_id,
            proposal.target.resource_id,
            intent.idempotency_key,
            _as_json(proposal),
            intent.payload_digest,
            intent.status.value,
            intent.attempt_count,
            _as_text(intent.next_attempt_at),
            _as_text(intent.next_reconcile_at),
            intent.reconcile_attempt_count,
            intent.reconcile_lease_owner,
            _as_text(intent.reconcile_lease_expires_at),
            intent.lease_owner,
            _as_text(intent.lease_expires_at),
            _as_json(intent.last_failure) if intent.last_failure is not None else None,
            intent.version,
            str(intent.created_at),
            str(intent.updated_at),
        )

    @staticmethod
    def _from_row(row: sqlite3.Row) -> WriteIntent:
        return WriteIntent.model_validate(
            {
                "id": row["id"],
                "meeting_id": row["meeting_id"],
                "approval_id": row["approval_id"],
                "idempotency_key": row["idempotency_key"],
                "proposal": _load_json(row["payload_json"]),
                "payload_digest": row["payload_sha256"],
                "status": row["status"],
                "attempt_count": row["attempt_count"],
                "next_attempt_at": row["next_attempt_at"],
                "next_reconcile_at": row["next_reconcile_at"],
                "reconcile_attempt_count": row["reconcile_attempt_count"],
                "reconcile_lease_owner": row["reconcile_lease_owner"],
                "reconcile_lease_expires_at": row["reconcile_lease_expires_at"],
                "lease_owner": row["lease_owner"],
                "lease_expires_at": row["lease_expires_at"],
                "last_failure": _load_json(row["last_failure_json"]),
                "version": row["version"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        )


class SqliteWriteReceiptRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def add(self, receipt: WriteReceipt) -> None:
        self._connection.execute(
            """
            INSERT INTO write_receipts (
                id, intent_id, idempotency_key, payload_digest, provider,
                external_id, external_url, reconciled, recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(receipt.id),
                str(receipt.intent_id),
                receipt.idempotency_key,
                receipt.payload_digest,
                receipt.provider,
                receipt.external_id,
                receipt.external_url,
                int(receipt.reconciled),
                str(receipt.recorded_at),
            ),
        )

    def for_intent(self, intent_id: UUID) -> WriteReceipt | None:
        row = self._connection.execute(
            "SELECT * FROM write_receipts WHERE intent_id = ?", (str(intent_id),)
        ).fetchone()
        return self._from_row(row)

    def find_by_idempotency_key(self, idempotency_key: str) -> WriteReceipt | None:
        row = self._connection.execute(
            "SELECT * FROM write_receipts WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
        return self._from_row(row)

    @staticmethod
    def _from_row(row: sqlite3.Row | None) -> WriteReceipt | None:
        if row is None:
            return None
        payload = dict(row)
        payload["reconciled"] = bool(payload["reconciled"])
        return WriteReceipt.model_validate(payload)


class SqliteUnitOfWork:
    def __init__(self, database: Database, *, immediate: bool = True) -> None:
        self._database = database
        self._immediate = immediate
        self._connection: sqlite3.Connection | None = None
        self._committed = False
        self.meetings: SqliteMeetingRepository
        self.ingest_requests: SqliteIngestRequestBindingRepository
        self.audio_assets: SqliteAudioAssetRepository
        self.recording_cleanups: SqliteRecordingCleanupRepository
        self.erasure_key_verifiers: SqliteErasureKeyVerifierRepository
        self.meeting_erasures: SqliteMeetingErasureRepository
        self.meeting_erasure_operations: SqliteMeetingErasureOperationRepository
        self.meeting_erasure_tombstones: SqliteMeetingErasureTombstoneRepository
        self.meeting_erasure_purge: SqliteMeetingErasurePurgeRepository
        self.transcripts: SqliteTranscriptRepository
        self.reviews: SqliteReviewRepository
        self.approvals: SqliteApprovalRepository
        self.recaps: SqliteRecapRepository
        self.delivery_operations: SqliteDeliveryOperationRepository
        self.meeting_operations: SqliteMeetingOperationRepository
        self.processing_jobs: SqliteProcessingJobRepository
        self.write_intents: SqliteWriteIntentRepository
        self.write_receipts: SqliteWriteReceiptRepository
        self.workflow_events: SqliteWorkflowEventRepository

    def __enter__(self) -> SqliteUnitOfWork:
        connection = self._database.connect()
        connection.execute("BEGIN IMMEDIATE" if self._immediate else "BEGIN")
        self._connection = connection
        self._committed = False
        self.meetings = SqliteMeetingRepository(connection)
        self.ingest_requests = SqliteIngestRequestBindingRepository(connection)
        self.audio_assets = SqliteAudioAssetRepository(connection)
        self.recording_cleanups = SqliteRecordingCleanupRepository(connection)
        self.erasure_key_verifiers = SqliteErasureKeyVerifierRepository(connection)
        self.meeting_erasures = SqliteMeetingErasureRepository(connection)
        self.meeting_erasure_operations = SqliteMeetingErasureOperationRepository(connection)
        self.meeting_erasure_tombstones = SqliteMeetingErasureTombstoneRepository(connection)
        self.meeting_erasure_purge = SqliteMeetingErasurePurgeRepository(connection)
        self.transcripts = SqliteTranscriptRepository(connection)
        self.reviews = SqliteReviewRepository(connection)
        self.approvals = SqliteApprovalRepository(connection)
        self.recaps = SqliteRecapRepository(connection)
        self.delivery_operations = SqliteDeliveryOperationRepository(connection)
        self.meeting_operations = SqliteMeetingOperationRepository(connection)
        self.processing_jobs = SqliteProcessingJobRepository(connection)
        self.write_intents = SqliteWriteIntentRepository(connection)
        self.write_receipts = SqliteWriteReceiptRepository(connection)
        self.workflow_events = SqliteWorkflowEventRepository(
            connection,
            writable=self._immediate,
        )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if self._connection is None:
            return False
        try:
            if exc_type is not None or not self._committed:
                self._connection.rollback()
        finally:
            self._connection.close()
            self._connection = None
        return False

    def commit(self) -> None:
        if self._connection is None:
            raise RuntimeError("The unit of work is not active")
        self._connection.commit()
        self._committed = True

    def rollback(self) -> None:
        if self._connection is None:
            raise RuntimeError("The unit of work is not active")
        self._connection.rollback()
