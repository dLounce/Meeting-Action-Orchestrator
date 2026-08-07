from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from datetime import datetime
from types import TracebackType
from uuid import UUID

from meeting_action_orchestrator.application.ports import MeetingListCursor
from meeting_action_orchestrator.domain.enums import (
    FailureDisposition,
    MeetingStatus,
    ProcessingJobStatus,
    ProcessingStage,
    WriteStatus,
)
from meeting_action_orchestrator.domain.hashing import canonical_json
from meeting_action_orchestrator.domain.models import (
    Approval,
    AudioAsset,
    DeliveryOperationBinding,
    Meeting,
    MeetingOperationBinding,
    ProcessingJob,
    RecapArtifact,
    ReviewRevision,
    Transcript,
    WorkflowFailure,
    WriteIntent,
    WriteReceipt,
)
from meeting_action_orchestrator.infrastructure.database import Database


class PersistenceConflictError(RuntimeError):
    pass


def _as_text(value: object | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _as_json(value: object) -> str:
    return canonical_json(value)


def _load_json(value: str | None, default: object = None) -> object:
    if value is None:
        return default
    return json.loads(value)


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
        if limit <= 0:
            return ()
        rows = self._connection.execute(
            """
            SELECT id FROM write_intents
            WHERE status IN (?, ?)
              AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
              AND lease_owner IS NULL
            ORDER BY created_at, id LIMIT ?
            """,
            (WriteStatus.PENDING.value, WriteStatus.RETRY_WAIT.value, str(now), limit),
        ).fetchall()
        claimed: list[WriteIntent] = []
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
            item = self.get(UUID(row["id"]))
            if item is not None:
                claimed.append(item)
        return tuple(claimed)

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
        self.audio_assets: SqliteAudioAssetRepository
        self.transcripts: SqliteTranscriptRepository
        self.reviews: SqliteReviewRepository
        self.approvals: SqliteApprovalRepository
        self.recaps: SqliteRecapRepository
        self.delivery_operations: SqliteDeliveryOperationRepository
        self.meeting_operations: SqliteMeetingOperationRepository
        self.processing_jobs: SqliteProcessingJobRepository
        self.write_intents: SqliteWriteIntentRepository
        self.write_receipts: SqliteWriteReceiptRepository

    def __enter__(self) -> SqliteUnitOfWork:
        connection = self._database.connect()
        connection.execute("BEGIN IMMEDIATE" if self._immediate else "BEGIN")
        self._connection = connection
        self._committed = False
        self.meetings = SqliteMeetingRepository(connection)
        self.audio_assets = SqliteAudioAssetRepository(connection)
        self.transcripts = SqliteTranscriptRepository(connection)
        self.reviews = SqliteReviewRepository(connection)
        self.approvals = SqliteApprovalRepository(connection)
        self.recaps = SqliteRecapRepository(connection)
        self.delivery_operations = SqliteDeliveryOperationRepository(connection)
        self.meeting_operations = SqliteMeetingOperationRepository(connection)
        self.processing_jobs = SqliteProcessingJobRepository(connection)
        self.write_intents = SqliteWriteIntentRepository(connection)
        self.write_receipts = SqliteWriteReceiptRepository(connection)
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
