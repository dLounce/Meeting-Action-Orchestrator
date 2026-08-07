from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from meeting_action_orchestrator.application.processing import FullJitterRetryScheduler
from meeting_action_orchestrator.domain.enums import ProcessingStage
from meeting_action_orchestrator.domain.models import ProcessingJob

NOW = datetime(2026, 8, 7, 9, 0, tzinfo=timezone.utc)
JOB_ID = UUID("dff1bba6-c18c-48de-9556-f1e41bdc8482")
MEETING_ID = UUID("12007b21-b54b-4497-a6f5-4bcf8ef8610a")


def test_processing_job_enforces_stage_attempt_limit() -> None:
    with pytest.raises(ValidationError, match="retry limit does not match"):
        ProcessingJob(
            id=JOB_ID,
            meeting_id=MEETING_ID,
            stage=ProcessingStage.TRANSCRIPTION,
            max_attempts=2,
            created_at=NOW,
            updated_at=NOW,
        )


def test_full_jitter_uses_exponential_ceiling() -> None:
    scheduler = FullJitterRetryScheduler(
        base_delay=timedelta(seconds=4),
        maximum_delay=timedelta(seconds=30),
        random_value=lambda: 0.5,
    )

    scheduled = scheduler.schedule(NOW, attempt_count=3)

    assert scheduled == NOW + timedelta(seconds=8)


def test_full_jitter_caps_delay_and_validates_random_source() -> None:
    capped = FullJitterRetryScheduler(
        base_delay=timedelta(seconds=10),
        maximum_delay=timedelta(seconds=20),
        random_value=lambda: 1.0,
    )
    invalid = FullJitterRetryScheduler(random_value=lambda: 1.1)

    assert capped.schedule(NOW, attempt_count=5) == NOW + timedelta(seconds=20)
    with pytest.raises(ValueError, match="between zero and one"):
        invalid.schedule(NOW, attempt_count=1)
