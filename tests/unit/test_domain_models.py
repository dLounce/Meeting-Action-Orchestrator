import hashlib
from datetime import date, datetime, timedelta, timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from meeting_action_orchestrator.domain import (
    INGEST_REQUEST_FINGERPRINT_VERSION,
    ActionItem,
    AudioAsset,
    AudioMediaType,
    ConnectorTarget,
    DateDeadline,
    DeadlineResolution,
    Decision,
    DeliveryDirective,
    DomainInvariantError,
    EvidenceRef,
    FailureCode,
    FailureDisposition,
    IngestAudioIdentity,
    IngestRequestBinding,
    IngestRequestIdentity,
    InvalidDomainValueError,
    OpenQuestion,
    PersonRef,
    Priority,
    ReviewOrigin,
    ReviewRevision,
    Risk,
    Transcript,
    TranscriptSegment,
    WorkflowFailure,
    canonical_json,
    canonical_sha256,
    validate_review_evidence,
)

NOW = datetime(2026, 6, 7, 9, 0, tzinfo=timezone.utc)
SHA256_LENGTH = 64


def uid(value: int) -> UUID:
    return UUID(int=value)


def make_transcript() -> Transcript:
    return Transcript(
        id=uid(10),
        meeting_id=uid(1),
        audio_asset_id=uid(2),
        provider="openai",
        model="gpt-4o-mini-transcribe",
        language="en",
        text="Mira will send the launch brief by Friday.",
        segments=(
            TranscriptSegment(
                id=uid(11),
                ordinal=0,
                start_ms=0,
                end_ms=2_000,
                speaker="Mira",
                text="Mira will send the launch brief by Friday.",
            ),
        ),
        created_at=NOW,
    )


def make_action() -> ActionItem:
    return ActionItem(
        id=uid(20),
        title="Send launch brief",
        assignee={"display_name": "Mira", "email": "mira@example.com"},
        deadline=DateDeadline(
            value=date(2026, 6, 12),
            timezone="Asia/Calcutta",
            source_text="by Friday",
            resolution=DeadlineResolution.RELATIVE_TO_MEETING,
        ),
        priority=Priority.HIGH,
        confidence=0.96,
        evidence=(EvidenceRef(segment_ids=(uid(11),), quote="send the launch brief"),),
    )


def make_review(**changes: object) -> ReviewRevision:
    data: dict[str, object] = {
        "id": uid(30),
        "meeting_id": uid(1),
        "transcript_id": uid(10),
        "revision_number": 1,
        "origin": ReviewOrigin.MODEL,
        "purpose": "Prepare the launch team",
        "recap_markdown": "# Launch planning\n\nMira owns the launch brief.",
        "action_items": (make_action(),),
        "open_questions": (
            OpenQuestion(
                id=uid(22),
                question="Who approves the final brief?",
                evidence=(EvidenceRef(segment_ids=(uid(11),), quote="launch brief"),),
            ),
        ),
        "risks": (
            Risk(
                id=uid(23),
                description="Approval could miss the launch window.",
                evidence=(EvidenceRef(segment_ids=(uid(11),), quote="by Friday"),),
            ),
        ),
        "directives": (
            DeliveryDirective(
                action_item_id=uid(20),
                create_task=True,
                task_target=ConnectorTarget(connector_id="tasks", resource_id="inbox"),
                create_calendar_event=True,
                calendar_target=ConnectorTarget(connector_id="calendar", resource_id="primary"),
            ),
        ),
        "created_at": NOW,
    }
    data.update(changes)
    return ReviewRevision.model_validate(data)


def test_canonical_hash_is_order_independent_for_mappings() -> None:
    left = {"meeting": uid(1), "values": [2, 1], "at": NOW}
    right = {"at": NOW, "values": [2, 1], "meeting": uid(1)}

    assert canonical_json(left) == canonical_json(right)
    assert canonical_sha256(left) == canonical_sha256(right)


def test_ingest_request_fingerprint_uses_the_frozen_v1_projection() -> None:
    request = IngestRequestIdentity(
        ingest_key=" upload-one ",
        title=" Planning ",
        occurred_at=datetime(
            2026,
            8,
            7,
            13,
            30,
            tzinfo=timezone(timedelta(hours=5, minutes=30)),
        ),
        timezone="Asia/Calcutta",
        participants=(
            PersonRef(display_name=" Mira ", email="Mira@Example.com"),
            PersonRef(display_name="Dev", email=None),
        ),
    )
    audio = IngestAudioIdentity(sha256="a" * 64, size_bytes=128)
    expected_json = (
        '{"audio":{"sha256":"'
        + "a" * 64
        + '","size_bytes":128},"occurred_at":"2026-08-07T08:00:00.000000Z",'
        '"participants":[{"display_name":"Mira","email":"Mira@Example.com"},'
        '{"display_name":"Dev","email":null}],"schema":"meeting-ingest-request/v1",'
        '"timezone":"Asia/Calcutta","title":"Planning"}'
    )

    assert request.ingest_key == "upload-one"
    assert (
        request.fingerprint(audio, INGEST_REQUEST_FINGERPRINT_VERSION)
        == hashlib.sha256(expected_json.encode("utf-8")).hexdigest()
    )


def test_ingest_request_fingerprint_normalizes_equivalent_utc_instants() -> None:
    first = IngestRequestIdentity(
        ingest_key="first",
        title="Planning",
        occurred_at=datetime(2026, 8, 7, 8, 0, tzinfo=timezone.utc),
        timezone="UTC",
    )
    second = first.model_copy(
        update={
            "ingest_key": "second",
            "occurred_at": datetime(
                2026,
                8,
                7,
                13,
                30,
                tzinfo=timezone(timedelta(hours=5, minutes=30)),
            ),
        }
    )
    audio = IngestAudioIdentity(sha256="a" * 64, size_bytes=128)

    assert first.fingerprint(audio, 1) == second.fingerprint(audio, 1)


@pytest.mark.parametrize(
    ("request_update", "audio"),
    [
        ({"title": "Retrospective"}, IngestAudioIdentity(sha256="a" * 64, size_bytes=128)),
        (
            {"occurred_at": datetime(2026, 8, 7, 8, 1, tzinfo=timezone.utc)},
            IngestAudioIdentity(sha256="a" * 64, size_bytes=128),
        ),
        ({"timezone": "Asia/Calcutta"}, IngestAudioIdentity(sha256="a" * 64, size_bytes=128)),
        (
            {"participants": (PersonRef(display_name="Mira", email=None),)},
            IngestAudioIdentity(sha256="a" * 64, size_bytes=128),
        ),
        ({}, IngestAudioIdentity(sha256="b" * 64, size_bytes=128)),
        ({}, IngestAudioIdentity(sha256="a" * 64, size_bytes=129)),
    ],
)
def test_ingest_request_fingerprint_binds_every_request_dimension(
    request_update: dict[str, object],
    audio: IngestAudioIdentity,
) -> None:
    request = IngestRequestIdentity(
        ingest_key="upload-one",
        title="Planning",
        occurred_at=datetime(2026, 8, 7, 8, 0, tzinfo=timezone.utc),
        timezone="UTC",
    )
    baseline_audio = IngestAudioIdentity(sha256="a" * 64, size_bytes=128)

    assert request.fingerprint(baseline_audio, 1) != request.model_copy(
        update=request_update
    ).fingerprint(audio, 1)


def test_ingest_request_fingerprint_preserves_participant_order_and_email_case() -> None:
    participants = (
        PersonRef(display_name="Mira", email="Mira@example.com"),
        PersonRef(display_name="Dev", email=None),
    )
    request = IngestRequestIdentity(
        ingest_key="upload-one",
        title="Planning",
        occurred_at=NOW,
        timezone="UTC",
        participants=participants,
    )
    audio = IngestAudioIdentity(sha256="a" * 64, size_bytes=128)
    reordered = request.model_copy(update={"participants": tuple(reversed(participants))})
    changed_case = request.model_copy(
        update={
            "participants": (
                participants[0].model_copy(update={"email": "mira@example.com"}),
                participants[1],
            )
        }
    )

    assert request.fingerprint(audio, 1) != reordered.fingerprint(audio, 1)
    assert request.fingerprint(audio, 1) != changed_case.fingerprint(audio, 1)


def test_ingest_request_binding_rejects_unsupported_fingerprint_versions() -> None:
    request = IngestRequestIdentity(
        ingest_key="upload-one",
        title="Planning",
        occurred_at=NOW,
        timezone="UTC",
    )
    audio = IngestAudioIdentity(sha256="a" * 64, size_bytes=128)

    with pytest.raises(InvalidDomainValueError, match="fingerprint version is unsupported"):
        IngestRequestBinding.create(request, audio, NOW, fingerprint_version=2)


def test_ingest_identity_digests_are_hidden_from_representations() -> None:
    request = IngestRequestIdentity(
        ingest_key="upload-one",
        title="Planning",
        occurred_at=NOW,
        timezone="UTC",
    )
    audio = IngestAudioIdentity(sha256="a" * 64, size_bytes=128)
    binding = IngestRequestBinding.create(request, audio, NOW)

    assert "a" * 64 not in repr(audio)
    assert binding.request_fingerprint not in repr(binding)


def test_audio_asset_rejects_path_as_original_name() -> None:
    with pytest.raises(ValidationError, match="base file name"):
        AudioAsset(
            id=uid(2),
            storage_key="audio/recording.mp3",
            original_name="../recording.mp3",
            detected_media_type=AudioMediaType.MP3,
            size_bytes=1_000,
            duration_ms=5_000,
            sha256="a" * 64,
            created_at=NOW,
        )


def test_transcript_derives_hash_and_rejects_tampering() -> None:
    transcript = make_transcript()

    assert len(transcript.sha256) == SHA256_LENGTH
    with pytest.raises(ValidationError, match="does not match transcript text"):
        Transcript.model_validate(transcript.model_dump() | {"sha256": "f" * 64})


def test_transcript_requires_contiguous_ordered_segments() -> None:
    transcript = make_transcript()
    segment = transcript.segments[0].model_copy(update={"ordinal": 1})

    with pytest.raises(ValidationError, match="ordinals must be contiguous"):
        Transcript.model_validate(transcript.model_dump() | {"segments": (segment,)})


def test_transcript_rejects_content_beyond_agent_input_boundary() -> None:
    with pytest.raises(ValidationError, match="at most 250000"):
        Transcript.model_validate(make_transcript().model_dump() | {"text": "x" * 250_001})


def test_model_items_require_evidence_but_human_items_do_not() -> None:
    with pytest.raises(ValidationError, match="requires decision evidence"):
        Decision(id=uid(21), summary="Ship Friday", confidence=0.9)

    decision = Decision(
        id=uid(21),
        summary="Ship Friday",
        confidence=1.0,
        origin=ReviewOrigin.HUMAN,
    )

    assert decision.evidence == ()


def test_review_digest_ignores_revision_metadata() -> None:
    first = make_review()
    second = make_review(
        id=uid(31),
        revision_number=2,
        origin=ReviewOrigin.HUMAN,
        actor_id="reviewer",
        created_at=datetime(2026, 6, 8, 9, 0, tzinfo=timezone.utc),
    )

    assert first.content_digest == second.content_digest


def test_review_digest_covers_user_visible_recap() -> None:
    first = make_review()
    second = make_review(recap_markdown="# Launch planning\n\nThe launch brief is deferred.")

    assert first.content_digest != second.content_digest


def test_review_rejects_supplied_digest_that_does_not_match() -> None:
    with pytest.raises(ValidationError, match="does not match review content"):
        make_review(content_digest="f" * 64)


def test_review_requires_exactly_one_directive_for_each_action() -> None:
    with pytest.raises(ValidationError, match="each action requires one directive"):
        make_review(directives=())


def test_review_rejects_calendar_delivery_without_deadline() -> None:
    action = make_action().model_copy(update={"deadline": None})

    with pytest.raises(ValidationError, match="requires a resolved deadline"):
        make_review(action_items=(action,))


def test_evidence_must_refer_to_matching_transcript_text() -> None:
    transcript = make_transcript()
    review = make_review()

    validate_review_evidence(review, transcript)
    bad_action = make_action().model_copy(
        update={"evidence": (EvidenceRef(segment_ids=(uid(11),), quote="approve budget"),)}
    )
    bad_review = make_review(action_items=(bad_action,))

    with pytest.raises(DomainInvariantError, match="quote is absent"):
        validate_review_evidence(bad_review, transcript)


def test_timezone_must_be_from_iana_database() -> None:
    with pytest.raises(ValidationError, match="IANA timezone"):
        DateDeadline(
            value=date(2026, 6, 12),
            timezone="Mars/Olympus",
            source_text="Friday",
            resolution=DeadlineResolution.EXPLICIT,
        )


def test_canonical_json_rejects_unknown_types() -> None:
    with pytest.raises(InvalidDomainValueError, match="unsupported value type"):
        canonical_json(object())


@pytest.mark.parametrize("value", [-1.0, 600.000001, float("inf"), float("nan")])
def test_workflow_failure_rejects_invalid_provider_retry_minimum(value: float) -> None:
    with pytest.raises(ValidationError):
        WorkflowFailure(
            code=FailureCode.PROVIDER_UNAVAILABLE,
            disposition=FailureDisposition.RETRYABLE,
            safe_message="The provider is temporarily unavailable",
            retry_after_seconds=value,
            occurred_at=NOW,
        )


@pytest.mark.parametrize(
    "disposition",
    [FailureDisposition.PERMANENT, FailureDisposition.UNKNOWN_OUTCOME],
)
def test_workflow_failure_limits_provider_retry_minimum_to_retryable_failures(
    disposition: FailureDisposition,
) -> None:
    with pytest.raises(ValidationError, match="retry requires a retryable failure"):
        WorkflowFailure(
            code=FailureCode.PROVIDER_UNAVAILABLE,
            disposition=disposition,
            safe_message="The provider is unavailable",
            retry_after_seconds=1,
            occurred_at=NOW,
        )
