from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from meeting_action_orchestrator.domain.enums import MeetingStatus, WriteStatus

MEETING_TRANSITIONS: Mapping[MeetingStatus, frozenset[MeetingStatus]] = MappingProxyType(
    {
        MeetingStatus.INGESTED: frozenset({MeetingStatus.TRANSCRIBING, MeetingStatus.CANCELLED}),
        MeetingStatus.TRANSCRIBING: frozenset(
            {MeetingStatus.TRANSCRIBED, MeetingStatus.TRANSCRIPTION_FAILED}
        ),
        MeetingStatus.TRANSCRIPTION_FAILED: frozenset(
            {MeetingStatus.TRANSCRIBING, MeetingStatus.CANCELLED}
        ),
        MeetingStatus.TRANSCRIBED: frozenset({MeetingStatus.EXTRACTING, MeetingStatus.CANCELLED}),
        MeetingStatus.EXTRACTING: frozenset(
            {MeetingStatus.AWAITING_APPROVAL, MeetingStatus.EXTRACTION_FAILED}
        ),
        MeetingStatus.EXTRACTION_FAILED: frozenset(
            {MeetingStatus.EXTRACTING, MeetingStatus.CANCELLED}
        ),
        MeetingStatus.AWAITING_APPROVAL: frozenset(
            {
                MeetingStatus.AWAITING_APPROVAL,
                MeetingStatus.APPROVED,
                MeetingStatus.CANCELLED,
            }
        ),
        MeetingStatus.APPROVED: frozenset({MeetingStatus.FILING, MeetingStatus.COMPLETED}),
        MeetingStatus.FILING: frozenset(
            {
                MeetingStatus.FILING,
                MeetingStatus.PARTIALLY_FILED,
                MeetingStatus.FILING_FAILED,
                MeetingStatus.COMPLETED,
            }
        ),
        MeetingStatus.PARTIALLY_FILED: frozenset({MeetingStatus.FILING}),
        MeetingStatus.FILING_FAILED: frozenset({MeetingStatus.FILING}),
        MeetingStatus.COMPLETED: frozenset(),
        MeetingStatus.CANCELLED: frozenset(),
    }
)

WRITE_TRANSITIONS: Mapping[WriteStatus, frozenset[WriteStatus]] = MappingProxyType(
    {
        WriteStatus.PENDING: frozenset({WriteStatus.IN_FLIGHT}),
        WriteStatus.IN_FLIGHT: frozenset(
            {
                WriteStatus.SUCCEEDED,
                WriteStatus.RETRY_WAIT,
                WriteStatus.UNKNOWN,
                WriteStatus.PERMANENT_FAILED,
            }
        ),
        WriteStatus.RETRY_WAIT: frozenset({WriteStatus.IN_FLIGHT}),
        WriteStatus.UNKNOWN: frozenset(
            {WriteStatus.SUCCEEDED, WriteStatus.RETRY_WAIT, WriteStatus.PERMANENT_FAILED}
        ),
        WriteStatus.SUCCEEDED: frozenset(),
        WriteStatus.PERMANENT_FAILED: frozenset({WriteStatus.RETRY_WAIT, WriteStatus.UNKNOWN}),
    }
)


def can_transition_meeting(current: MeetingStatus, target: MeetingStatus) -> bool:
    return target in MEETING_TRANSITIONS[current]


def can_transition_write(current: WriteStatus, target: WriteStatus) -> bool:
    return target in WRITE_TRANSITIONS[current]
