from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import re
from datetime import datetime
from typing import Annotated, cast
from uuid import UUID

from fastapi import Depends, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from meeting_action_orchestrator.api.contracts import ApiDependencies, Principal
from meeting_action_orchestrator.api.problems import ProblemError, create_problem
from meeting_action_orchestrator.application.ports import MeetingListCursor
from meeting_action_orchestrator.domain.enums import MeetingStatus

IDEMPOTENCY_KEY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._~:+/-]{0,199}")
REVIEW_ETAG_PATTERN = re.compile(r'"([0-9a-f]{64})"')
MEETING_ETAG_PATTERN = re.compile(r'"meeting-(0|[1-9][0-9]{0,18})"')
ERASURE_ETAG_PATTERN = re.compile(r'"erasure-(0|[1-9][0-9]{0,18})"')
MEETING_CURSOR_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,128}")
MEETING_CURSOR_VERSION = 1
MEETING_CURSOR_STATUS_BYTES = 8
MEETING_CURSOR_UUID_BYTES = 16
MEETING_CURSOR_CHECKSUM_BYTES = 8
MEETING_CURSOR_TIMESTAMP_MAX_BYTES = 48
MEETING_CURSOR_CHECKSUM_CONTEXT = b"meeting-action-orchestrator:meeting-cursor:v1:"
_bearer = HTTPBearer(auto_error=False)


async def get_api_dependencies(request: Request) -> ApiDependencies:
    return cast(ApiDependencies, request.app.state.api_dependencies)


ApiDependenciesValue = Annotated[ApiDependencies, Depends(get_api_dependencies)]


async def get_principal(
    dependencies: ApiDependenciesValue,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Security(_bearer)],
) -> Principal:
    if credentials is None:
        raise ProblemError(
            create_problem(
                401,
                detail="A bearer access token is required.",
                type_uri="urn:meeting-action-orchestrator:problem:authentication-required",
            ),
            headers={"WWW-Authenticate": "Bearer"},
        )
    principal = await dependencies.authenticator.authenticate(credentials.credentials)
    if principal is None:
        raise ProblemError(
            create_problem(
                401,
                detail="The bearer access token is invalid.",
                type_uri="urn:meeting-action-orchestrator:problem:invalid-access-token",
            ),
            headers={"WWW-Authenticate": "Bearer"},
        )
    return principal


PrincipalValue = Annotated[Principal, Depends(get_principal)]


def parse_review_precondition(if_match: str | None) -> str:
    if if_match is None:
        raise ProblemError(
            create_problem(
                428,
                detail="If-Match must identify the review revision being changed.",
                type_uri="urn:meeting-action-orchestrator:problem:precondition-required",
            )
        )
    match = REVIEW_ETAG_PATTERN.fullmatch(if_match.strip())
    if match is None:
        raise ProblemError(
            create_problem(
                400,
                detail="If-Match must contain one strong review ETag.",
                type_uri="urn:meeting-action-orchestrator:problem:invalid-precondition",
            )
        )
    return match.group(1)


def parse_meeting_precondition(if_match: str | None) -> int:
    if if_match is None:
        raise ProblemError(
            create_problem(
                428,
                detail="If-Match must identify the meeting version being changed.",
                type_uri="urn:meeting-action-orchestrator:problem:precondition-required",
            )
        )
    match = MEETING_ETAG_PATTERN.fullmatch(if_match.strip())
    if match is None:
        raise ProblemError(
            create_problem(
                400,
                detail="If-Match must contain one strong meeting ETag.",
                type_uri="urn:meeting-action-orchestrator:problem:invalid-precondition",
            )
        )
    version = int(match.group(1))
    if version > 9_223_372_036_854_775_807:
        raise ProblemError(
            create_problem(
                400,
                detail="If-Match contains an unsupported meeting version.",
                type_uri="urn:meeting-action-orchestrator:problem:invalid-precondition",
            )
        )
    return version


def parse_erasure_precondition(if_match: str | None) -> int:
    if if_match is None:
        raise ProblemError(
            create_problem(
                428,
                detail="If-Match must identify the erasure job version being changed.",
                type_uri="urn:meeting-action-orchestrator:problem:precondition-required",
            )
        )
    match = ERASURE_ETAG_PATTERN.fullmatch(if_match.strip())
    if match is None:
        raise ProblemError(
            create_problem(
                400,
                detail="If-Match must contain one strong erasure ETag.",
                type_uri="urn:meeting-action-orchestrator:problem:invalid-precondition",
            )
        )
    version = int(match.group(1))
    if version > 9_223_372_036_854_775_807:
        raise ProblemError(
            create_problem(
                400,
                detail="If-Match contains an unsupported erasure job version.",
                type_uri="urn:meeting-action-orchestrator:problem:invalid-precondition",
            )
        )
    return version


def parse_idempotency_key(value: str | None) -> str:
    if value is None:
        raise ProblemError(
            create_problem(
                400,
                detail="Idempotency-Key is required for this operation.",
                type_uri="urn:meeting-action-orchestrator:problem:idempotency-key-required",
            )
        )
    key = value.strip()
    if IDEMPOTENCY_KEY_PATTERN.fullmatch(key) is None:
        raise ProblemError(
            create_problem(
                400,
                detail="Idempotency-Key contains unsupported characters or length.",
                type_uri="urn:meeting-action-orchestrator:problem:invalid-idempotency-key",
            )
        )
    return key


def parse_meeting_cursor(
    value: str | None,
    status: MeetingStatus | None,
) -> MeetingListCursor | None:
    if value is None:
        return None
    if MEETING_CURSOR_PATTERN.fullmatch(value) is None:
        raise _invalid_meeting_cursor()
    try:
        decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except (binascii.Error, ValueError) as exc:
        raise _invalid_meeting_cursor() from exc
    if _encode_cursor_bytes(decoded) != value:
        raise _invalid_meeting_cursor()
    minimum_size = (
        2 + MEETING_CURSOR_STATUS_BYTES + MEETING_CURSOR_UUID_BYTES + MEETING_CURSOR_CHECKSUM_BYTES
    )
    if len(decoded) < minimum_size or decoded[0] != MEETING_CURSOR_VERSION:
        raise _invalid_meeting_cursor()
    timestamp_size = decoded[1]
    expected_size = minimum_size + timestamp_size
    if not 1 <= timestamp_size <= MEETING_CURSOR_TIMESTAMP_MAX_BYTES:
        raise _invalid_meeting_cursor()
    if len(decoded) != expected_size:
        raise _invalid_meeting_cursor()
    body = decoded[:-MEETING_CURSOR_CHECKSUM_BYTES]
    checksum = decoded[-MEETING_CURSOR_CHECKSUM_BYTES:]
    if not hmac.compare_digest(checksum, _meeting_cursor_checksum(body)):
        raise _invalid_meeting_cursor()
    status_start = 2
    timestamp_start = status_start + MEETING_CURSOR_STATUS_BYTES
    uuid_start = timestamp_start + timestamp_size
    if not hmac.compare_digest(
        decoded[status_start:timestamp_start],
        _meeting_status_fingerprint(status),
    ):
        raise _invalid_meeting_cursor()
    try:
        timestamp_text = decoded[timestamp_start:uuid_start].decode("ascii")
        created_at = datetime.fromisoformat(timestamp_text)
        meeting_id = UUID(bytes=decoded[uuid_start : uuid_start + MEETING_CURSOR_UUID_BYTES])
    except (UnicodeDecodeError, ValueError) as exc:
        raise _invalid_meeting_cursor() from exc
    if created_at.utcoffset() is None or str(created_at) != timestamp_text:
        raise _invalid_meeting_cursor()
    return MeetingListCursor(created_at=created_at, id=meeting_id)


def format_meeting_cursor(
    cursor: MeetingListCursor | None,
    status: MeetingStatus | None,
) -> str | None:
    if cursor is None:
        return None
    timestamp = str(cursor.created_at).encode("ascii")
    if not 1 <= len(timestamp) <= MEETING_CURSOR_TIMESTAMP_MAX_BYTES:
        raise ValueError("created_at cannot be represented in a meeting cursor")
    body = b"".join(
        (
            bytes((MEETING_CURSOR_VERSION, len(timestamp))),
            _meeting_status_fingerprint(status),
            timestamp,
            cursor.id.bytes,
        )
    )
    return _encode_cursor_bytes(body + _meeting_cursor_checksum(body))


def format_etag(value: str) -> str:
    return f'"{value}"'


def _meeting_status_fingerprint(status: MeetingStatus | None) -> bytes:
    value = status.value if status is not None else "*"
    return hashlib.sha256(value.encode("ascii")).digest()[:MEETING_CURSOR_STATUS_BYTES]


def _meeting_cursor_checksum(body: bytes) -> bytes:
    return hashlib.sha256(MEETING_CURSOR_CHECKSUM_CONTEXT + body).digest()[
        :MEETING_CURSOR_CHECKSUM_BYTES
    ]


def _encode_cursor_bytes(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _invalid_meeting_cursor() -> ProblemError:
    return ProblemError(
        create_problem(
            400,
            detail="The meeting page cursor is invalid for this query.",
            type_uri="urn:meeting-action-orchestrator:problem:invalid-page-cursor",
        )
    )
