from __future__ import annotations

import re
from typing import Annotated, cast

from fastapi import Depends, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from meeting_action_orchestrator.api.contracts import ApiDependencies, Principal
from meeting_action_orchestrator.api.problems import ProblemError, create_problem

IDEMPOTENCY_KEY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._~:+/-]{0,199}")
REVIEW_ETAG_PATTERN = re.compile(r'"([0-9a-f]{64})"')
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


def format_etag(value: str) -> str:
    return f'"{value}"'
