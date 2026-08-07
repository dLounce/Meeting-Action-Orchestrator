from __future__ import annotations

import asyncio
import hmac
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import TypeVar

from meeting_action_orchestrator.application.ports import Clock, UnitOfWork
from meeting_action_orchestrator.domain.models import ErasureToken, MeetingErasureJob

UnitOfWorkFactory = Callable[[], UnitOfWork]
T = TypeVar("T")


def matches_token(
    tokens: Sequence[ErasureToken],
    token_version: int,
    key_id: str,
    digest: str,
) -> bool:
    return any(
        token.token_version == token_version
        and token.key_id == key_id
        and hmac.compare_digest(token.digest, digest)
        for token in tokens
    )


def replace_erasure_job(
    job: MeetingErasureJob,
    **updates: object,
) -> MeetingErasureJob:
    return MeetingErasureJob.model_validate(job.model_dump(mode="python") | updates)


def aware_now(clock: Clock) -> datetime:
    now = clock.now()
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("Clock must return a timezone-aware timestamp")
    return now


async def shielded_thread(call: Callable[[], T]) -> T:
    task = asyncio.create_task(asyncio.to_thread(call))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise
