from __future__ import annotations

from io import BytesIO

import pytest
from fastapi import UploadFile
from starlette.datastructures import Headers

from meeting_action_orchestrator.api.problems import ProblemError
from meeting_action_orchestrator.api.routes import _validate_upload


class InMemoryUpload(BytesIO):
    _rolled = False


def upload(
    content: bytes,
    *,
    filename: str = "meeting.wav",
    content_type: str = "audio/wav",
    size: int | None = None,
) -> UploadFile:
    return UploadFile(
        InMemoryUpload(content),
        size=size,
        filename=filename,
        headers=Headers({"content-type": content_type}),
    )


@pytest.mark.parametrize(
    "filename",
    ["", "../meeting.wav", "folder\\meeting.wav", ".", "..", "bad\x00name.wav", "m" * 201],
)
async def test_upload_rejects_unsafe_filenames(filename: str) -> None:
    recording = upload(b"audio", filename=filename, size=5)

    with pytest.raises(ProblemError) as failure:
        await _validate_upload(recording, 100)

    assert failure.value.problem.status == 422
    assert failure.value.problem.detail == "The recording filename is invalid."


async def test_upload_rejects_unsupported_declared_media_type() -> None:
    recording = upload(b"audio", content_type="text/plain", size=5)

    with pytest.raises(ProblemError) as failure:
        await _validate_upload(recording, 100)

    assert failure.value.problem.status == 422
    assert (
        failure.value.problem.detail
        == "The recording must be declared as MP3, M4A, MP4, or WAV audio."
    )


async def test_upload_measures_unknown_size_and_rewinds_the_stream() -> None:
    recording = upload(b"audio", size=None)

    assert await _validate_upload(recording, 100) == "meeting.wav"
    assert await recording.read() == b"audio"


async def test_upload_rejects_empty_unknown_size() -> None:
    recording = upload(b"", size=None)

    with pytest.raises(ProblemError) as failure:
        await _validate_upload(recording, 100)

    assert failure.value.problem.status == 422
    assert failure.value.problem.detail == "The recording is empty."


async def test_upload_stops_unknown_size_scan_at_the_limit_and_rewinds() -> None:
    recording = upload(b"too-large", size=None)

    with pytest.raises(ProblemError) as failure:
        await _validate_upload(recording, 4)

    assert failure.value.problem.status == 413
    assert await recording.read() == b"too-large"
