from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from meeting_action_orchestrator.application.errors import (
    ProviderConfigurationError,
    ProviderInputError,
    ProviderOutputError,
    ProviderTransientError,
)
from meeting_action_orchestrator.infrastructure.openai_transcription import (
    OpenAITranscriber,
    OpenAITranscriptionConfigurationError,
    OpenAITranscriptionInputError,
    OpenAITranscriptionOutputError,
    OpenAITranscriptionTransientError,
)


class FakeTranscriptions:
    def __init__(self, response: object) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    async def create(self, **arguments: Any) -> object:
        self.calls.append(arguments)
        return self.response


class FakeAudio:
    def __init__(self, transcriptions: FakeTranscriptions) -> None:
        self.transcriptions = transcriptions


class FakeClient:
    def __init__(self, response: object) -> None:
        self.transcriptions = FakeTranscriptions(response)
        self.audio = FakeAudio(self.transcriptions)
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1


class FakeResponse:
    def __init__(self, payload: dict[str, Any], request_id: str) -> None:
        self._payload = payload
        self._request_id = request_id

    def model_dump(self, mode: str) -> dict[str, Any]:
        assert mode == "json"
        return self._payload


def _diarized_segment(**updates: object) -> dict[str, object]:
    segment: dict[str, object] = {
        "id": "seg_1",
        "start": 0.0,
        "end": 1.0,
        "speaker": "A",
        "text": "Ship it",
    }
    segment.update(updates)
    return segment


async def _assert_diarized_output_rejected(tmp_path: Path, segments: object) -> None:
    audio_path = tmp_path / "meeting.wav"
    audio_path.write_bytes(b"audio")
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe-diarize",
        client=FakeClient({"text": "Ship it", "segments": segments}),
    )

    with pytest.raises(
        OpenAITranscriptionOutputError,
        match=r"^OpenAI transcription failed with invalid_output$",
    ):
        await transcriber.transcribe(audio_path)


def test_transcriber_rejects_hidden_sdk_retries() -> None:
    with pytest.raises(OpenAITranscriptionConfigurationError):
        OpenAITranscriber(
            api_key="test",
            model="gpt-4o-transcribe",
            max_retries=1,
        )


def test_transcription_errors_implement_provider_failure_contracts() -> None:
    assert isinstance(OpenAITranscriptionConfigurationError(), ProviderConfigurationError)
    assert isinstance(OpenAITranscriptionInputError(), ProviderInputError)
    assert isinstance(OpenAITranscriptionTransientError(), ProviderTransientError)
    assert isinstance(OpenAITranscriptionOutputError(), ProviderOutputError)


@pytest.mark.asyncio
async def test_transcriber_closes_and_recreates_its_owned_client() -> None:
    first = FakeClient({})
    second = FakeClient({})
    clients = iter((first, second))

    def create_client(**_arguments: object) -> FakeClient:
        return next(clients)

    transcriber = OpenAITranscriber(api_key="test", model="gpt-4o-transcribe")
    transcriber._openai = SimpleNamespace(AsyncOpenAI=create_client)

    assert transcriber._get_client() is first
    await transcriber.close()
    await transcriber.close()

    assert first.close_calls == 1
    assert transcriber._get_client() is second


@pytest.mark.asyncio
async def test_transcriber_does_not_close_an_injected_client() -> None:
    client = FakeClient({})
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe",
        client=client,
    )

    await transcriber.close()

    assert client.close_calls == 0
    assert transcriber._get_client() is client


@pytest.mark.asyncio
async def test_diarized_transcription_maps_speaker_segments(tmp_path: Path) -> None:
    duration_seconds = 2.5
    start_seconds = 0.1
    end_seconds = 2.4
    total_tokens = 15
    audio_path = tmp_path / "meeting.mp3"
    audio_path.write_bytes(b"audio")
    request_id = "req_transcription_1"
    client = FakeClient(
        FakeResponse(
            {
                "text": "We will ship it.",
                "language": "en",
                "duration": duration_seconds,
                "segments": [
                    {
                        "id": "seg_1",
                        "start": start_seconds,
                        "end": end_seconds,
                        "speaker": "A",
                        "text": "We will ship it.",
                    }
                ],
                "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": total_tokens},
            },
            request_id,
        )
    )
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe-diarize",
        client=client,
    )

    result = await transcriber.transcribe(audio_path, language="en")

    call = client.transcriptions.calls[0]
    assert call["model"] == "gpt-4o-transcribe-diarize"
    assert call["response_format"] == "diarized_json"
    assert call["chunking_strategy"] == "auto"
    assert call["language"] == "en"
    assert result.duration_seconds == duration_seconds
    assert result.provider_request_id == request_id
    assert result.segments[0].id == "seg_1"
    assert result.segments[0].start_ms == round(start_seconds * 1000)
    assert result.segments[0].end_ms == round(end_seconds * 1000)
    assert result.segments[0].speaker == "A"
    assert result.usage.total_tokens == total_tokens


@pytest.mark.asyncio
async def test_diarized_transcription_accepts_ordered_zero_length_segments(
    tmp_path: Path,
) -> None:
    audio_path = tmp_path / "meeting.wav"
    audio_path.write_bytes(b"audio")
    client = FakeClient(
        {
            "text": "One Two",
            "segments": [
                _diarized_segment(start=0, end=0, speaker=" A ", text="One"),
                _diarized_segment(id="seg_2", start=0, end=1.25, speaker="B", text="Two"),
            ],
        }
    )
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe-diarize",
        client=client,
    )

    result = await transcriber.transcribe(audio_path)

    assert [(segment.start_ms, segment.end_ms) for segment in result.segments] == [
        (0, 0),
        (0, 1250),
    ]
    assert [segment.speaker for segment in result.segments] == ["A", "B"]


@pytest.mark.parametrize(
    "segments",
    [
        pytest.param(None, id="missing"),
        pytest.param([], id="empty"),
        pytest.param({}, id="mapping"),
        pytest.param((), id="tuple"),
    ],
)
@pytest.mark.asyncio
async def test_diarized_transcription_rejects_missing_or_non_list_segments(
    tmp_path: Path,
    segments: object,
) -> None:
    await _assert_diarized_output_rejected(tmp_path, segments)


@pytest.mark.parametrize(
    "speaker",
    [
        pytest.param(None, id="missing"),
        pytest.param("", id="empty"),
        pytest.param("   ", id="blank"),
        pytest.param(1, id="non_string"),
    ],
)
@pytest.mark.asyncio
async def test_diarized_transcription_requires_a_speaker_on_every_segment(
    tmp_path: Path,
    speaker: object,
) -> None:
    segments = [
        _diarized_segment(text="First"),
        _diarized_segment(id="seg_2", start=1, end=2, speaker=speaker, text="Second"),
    ]

    await _assert_diarized_output_rejected(tmp_path, segments)


@pytest.mark.parametrize(
    ("start", "end"),
    [
        pytest.param(None, 1, id="missing-start"),
        pytest.param(0, None, id="missing-end"),
        pytest.param("0", 1, id="string-start"),
        pytest.param(0, "1", id="string-end"),
        pytest.param(False, 1, id="boolean-start"),
        pytest.param(0, True, id="boolean-end"),
        pytest.param(float("nan"), 1, id="nan-start"),
        pytest.param(0, float("nan"), id="nan-end"),
        pytest.param(float("inf"), 1, id="infinite-start"),
        pytest.param(0, float("inf"), id="infinite-end"),
        pytest.param(float("-inf"), 1, id="negative-infinite-start"),
        pytest.param(0, float("-inf"), id="negative-infinite-end"),
        pytest.param(-0.001, 1, id="negative-start"),
        pytest.param(2, 1, id="end-before-start"),
    ],
)
@pytest.mark.asyncio
async def test_diarized_transcription_requires_finite_ordered_timestamps(
    tmp_path: Path,
    start: object,
    end: object,
) -> None:
    await _assert_diarized_output_rejected(
        tmp_path,
        [_diarized_segment(start=start, end=end)],
    )


@pytest.mark.asyncio
async def test_diarized_transcription_rejects_out_of_order_segments(tmp_path: Path) -> None:
    segments = [
        _diarized_segment(start=2, end=3, text="Later"),
        _diarized_segment(id="seg_2", start=1, end=2, text="Earlier"),
    ]

    await _assert_diarized_output_rejected(tmp_path, segments)


@pytest.mark.parametrize(
    "invalid_segment",
    [
        pytest.param(None, id="non-mapping"),
        pytest.param(_diarized_segment(text=""), id="empty-text"),
        pytest.param(_diarized_segment(text="   "), id="blank-text"),
    ],
)
@pytest.mark.asyncio
async def test_diarized_transcription_rejects_every_malformed_segment(
    tmp_path: Path,
    invalid_segment: object,
) -> None:
    await _assert_diarized_output_rejected(
        tmp_path,
        [_diarized_segment(text="Valid"), invalid_segment],
    )


@pytest.mark.asyncio
async def test_plain_transcription_builds_fallback_segment(tmp_path: Path) -> None:
    usage_seconds = 3.0
    audio_path = tmp_path / "meeting.wav"
    audio_path.write_bytes(b"audio")
    client = FakeClient({"text": "Ship it", "usage": {"seconds": usage_seconds}})
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe",
        client=client,
    )

    result = await transcriber.transcribe(audio_path)

    call = client.transcriptions.calls[0]
    assert call["response_format"] == "json"
    assert "chunking_strategy" not in call
    assert result.segments[0].id == "segment_0001"
    assert result.segments[0].text == "Ship it"
    assert result.usage.seconds == usage_seconds


@pytest.mark.asyncio
async def test_transcription_rejects_empty_provider_output(tmp_path: Path) -> None:
    audio_path = tmp_path / "meeting.wav"
    audio_path.write_bytes(b"audio")
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe",
        client=FakeClient({"text": ""}),
    )

    with pytest.raises(OpenAITranscriptionOutputError):
        await transcriber.transcribe(audio_path)


@pytest.mark.asyncio
async def test_transcription_rejects_missing_audio_file(tmp_path: Path) -> None:
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe",
        client=FakeClient({}),
    )

    with pytest.raises(OpenAITranscriptionInputError):
        await transcriber.transcribe(tmp_path / "missing.mp3")
