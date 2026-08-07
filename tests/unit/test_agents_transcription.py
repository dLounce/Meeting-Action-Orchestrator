import errno
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import httpx
import pytest

from meeting_action_orchestrator.application.errors import (
    AudioAssetIdentityMismatchError,
    ProviderConfigurationError,
    ProviderInputError,
    ProviderOutputError,
    ProviderPermanentError,
    ProviderRateLimitError,
    ProviderTimeoutError,
    ProviderTransientError,
)
from meeting_action_orchestrator.domain.enums import ProviderOperation, ProviderUsageKind
from meeting_action_orchestrator.domain.hashing import canonical_sha256
from meeting_action_orchestrator.infrastructure.openai_transcription import (
    OpenAITranscriber,
    OpenAITranscriptionConfigurationError,
    OpenAITranscriptionInputError,
    OpenAITranscriptionOutputError,
    OpenAITranscriptionPermanentError,
    OpenAITranscriptionRateLimitError,
    OpenAITranscriptionTimeoutError,
    OpenAITranscriptionTransientError,
)
from tests.provider_budget_support import FakeBudgetController, transcription_context


class FakeTranscriptions:
    def __init__(self, response: object) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    async def create(self, **arguments: Any) -> object:
        self.calls.append(arguments)
        if isinstance(self.response, dict) and "usage" not in self.response:
            return {
                **self.response,
                "usage": {"type": "duration", "seconds": 1.0},
            }
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


class FakeProviderFailureError(Exception):
    def __init__(
        self,
        *,
        status_code: int | None = None,
        code: str | None = None,
        body: object = None,
        response: object = None,
    ) -> None:
        self.status_code = status_code
        self.code = code
        self.body = body
        self.response = response
        super().__init__("private transcription provider detail")


class APIConnectionError(FakeProviderFailureError):
    pass


class APITimeoutError(FakeProviderFailureError):
    pass


class RateLimitError(FakeProviderFailureError):
    pass


class InternalServerError(FakeProviderFailureError):
    pass


class AuthenticationError(FakeProviderFailureError):
    pass


class PermissionDeniedError(FakeProviderFailureError):
    pass


class NotFoundError(FakeProviderFailureError):
    pass


class BadRequestError(FakeProviderFailureError):
    pass


class UnprocessableEntityError(FakeProviderFailureError):
    pass


class FailingTranscriptions(FakeTranscriptions):
    def __init__(self, error: Exception) -> None:
        super().__init__({})
        self.error = error

    async def create(self, **arguments: Any) -> object:
        self.calls.append(arguments)
        raise self.error


def configured_transcriber() -> OpenAITranscriber:
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe",
        budget_controller=FakeBudgetController(),
        client=FakeClient({}),
    )
    transcriber._openai = SimpleNamespace(
        APIConnectionError=APIConnectionError,
        APITimeoutError=APITimeoutError,
        RateLimitError=RateLimitError,
        InternalServerError=InternalServerError,
        AuthenticationError=AuthenticationError,
        PermissionDeniedError=PermissionDeniedError,
        NotFoundError=NotFoundError,
        BadRequestError=BadRequestError,
        UnprocessableEntityError=UnprocessableEntityError,
    )
    return transcriber


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
        budget_controller=FakeBudgetController(),
        client=FakeClient({"text": "Ship it", "segments": segments}),
    )

    with pytest.raises(
        OpenAITranscriptionOutputError,
        match=r"^OpenAI transcription failed with invalid_output$",
    ):
        await transcriber.transcribe(audio_path, context=transcription_context(audio_path))


def test_transcriber_rejects_hidden_sdk_retries() -> None:
    with pytest.raises(OpenAITranscriptionConfigurationError):
        OpenAITranscriber(
            api_key="test",
            model="gpt-4o-transcribe",
            budget_controller=FakeBudgetController(),
            max_retries=1,
        )


@pytest.mark.parametrize("model", ["", " ", "g" * 201])
def test_transcriber_rejects_invalid_model(model: str) -> None:
    with pytest.raises(OpenAITranscriptionConfigurationError) as captured:
        OpenAITranscriber(
            api_key="test",
            model=model,
            budget_controller=FakeBudgetController(),
        )

    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


def test_transcriber_normalizes_model_name() -> None:
    transcriber = OpenAITranscriber(
        api_key="test",
        model=" gpt-4o-transcribe ",
        budget_controller=FakeBudgetController(),
    )

    assert transcriber._model == "gpt-4o-transcribe"


def test_transcription_errors_implement_provider_failure_contracts() -> None:
    assert isinstance(OpenAITranscriptionConfigurationError(), ProviderConfigurationError)
    assert isinstance(OpenAITranscriptionInputError(), ProviderInputError)
    assert isinstance(OpenAITranscriptionTransientError(), ProviderTransientError)
    assert isinstance(OpenAITranscriptionTimeoutError(), ProviderTimeoutError)
    assert isinstance(OpenAITranscriptionRateLimitError(), ProviderRateLimitError)
    assert isinstance(OpenAITranscriptionOutputError(), ProviderOutputError)
    assert isinstance(OpenAITranscriptionPermanentError(), ProviderPermanentError)


@pytest.mark.asyncio
async def test_transcriber_closes_and_recreates_its_owned_client() -> None:
    first = FakeClient({})
    second = FakeClient({})
    clients = iter((first, second))

    def create_client(**_arguments: object) -> FakeClient:
        return next(clients)

    transcriber = OpenAITranscriber(
        api_key="test",
        model="gpt-4o-transcribe",
        budget_controller=FakeBudgetController(),
    )
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
        budget_controller=FakeBudgetController(),
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
                "usage": {
                    "type": "tokens",
                    "input_tokens": 10,
                    "output_tokens": 5,
                    "total_tokens": total_tokens,
                },
            },
            request_id,
        )
    )
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe-diarize",
        budget_controller=FakeBudgetController(),
        client=client,
    )

    result = await transcriber.transcribe(
        audio_path,
        language="en",
        context=transcription_context(audio_path),
    )

    call = client.transcriptions.calls[0]
    assert call["model"] == "gpt-4o-transcribe-diarize"
    assert call["response_format"] == "diarized_json"
    assert call["chunking_strategy"] == "auto"
    assert call["language"] == "en"
    client_request_id = call["extra_headers"]["X-Client-Request-Id"]
    assert str(UUID(client_request_id)) == client_request_id
    assert result.duration_seconds == duration_seconds
    assert result.provider_request_id == request_id
    assert result.segments[0].id == "seg_1"
    assert result.segments[0].start_ms == round(start_seconds * 1000)
    assert result.segments[0].end_ms == round(end_seconds * 1000)
    assert result.segments[0].speaker == "A"
    assert result.usage is not None
    assert result.usage.kind is ProviderUsageKind.TOKENS
    assert result.usage.input_tokens == 10
    assert result.usage.output_tokens == total_tokens - 10


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
        budget_controller=FakeBudgetController(),
        client=client,
    )

    result = await transcriber.transcribe(
        audio_path,
        context=transcription_context(audio_path),
    )

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
    client = FakeClient(
        {"text": "Ship it", "usage": {"type": "duration", "seconds": usage_seconds}}
    )
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe",
        budget_controller=FakeBudgetController(),
        client=client,
    )

    result = await transcriber.transcribe(
        audio_path,
        context=transcription_context(audio_path),
    )

    call = client.transcriptions.calls[0]
    assert call["response_format"] == "json"
    assert "chunking_strategy" not in call
    assert result.segments[0].id == "segment_0001"
    assert result.segments[0].text == "Ship it"
    assert result.usage is not None
    assert result.usage.kind is ProviderUsageKind.DURATION
    assert result.usage.audio_duration_ms == round(usage_seconds * 1000)


@pytest.mark.asyncio
async def test_transcriber_sends_a_unique_client_request_id_per_call(tmp_path: Path) -> None:
    audio_path = tmp_path / "meeting.wav"
    audio_path.write_bytes(b"audio")
    client = FakeClient({"text": "Ship it"})
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe",
        budget_controller=FakeBudgetController(),
        client=client,
    )

    context = transcription_context(audio_path)
    first = await transcriber.transcribe(audio_path, context=context)
    second = await transcriber.transcribe(audio_path, context=context)

    client_request_ids = tuple(
        call["extra_headers"]["X-Client-Request-Id"] for call in client.transcriptions.calls
    )
    assert len(set(client_request_ids)) == 2
    assert all(str(UUID(value)) == value for value in client_request_ids)
    assert first.provider_request_id == client_request_ids[0]
    assert second.provider_request_id == client_request_ids[1]


@pytest.mark.asyncio
async def test_transcription_rejects_empty_provider_output(tmp_path: Path) -> None:
    audio_path = tmp_path / "meeting.wav"
    audio_path.write_bytes(b"audio")
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe",
        budget_controller=FakeBudgetController(),
        client=FakeClient({"text": ""}),
    )

    with pytest.raises(OpenAITranscriptionOutputError):
        await transcriber.transcribe(audio_path, context=transcription_context(audio_path))


@pytest.mark.asyncio
async def test_transcription_rejects_missing_audio_file(tmp_path: Path) -> None:
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe",
        budget_controller=FakeBudgetController(),
        client=FakeClient({}),
    )

    missing = tmp_path / "missing.mp3"
    with pytest.raises(AudioAssetIdentityMismatchError):
        await transcriber.transcribe(missing, context=transcription_context(missing))


@pytest.mark.parametrize(
    ("error", "expected_type"),
    [
        (APIConnectionError(), OpenAITranscriptionTransientError),
        (APITimeoutError(), OpenAITranscriptionTimeoutError),
        (RateLimitError(status_code=429), OpenAITranscriptionRateLimitError),
        (InternalServerError(status_code=503), OpenAITranscriptionTransientError),
        (AuthenticationError(status_code=401), OpenAITranscriptionConfigurationError),
        (PermissionDeniedError(status_code=403), OpenAITranscriptionConfigurationError),
        (NotFoundError(status_code=404), OpenAITranscriptionConfigurationError),
        (BadRequestError(status_code=400), OpenAITranscriptionInputError),
        (UnprocessableEntityError(status_code=422), OpenAITranscriptionInputError),
        (TypeError("adapter mismatch"), OpenAITranscriptionConfigurationError),
        (FakeProviderFailureError(status_code=503), OpenAITranscriptionTransientError),
        (FakeProviderFailureError(status_code=408), OpenAITranscriptionTimeoutError),
        (FakeProviderFailureError(status_code=409), OpenAITranscriptionTransientError),
        (RuntimeError("unknown"), OpenAITranscriptionPermanentError),
    ],
)
def test_transcriber_classifies_provider_failures(
    error: Exception,
    expected_type: type[Exception],
) -> None:
    translated = configured_transcriber()._translate_error(error)

    assert isinstance(translated, expected_type)
    assert "private transcription provider detail" not in str(translated)


def test_transcriber_does_not_retry_quota_or_oversized_retry_after() -> None:
    quota = RateLimitError(
        status_code=429,
        code="insufficient_quota",
        body={"message": "billing detail"},
    )
    oversized = RateLimitError(
        status_code=429,
        response=SimpleNamespace(status_code=429, headers={"retry-after": "601"}),
    )

    quota_error = configured_transcriber()._translate_error(quota)
    oversized_error = configured_transcriber()._translate_error(oversized)

    assert isinstance(quota_error, OpenAITranscriptionConfigurationError)
    assert isinstance(oversized_error, OpenAITranscriptionPermanentError)
    assert oversized_error.retry_after_seconds is None


def test_transcriber_honors_provider_retry_directive_precedence() -> None:
    retry_input = BadRequestError(
        status_code=400,
        response=SimpleNamespace(
            status_code=400,
            headers=httpx.Headers({"X-Should-Retry": "true"}),
        ),
    )
    reject_server = InternalServerError(
        status_code=503,
        response=SimpleNamespace(
            status_code=503,
            headers=httpx.Headers({"x-should-retry": "false"}),
        ),
    )
    quota = RateLimitError(
        status_code=429,
        code="insufficient_quota",
        response=SimpleNamespace(
            status_code=429,
            headers=httpx.Headers({"x-should-retry": "true"}),
        ),
    )

    assert isinstance(
        configured_transcriber()._translate_error(retry_input),
        OpenAITranscriptionTransientError,
    )
    assert isinstance(
        configured_transcriber()._translate_error(reject_server),
        OpenAITranscriptionPermanentError,
    )
    assert isinstance(
        configured_transcriber()._translate_error(quota),
        OpenAITranscriptionConfigurationError,
    )


def test_transcriber_fails_closed_on_ambiguous_retry_headers() -> None:
    error = RateLimitError(
        status_code=429,
        response=SimpleNamespace(
            status_code=429,
            headers=httpx.Headers([("retry-after", "5"), ("Retry-After", "6")]),
        ),
    )

    translated = configured_transcriber()._translate_error(error)

    assert isinstance(translated, OpenAITranscriptionPermanentError)
    assert translated.retry_after_seconds is None


@pytest.mark.asyncio
async def test_injected_transcriber_classifies_statusless_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audio_path = tmp_path / "meeting.wav"
    audio_path.write_bytes(b"audio")
    client = FakeClient({})
    failing = FailingTranscriptions(APITimeoutError())
    client.audio = FakeAudio(failing)
    module = configured_transcriber()._openai
    monkeypatch.setattr(
        "meeting_action_orchestrator.infrastructure.openai_transcription.import_module",
        lambda _name: module,
    )
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe",
        budget_controller=FakeBudgetController(),
        client=client,
    )

    with pytest.raises(OpenAITranscriptionTimeoutError) as captured:
        await transcriber.transcribe(audio_path, context=transcription_context(audio_path))

    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    client_request_id = failing.calls[0]["extra_headers"]["X-Client-Request-Id"]
    assert str(UUID(client_request_id)) == client_request_id
    assert captured.value.request_id == client_request_id


def test_transcriber_detaches_sdk_import_error_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = "private sdk import detail"

    def fail_import(_name: str) -> object:
        raise ImportError(marker)

    monkeypatch.setattr(
        "meeting_action_orchestrator.infrastructure.openai_transcription.import_module",
        fail_import,
    )
    transcriber = OpenAITranscriber(
        api_key="test",
        model="gpt-4o-transcribe",
        budget_controller=FakeBudgetController(),
    )

    with pytest.raises(OpenAITranscriptionConfigurationError) as captured:
        transcriber._get_client()

    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert marker not in str(captured.value)


@pytest.mark.asyncio
async def test_transcriber_raises_sanitized_error_without_raw_exception_context(
    tmp_path: Path,
) -> None:
    audio_path = tmp_path / "meeting.wav"
    audio_path.write_bytes(b"audio")
    provider_error = RateLimitError(
        status_code=429,
        code="rate_limit_exceeded",
        body={"message": "private transcription provider detail"},
        response=SimpleNamespace(
            status_code=429,
            headers={"retry-after": "9", "x-request-id": "req_transcription_2"},
        ),
    )
    client = FakeClient({})
    failing = FailingTranscriptions(provider_error)
    client.audio = FakeAudio(failing)
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe",
        budget_controller=FakeBudgetController(),
        client=client,
    )
    transcriber._openai = configured_transcriber()._openai

    with pytest.raises(OpenAITranscriptionTransientError) as captured:
        await transcriber.transcribe(audio_path, context=transcription_context(audio_path))

    error = captured.value
    assert error.request_id == "req_transcription_2"
    assert error.retry_after_seconds == 9
    assert error.__cause__ is None
    assert error.__context__ is None
    assert "private transcription provider detail" not in str(error)
    client_request_id = failing.calls[0]["extra_headers"]["X-Client-Request-Id"]
    assert error.request_id != client_request_id


@pytest.mark.asyncio
async def test_transcriber_reserves_exact_verified_audio_before_mock_transport(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            headers={"x-request-id": "req_transcription_budgeted"},
            json={
                "text": "Ship it",
                "usage": {"type": "duration", "seconds": 1.2501},
            },
        )

    audio_path = tmp_path / "meeting.wav"
    audio_path.write_bytes(b"verified audio")
    context = transcription_context(audio_path, audio_duration_ms=2500)
    controller = FakeBudgetController()
    transcriber = OpenAITranscriber(
        api_key="test",
        model="gpt-4o-transcribe",
        budget_controller=controller,
        http_transport=httpx.MockTransport(handler),
    )

    result = await transcriber.transcribe(audio_path, language="en", context=context)
    http_client = transcriber._http_client
    await transcriber.close()

    assert http_client is not None
    assert http_client.follow_redirects is False
    assert len(requests) == 1
    assert len(controller.reservations) == 1
    reserved_context, reservation = controller.reservations[0]
    assert reserved_context == context.dispatch
    assert reservation.operation is ProviderOperation.TRANSCRIPTION_CREATE
    assert reservation.dispatch_key == requests[0].headers["X-Client-Request-Id"]
    assert reservation.reserved_audio_duration_ms == context.audio_duration_ms
    assert reservation.operation_digest == canonical_sha256(
        {
            "audio": {
                "duration_ms": context.audio_duration_ms,
                "sha256": context.audio_sha256,
                "size_bytes": context.audio_size_bytes,
            },
            "request": {
                "language": "en",
                "model": "gpt-4o-transcribe",
                "response_format": "json",
                "temperature": 0,
            },
        }
    )
    assert result.usage is not None
    assert result.usage.kind is ProviderUsageKind.DURATION
    assert result.usage.audio_duration_ms == 1251
    assert controller.settlements[0][2] == result.usage


@pytest.mark.asyncio
async def test_transcriber_settles_usage_before_rejecting_output(tmp_path: Path) -> None:
    audio_path = tmp_path / "meeting.wav"
    audio_path.write_bytes(b"audio")
    controller = FakeBudgetController()
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe",
        budget_controller=controller,
        client=FakeClient(
            {
                "text": "",
                "usage": {
                    "type": "tokens",
                    "input_tokens": 8,
                    "output_tokens": 3,
                    "total_tokens": 11,
                },
            }
        ),
    )

    with pytest.raises(OpenAITranscriptionOutputError):
        await transcriber.transcribe(
            audio_path,
            context=transcription_context(audio_path),
        )

    assert len(controller.reservations) == 1
    assert len(controller.settlements) == 1
    assert controller.settlements[0][2].input_tokens == 8
    assert controller.settlements[0][2].output_tokens == 3


@pytest.mark.asyncio
async def test_transcriber_rejects_changed_audio_before_reservation(tmp_path: Path) -> None:
    audio_path = tmp_path / "meeting.wav"
    audio_path.write_bytes(b"audio")
    context = transcription_context(audio_path)
    audio_path.write_bytes(b"other")
    controller = FakeBudgetController()
    client = FakeClient({"text": "Ship it"})
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe",
        budget_controller=controller,
        client=client,
    )

    with pytest.raises(AudioAssetIdentityMismatchError):
        await transcriber.transcribe(audio_path, context=context)

    assert controller.reservations == []
    assert client.transcriptions.calls == []


@pytest.mark.asyncio
async def test_transcriber_verifies_audio_without_no_follow_support(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audio_path = tmp_path / "meeting.wav"
    audio_path.write_bytes(b"audio")
    controller = FakeBudgetController()
    client = FakeClient({"text": "Ship it"})
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe",
        budget_controller=controller,
        client=client,
    )
    monkeypatch.setattr(
        "meeting_action_orchestrator.infrastructure.openai_transcription.os.O_NOFOLLOW",
        0,
        raising=False,
    )

    result = await transcriber.transcribe(
        audio_path,
        context=transcription_context(audio_path),
    )

    assert result.text == "Ship it"
    assert len(controller.reservations) == 1
    assert len(client.transcriptions.calls) == 1


@pytest.mark.asyncio
async def test_transcriber_retries_pre_dispatch_audio_sharing_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audio_path = tmp_path / "meeting.wav"
    audio_path.write_bytes(b"audio")
    controller = FakeBudgetController()
    client = FakeClient({"text": "Ship it"})
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe",
        budget_controller=controller,
        client=client,
    )

    def unavailable(*_arguments: object, **_keywords: object) -> int:
        raise PermissionError(errno.EACCES, "private path detail")

    monkeypatch.setattr(
        "meeting_action_orchestrator.infrastructure.openai_transcription.os.open",
        unavailable,
    )

    with pytest.raises(
        OpenAITranscriptionTransientError,
        match=r"^OpenAI transcription failed with local_audio_unavailable$",
    ):
        await transcriber.transcribe(
            audio_path,
            context=transcription_context(audio_path),
        )

    assert controller.reservations == []
    assert client.transcriptions.calls == []


@pytest.mark.parametrize(
    "usage",
    [
        pytest.param({}, id="missing"),
        pytest.param({"input_tokens": 1, "output_tokens": 1}, id="missing-type"),
        pytest.param(
            {
                "type": "tokens",
                "input_tokens": True,
                "output_tokens": 1,
                "total_tokens": 2,
            },
            id="boolean-token",
        ),
        pytest.param(
            {
                "type": "tokens",
                "input_tokens": 1,
                "output_tokens": 1,
                "total_tokens": 3,
            },
            id="inconsistent-total",
        ),
        pytest.param({"type": "duration", "seconds": 0}, id="zero-duration"),
        pytest.param({"type": "duration", "seconds": float("nan")}, id="nan-duration"),
    ],
)
def test_transcriber_keeps_invalid_usage_unsettled(usage: object) -> None:
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe",
        budget_controller=FakeBudgetController(),
        client=FakeClient({}),
    )

    assert transcriber._map_usage(usage) is None


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({"text": "Ship it"}, id="absent"),
        pytest.param(
            {
                "text": "Ship it",
                "usage": {
                    "type": "tokens",
                    "input_tokens": 4,
                    "output_tokens": 2,
                    "total_tokens": 7,
                },
            },
            id="malformed-tokens",
        ),
        pytest.param(
            {"text": "Ship it", "usage": {"type": "duration", "seconds": 0}},
            id="malformed-duration",
        ),
    ],
)
@pytest.mark.asyncio
async def test_transcriber_keeps_envelope_when_optional_usage_is_invalid(
    tmp_path: Path,
    payload: dict[str, object],
) -> None:
    audio_path = tmp_path / "meeting.wav"
    audio_path.write_bytes(b"audio")
    controller = FakeBudgetController()
    client = FakeClient(FakeResponse(payload, "req_usage_invalid"))
    transcriber = OpenAITranscriber(
        api_key="",
        model="gpt-4o-transcribe",
        budget_controller=controller,
        client=client,
    )

    result = await transcriber.transcribe(
        audio_path,
        context=transcription_context(audio_path),
    )

    assert result.text == "Ship it"
    assert result.usage is None
    assert len(controller.reservations) == 1
    assert controller.settlements == []
    assert len(client.transcriptions.calls) == 1
