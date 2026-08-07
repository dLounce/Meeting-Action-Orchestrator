from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from importlib import import_module
from math import isfinite
from pathlib import Path
from typing import Any
from uuid import uuid4

from meeting_action_orchestrator.application.errors import (
    ProviderConfigurationError,
    ProviderError,
    ProviderInputError,
    ProviderOutputError,
    ProviderPermanentError,
    ProviderRateLimitError,
    ProviderTimeoutError,
    ProviderTransientError,
)
from meeting_action_orchestrator.application.provider_policy import (
    ProviderErrorMetadata,
    provider_error_metadata,
    provider_error_requires_action,
    sanitize_provider_identifier,
)

_DIARIZED_TRANSCRIPTION_MODELS = frozenset({"gpt-4o-transcribe-diarize"})


def _requires_diarized_segments(model: str) -> bool:
    return model in _DIARIZED_TRANSCRIPTION_MODELS


class OpenAITranscriptionError(ProviderError):
    def __init__(
        self,
        error_type: str | None = None,
        *,
        metadata: ProviderErrorMetadata | None = None,
    ) -> None:
        message = "OpenAI transcription failed"
        if error_type is not None:
            message = f"{message} with {error_type}"
        super().__init__(message, metadata=metadata)


class OpenAITranscriptionConfigurationError(
    OpenAITranscriptionError,
    ProviderConfigurationError,
):
    def __init__(self, *, metadata: ProviderErrorMetadata | None = None) -> None:
        super().__init__("configuration_error", metadata=metadata)


class OpenAITranscriptionInputError(OpenAITranscriptionError, ProviderInputError):
    def __init__(self, *, metadata: ProviderErrorMetadata | None = None) -> None:
        super().__init__("invalid_input", metadata=metadata)


class OpenAITranscriptionTransientError(OpenAITranscriptionError, ProviderTransientError):
    def __init__(
        self,
        error_type: str = "transient_error",
        *,
        metadata: ProviderErrorMetadata | None = None,
    ) -> None:
        super().__init__(error_type, metadata=metadata)


class OpenAITranscriptionTimeoutError(
    OpenAITranscriptionTransientError,
    ProviderTimeoutError,
):
    def __init__(self, *, metadata: ProviderErrorMetadata | None = None) -> None:
        super().__init__("timeout", metadata=metadata)


class OpenAITranscriptionRateLimitError(
    OpenAITranscriptionTransientError,
    ProviderRateLimitError,
):
    def __init__(self, *, metadata: ProviderErrorMetadata | None = None) -> None:
        super().__init__("rate_limited", metadata=metadata)


class OpenAITranscriptionOutputError(OpenAITranscriptionError, ProviderOutputError):
    def __init__(self, *, metadata: ProviderErrorMetadata | None = None) -> None:
        super().__init__("invalid_output", metadata=metadata)


class OpenAITranscriptionPermanentError(
    OpenAITranscriptionError,
    ProviderPermanentError,
):
    def __init__(self, *, metadata: ProviderErrorMetadata | None = None) -> None:
        super().__init__("permanent_error", metadata=metadata)


@dataclass(frozen=True)
class TranscriptionSegment:
    id: str
    start_ms: int
    end_ms: int | None
    speaker: str | None
    text: str


@dataclass(frozen=True)
class TranscriptionUsage:
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    seconds: float | None = None


@dataclass(frozen=True)
class TranscriptionOutput:
    model: str
    provider_request_id: str | None
    language: str | None
    text: str
    duration_seconds: float | None
    segments: tuple[TranscriptionSegment, ...]
    usage: TranscriptionUsage


class OpenAITranscriber:
    def __init__(
        self,
        api_key: str,
        model: str,
        timeout_seconds: float = 120.0,
        max_retries: int = 0,
        client: Any = None,
    ) -> None:
        if not api_key and client is None:
            raise OpenAITranscriptionConfigurationError
        if not model:
            raise OpenAITranscriptionConfigurationError
        if max_retries != 0:
            raise OpenAITranscriptionConfigurationError
        self._api_key = api_key
        self._model = model
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries
        self._diarization_required = _requires_diarized_segments(model)
        self._client = client
        self._openai: Any = None
        self._owns_client = False

    async def close(self) -> None:
        if not self._owns_client:
            return
        client = self._client
        self._client = None
        self._owns_client = False
        if client is not None:
            await client.close()

    async def transcribe(
        self,
        audio_path: Path,
        language: str | None = None,
    ) -> TranscriptionOutput:
        if not await asyncio.to_thread(audio_path.is_file):
            raise OpenAITranscriptionInputError
        client = self._get_client()
        client_request_id = str(uuid4())
        arguments: dict[str, Any] = {
            "model": self._model,
            "response_format": "json",
            "temperature": 0,
            "extra_headers": {"X-Client-Request-Id": client_request_id},
        }
        if self._diarization_required:
            arguments["response_format"] = "diarized_json"
            arguments["chunking_strategy"] = "auto"
        if language is not None:
            arguments["language"] = language
        try:
            with audio_path.open("rb") as audio_file:
                response = await client.audio.transcriptions.create(
                    file=audio_file,
                    **arguments,
                )
        except Exception as error:
            translated = self._translate_error(error, client_request_id)
        else:
            return self._map_response(response, client_request_id)
        raise translated

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        module = self._load_openai()
        if module is None:
            raise OpenAITranscriptionConfigurationError
        client = None
        with suppress(Exception):
            client = module.AsyncOpenAI(
                api_key=self._api_key,
                timeout=self._timeout_seconds,
                max_retries=self._max_retries,
            )
        if client is None:
            raise OpenAITranscriptionConfigurationError
        self._client = client
        self._owns_client = True
        return self._client

    def _load_openai(self) -> Any:
        if self._openai is not None:
            return self._openai
        module = None
        with suppress(AttributeError, ImportError):
            module = import_module("openai")
        if module is not None:
            self._openai = module
        return module

    def _map_response(
        self,
        response: Any,
        client_request_id: str | None = None,
    ) -> TranscriptionOutput:
        data = self._as_mapping(response)
        text = self._string_value(data.get("text"))
        if not text:
            raise OpenAITranscriptionOutputError
        duration = self._float_value(data.get("duration"))
        segments = self._map_segments(data.get("segments"), text, duration)
        usage = self._map_usage(data.get("usage"))
        return TranscriptionOutput(
            model=self._model,
            provider_request_id=self._provider_request_id(response, client_request_id),
            language=self._optional_string(data.get("language")),
            text=text,
            duration_seconds=duration,
            segments=segments,
            usage=usage,
        )

    def _map_segments(
        self,
        raw_segments: object,
        text: str,
        duration: float | None,
    ) -> tuple[TranscriptionSegment, ...]:
        if self._diarization_required:
            return self._map_diarized_segments(raw_segments)
        if not isinstance(raw_segments, list) or not raw_segments:
            end_ms = round(duration * 1000) if duration is not None else None
            return (
                TranscriptionSegment(
                    id="segment_0001",
                    start_ms=0,
                    end_ms=end_ms,
                    speaker=None,
                    text=text,
                ),
            )
        segments: list[TranscriptionSegment] = []
        for index, raw_segment in enumerate(raw_segments, start=1):
            segment = self._as_mapping(raw_segment)
            segment_text = self._string_value(segment.get("text"))
            if not segment_text:
                continue
            start = self._float_value(segment.get("start")) or 0.0
            end = self._float_value(segment.get("end"))
            segments.append(
                TranscriptionSegment(
                    id=self._optional_string(segment.get("id")) or f"segment_{index:04d}",
                    start_ms=round(start * 1000),
                    end_ms=round(end * 1000) if end is not None else None,
                    speaker=self._optional_string(segment.get("speaker")),
                    text=segment_text,
                )
            )
        if not segments:
            raise OpenAITranscriptionOutputError
        return tuple(segments)

    def _map_diarized_segments(
        self,
        raw_segments: object,
    ) -> tuple[TranscriptionSegment, ...]:
        if not isinstance(raw_segments, list) or not raw_segments:
            raise OpenAITranscriptionOutputError
        segments: list[TranscriptionSegment] = []
        previous_start: float | None = None
        for index, raw_segment in enumerate(raw_segments, start=1):
            segment = self._as_mapping(raw_segment)
            segment_text = self._optional_string(segment.get("text"))
            speaker = self._optional_string(segment.get("speaker"))
            start = self._finite_float_value(segment.get("start"))
            end = self._finite_float_value(segment.get("end"))
            if (
                segment_text is None
                or speaker is None
                or start is None
                or end is None
                or start < 0
                or end < start
                or (previous_start is not None and start < previous_start)
            ):
                raise OpenAITranscriptionOutputError
            segments.append(
                TranscriptionSegment(
                    id=self._optional_string(segment.get("id")) or f"segment_{index:04d}",
                    start_ms=round(start * 1000),
                    end_ms=round(end * 1000),
                    speaker=speaker,
                    text=segment_text,
                )
            )
            previous_start = start
        return tuple(segments)

    def _map_usage(self, raw_usage: object) -> TranscriptionUsage:
        usage = self._as_mapping(raw_usage)
        return TranscriptionUsage(
            input_tokens=self._integer_value(usage.get("input_tokens")),
            output_tokens=self._integer_value(usage.get("output_tokens")),
            total_tokens=self._integer_value(usage.get("total_tokens")),
            seconds=self._float_value(usage.get("seconds")),
        )

    def _translate_error(
        self,
        error: Exception,
        client_request_id: str | None = None,
    ) -> OpenAITranscriptionError:
        module = self._load_openai()
        metadata = provider_error_metadata(error, client_request_id)
        if provider_error_requires_action(error):
            return OpenAITranscriptionConfigurationError(metadata=metadata)
        if metadata.retry_control_rejected or metadata.provider_should_retry is False:
            return self._non_retryable_error(error, module, metadata)
        if metadata.provider_should_retry is True:
            return self._transient_error(error, module, metadata)
        status = metadata.http_status
        if status in {408, 409, 429} or (status is not None and status >= 500):
            return self._transient_error(error, module, metadata)
        transient_names = (
            "APIConnectionError",
            "APITimeoutError",
            "RateLimitError",
            "InternalServerError",
        )
        if any(self._is_exception(error, module, name) for name in transient_names):
            return self._transient_error(error, module, metadata)
        configuration_names = (
            "AuthenticationError",
            "PermissionDeniedError",
            "NotFoundError",
        )
        if status in {401, 403, 404} or any(
            self._is_exception(error, module, name) for name in configuration_names
        ):
            return OpenAITranscriptionConfigurationError(metadata=metadata)
        if isinstance(error, (AttributeError, TypeError)):
            return OpenAITranscriptionConfigurationError(metadata=metadata)
        input_names = ("BadRequestError", "UnprocessableEntityError")
        if (status is not None and 400 <= status < 500 and status not in {408, 409, 429}) or any(
            self._is_exception(error, module, name) for name in input_names
        ):
            return OpenAITranscriptionInputError(metadata=metadata)
        return OpenAITranscriptionPermanentError(metadata=metadata)

    def _transient_error(
        self,
        error: Exception,
        module: Any,
        metadata: ProviderErrorMetadata,
    ) -> OpenAITranscriptionError:
        if metadata.http_status == 408 or self._is_exception(
            error,
            module,
            "APITimeoutError",
        ):
            return OpenAITranscriptionTimeoutError(metadata=metadata)
        if metadata.http_status == 429 or self._is_exception(
            error,
            module,
            "RateLimitError",
        ):
            return OpenAITranscriptionRateLimitError(metadata=metadata)
        return OpenAITranscriptionTransientError(metadata=metadata)

    def _non_retryable_error(
        self,
        error: Exception,
        module: Any,
        metadata: ProviderErrorMetadata,
    ) -> OpenAITranscriptionError:
        configuration_names = (
            "AuthenticationError",
            "PermissionDeniedError",
            "NotFoundError",
        )
        if metadata.http_status in {401, 403, 404} or any(
            self._is_exception(error, module, name) for name in configuration_names
        ):
            return OpenAITranscriptionConfigurationError(metadata=metadata)
        input_names = ("BadRequestError", "UnprocessableEntityError")
        status = metadata.http_status
        if (status is not None and 400 <= status < 500 and status not in {408, 409, 429}) or any(
            self._is_exception(error, module, name) for name in input_names
        ):
            return OpenAITranscriptionInputError(metadata=metadata)
        return OpenAITranscriptionPermanentError(metadata=metadata)

    @staticmethod
    def _is_exception(error: Exception, module: Any, name: str) -> bool:
        if module is None:
            return False
        exception_type = getattr(module, name, None)
        return isinstance(exception_type, type) and isinstance(error, exception_type)

    @staticmethod
    def _as_mapping(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        model_dump = getattr(value, "model_dump", None)
        if callable(model_dump):
            dumped = model_dump(mode="json")
            return dumped if isinstance(dumped, dict) else {}
        return {}

    @staticmethod
    def _string_value(value: object) -> str:
        return value.strip() if isinstance(value, str) else ""

    @classmethod
    def _optional_string(cls, value: object) -> str | None:
        text = cls._string_value(value)
        return text or None

    @staticmethod
    def _integer_value(value: object) -> int:
        return value if isinstance(value, int) else 0

    @staticmethod
    def _float_value(value: object) -> float | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        return None

    @classmethod
    def _finite_float_value(cls, value: object) -> float | None:
        number = cls._float_value(value)
        if number is None or not isfinite(number):
            return None
        return number

    @staticmethod
    def _provider_request_id(
        response: Any,
        client_request_id: str | None = None,
    ) -> str | None:
        for attribute in ("_request_id", "request_id"):
            value = sanitize_provider_identifier(getattr(response, attribute, None))
            if value is not None:
                return value
        return sanitize_provider_identifier(client_request_id)
