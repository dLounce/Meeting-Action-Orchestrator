from __future__ import annotations

import asyncio
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any

from meeting_action_orchestrator.application.errors import (
    ProviderConfigurationError,
    ProviderError,
    ProviderInputError,
    ProviderOutputError,
    ProviderTransientError,
)


class OpenAITranscriptionError(ProviderError):
    def __init__(self, error_type: str | None = None) -> None:
        message = "OpenAI transcription failed"
        if error_type is not None:
            message = f"{message} with {error_type}"
        super().__init__(message)


class OpenAITranscriptionConfigurationError(
    OpenAITranscriptionError,
    ProviderConfigurationError,
):
    def __init__(self) -> None:
        super().__init__("configuration_error")


class OpenAITranscriptionInputError(OpenAITranscriptionError, ProviderInputError):
    def __init__(self) -> None:
        super().__init__("invalid_input")


class OpenAITranscriptionTransientError(OpenAITranscriptionError, ProviderTransientError):
    def __init__(self) -> None:
        super().__init__("transient_error")


class OpenAITranscriptionOutputError(OpenAITranscriptionError, ProviderOutputError):
    def __init__(self) -> None:
        super().__init__("invalid_output")


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
        arguments: dict[str, Any] = {
            "model": self._model,
            "response_format": "json",
            "temperature": 0,
        }
        if "diarize" in self._model:
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
            raise self._translate_error(error) from error
        return self._map_response(response)

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            if self._openai is None:
                self._openai = import_module("openai")
            self._client = self._openai.AsyncOpenAI(
                api_key=self._api_key,
                timeout=self._timeout_seconds,
                max_retries=self._max_retries,
            )
            self._owns_client = True
        except (AttributeError, ImportError) as error:
            raise OpenAITranscriptionConfigurationError from error
        return self._client

    def _map_response(self, response: Any) -> TranscriptionOutput:
        data = self._as_mapping(response)
        text = self._string_value(data.get("text"))
        if not text:
            raise OpenAITranscriptionOutputError
        duration = self._float_value(data.get("duration"))
        segments = self._map_segments(data.get("segments"), text, duration)
        usage = self._map_usage(data.get("usage"))
        return TranscriptionOutput(
            model=self._model,
            provider_request_id=self._provider_request_id(response),
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

    def _map_usage(self, raw_usage: object) -> TranscriptionUsage:
        usage = self._as_mapping(raw_usage)
        return TranscriptionUsage(
            input_tokens=self._integer_value(usage.get("input_tokens")),
            output_tokens=self._integer_value(usage.get("output_tokens")),
            total_tokens=self._integer_value(usage.get("total_tokens")),
            seconds=self._float_value(usage.get("seconds")),
        )

    def _translate_error(self, error: Exception) -> OpenAITranscriptionError:
        module = self._openai
        transient_names = (
            "APIConnectionError",
            "APITimeoutError",
            "RateLimitError",
            "InternalServerError",
        )
        if any(self._is_exception(error, module, name) for name in transient_names):
            return OpenAITranscriptionTransientError()
        configuration_names = (
            "AuthenticationError",
            "PermissionDeniedError",
            "NotFoundError",
        )
        if any(self._is_exception(error, module, name) for name in configuration_names):
            return OpenAITranscriptionConfigurationError()
        if self._is_exception(error, module, "BadRequestError"):
            return OpenAITranscriptionInputError()
        return OpenAITranscriptionError(type(error).__name__)

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

    @staticmethod
    def _provider_request_id(response: Any) -> str | None:
        for attribute in ("_request_id", "request_id"):
            value = getattr(response, attribute, None)
            if isinstance(value, str) and value:
                return value
        return None
