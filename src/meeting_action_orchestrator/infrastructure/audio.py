from __future__ import annotations

import hashlib
import json
import os
import subprocess
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import BinaryIO, ClassVar, Protocol
from uuid import uuid4

from meeting_action_orchestrator.application.ports import AudioMetadata, StoredAudio


class AudioValidationError(ValueError):
    pass


class AudioInspector(Protocol):
    def inspect(self, path: Path, detected_media_type: str) -> AudioMetadata: ...


class FFprobeAudioInspector:
    def __init__(
        self,
        *,
        executable: str = "ffprobe",
        timeout_seconds: float = 15,
        max_duration_seconds: float = 7200,
        runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        self._executable = executable
        self._timeout_seconds = timeout_seconds
        self._max_duration_seconds = max_duration_seconds
        self._runner = runner

    def inspect(self, path: Path, detected_media_type: str) -> AudioMetadata:
        result = self._runner(
            [
                self._executable,
                "-v",
                "error",
                "-show_entries",
                "format=duration:stream=codec_type,codec_name,sample_rate,channels",
                "-of",
                "json",
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=self._timeout_seconds,
            check=False,
        )
        if result.returncode != 0:
            raise AudioValidationError("The uploaded file could not be decoded as audio")
        try:
            payload = json.loads(result.stdout)
            streams = [item for item in payload["streams"] if item["codec_type"] == "audio"]
            duration_seconds = float(payload["format"]["duration"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise AudioValidationError("Audio metadata is incomplete") from error
        if len(streams) != 1:
            raise AudioValidationError("The recording must contain exactly one audio stream")
        if duration_seconds <= 0 or duration_seconds > self._max_duration_seconds:
            raise AudioValidationError("The recording duration is outside the supported range")
        stream = streams[0]
        try:
            sample_rate_hz = int(stream["sample_rate"])
            channels = int(stream["channels"])
            codec = str(stream["codec_name"])
        except (KeyError, TypeError, ValueError) as error:
            raise AudioValidationError("Audio stream metadata is incomplete") from error
        if sample_rate_hz <= 0 or channels <= 0:
            raise AudioValidationError("Audio stream metadata is invalid")
        return AudioMetadata(
            media_type=detected_media_type,
            duration_ms=round(duration_seconds * 1000),
            codec=codec,
            sample_rate_hz=sample_rate_hz,
            channels=channels,
        )


class LocalAudioStore:
    _suffixes: ClassVar[dict[str, str]] = {
        "audio/mpeg": ".mp3",
        "audio/mp4": ".m4a",
        "audio/wav": ".wav",
    }

    def __init__(self, root: Path, inspector: AudioInspector, max_bytes: int) -> None:
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        self._root = root
        self._inspector = inspector
        self._max_bytes = max_bytes

    def put(self, stream: BinaryIO, original_name: str) -> StoredAudio:
        self._root.mkdir(parents=True, exist_ok=True, mode=0o700)
        _restrict_permissions(self._root, 0o700)
        safe_name = Path(original_name.replace("\\", "/")).name
        if not safe_name:
            raise AudioValidationError("A filename is required")
        storage_id = uuid4().hex
        temporary_path = self._root / f".{storage_id}.part"
        digest = hashlib.sha256()
        size_bytes = 0
        header = b""
        try:
            descriptor = os.open(
                temporary_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            with os.fdopen(descriptor, "wb") as destination:
                while chunk := stream.read(1024 * 1024):
                    size_bytes += len(chunk)
                    if size_bytes > self._max_bytes:
                        raise AudioValidationError("The recording exceeds the upload limit")
                    if len(header) < 16:
                        header = (header + chunk)[:16]
                    digest.update(chunk)
                    destination.write(chunk)
                destination.flush()
                os.fsync(destination.fileno())
            if size_bytes == 0:
                raise AudioValidationError("The recording is empty")
            media_type = detect_audio_type(header)
            suffix = self._suffixes[media_type]
            file_digest = digest.hexdigest()
            final_path = self._root / f"{storage_id}{suffix}"
            metadata = self._inspector.inspect(temporary_path, media_type)
            temporary_path.replace(final_path)
            return StoredAudio(
                storage_key=final_path.name,
                original_name=safe_name,
                path=final_path,
                size_bytes=size_bytes,
                sha256=file_digest,
                metadata=metadata,
            )
        except BaseException:
            temporary_path.unlink(missing_ok=True)
            raise

    def open(self, storage_key: str) -> BinaryIO:
        return self.path(storage_key).open("rb")

    def path(self, storage_key: str) -> Path:
        if Path(storage_key).name != storage_key:
            raise AudioValidationError("The storage key is invalid")
        return self._root / storage_key

    def delete(self, storage_key: str) -> None:
        self.path(storage_key).unlink(missing_ok=True)


def detect_audio_type(header: bytes) -> str:
    if len(header) >= 12 and header[:4] == b"RIFF" and header[8:12] == b"WAVE":
        return "audio/wav"
    if header[:3] == b"ID3":
        return "audio/mpeg"
    if len(header) >= 2 and header[0] == 0xFF and header[1] & 0xE0 == 0xE0:
        return "audio/mpeg"
    if len(header) >= 12 and header[4:8] == b"ftyp":
        return "audio/mp4"
    raise AudioValidationError("Only MP3, M4A, and WAV recordings are supported")


def _restrict_permissions(path: Path, mode: int) -> None:
    with suppress(OSError):
        path.chmod(mode)
