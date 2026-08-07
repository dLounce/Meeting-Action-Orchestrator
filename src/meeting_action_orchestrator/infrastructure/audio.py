from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import stat
import subprocess
import threading
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import BinaryIO, ClassVar, Protocol
from uuid import uuid4

from meeting_action_orchestrator.application.ports import AudioMetadata, StoredAudio

FINAL_RECORDING_KEY_PATTERN = re.compile(r"[0-9a-f]{32}\.(?:wav|mp3|m4a)")
TEMPORARY_RECORDING_KEY_PATTERN = re.compile(r"\.[0-9a-f]{32}\.part")
_LINK_FALLBACK_ERRNOS = frozenset(
    value for name in ("ENOTSUP", "EOPNOTSUPP") if (value := getattr(errno, name, None)) is not None
)
_DIRECTORY_SYNC_UNSUPPORTED_ERRNOS = frozenset(
    value
    for name in ("EINVAL", "ENOTSUP", "EOPNOTSUPP")
    if (value := getattr(errno, name, None)) is not None
)


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
        self._active_temporary_keys: set[str] = set()
        self._active_temporary_keys_lock = threading.Lock()

    def put(self, stream: BinaryIO, original_name: str) -> StoredAudio:
        self._root.mkdir(parents=True, exist_ok=True, mode=0o700)
        _restrict_permissions(self._root, 0o700)
        safe_name = Path(original_name.replace("\\", "/")).name
        if not safe_name:
            raise AudioValidationError("A filename is required")
        storage_id = uuid4().hex
        temporary_key = f".{storage_id}.part"
        temporary_path = self._root / temporary_key
        temporary_state: tuple[int, int, int, int, int, int] | None = None
        self._activate_temporary_key(temporary_key)
        try:
            try:
                size_bytes, header, file_digest, temporary_state = self._stage(
                    stream,
                    temporary_path,
                )
                media_type = detect_audio_type(header)
                suffix = self._suffixes[media_type]
                final_path = self._root / f"{storage_id}{suffix}"
                metadata = self._inspector.inspect(temporary_path, media_type)
                verified_state = _verify_recording_file(
                    temporary_path,
                    temporary_state[:2],
                    size_bytes,
                    file_digest,
                )
                temporary_state = _link_without_overwrite(
                    temporary_path,
                    final_path,
                    verified_state,
                )
                _sync_directory(self._root)
                if not _unlink_owned_file(temporary_path, temporary_state):
                    raise AudioValidationError("The recording could not be stored safely")
                temporary_state = None
                _sync_directory(self._root)
                return StoredAudio(
                    storage_key=final_path.name,
                    original_name=safe_name,
                    path=final_path,
                    size_bytes=size_bytes,
                    sha256=file_digest,
                    metadata=metadata,
                )
            except BaseException:
                if temporary_state is not None:
                    _unlink_owned_file(temporary_path, temporary_state)
                raise
        finally:
            self._deactivate_temporary_key(temporary_key)

    def open(self, storage_key: str) -> BinaryIO:
        return self.path(storage_key).open("rb")

    def path(self, storage_key: str) -> Path:
        if FINAL_RECORDING_KEY_PATTERN.fullmatch(storage_key) is None:
            raise AudioValidationError("The storage key is invalid")
        return self._root / storage_key

    def active_temporary_keys(self) -> frozenset[str]:
        with self._active_temporary_keys_lock:
            return frozenset(self._active_temporary_keys)

    def _activate_temporary_key(self, storage_key: str) -> None:
        with self._active_temporary_keys_lock:
            self._active_temporary_keys.add(storage_key)

    def _deactivate_temporary_key(self, storage_key: str) -> None:
        with self._active_temporary_keys_lock:
            self._active_temporary_keys.discard(storage_key)

    def _stage(
        self,
        stream: BinaryIO,
        path: Path,
    ) -> tuple[int, bytes, str, tuple[int, int, int, int, int, int]]:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        descriptor_open = True
        state: tuple[int, int, int, int, int, int] | None = None
        digest = hashlib.sha256()
        size_bytes = 0
        header = b""
        try:
            state = _file_state(os.fstat(descriptor))
            destination = os.fdopen(descriptor, "wb")
            descriptor_open = False
            with destination:
                try:
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
                finally:
                    with suppress(OSError):
                        state = _file_state(os.fstat(destination.fileno()))
            if size_bytes == 0:
                raise AudioValidationError("The recording is empty")
        except BaseException:
            if descriptor_open:
                with suppress(OSError):
                    os.close(descriptor)
            if state is not None:
                _unlink_owned_file(path, state)
            raise
        if state is None:
            raise AudioValidationError("The recording could not be staged safely")
        return size_bytes, header, digest.hexdigest(), state


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


def _sync_directory(path: Path) -> None:
    directory_flag = getattr(os, "O_DIRECTORY", 0)
    if directory_flag == 0:
        return
    try:
        descriptor = os.open(path, os.O_RDONLY | directory_flag)
    except OSError as error:
        if error.errno in _DIRECTORY_SYNC_UNSUPPORTED_ERRNOS:
            return
        raise
    try:
        os.fsync(descriptor)
    except OSError as error:
        if error.errno not in _DIRECTORY_SYNC_UNSUPPORTED_ERRNOS:
            raise
    finally:
        os.close(descriptor)


def _link_without_overwrite(
    source: Path,
    target: Path,
    expected_state: tuple[int, int, int, int, int, int],
) -> tuple[int, int, int, int, int, int]:
    source_stat = os.lstat(source)
    if not stat.S_ISREG(source_stat.st_mode) or _file_state(source_stat) != expected_state:
        raise AudioValidationError("The recording could not be stored safely")
    try:
        os.link(source, target, follow_symlinks=False)
    except (NotImplementedError, TypeError):
        _link_with_fallback(source, target)
    except OSError as error:
        if error.errno not in _LINK_FALLBACK_ERRNOS:
            raise
        _link_with_fallback(source, target)
    target_stat = os.lstat(target)
    source_after = os.lstat(source)
    expected_publication_state = expected_state[:5]
    if (
        not stat.S_ISREG(source_after.st_mode)
        or not stat.S_ISREG(target_stat.st_mode)
        or _publication_state(source_after) != expected_publication_state
        or _publication_state(target_stat) != expected_publication_state
    ):
        raise AudioValidationError("The recording could not be stored safely")
    return _file_state(source_after)


def _link_with_fallback(source: Path, target: Path) -> None:
    try:
        os.link(source, target)
    except (NotImplementedError, TypeError):
        raise AudioValidationError("Recording publication is not supported") from None


def _unlink_owned_file(
    path: Path,
    expected_state: tuple[int, int, int, int, int, int],
) -> bool:
    try:
        value = os.lstat(path)
        if stat.S_ISREG(value.st_mode) and _file_state(value) == expected_state:
            os.unlink(path)
            return True
    except OSError:
        return False
    return False


def _verify_recording_file(
    path: Path,
    expected_identity: tuple[int, int],
    expected_size_bytes: int,
    expected_sha256: str,
) -> tuple[int, int, int, int, int, int]:
    initial = os.lstat(path)
    if (
        not stat.S_ISREG(initial.st_mode)
        or (initial.st_dev, initial.st_ino) != expected_identity
        or initial.st_size != expected_size_bytes
    ):
        raise AudioValidationError("The recording changed before publication")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or (before.st_dev, before.st_ino) != expected_identity
            or before.st_size != expected_size_bytes
        ):
            raise AudioValidationError("The recording changed before publication")
        digest = hashlib.sha256()
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
        after = os.fstat(descriptor)
        final = os.lstat(path)
    finally:
        os.close(descriptor)
    if (
        _file_state(before) != _file_state(after)
        or _file_state(after) != _file_state(final)
        or digest.hexdigest() != expected_sha256
    ):
        raise AudioValidationError("The recording changed before publication")
    return _file_state(final)


def _file_state(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _publication_state(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
    )
