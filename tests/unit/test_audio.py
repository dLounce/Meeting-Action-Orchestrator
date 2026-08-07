from __future__ import annotations

import errno
import io
import os
import subprocess
from pathlib import Path
from threading import Event, Thread
from uuid import UUID

import pytest

from meeting_action_orchestrator.infrastructure import audio
from meeting_action_orchestrator.infrastructure.audio import (
    AudioMetadata,
    AudioValidationError,
    FFprobeAudioInspector,
    LocalAudioStore,
    detect_audio_type,
)


class StubInspector:
    def inspect(self, path: Path, detected_media_type: str) -> AudioMetadata:
        assert path.exists()
        return AudioMetadata(detected_media_type, 1000, "pcm_s16le", 16000, 1)


def test_detect_audio_type_recognizes_supported_signatures() -> None:
    assert detect_audio_type(b"RIFF\x00\x00\x00\x00WAVE") == "audio/wav"
    assert detect_audio_type(b"ID3\x04\x00\x00") == "audio/mpeg"
    assert detect_audio_type(b"\xff\xfb\x90\x64") == "audio/mpeg"
    assert detect_audio_type(b"\x00\x00\x00\x18ftypM4A ") == "audio/mp4"


def test_detect_audio_type_rejects_unknown_content() -> None:
    with pytest.raises(AudioValidationError, match="Only MP3"):
        detect_audio_type(b"not audio")


def test_store_hashes_and_uses_safe_generated_name(tmp_path: Path) -> None:
    store = LocalAudioStore(tmp_path, StubInspector(), max_bytes=1024)

    stored = store.put(io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"), "../../meeting.wav")

    assert stored.original_name == "meeting.wav"
    assert stored.storage_key.endswith(".wav")
    assert UUID(Path(stored.storage_key).stem).version == 4
    assert not stored.storage_key.startswith(stored.sha256)
    assert stored.path.read_bytes() == b"RIFF\x00\x00\x00\x00WAVEdata"
    assert len(stored.sha256) == 64


def test_store_rejects_oversized_content_without_leaving_partial_file(tmp_path: Path) -> None:
    store = LocalAudioStore(tmp_path, StubInspector(), max_bytes=8)

    with pytest.raises(AudioValidationError, match="upload limit"):
        store.put(io.BytesIO(b"RIFF\x00\x00\x00\x00WAVE"), "meeting.wav")

    assert list(tmp_path.iterdir()) == []
    assert store.active_temporary_keys() == frozenset()


def test_store_stages_identical_content_under_unique_keys(tmp_path: Path) -> None:
    store = LocalAudioStore(tmp_path, StubInspector(), max_bytes=1024)
    content = b"RIFF\x00\x00\x00\x00WAVEdata"

    first = store.put(io.BytesIO(content), "first.wav")
    second = store.put(io.BytesIO(content), "second.wav")

    assert first.storage_key != second.storage_key
    assert first.sha256 == second.sha256
    assert len(list(tmp_path.iterdir())) == 2


def test_store_tracks_temporary_key_until_inspection_finishes(tmp_path: Path) -> None:
    entered = Event()
    release = Event()

    class BlockingInspector:
        def inspect(self, path: Path, detected_media_type: str) -> AudioMetadata:
            del path
            entered.set()
            assert release.wait(timeout=5)
            return AudioMetadata(detected_media_type, 1000, "pcm_s16le", 16000, 1)

    store = LocalAudioStore(tmp_path, BlockingInspector(), max_bytes=1024)
    failures: list[BaseException] = []

    def upload() -> None:
        try:
            store.put(io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"), "meeting.wav")
        except BaseException as error:
            failures.append(error)

    thread = Thread(target=upload)
    thread.start()
    assert entered.wait(timeout=5)

    active = store.active_temporary_keys()

    assert len(active) == 1
    assert next(iter(active)).startswith(".")
    assert next(iter(active)).endswith(".part")
    release.set()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert failures == []
    assert store.active_temporary_keys() == frozenset()


def test_store_preserves_existing_final_file_on_generated_name_collision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identifier = UUID("10000000-0000-4000-8000-000000000001")
    final_path = tmp_path / f"{identifier.hex}.wav"
    tmp_path.mkdir(exist_ok=True)
    final_path.write_bytes(b"existing recording")
    monkeypatch.setattr(audio, "uuid4", lambda: identifier)
    store = LocalAudioStore(tmp_path, StubInspector(), max_bytes=1024)

    with pytest.raises(FileExistsError):
        store.put(io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"), "meeting.wav")

    assert final_path.read_bytes() == b"existing recording"
    assert not (tmp_path / f".{identifier.hex}.part").exists()
    assert store.active_temporary_keys() == frozenset()


def test_store_preserves_existing_temporary_file_on_generated_name_collision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identifier = UUID("10000000-0000-4000-8000-000000000003")
    temporary_path = tmp_path / f".{identifier.hex}.part"
    tmp_path.mkdir(exist_ok=True)
    temporary_path.write_bytes(b"active recording")
    monkeypatch.setattr(audio, "uuid4", lambda: identifier)
    store = LocalAudioStore(tmp_path, StubInspector(), max_bytes=1024)

    with pytest.raises(FileExistsError):
        store.put(io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"), "meeting.wav")

    assert temporary_path.read_bytes() == b"active recording"
    assert store.active_temporary_keys() == frozenset()


def test_store_uses_plain_link_fallback_without_overwrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_link = os.link

    def fallback_link(
        source: os.PathLike[str] | str,
        target: os.PathLike[str] | str,
        *args: object,
        **kwargs: object,
    ) -> None:
        if kwargs:
            raise TypeError("follow_symlinks is unavailable")
        real_link(source, target)

    monkeypatch.setattr(audio.os, "link", fallback_link)
    store = LocalAudioStore(tmp_path, StubInspector(), max_bytes=1024)

    stored = store.put(io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"), "meeting.wav")

    assert stored.path.read_bytes() == b"RIFF\x00\x00\x00\x00WAVEdata"


def test_store_detects_post_link_swap_and_preserves_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identifier = UUID("10000000-0000-4000-8000-000000000005")
    temporary_path = tmp_path / f".{identifier.hex}.part"
    final_path = tmp_path / f"{identifier.hex}.wav"
    real_link = os.link
    real_unlink = os.unlink

    def swapping_link(
        source: os.PathLike[str] | str,
        target: os.PathLike[str] | str,
        *args: object,
        **kwargs: object,
    ) -> None:
        real_link(source, target, *args, **kwargs)
        real_unlink(source)
        Path(source).write_bytes(b"replacement owner")

    monkeypatch.setattr(audio, "uuid4", lambda: identifier)
    monkeypatch.setattr(audio.os, "link", swapping_link)
    store = LocalAudioStore(tmp_path, StubInspector(), max_bytes=1024)

    with pytest.raises(AudioValidationError, match="stored safely"):
        store.put(io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"), "meeting.wav")

    assert temporary_path.read_bytes() == b"replacement owner"
    assert final_path.read_bytes() == b"RIFF\x00\x00\x00\x00WAVEdata"


def test_store_rejects_inspector_replacement_before_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identifier = UUID("10000000-0000-4000-8000-000000000006")
    temporary_path = tmp_path / f".{identifier.hex}.part"
    final_path = tmp_path / f"{identifier.hex}.wav"

    class ReplacingInspector:
        def inspect(self, path: Path, detected_media_type: str) -> AudioMetadata:
            path.unlink()
            path.write_bytes(b"replacement owner")
            return AudioMetadata(detected_media_type, 1000, "pcm_s16le", 16000, 1)

    monkeypatch.setattr(audio, "uuid4", lambda: identifier)
    store = LocalAudioStore(tmp_path, ReplacingInspector(), max_bytes=1024)

    with pytest.raises(AudioValidationError, match="changed before publication"):
        store.put(io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"), "meeting.wav")

    assert temporary_path.read_bytes() == b"replacement owner"
    assert not final_path.exists()


def test_store_detects_post_link_swap_through_plain_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identifier = UUID("10000000-0000-4000-8000-000000000007")
    temporary_path = tmp_path / f".{identifier.hex}.part"
    final_path = tmp_path / f"{identifier.hex}.wav"
    real_link = os.link
    real_unlink = os.unlink

    def fallback_swapping_link(
        source: os.PathLike[str] | str,
        target: os.PathLike[str] | str,
        *args: object,
        **kwargs: object,
    ) -> None:
        if kwargs:
            raise TypeError("follow_symlinks is unavailable")
        real_link(source, target)
        real_unlink(source)
        Path(source).write_bytes(b"replacement owner")

    monkeypatch.setattr(audio, "uuid4", lambda: identifier)
    monkeypatch.setattr(audio.os, "link", fallback_swapping_link)
    store = LocalAudioStore(tmp_path, StubInspector(), max_bytes=1024)

    with pytest.raises(AudioValidationError, match="stored safely"):
        store.put(io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"), "meeting.wav")

    assert temporary_path.read_bytes() == b"replacement owner"
    assert final_path.read_bytes() == b"RIFF\x00\x00\x00\x00WAVEdata"


def test_store_rejects_in_place_change_during_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identifier = UUID("10000000-0000-4000-8000-000000000008")
    temporary_path = tmp_path / f".{identifier.hex}.part"
    final_path = tmp_path / f"{identifier.hex}.wav"
    real_link = os.link

    def changing_link(
        source: os.PathLike[str] | str,
        target: os.PathLike[str] | str,
        *args: object,
        **kwargs: object,
    ) -> None:
        with Path(source).open("ab") as changed:
            changed.write(b"changed")
            changed.flush()
            os.fsync(changed.fileno())
        real_link(source, target, *args, **kwargs)

    monkeypatch.setattr(audio, "uuid4", lambda: identifier)
    monkeypatch.setattr(audio.os, "link", changing_link)
    store = LocalAudioStore(tmp_path, StubInspector(), max_bytes=1024)

    with pytest.raises(AudioValidationError, match="stored safely"):
        store.put(io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"), "meeting.wav")

    assert temporary_path.read_bytes().endswith(b"changed")
    assert final_path.read_bytes().endswith(b"changed")


def test_store_closes_descriptor_and_preserves_orphan_when_initial_fstat_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identifier = UUID("10000000-0000-4000-8000-000000000009")
    temporary_path = tmp_path / f".{identifier.hex}.part"
    real_fstat = os.fstat
    captured_descriptor: int | None = None

    def failing_fstat(descriptor: int) -> os.stat_result:
        nonlocal captured_descriptor
        captured_descriptor = descriptor
        raise OSError(errno.EIO, "private operating system message")

    monkeypatch.setattr(audio, "uuid4", lambda: identifier)
    monkeypatch.setattr(audio.os, "fstat", failing_fstat)
    store = LocalAudioStore(tmp_path, StubInspector(), max_bytes=1024)

    with pytest.raises(OSError, match="private operating system message"):
        store.put(io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"), "meeting.wav")

    assert captured_descriptor is not None
    with pytest.raises(OSError, match=r".") as captured:
        real_fstat(captured_descriptor)
    assert captured.value.errno == errno.EBADF
    assert temporary_path.read_bytes() == b""
    assert store.active_temporary_keys() == frozenset()


def test_store_cleans_owned_temporary_file_when_fdopen_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identifier = UUID("10000000-0000-4000-8000-00000000000a")
    temporary_path = tmp_path / f".{identifier.hex}.part"

    def failing_fdopen(*args: object, **kwargs: object) -> None:
        raise RuntimeError("fdopen failed")

    monkeypatch.setattr(audio, "uuid4", lambda: identifier)
    monkeypatch.setattr(audio.os, "fdopen", failing_fdopen)
    store = LocalAudioStore(tmp_path, StubInspector(), max_bytes=1024)

    with pytest.raises(RuntimeError, match="fdopen failed"):
        store.put(io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"), "meeting.wav")

    assert not temporary_path.exists()
    assert store.active_temporary_keys() == frozenset()


def test_store_syncs_directory_before_and_after_temporary_unlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[Path] = []

    def record_sync(path: Path) -> None:
        calls.append(path)

    monkeypatch.setattr(audio, "_sync_directory", record_sync)
    store = LocalAudioStore(tmp_path, StubInspector(), max_bytes=1024)

    store.put(io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"), "meeting.wav")

    assert calls == [tmp_path, tmp_path]


def test_store_leaves_published_orphan_when_directory_sync_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identifier = UUID("10000000-0000-4000-8000-000000000002")
    monkeypatch.setattr(audio, "uuid4", lambda: identifier)

    def failing_sync(path: Path) -> None:
        raise OSError(errno.EIO, "private operating system message", str(path))

    monkeypatch.setattr(audio, "_sync_directory", failing_sync)
    store = LocalAudioStore(tmp_path, StubInspector(), max_bytes=1024)

    with pytest.raises(OSError, match="private operating system message"):
        store.put(io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"), "meeting.wav")

    assert (tmp_path / f"{identifier.hex}.wav").exists()
    assert not (tmp_path / f".{identifier.hex}.part").exists()
    assert store.active_temporary_keys() == frozenset()


def test_directory_sync_ignores_only_explicitly_unsupported_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unsupported_sync(_descriptor: int) -> None:
        raise OSError(errno.EINVAL, "unsupported")

    monkeypatch.setattr(audio.os, "fsync", unsupported_sync)

    audio._sync_directory(tmp_path)

    def failed_sync(_descriptor: int) -> None:
        raise OSError(errno.EIO, "storage failure")

    monkeypatch.setattr(audio.os, "fsync", failed_sync)
    with pytest.raises(OSError, match="storage failure"):
        audio._sync_directory(tmp_path)


def test_ffprobe_inspector_rejects_multiple_audio_streams(tmp_path: Path) -> None:
    payload = (
        '{"streams":[{"codec_type":"audio"},{"codec_type":"audio"}],"format":{"duration":"1"}}'
    )

    def runner(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess([], 0, payload, "")

    inspector = FFprobeAudioInspector(runner=runner)

    with pytest.raises(AudioValidationError, match="exactly one"):
        inspector.inspect(tmp_path / "audio.wav", "audio/wav")


def test_ffprobe_inspector_returns_normalized_metadata(tmp_path: Path) -> None:
    payload = (
        '{"streams":[{"codec_type":"audio","codec_name":"aac","sample_rate":"48000",'
        '"channels":2}],"format":{"duration":"1.25"}}'
    )

    def runner(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess([], 0, payload, "")

    result = FFprobeAudioInspector(runner=runner).inspect(tmp_path / "audio.m4a", "audio/mp4")

    assert result == AudioMetadata("audio/mp4", 1250, "aac", 48000, 2)
