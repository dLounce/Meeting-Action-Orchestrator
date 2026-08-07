from __future__ import annotations

import io
import subprocess
from pathlib import Path

import pytest

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
    assert stored.path.read_bytes() == b"RIFF\x00\x00\x00\x00WAVEdata"
    assert len(stored.sha256) == 64


def test_store_rejects_oversized_content_without_leaving_partial_file(tmp_path: Path) -> None:
    store = LocalAudioStore(tmp_path, StubInspector(), max_bytes=8)

    with pytest.raises(AudioValidationError, match="upload limit"):
        store.put(io.BytesIO(b"RIFF\x00\x00\x00\x00WAVE"), "meeting.wav")

    assert list(tmp_path.iterdir()) == []


def test_store_deduplicates_identical_content(tmp_path: Path) -> None:
    store = LocalAudioStore(tmp_path, StubInspector(), max_bytes=1024)
    content = b"RIFF\x00\x00\x00\x00WAVEdata"

    first = store.put(io.BytesIO(content), "first.wav")
    second = store.put(io.BytesIO(content), "second.wav")

    assert first.storage_key == second.storage_key
    assert len(list(tmp_path.iterdir())) == 1


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
