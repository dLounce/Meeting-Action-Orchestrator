from __future__ import annotations

import errno
import hashlib
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest

from meeting_action_orchestrator.application.errors import (
    PermanentRecordingCleanupError,
    RetryableRecordingCleanupError,
)
from meeting_action_orchestrator.domain.enums import RecordingCleanupReason
from meeting_action_orchestrator.domain.models import RecordingCleanupJob
from meeting_action_orchestrator.infrastructure import recording_quarantine
from meeting_action_orchestrator.infrastructure.recording_quarantine import (
    LocalRecordingQuarantine,
    RecordingIdentityMismatchError,
)

NOW = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)
CONTENT = b"verified recording bytes"


class WindowsSharingViolationError(OSError):
    winerror: int


def storage_key(character: str, suffix: str = ".wav") -> str:
    return character * 32 + suffix


def cleanup_job(
    key: str = storage_key("1"),
    content: bytes = CONTENT,
) -> RecordingCleanupJob:
    return RecordingCleanupJob(
        id=UUID("10000000-0000-4000-8000-000000000001"),
        storage_key=key,
        expected_sha256=hashlib.sha256(content).hexdigest(),
        expected_size_bytes=len(content),
        reason=RecordingCleanupReason.ABANDONED_INGEST,
        max_attempts=5,
        created_at=NOW,
        updated_at=NOW,
    )


def write_source(root: Path, job: RecordingCleanupJob, content: bytes = CONTENT) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    path = root / job.storage_key
    path.write_bytes(content)
    return path


def test_source_is_quarantined_verified_and_removed(tmp_path: Path) -> None:
    root = tmp_path / "uploads"
    job = cleanup_job()
    source = write_source(root, job)
    quarantine = root / ".quarantine" / job.storage_key

    LocalRecordingQuarantine(root).execute(job)

    assert not source.exists()
    assert not quarantine.exists()
    assert root.is_dir()
    assert (root / ".quarantine").is_dir()


def test_quarantine_only_and_missing_states_are_idempotent(tmp_path: Path) -> None:
    root = tmp_path / "uploads"
    job = cleanup_job()
    quarantined = root / ".quarantine" / job.storage_key
    quarantined.parent.mkdir(parents=True)
    quarantined.write_bytes(CONTENT)
    executor = LocalRecordingQuarantine(root)

    executor.execute(job)
    executor.execute(job)

    assert not quarantined.exists()


def test_identity_mismatch_preserves_source_and_hides_metadata(tmp_path: Path) -> None:
    root = tmp_path / "uploads"
    job = cleanup_job()
    source = write_source(root, job, b"different recording")

    with pytest.raises(RecordingIdentityMismatchError) as captured:
        LocalRecordingQuarantine(root).execute(job)

    assert source.read_bytes() == b"different recording"
    assert job.storage_key not in str(captured.value)
    assert job.expected_sha256 not in str(captured.value)
    assert str(root) not in str(captured.value)


def test_both_paths_with_same_content_but_different_inodes_are_preserved(tmp_path: Path) -> None:
    root = tmp_path / "uploads"
    job = cleanup_job()
    source = write_source(root, job)
    quarantined = root / ".quarantine" / job.storage_key
    quarantined.parent.mkdir(parents=True)
    quarantined.write_bytes(CONTENT)

    with pytest.raises(RecordingIdentityMismatchError):
        LocalRecordingQuarantine(root).execute(job)

    assert source.read_bytes() == CONTENT
    assert quarantined.read_bytes() == CONTENT
    assert source.stat().st_ino != quarantined.stat().st_ino


@pytest.mark.parametrize("unsafe_type", ["symlink", "directory"])
def test_non_regular_sources_are_rejected_without_mutation(
    tmp_path: Path,
    unsafe_type: str,
) -> None:
    root = tmp_path / "uploads"
    root.mkdir()
    job = cleanup_job()
    source = root / job.storage_key
    target = root / "target"
    if unsafe_type == "symlink":
        target.write_bytes(CONTENT)
        source.symlink_to(target.name)
    else:
        source.mkdir()

    with pytest.raises(PermanentRecordingCleanupError):
        LocalRecordingQuarantine(root).execute(job)

    assert source.exists()
    if unsafe_type == "symlink":
        assert source.is_symlink()
        assert target.read_bytes() == CONTENT


def test_retry_after_link_resumes_from_both_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "uploads"
    job = cleanup_job()
    source = write_source(root, job)
    quarantined = root / ".quarantine" / job.storage_key
    real_unlink = os.unlink

    def busy_unlink(path: os.PathLike[str] | str) -> None:
        if Path(path) == source and quarantined.exists():
            raise OSError(errno.EBUSY, "private operating system message", str(path))
        real_unlink(path)

    monkeypatch.setattr(recording_quarantine.os, "unlink", busy_unlink)
    executor = LocalRecordingQuarantine(root)

    with pytest.raises(RetryableRecordingCleanupError) as captured:
        executor.execute(job)

    assert source.exists()
    assert quarantined.exists()
    assert source.stat().st_ino == quarantined.stat().st_ino
    assert "private operating system message" not in str(captured.value)
    assert str(source) not in str(captured.value)

    monkeypatch.setattr(recording_quarantine.os, "unlink", real_unlink)
    executor.execute(job)

    assert not source.exists()
    assert not quarantined.exists()


def test_quarantine_only_after_source_unlink_resumes_safely(tmp_path: Path) -> None:
    root = tmp_path / "uploads"
    job = cleanup_job()
    source = write_source(root, job)
    quarantined = root / ".quarantine" / job.storage_key
    quarantined.parent.mkdir(parents=True)
    os.link(source, quarantined)
    source.unlink()

    LocalRecordingQuarantine(root).execute(job)

    assert not quarantined.exists()


def test_post_link_source_swap_is_detected_before_removal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "uploads"
    job = cleanup_job()
    source = write_source(root, job)
    quarantined = root / ".quarantine" / job.storage_key
    real_link = os.link
    real_unlink = os.unlink

    def swapping_link(
        source_path: os.PathLike[str] | str,
        target_path: os.PathLike[str] | str,
        *args: object,
        **kwargs: object,
    ) -> None:
        real_link(source_path, target_path, *args, **kwargs)
        real_unlink(source_path)
        Path(source_path).write_bytes(CONTENT)

    monkeypatch.setattr(recording_quarantine.os, "link", swapping_link)

    with pytest.raises(RecordingIdentityMismatchError):
        LocalRecordingQuarantine(root).execute(job)

    assert source.read_bytes() == CONTENT
    assert quarantined.read_bytes() == CONTENT
    assert source.stat().st_ino != quarantined.stat().st_ino


def test_plain_link_fallback_keeps_identity_guards(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "uploads"
    job = cleanup_job()
    source = write_source(root, job)
    real_link = os.link

    def fallback_link(
        source_path: os.PathLike[str] | str,
        target_path: os.PathLike[str] | str,
        *args: object,
        **kwargs: object,
    ) -> None:
        if kwargs:
            raise TypeError("follow_symlinks is unavailable")
        real_link(source_path, target_path)

    monkeypatch.setattr(recording_quarantine.os, "O_NOFOLLOW", 0)
    monkeypatch.setattr(recording_quarantine.os, "O_DIRECTORY", 0)
    monkeypatch.setattr(recording_quarantine.os, "link", fallback_link)

    LocalRecordingQuarantine(root).execute(job)

    assert not source.exists()


def test_plain_link_fallback_detects_post_link_source_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "uploads"
    job = cleanup_job()
    source = write_source(root, job)
    quarantined = root / ".quarantine" / job.storage_key
    real_link = os.link
    real_unlink = os.unlink

    def fallback_swapping_link(
        source_path: os.PathLike[str] | str,
        target_path: os.PathLike[str] | str,
        *args: object,
        **kwargs: object,
    ) -> None:
        if kwargs:
            raise TypeError("follow_symlinks is unavailable")
        real_link(source_path, target_path)
        real_unlink(source_path)
        Path(source_path).write_bytes(CONTENT)

    monkeypatch.setattr(recording_quarantine.os, "link", fallback_swapping_link)

    with pytest.raises(RecordingIdentityMismatchError):
        LocalRecordingQuarantine(root).execute(job)

    assert source.read_bytes() == CONTENT
    assert quarantined.read_bytes() == CONTENT
    assert source.stat().st_ino != quarantined.stat().st_ino


def test_unsupported_plain_link_is_a_permanent_capability_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "uploads"
    job = cleanup_job()
    source = write_source(root, job)

    def unsupported_link(*args: object, **kwargs: object) -> None:
        raise TypeError("hard links are unavailable")

    monkeypatch.setattr(recording_quarantine.os, "link", unsupported_link)

    with pytest.raises(PermanentRecordingCleanupError):
        LocalRecordingQuarantine(root).execute(job)

    assert source.read_bytes() == CONTENT


@pytest.mark.parametrize(
    "error_number",
    [
        errno.EACCES,
        errno.EAGAIN,
        errno.EBUSY,
        errno.EINTR,
        errno.EIO,
        errno.EMFILE,
        errno.ENFILE,
        errno.ENOSPC,
        errno.EPERM,
    ],
)
def test_operational_link_errors_are_retryable_and_private(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    error_number: int,
) -> None:
    root = tmp_path / "uploads"
    job = cleanup_job()
    source = write_source(root, job)

    def failing_link(*args: object, **kwargs: object) -> None:
        raise OSError(error_number, "private operating system message", str(source))

    monkeypatch.setattr(recording_quarantine.os, "link", failing_link)

    with pytest.raises(RetryableRecordingCleanupError) as captured:
        LocalRecordingQuarantine(root).execute(job)

    assert source.read_bytes() == CONTENT
    assert "private operating system message" not in str(captured.value)
    assert str(source) not in str(captured.value)


def test_windows_sharing_violation_is_retryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "uploads"
    job = cleanup_job()
    source = write_source(root, job)

    def failing_link(*args: object, **kwargs: object) -> None:
        error = WindowsSharingViolationError(
            errno.EINVAL,
            "private operating system message",
            str(source),
        )
        error.winerror = 32
        raise error

    monkeypatch.setattr(recording_quarantine.os, "link", failing_link)

    with pytest.raises(RetryableRecordingCleanupError):
        LocalRecordingQuarantine(root).execute(job)


def test_scanner_is_bounded_sorted_cursor_paged_and_conservative(tmp_path: Path) -> None:
    root = tmp_path / "uploads"
    root.mkdir()
    active_part = "." + "0" * 32 + ".part"
    stale_part = "." + "1" * 32 + ".part"
    first_final = storage_key("2")
    second_final = storage_key("3", ".mp3")
    fresh_final = storage_key("4", ".m4a")
    directory_key = storage_key("5")
    symlink_key = storage_key("6")
    old_timestamp = (NOW - timedelta(hours=2)).timestamp()
    for key in (active_part, stale_part, first_final, second_final):
        path = root / key
        path.write_bytes(key.encode())
        os.utime(path, (old_timestamp, old_timestamp))
    (root / fresh_final).write_bytes(b"fresh")
    (root / directory_key).mkdir()
    (root / "target").write_bytes(b"target")
    (root / symlink_key).symlink_to("target")
    (root / "legacy.wav").write_bytes(b"legacy")
    quarantine = root / ".quarantine"
    quarantine.mkdir()
    (quarantine / storage_key("7")).write_bytes(b"quarantined")
    scanner = LocalRecordingQuarantine(root)

    first_page = scanner.scan_stale_candidates(
        now=NOW,
        grace_period=timedelta(hours=1),
        limit=2,
        active_temporary_keys={active_part},
    )
    second_page = scanner.scan_stale_candidates(
        now=NOW,
        grace_period=timedelta(hours=1),
        limit=2,
        after_storage_key=first_page[-1].storage_key,
        active_temporary_keys={active_part},
    )

    assert [candidate.storage_key for candidate in first_page] == [stale_part, first_final]
    assert [candidate.storage_key for candidate in second_page] == [second_final]
    identity = scanner.identify(first_page[0])
    assert identity is not None
    assert identity.storage_key == stale_part
    assert identity.size_bytes == len(stale_part.encode())
    assert identity.sha256 == hashlib.sha256(stale_part.encode()).hexdigest()


def test_identify_rejects_path_swap_after_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "uploads"
    key = storage_key("8")
    path = root / key
    root.mkdir()
    path.write_bytes(CONTENT)
    old_timestamp = (NOW - timedelta(hours=2)).timestamp()
    os.utime(path, (old_timestamp, old_timestamp))
    scanner = LocalRecordingQuarantine(root)
    candidate = scanner.scan_stale_candidates(
        now=NOW,
        grace_period=timedelta(hours=1),
        limit=1,
    )[0]
    real_read = os.read
    real_unlink = os.unlink
    swapped = False

    def swapping_read(descriptor: int, size: int) -> bytes:
        nonlocal swapped
        chunk = real_read(descriptor, size)
        if not chunk and not swapped:
            swapped = True
            real_unlink(path)
            path.write_bytes(CONTENT)
        return chunk

    monkeypatch.setattr(recording_quarantine.os, "read", swapping_read)

    assert scanner.identify(candidate) is None
    assert path.read_bytes() == CONTENT


def test_healthcheck_exercises_link_and_leaves_no_probe_files(tmp_path: Path) -> None:
    root = tmp_path / "uploads"
    executor = LocalRecordingQuarantine(root)

    assert executor.healthcheck() is True
    assert [path for path in root.rglob("*") if path.name.startswith(".health-")] == []


def test_healthcheck_fails_when_hard_links_cross_devices(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "uploads"

    def cross_device_link(*args: object, **kwargs: object) -> None:
        raise OSError(errno.EXDEV, "private operating system message")

    monkeypatch.setattr(recording_quarantine.os, "link", cross_device_link)

    assert LocalRecordingQuarantine(root).healthcheck() is False


def test_healthcheck_does_not_remove_a_colliding_probe_name(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "uploads"
    root.mkdir()
    identifier = UUID("10000000-0000-4000-8000-000000000004")
    existing = root / f".health-{identifier.hex}"
    existing.write_bytes(b"existing probe owner")
    monkeypatch.setattr(recording_quarantine, "uuid4", lambda: identifier)

    assert LocalRecordingQuarantine(root).healthcheck() is False
    assert existing.read_bytes() == b"existing probe owner"


def test_posix_mode_retention_fails_readiness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "uploads"
    root.mkdir(mode=0o755)
    root.chmod(0o755)

    def retain_mode(*_args: object) -> None:
        return None

    monkeypatch.setattr(recording_quarantine.os, "fchmod", retain_mode)

    assert LocalRecordingQuarantine(root).healthcheck() is False


def test_windows_fchmod_absence_uses_best_effort_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "uploads"
    job = cleanup_job()
    source = write_source(root, job)
    monkeypatch.setattr(recording_quarantine, "_IS_WINDOWS", True)
    monkeypatch.delattr(recording_quarantine.os, "fchmod")

    LocalRecordingQuarantine(root).execute(job)

    assert not source.exists()
