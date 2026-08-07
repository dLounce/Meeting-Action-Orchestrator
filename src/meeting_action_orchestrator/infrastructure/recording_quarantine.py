from __future__ import annotations

import errno
import hashlib
import hmac
import os
import stat
from bisect import insort
from collections.abc import Set
from contextlib import ExitStack, suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import NoReturn
from uuid import uuid4

from meeting_action_orchestrator.application.errors import (
    PermanentRecordingCleanupError,
    RecordingCleanupError,
    RetryableRecordingCleanupError,
)
from meeting_action_orchestrator.application.recording_cleanup import (
    RecordingIdentity,
    StaleRecordingCandidate,
)
from meeting_action_orchestrator.domain.models import RecordingCleanupJob
from meeting_action_orchestrator.infrastructure.audio import (
    FINAL_RECORDING_KEY_PATTERN,
    TEMPORARY_RECORDING_KEY_PATTERN,
)

_MAX_STATE_REEVALUATIONS = 8
_MAX_SCAN_LIMIT = 1_000
_READ_SIZE = 1024 * 1024
_RETRYABLE_ERRNO_NAMES = (
    "EACCES",
    "EAGAIN",
    "EBUSY",
    "EDQUOT",
    "EINTR",
    "EIO",
    "EMFILE",
    "ENFILE",
    "ENOSPC",
    "EPERM",
    "ESTALE",
    "ETIMEDOUT",
    "ETXTBSY",
)
_RETRYABLE_ERRNOS = frozenset(
    value for name in _RETRYABLE_ERRNO_NAMES if (value := getattr(errno, name, None)) is not None
)
_CAPABILITY_ERRNOS = frozenset(
    value
    for name in ("EXDEV", "ENOTSUP", "EOPNOTSUPP")
    if (value := getattr(errno, name, None)) is not None
)
_RACE_ERRNOS = frozenset({errno.EEXIST, errno.ENOENT})
_RETRYABLE_WINDOWS_ERRORS = frozenset({5, 32, 33})
_DIRECTORY_SYNC_UNSUPPORTED_ERRNOS = frozenset(
    value
    for name in ("EINVAL", "ENOTSUP", "EOPNOTSUPP")
    if (value := getattr(errno, name, None)) is not None
)
_IS_WINDOWS = os.name == "nt"
_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class RecordingIdentityMismatchError(PermanentRecordingCleanupError):
    pass


class _StateChangedError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _FileState:
    device: int
    inode: int
    mode: int
    size_bytes: int
    modified_ns: int
    changed_ns: int

    @classmethod
    def from_stat(cls, value: os.stat_result) -> _FileState:
        return cls(
            device=value.st_dev,
            inode=value.st_ino,
            mode=value.st_mode,
            size_bytes=value.st_size,
            modified_ns=value.st_mtime_ns,
            changed_ns=value.st_ctime_ns,
        )


class LocalRecordingQuarantine:
    def __init__(self, root: Path) -> None:
        self._root = root
        self._quarantine = root / ".quarantine"

    def execute(self, job: RecordingCleanupJob) -> None:
        self._validate_identity(job.storage_key, job.expected_sha256, job.expected_size_bytes)
        self._ensure_layout()
        source_path = self._root / job.storage_key
        quarantine_path = self._quarantine / job.storage_key
        for _ in range(_MAX_STATE_REEVALUATIONS):
            try:
                source = self._verify_if_present(
                    source_path,
                    job.expected_sha256,
                    job.expected_size_bytes,
                )
                quarantined = self._verify_if_present(
                    quarantine_path,
                    job.expected_sha256,
                    job.expected_size_bytes,
                )
                if source is None and quarantined is None:
                    return
                if source is not None and quarantined is None:
                    self._link_verified(source_path, quarantine_path, source)
                    self._sync_directory(self._quarantine)
                    continue
                if (
                    source is not None
                    and quarantined is not None
                    and (
                        source.device,
                        source.inode,
                    )
                    != (
                        quarantined.device,
                        quarantined.inode,
                    )
                ):
                    raise RecordingIdentityMismatchError
                if source is not None:
                    self._unlink_verified(source_path, source)
                    self._sync_directory(self._root)
                if quarantined is not None:
                    self._unlink_verified(quarantine_path, quarantined)
                    self._sync_directory(self._quarantine)
                return
            except _StateChangedError:
                continue
        raise RetryableRecordingCleanupError

    def scan_stale_candidates(
        self,
        *,
        now: datetime,
        grace_period: timedelta,
        limit: int,
        after_storage_key: str | None = None,
        active_temporary_keys: Set[str] = frozenset(),
    ) -> tuple[StaleRecordingCandidate, ...]:
        self._validate_scan(now, grace_period, limit, after_storage_key)
        self._ensure_layout()
        cutoff = now - grace_period
        cutoff_ns = self._datetime_ns(cutoff)
        candidate_keys: list[str] = []
        candidates: dict[str, StaleRecordingCandidate] = {}
        try:
            with os.scandir(self._root) as entries:
                for entry in entries:
                    storage_key = entry.name
                    if after_storage_key is not None and storage_key <= after_storage_key:
                        continue
                    is_temporary = (
                        TEMPORARY_RECORDING_KEY_PATTERN.fullmatch(storage_key) is not None
                    )
                    if (
                        not is_temporary
                        and FINAL_RECORDING_KEY_PATTERN.fullmatch(storage_key) is None
                    ):
                        continue
                    if is_temporary and storage_key in active_temporary_keys:
                        continue
                    state = self._optional_regular_state(self._root / storage_key)
                    if state is None:
                        continue
                    if state.modified_ns >= cutoff_ns:
                        continue
                    modified_at = datetime.fromtimestamp(
                        state.modified_ns / 1_000_000_000,
                        timezone.utc,
                    )
                    candidate = StaleRecordingCandidate(
                        storage_key=storage_key,
                        size_bytes=state.size_bytes,
                        modified_at=modified_at,
                        stat_device=state.device,
                        stat_inode=state.inode,
                        stat_modified_ns=state.modified_ns,
                        stat_changed_ns=state.changed_ns,
                    )
                    insort(candidate_keys, storage_key)
                    candidates[storage_key] = candidate
                    if len(candidate_keys) > limit:
                        candidates.pop(candidate_keys.pop())
        except OSError as error:
            self._raise_storage_error(error)
        return tuple(candidates[storage_key] for storage_key in candidate_keys)

    def identify(self, candidate: StaleRecordingCandidate) -> RecordingIdentity | None:
        self._validate_storage_key(candidate.storage_key)
        self._ensure_layout()
        path = self._root / candidate.storage_key
        state = self._optional_regular_state(path)
        if state is None or not self._matches_candidate(state, candidate):
            return None
        try:
            digest, stable_state = self._hash_stable(path, state)
        except _StateChangedError:
            return None
        if not self._matches_candidate(stable_state, candidate):
            return None
        return RecordingIdentity(
            storage_key=candidate.storage_key,
            sha256=digest,
            size_bytes=stable_state.size_bytes,
        )

    def healthcheck(self) -> bool:
        try:
            self._exercise_healthcheck()
        except (OSError, RecordingCleanupError, NotImplementedError, TypeError):
            return False
        return True

    def _exercise_healthcheck(self) -> None:
        self._ensure_layout()
        probe_name = f".health-{uuid4().hex}"
        source_path = self._root / probe_name
        quarantine_path = self._quarantine / probe_name
        symlink_path = self._root / f"{probe_name}-link"
        with ExitStack() as cleanup:
            self._write_health_probe(source_path)
            cleanup.callback(self._remove_probe, source_path)
            source = self._required_regular_state(source_path)
            if source is None:
                raise RetryableRecordingCleanupError
            source_digest, source = self._hash_stable(source_path, source)
            if self._create_symlink_probe(source_path, symlink_path):
                cleanup.callback(self._remove_probe, symlink_path)
                self._verify_nofollow_rejection(symlink_path)
                self._remove_probe(symlink_path)
                self._sync_directory(self._root)
            self._link_verified(source_path, quarantine_path, source)
            cleanup.callback(self._remove_probe, quarantine_path)
            self._verify_health_link(source_path, quarantine_path, source_digest)

    def _verify_health_link(
        self,
        source_path: Path,
        quarantine_path: Path,
        source_digest: str,
    ) -> None:
        self._sync_directory(self._quarantine)
        source = self._required_regular_state(source_path)
        quarantined = self._required_regular_state(quarantine_path)
        if source is None or quarantined is None:
            raise RetryableRecordingCleanupError
        if (source.device, source.inode) != (quarantined.device, quarantined.inode):
            raise PermanentRecordingCleanupError
        quarantine_digest, quarantined = self._hash_stable(quarantine_path, quarantined)
        if not hmac.compare_digest(source_digest, quarantine_digest):
            raise PermanentRecordingCleanupError
        self._unlink_verified(source_path, source)
        self._sync_directory(self._root)
        quarantined = self._required_regular_state(quarantine_path)
        if quarantined is None:
            raise RetryableRecordingCleanupError
        self._unlink_verified(quarantine_path, quarantined)
        self._sync_directory(self._quarantine)

    def _write_health_probe(self, path: Path) -> None:
        created = False
        try:
            descriptor = os.open(
                path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            created = True
            try:
                os.write(descriptor, b"recording-storage-readiness")
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except BaseException:
            if created:
                self._remove_probe(path)
            raise

    @staticmethod
    def _remove_probe(path: Path) -> None:
        with suppress(OSError):
            os.unlink(path)

    def _ensure_layout(self) -> None:
        root = self._ensure_directory(self._root)
        quarantine = self._ensure_directory(self._quarantine)
        if root.st_dev != quarantine.st_dev:
            raise PermanentRecordingCleanupError

    def _ensure_directory(self, path: Path) -> os.stat_result:
        try:
            path.mkdir(parents=True, exist_ok=True, mode=0o700)
        except FileExistsError:
            pass
        except OSError as error:
            self._raise_storage_error(error)
        try:
            value = os.lstat(path)
        except OSError as error:
            self._raise_storage_error(error)
        if stat.S_ISLNK(value.st_mode) or not stat.S_ISDIR(value.st_mode):
            raise PermanentRecordingCleanupError
        expected_identity = (value.st_dev, value.st_ino)
        self._restrict_directory(path, value)
        try:
            value = os.lstat(path)
        except OSError as error:
            self._raise_storage_error(error)
        if (
            stat.S_ISLNK(value.st_mode)
            or not stat.S_ISDIR(value.st_mode)
            or (value.st_dev, value.st_ino) != expected_identity
        ):
            raise PermanentRecordingCleanupError
        return value

    def _verify_if_present(
        self,
        path: Path,
        expected_sha256: str,
        expected_size_bytes: int,
    ) -> _FileState | None:
        state = self._required_regular_state(path)
        if state is None:
            return None
        if state.size_bytes != expected_size_bytes:
            raise RecordingIdentityMismatchError
        digest, stable_state = self._hash_stable(path, state)
        if stable_state.size_bytes != expected_size_bytes or not hmac.compare_digest(
            digest,
            expected_sha256,
        ):
            raise RecordingIdentityMismatchError
        return stable_state

    def _required_regular_state(self, path: Path) -> _FileState | None:
        try:
            value = os.lstat(path)
        except OSError as error:
            if self._is_missing(error):
                return None
            self._raise_storage_error(error)
        if stat.S_ISLNK(value.st_mode) or not stat.S_ISREG(value.st_mode):
            raise PermanentRecordingCleanupError
        return _FileState.from_stat(value)

    def _optional_regular_state(self, path: Path) -> _FileState | None:
        try:
            value = os.lstat(path)
        except OSError as error:
            if self._is_missing(error):
                return None
            self._raise_storage_error(error)
        if stat.S_ISLNK(value.st_mode) or not stat.S_ISREG(value.st_mode):
            return None
        return _FileState.from_stat(value)

    def _hash_stable(self, path: Path, initial: _FileState) -> tuple[str, _FileState]:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as error:
            if self._is_path_race(error):
                raise _StateChangedError from None
            self._raise_storage_error(error)
        try:
            before = _FileState.from_stat(os.fstat(descriptor))
            if not stat.S_ISREG(before.mode):
                raise PermanentRecordingCleanupError
            if before != initial:
                raise _StateChangedError
            digest = hashlib.sha256()
            while chunk := os.read(descriptor, _READ_SIZE):
                digest.update(chunk)
            after = _FileState.from_stat(os.fstat(descriptor))
            path_after = self._optional_regular_state(path)
        except OSError as error:
            self._raise_storage_error(error)
        finally:
            with suppress(OSError):
                os.close(descriptor)
        if after != before or path_after != after:
            raise _StateChangedError
        return digest.hexdigest(), after

    def _link_verified(self, source: Path, target: Path, expected: _FileState) -> None:
        current = self._required_regular_state(source)
        if current != expected:
            raise _StateChangedError
        try:
            os.link(source, target, follow_symlinks=False)
        except (NotImplementedError, TypeError):
            self._link_with_verified_fallback(source, target)
        except OSError as error:
            if error.errno in _CAPABILITY_ERRNOS:
                self._link_with_verified_fallback(source, target)
            elif error.errno in _RACE_ERRNOS:
                raise _StateChangedError from None
            else:
                self._raise_storage_error(error)
        source_after = self._required_regular_state(source)
        target_after = self._required_regular_state(target)
        if source_after is None or target_after is None:
            raise _StateChangedError
        expected_identity = (expected.device, expected.inode)
        if (source_after.device, source_after.inode) != expected_identity or (
            target_after.device,
            target_after.inode,
        ) != expected_identity:
            raise RecordingIdentityMismatchError

    def _link_with_verified_fallback(self, source: Path, target: Path) -> None:
        try:
            os.link(source, target)
        except (NotImplementedError, TypeError):
            raise PermanentRecordingCleanupError from None
        except OSError as error:
            if error.errno in _RACE_ERRNOS:
                raise _StateChangedError from None
            self._raise_storage_error(error)

    def _unlink_verified(self, path: Path, expected: _FileState) -> None:
        current = self._required_regular_state(path)
        if current != expected:
            raise _StateChangedError
        try:
            os.unlink(path)
        except OSError as error:
            if self._is_missing(error):
                raise _StateChangedError from None
            self._raise_storage_error(error)

    def _sync_directory(self, path: Path) -> None:
        directory_flag = getattr(os, "O_DIRECTORY", 0)
        if directory_flag == 0:
            return
        try:
            descriptor = os.open(path, os.O_RDONLY | directory_flag)
        except OSError as error:
            if error.errno in _DIRECTORY_SYNC_UNSUPPORTED_ERRNOS:
                return
            self._raise_storage_error(error)
        try:
            os.fsync(descriptor)
        except OSError as error:
            if error.errno in _DIRECTORY_SYNC_UNSUPPORTED_ERRNOS:
                return
            self._raise_storage_error(error)
        finally:
            with suppress(OSError):
                os.close(descriptor)

    def _verify_nofollow_rejection(self, path: Path) -> None:
        nofollow_flag = getattr(os, "O_NOFOLLOW", 0)
        if nofollow_flag == 0:
            return
        descriptor: int | None = None
        try:
            descriptor = os.open(path, os.O_RDONLY | nofollow_flag)
        except OSError as error:
            if error.errno == getattr(errno, "ELOOP", None):
                return
            raise PermanentRecordingCleanupError from None
        finally:
            if descriptor is not None:
                with suppress(OSError):
                    os.close(descriptor)
        raise PermanentRecordingCleanupError

    def _create_symlink_probe(self, source: Path, target: Path) -> bool:
        if getattr(os, "O_NOFOLLOW", 0) == 0:
            return False
        try:
            os.symlink(source.name, target)
        except NotImplementedError:
            return False
        except OSError as error:
            unsupported = {
                getattr(errno, "EACCES", None),
                getattr(errno, "ENOSYS", None),
                getattr(errno, "EPERM", None),
            }
            if error.errno in unsupported or getattr(error, "winerror", None) == 1314:
                return False
            self._raise_storage_error(error)
        return True

    def _restrict_directory(self, path: Path, initial: os.stat_result) -> None:
        directory_flag = getattr(os, "O_DIRECTORY", 0)
        nofollow_flag = getattr(os, "O_NOFOLLOW", 0)
        flags = os.O_RDONLY | directory_flag | nofollow_flag
        try:
            descriptor = os.open(path, flags)
        except OSError as error:
            if not _IS_WINDOWS:
                self._raise_storage_error(error)
            self._restrict_directory_path(path, initial)
            return
        try:
            self._restrict_open_directory(descriptor, initial)
        finally:
            with suppress(OSError):
                os.close(descriptor)

    def _restrict_open_directory(self, descriptor: int, initial: os.stat_result) -> None:
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISDIR(opened.st_mode) or (opened.st_dev, opened.st_ino) != (
                initial.st_dev,
                initial.st_ino,
            ):
                raise PermanentRecordingCleanupError
            if hasattr(os, "fchmod"):
                os.fchmod(descriptor, 0o700)
            elif not _IS_WINDOWS:
                raise PermanentRecordingCleanupError
            restricted = os.fstat(descriptor)
            if not _IS_WINDOWS and restricted.st_mode & 0o077:
                raise PermanentRecordingCleanupError
        except OSError as error:
            if not _IS_WINDOWS:
                self._raise_storage_error(error)

    def _restrict_directory_path(self, path: Path, initial: os.stat_result) -> None:
        try:
            os.chmod(path, 0o700)
        except OSError:
            if not _IS_WINDOWS:
                raise RetryableRecordingCleanupError from None
        try:
            value = os.lstat(path)
        except OSError as error:
            self._raise_storage_error(error)
        if (
            not stat.S_ISDIR(value.st_mode)
            or (value.st_dev, value.st_ino) != (initial.st_dev, initial.st_ino)
            or (not _IS_WINDOWS and value.st_mode & 0o077)
        ):
            raise PermanentRecordingCleanupError

    def _validate_identity(
        self,
        storage_key: str,
        expected_sha256: str,
        expected_size_bytes: int,
    ) -> None:
        self._validate_storage_key(storage_key)
        if (
            len(expected_sha256) != 64
            or any(character not in "0123456789abcdef" for character in expected_sha256)
            or expected_size_bytes < 0
        ):
            raise PermanentRecordingCleanupError

    def _validate_storage_key(self, storage_key: str) -> None:
        if (
            FINAL_RECORDING_KEY_PATTERN.fullmatch(storage_key) is None
            and TEMPORARY_RECORDING_KEY_PATTERN.fullmatch(storage_key) is None
        ):
            raise PermanentRecordingCleanupError

    def _validate_scan(
        self,
        now: datetime,
        grace_period: timedelta,
        limit: int,
        after_storage_key: str | None,
    ) -> None:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("Scan time must include a UTC offset")
        if grace_period <= timedelta(0):
            raise ValueError("Scan grace period must be positive")
        if not 1 <= limit <= _MAX_SCAN_LIMIT:
            raise ValueError("Scan limit must be between one and one thousand")
        if after_storage_key is not None:
            self._validate_storage_key(after_storage_key)

    def _raise_storage_error(self, error: OSError) -> NoReturn:
        if error.errno in _CAPABILITY_ERRNOS:
            raise PermanentRecordingCleanupError from None
        if (
            error.errno in _RETRYABLE_ERRNOS
            or getattr(
                error,
                "winerror",
                None,
            )
            in _RETRYABLE_WINDOWS_ERRORS
        ):
            raise RetryableRecordingCleanupError from None
        raise RetryableRecordingCleanupError from None

    @staticmethod
    def _is_missing(error: OSError) -> bool:
        return error.errno == errno.ENOENT or getattr(error, "winerror", None) in {2, 3}

    @classmethod
    def _is_path_race(cls, error: OSError) -> bool:
        return cls._is_missing(error) or error.errno == getattr(errno, "ELOOP", None)

    @staticmethod
    def _matches_candidate(state: _FileState, candidate: StaleRecordingCandidate) -> bool:
        return (
            state.device == candidate.stat_device
            and state.inode == candidate.stat_inode
            and state.size_bytes == candidate.size_bytes
            and state.modified_ns == candidate.stat_modified_ns
            and state.changed_ns == candidate.stat_changed_ns
        )

    @staticmethod
    def _datetime_ns(value: datetime) -> int:
        elapsed = value.astimezone(timezone.utc) - _UNIX_EPOCH
        seconds = elapsed.days * 86_400 + elapsed.seconds
        return (seconds * 1_000_000 + elapsed.microseconds) * 1_000
