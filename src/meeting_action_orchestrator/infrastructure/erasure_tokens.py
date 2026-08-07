from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from types import MappingProxyType
from uuid import UUID

from meeting_action_orchestrator.domain.errors import DomainValueCode, InvalidDomainValueError
from meeting_action_orchestrator.domain.hashing import canonical_json
from meeting_action_orchestrator.domain.models import (
    ERASURE_TOKEN_VERSION,
    ERASURE_VERIFIER_VERSION,
    ErasureKeyVerifier,
    ErasureToken,
    ErasureTokenIdentity,
)


class ErasureTokenConfigurationError(ValueError):
    def __init__(self) -> None:
        super().__init__("Erasure HMAC key configuration is invalid")


class ErasureKeyVerificationError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Erasure HMAC key verification failed")


class ErasureTokenKeyring:
    __slots__ = ("_active_key_id", "_key_ids", "_keys")

    def __init__(self, active_key_id: str, keys: Mapping[str, bytes]) -> None:
        normalized = dict(keys)
        if not 1 <= len(normalized) <= 8:
            raise ErasureTokenConfigurationError
        if active_key_id not in normalized:
            raise ErasureTokenConfigurationError
        for key_id, secret in normalized.items():
            if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", key_id) is None:
                raise ErasureTokenConfigurationError
            if not isinstance(secret, bytes) or not 32 <= len(secret) <= 64:
                raise ErasureTokenConfigurationError
        if len(set(normalized.values())) != len(normalized):
            raise ErasureTokenConfigurationError
        self._active_key_id = active_key_id
        self._key_ids = (
            active_key_id,
            *sorted(key_id for key_id in normalized if key_id != active_key_id),
        )
        self._keys = MappingProxyType(
            {key_id: bytes(secret) for key_id, secret in normalized.items()}
        )

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(active_key_id={self._active_key_id!r}, "
            f"key_count={len(self._keys)})"
        )

    @classmethod
    def from_encoded(cls, active_key_id: str, encoded_keys: str) -> ErasureTokenKeyring:
        keys = _parse_encoded_keys(encoded_keys)
        if keys is None:
            raise ErasureTokenConfigurationError
        return cls(active_key_id, keys)

    @property
    def active_key_id(self) -> str:
        return self._active_key_id

    @property
    def key_ids(self) -> tuple[str, ...]:
        return self._key_ids

    def meeting_token(self, meeting_id: UUID) -> ErasureToken:
        return self._active_token("meeting-id", str(meeting_id))

    def meeting_tokens(self, meeting_id: UUID) -> tuple[ErasureToken, ...]:
        return self._all_tokens("meeting-id", str(meeting_id))

    def ingest_key_token(self, ingest_key: str) -> ErasureToken:
        return self._active_token("ingest-key", _validated_text(ingest_key))

    def ingest_key_tokens(self, ingest_key: str) -> tuple[ErasureToken, ...]:
        return self._all_tokens("ingest-key", _validated_text(ingest_key))

    def request_key_token(self, request_key: str) -> ErasureToken:
        return self._active_token("request-key", _validated_text(request_key))

    def request_key_tokens(self, request_key: str) -> tuple[ErasureToken, ...]:
        return self._all_tokens("request-key", _validated_text(request_key))

    def actor_token(self, actor_id: str) -> ErasureToken:
        return self._active_token("actor-id", _validated_text(actor_id))

    def actor_tokens(self, actor_id: str) -> tuple[ErasureToken, ...]:
        return self._all_tokens("actor-id", _validated_text(actor_id))

    def erasure_job_token(self, erasure_job_id: UUID) -> ErasureToken:
        return self._active_token("erasure-job-id", str(erasure_job_id))

    def erasure_job_tokens(self, erasure_job_id: UUID) -> tuple[ErasureToken, ...]:
        return self._all_tokens("erasure-job-id", str(erasure_job_id))

    def verifier(self, key_id: str, created_at: datetime) -> ErasureKeyVerifier:
        if key_id not in self._keys:
            raise ErasureTokenConfigurationError
        return ErasureKeyVerifier(
            key_id=key_id,
            verifier_version=ERASURE_VERIFIER_VERSION,
            verifier_digest=self._verifier_digest(key_id),
            created_at=created_at,
        )

    def verifiers(self, created_at: datetime) -> tuple[ErasureKeyVerifier, ...]:
        return tuple(self.verifier(key_id, created_at) for key_id in self._key_ids)

    def validate_verifiers(
        self,
        persisted: Sequence[ErasureKeyVerifier],
        referenced_tokens: Sequence[ErasureTokenIdentity] = (),
    ) -> None:
        by_id: dict[str, ErasureKeyVerifier] = {}
        valid = True
        referenced_key_ids: set[str] = set()
        for token in referenced_tokens:
            referenced_key_ids.add(token.key_id)
            if token.token_version != ERASURE_TOKEN_VERSION or token.key_id not in self._keys:
                valid = False
        for verifier in persisted:
            if verifier.key_id in by_id:
                valid = False
            by_id[verifier.key_id] = verifier
            secret = self._keys.get(verifier.key_id)
            if secret is None:
                if verifier.key_id in referenced_key_ids:
                    valid = False
                continue
            if verifier.verifier_version != ERASURE_VERIFIER_VERSION:
                valid = False
                continue
            expected = self._verifier_digest(verifier.key_id)
            if not hmac.compare_digest(verifier.verifier_digest, expected):
                valid = False
        if not set(self._keys) <= set(by_id):
            valid = False
        for token in referenced_tokens:
            if token.key_id not in by_id:
                valid = False
        if not valid:
            raise ErasureKeyVerificationError

    def _active_token(self, purpose: str, value: str) -> ErasureToken:
        return self._token(self._active_key_id, purpose, value)

    def _all_tokens(self, purpose: str, value: str) -> tuple[ErasureToken, ...]:
        return tuple(self._token(key_id, purpose, value) for key_id in self._key_ids)

    def _token(self, key_id: str, purpose: str, value: str) -> ErasureToken:
        payload = canonical_json(
            {
                "schema": "meeting-erasure-token/v1",
                "purpose": purpose,
                "value": value,
            }
        ).encode("utf-8")
        digest = hmac.new(self._keys[key_id], payload, hashlib.sha256).hexdigest()
        return ErasureToken(
            token_version=ERASURE_TOKEN_VERSION,
            key_id=key_id,
            digest=digest,
        )

    def _verifier_digest(self, key_id: str) -> str:
        payload = canonical_json(
            {
                "schema": "meeting-erasure-key-verifier/v1",
                "key_id": key_id,
            }
        ).encode("utf-8")
        return hmac.new(self._keys[key_id], payload, hashlib.sha256).hexdigest()


def _parse_encoded_keys(encoded_keys: str) -> dict[str, bytes] | None:
    try:
        if len(encoded_keys) > 4_096:
            return None
        payload = json.loads(encoded_keys, object_pairs_hook=_unique_object)
        if not isinstance(payload, dict):
            return None
        keys = {
            key_id: _decode_secret(encoded)
            for key_id, encoded in payload.items()
            if isinstance(key_id, str) and isinstance(encoded, str)
        }
        return keys if len(keys) == len(payload) else None
    except (ErasureTokenConfigurationError, TypeError, ValueError, binascii.Error):
        return None


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ErasureTokenConfigurationError
        result[key] = value
    return result


def _decode_secret(encoded: str) -> bytes:
    if re.fullmatch(r"[A-Za-z0-9_-]+={0,2}", encoded) is None:
        raise ErasureTokenConfigurationError
    unpadded = encoded.rstrip("=")
    if "=" in unpadded:
        raise ErasureTokenConfigurationError
    padded = unpadded + "=" * (-len(unpadded) % 4)
    return base64.b64decode(padded.encode("ascii"), altchars=b"-_", validate=True)


def _validated_text(value: str) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 200 or value != value.strip():
        raise InvalidDomainValueError(DomainValueCode.ERASURE_IDENTITY_INPUT)
    return value
