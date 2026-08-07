import base64
import json
from datetime import datetime, timezone
from uuid import UUID

import pytest

from meeting_action_orchestrator.config import (
    MissingErasureHMACConfigurationError,
    Settings,
)
from meeting_action_orchestrator.domain.models import ErasureTokenIdentity
from meeting_action_orchestrator.infrastructure.erasure_tokens import (
    ErasureKeyVerificationError,
    ErasureTokenConfigurationError,
    ErasureTokenKeyring,
)
from meeting_action_orchestrator.observability import REDACTED, sanitize

NOW = datetime(2026, 8, 7, 9, 0, tzinfo=timezone.utc)
MEETING_ID = UUID("10000000-0000-4000-8000-000000000001")
JOB_ID = UUID("20000000-0000-4000-8000-000000000001")


def encoded(secret: bytes) -> str:
    return base64.urlsafe_b64encode(secret).decode("ascii").rstrip("=")


def test_token_codec_has_stable_domain_separated_vectors() -> None:
    keyring = ErasureTokenKeyring("current", {"current": bytes(range(32))})

    assert keyring.meeting_token(MEETING_ID).digest == (
        "5f2027786d7b0c0c36d2a7a877bd56951cc6ef9f516dfe0bd6ee3b9a81229707"
    )
    assert keyring.ingest_key_token("ingest-1").digest == (
        "917c5747839365bbdbf8f591fe0fc52e0631775338bfbee34f95401ab0f935bf"
    )
    assert keyring.request_key_token("request-1").digest == (
        "29545139d9f324459e585a3fbbfb6dc18b532cf9286bd942bbd8e986050f43b8"
    )
    assert keyring.actor_token("actor-1").digest == (
        "0bfc04f6b39ce836425c4b6b25cc208316a58f00934b3f398376e1c20e8f9cab"
    )
    assert keyring.erasure_job_token(JOB_ID).digest == (
        "3b145162a05181dbd16f2ece68a782ed0b5725a64b53f5ed228ac1d1978a34fb"
    )
    assert keyring.verifier("current", NOW).verifier_digest == (
        "80a5233fd9c868515b8ceb6a7783b67bb35ea8e8f4112930aa40dd784026fc2a"
    )


def test_same_identity_is_separated_by_purpose_and_secret() -> None:
    value = str(MEETING_ID)
    keyring = ErasureTokenKeyring(
        "current",
        {"current": b"c" * 32, "previous": b"p" * 32},
    )

    purpose_digests = {
        keyring.ingest_key_token(value).digest,
        keyring.request_key_token(value).digest,
        keyring.actor_token(value).digest,
    }
    key_digests = {token.digest for token in keyring.request_key_tokens(value)}

    assert len(purpose_digests) == 3
    assert len(key_digests) == 2


def test_encoded_keyring_round_trip_and_decoded_secret_bounds() -> None:
    payload = json.dumps({"current": encoded(b"c" * 32)})
    keyring = ErasureTokenKeyring.from_encoded("current", payload)

    assert keyring.active_key_id == "current"
    assert keyring.key_ids == ("current",)
    assert keyring.request_key_token("request-1").digest
    for size in (31, 65):
        invalid = json.dumps({"current": encoded(b"x" * size)})
        with pytest.raises(ErasureTokenConfigurationError):
            ErasureTokenKeyring.from_encoded("current", invalid)


def test_rotation_candidates_are_active_first_then_stable() -> None:
    keyring = ErasureTokenKeyring(
        "new",
        {
            "old-z": b"o" * 32,
            "new": b"n" * 32,
            "old-a": b"a" * 32,
        },
    )

    candidates = keyring.meeting_tokens(MEETING_ID)

    assert tuple(candidate.key_id for candidate in candidates) == ("new", "old-a", "old-z")
    assert len({candidate.digest for candidate in candidates}) == 3


@pytest.mark.parametrize(
    "payload",
    [
        "not-json-TOPSECRET",
        '{"current":"TOPSECRET%%%"}',
        '{"current":"' + "A" * 43 + '","current":"' + "B" * 43 + '"}',
        "x" * 4_097,
    ],
)
def test_encoded_keyring_errors_drop_secret_context(payload: str) -> None:
    with pytest.raises(ErasureTokenConfigurationError) as raised:
        ErasureTokenKeyring.from_encoded("current", payload)

    error = raised.value
    assert error.__cause__ is None
    assert error.__context__ is None
    assert "TOPSECRET" not in str(error)
    assert "TOPSECRET" not in repr(error)
    assert all("TOPSECRET" not in str(argument) for argument in error.args)


def test_keyring_rejects_duplicate_secrets_and_invalid_runtime_identity() -> None:
    with pytest.raises(ErasureTokenConfigurationError):
        ErasureTokenKeyring("a", {"a": b"x" * 32, "b": b"x" * 32})
    keyring = ErasureTokenKeyring("a", {"a": b"x" * 32})
    with pytest.raises(ValueError, match="identity input is invalid"):
        keyring.request_key_token(" request ")


def test_verifier_validation_supports_rotation_and_fails_closed() -> None:
    rotating = ErasureTokenKeyring("new", {"new": b"n" * 32, "old": b"o" * 32})
    persisted = rotating.verifiers(NOW)
    old_reference = ErasureTokenIdentity(token_version=1, key_id="old")

    rotating.validate_verifiers(persisted, (old_reference,))
    retired = ErasureTokenKeyring("new", {"new": b"n" * 32})
    retired.validate_verifiers(persisted)
    with pytest.raises(ErasureKeyVerificationError):
        retired.validate_verifiers(persisted, (old_reference,))
    with pytest.raises(ErasureKeyVerificationError):
        rotating.validate_verifiers(
            persisted, (ErasureTokenIdentity(token_version=2, key_id="old"),)
        )
    with pytest.raises(ErasureKeyVerificationError):
        rotating.validate_verifiers(persisted[:1])
    wrong = ErasureTokenKeyring("new", {"new": b"z" * 32, "old": b"o" * 32})
    with pytest.raises(ErasureKeyVerificationError):
        wrong.validate_verifiers(persisted)


def test_settings_keep_keyring_opaque_until_explicit_access() -> None:
    marker = "TOPSECRET-ERASURE-KEYRING"
    settings = Settings(
        _env_file=None,
        erasure_hmac_active_key_id="current",
        erasure_hmac_keys=marker,
    )

    assert marker not in repr(settings)
    assert marker not in str(settings.model_dump())
    assert marker not in settings.model_dump_json()
    assert settings.require_erasure_hmac_configuration() == ("current", marker)
    invalid_object = Settings(
        _env_file=None,
        erasure_hmac_active_key_id="current",
        erasure_hmac_keys={"TOPSECRET": marker},
    )
    with pytest.raises(MissingErasureHMACConfigurationError) as raised:
        invalid_object.require_erasure_hmac_configuration()
    assert marker not in repr(raised.value)


def test_settings_missing_pair_uses_generic_error() -> None:
    settings = Settings(_env_file=None, erasure_hmac_keys=encoded(b"x" * 32))

    with pytest.raises(MissingErasureHMACConfigurationError) as raised:
        settings.require_erasure_hmac_configuration()

    assert raised.value.args == ("Erasure HMAC key ID and keyring are required",)


def test_erasure_digests_are_redacted_by_structured_sanitization() -> None:
    keyring = ErasureTokenKeyring("current", {"current": b"x" * 32})
    token_payload = sanitize(keyring.meeting_token(MEETING_ID).model_dump())
    verifier_payload = sanitize(keyring.verifier("current", NOW).model_dump())

    assert token_payload["digest"] == REDACTED
    assert verifier_payload["verifier_digest"] == REDACTED
    assert sanitize({"erasure_hmac_keys": "private"})["erasure_hmac_keys"] == REDACTED
