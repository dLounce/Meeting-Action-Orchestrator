from __future__ import annotations

from unittest.mock import patch

import pytest

from meeting_action_orchestrator.api.auth import StaticBearerAuthenticator
from meeting_action_orchestrator.config import MissingApiBearerTokenError, Settings

TOKEN = "a" * 32


async def test_static_bearer_authenticator_accepts_only_the_configured_token() -> None:
    authenticator = StaticBearerAuthenticator(TOKEN, "portfolio-owner")

    accepted = await authenticator.authenticate(TOKEN)
    rejected = await authenticator.authenticate("b" * 32)

    assert accepted is not None
    assert accepted.subject == "portfolio-owner"
    assert rejected is None


async def test_static_bearer_authenticator_compares_fixed_length_digests() -> None:
    authenticator = StaticBearerAuthenticator(TOKEN, "portfolio-owner")

    with patch(
        "meeting_action_orchestrator.api.auth.hmac.compare_digest", return_value=False
    ) as compare:
        await authenticator.authenticate("short")

    candidate, configured = compare.call_args.args
    assert len(candidate) == 32
    assert len(configured) == 32
    assert candidate != b"short"


@pytest.mark.parametrize(
    ("token", "subject", "message"),
    [
        ("short", "owner", "at least 32 bytes"),
        (TOKEN, "", "between 1 and 200 characters"),
        (TOKEN, "x" * 201, "between 1 and 200 characters"),
    ],
)
def test_static_bearer_authenticator_rejects_weak_configuration(
    token: str,
    subject: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        StaticBearerAuthenticator(token, subject)


def test_api_bearer_token_is_loaded_as_a_secret() -> None:
    settings = Settings(
        api_bearer_token="x" * 32,
        api_actor_subject="portfolio-owner",
    )

    assert settings.api_bearer_token is not None
    assert str(settings.api_bearer_token) == "**********"
    assert settings.require_api_bearer_token() == "x" * 32


def test_api_bearer_token_is_required_by_composition() -> None:
    settings = Settings(api_bearer_token=None)

    with pytest.raises(MissingApiBearerTokenError):
        settings.require_api_bearer_token()
