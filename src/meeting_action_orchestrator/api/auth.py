from __future__ import annotations

import hashlib
import hmac

from meeting_action_orchestrator.api.contracts import Principal

MINIMUM_BEARER_TOKEN_BYTES = 32
MAXIMUM_SUBJECT_LENGTH = 200


class StaticBearerAuthenticator:
    def __init__(self, token: str, subject: str) -> None:
        encoded = token.encode("utf-8")
        if len(encoded) < MINIMUM_BEARER_TOKEN_BYTES:
            raise ValueError("The bearer token must contain at least 32 bytes")
        normalized_subject = subject.strip()
        if not normalized_subject or len(normalized_subject) > MAXIMUM_SUBJECT_LENGTH:
            raise ValueError("The bearer subject must contain between 1 and 200 characters")
        self._token_digest = hashlib.sha256(encoded).digest()
        self._principal = Principal(normalized_subject)

    async def authenticate(self, token: str) -> Principal | None:
        candidate_digest = hashlib.sha256(token.encode("utf-8")).digest()
        if not hmac.compare_digest(candidate_digest, self._token_digest):
            return None
        return self._principal
