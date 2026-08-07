from __future__ import annotations

from functools import lru_cache
from ipaddress import ip_address
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class MissingOpenAIAPIKeyError(ValueError):
    def __init__(self) -> None:
        super().__init__("OPENAI_API_KEY is required")


class MissingApiBearerTokenError(ValueError):
    def __init__(self) -> None:
        super().__init__("API_BEARER_TOKEN is required")


class MissingErasureHMACConfigurationError(ValueError):
    def __init__(self) -> None:
        super().__init__("Erasure HMAC key ID and keyring are required")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        hide_input_in_errors=True,
    )

    app_env: Literal["development", "test", "production"] = "development"
    app_host: str = "127.0.0.1"
    app_port: int = Field(default=8000, ge=1, le=65535)
    database_path: Path = Path("runtime/orchestrator.sqlite3")
    upload_directory: Path = Path("uploads")
    max_upload_bytes: int = Field(default=26_214_400, gt=0)
    api_bearer_token: SecretStr | None = None
    api_actor_subject: str = Field(default="portfolio-owner", min_length=1, max_length=200)

    openai_api_key: SecretStr | None = None
    openai_recap_model: str = "gpt-5.6-terra"
    openai_worker_model: str = "gpt-5.4-mini"
    openai_transcription_model: str = "gpt-4o-transcribe-diarize"
    openai_timeout_seconds: float = Field(default=120.0, gt=0)
    openai_max_retries: int = Field(default=0, ge=0, le=0)
    openai_max_requests_per_run: int = Field(default=5, ge=3, le=20)
    openai_max_output_tokens_per_run: int = Field(default=12_000, ge=1_000)
    openai_extractor_max_output_tokens: int = Field(default=6_500, ge=500)
    openai_recap_max_output_tokens: int = Field(default=2_500, ge=500)
    openai_verifier_max_output_tokens: int = Field(default=3_000, ge=500)
    openai_tracing_enabled: bool = False

    mcp_server_url: str | None = None
    mcp_auth_token: SecretStr | None = None
    mcp_connector_id: str = Field(default="workspace", min_length=1, max_length=200)
    mcp_task_resource_id: str | None = Field(default=None, min_length=1, max_length=200)
    mcp_calendar_resource_id: str | None = Field(default=None, min_length=1, max_length=200)
    mcp_calendar_tool: str = "create_calendar_event"
    mcp_task_tool: str = "create_task"
    mcp_lookup_tool: str = "find_action_by_idempotency_key"
    mcp_request_timeout_seconds: float = Field(default=30.0, gt=0, le=300)
    mcp_sse_timeout_seconds: float = Field(default=300.0, gt=0, le=3_600)
    mcp_call_timeout_seconds: float = Field(default=30.0, gt=0, le=300)

    worker_poll_interval_seconds: float = Field(default=1.0, gt=0, le=60)
    processing_batch_size: int = Field(default=1, ge=1, le=10)
    delivery_batch_size: int = Field(default=20, ge=1, le=100)
    recording_cleanup_batch_size: int = Field(default=20, ge=1, le=100)
    recording_cleanup_lease_seconds: float = Field(default=300.0, ge=30, le=3_600)
    recording_orphan_scan_interval_seconds: float = Field(default=300.0, gt=0, le=86_400)
    recording_orphan_grace_seconds: float = Field(default=86_400.0, ge=300, le=604_800)
    recording_orphan_scan_batch_size: int = Field(default=100, ge=1, le=1_000)
    erasure_hmac_active_key_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$",
    )
    erasure_hmac_keys: SecretStr | None = None
    meeting_erasure_batch_size: int = Field(default=20, ge=1, le=100)
    meeting_erasure_lease_seconds: float = Field(default=300.0, ge=30, le=3_600)
    meeting_erasure_max_remediations: int = Field(default=3, ge=1, le=10)

    @field_validator(
        "api_bearer_token",
        "openai_api_key",
        "mcp_auth_token",
        mode="before",
    )
    @classmethod
    def empty_secret_is_none(cls, value: object) -> object:
        if value == "":
            return None
        return value

    @field_validator("erasure_hmac_keys", mode="before")
    @classmethod
    def normalize_erasure_keyring(cls, value: object) -> object:
        if value == "" or value is None:
            return None
        if isinstance(value, str | SecretStr):
            return value
        return None

    @field_validator("erasure_hmac_active_key_id", mode="before")
    @classmethod
    def normalize_erasure_key_id(cls, value: object) -> object:
        if isinstance(value, str):
            normalized = value.strip()
            return normalized or None
        return value

    @field_validator(
        "mcp_server_url",
        "mcp_task_resource_id",
        "mcp_calendar_resource_id",
        mode="before",
    )
    @classmethod
    def empty_optional_text_is_none(cls, value: object) -> object:
        if value == "":
            return None
        return value

    @model_validator(mode="after")
    def validate_mcp_transport(self) -> Settings:
        specialist_budget = (
            self.openai_extractor_max_output_tokens
            + self.openai_recap_max_output_tokens
            + self.openai_verifier_max_output_tokens
        )
        if self.openai_max_output_tokens_per_run < specialist_budget:
            raise ValueError("The run output budget must cover all specialist limits")
        if self.mcp_server_url is None:
            return self
        parsed = urlsplit(self.mcp_server_url)
        loopback = _is_loopback_host(parsed.hostname)
        if self.app_env == "production" and parsed.scheme != "https" and not loopback:
            raise ValueError("Production MCP connections must use HTTPS")
        if self.mcp_auth_token is not None and parsed.scheme != "https" and not loopback:
            raise ValueError("Authenticated MCP connections must use HTTPS")
        return self

    def require_openai_api_key(self) -> str:
        if self.openai_api_key is None:
            raise MissingOpenAIAPIKeyError
        return self.openai_api_key.get_secret_value()

    def require_api_bearer_token(self) -> str:
        if self.api_bearer_token is None:
            raise MissingApiBearerTokenError
        return self.api_bearer_token.get_secret_value()

    def require_erasure_hmac_configuration(self) -> tuple[str, str]:
        if self.erasure_hmac_active_key_id is None or self.erasure_hmac_keys is None:
            raise MissingErasureHMACConfigurationError
        return (
            self.erasure_hmac_active_key_id,
            self.erasure_hmac_keys.get_secret_value(),
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def _is_loopback_host(host: str | None) -> bool:
    if host is None:
        return False
    if host.casefold() == "localhost":
        return True
    try:
        return ip_address(host).is_loopback
    except ValueError:
        return False
