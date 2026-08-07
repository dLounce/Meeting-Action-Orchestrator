from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class MissingOpenAIAPIKeyError(ValueError):
    def __init__(self) -> None:
        super().__init__("OPENAI_API_KEY is required")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_env: Literal["development", "test", "production"] = "development"
    app_host: str = "127.0.0.1"
    app_port: int = Field(default=8000, ge=1, le=65535)
    app_base_url: str = "http://127.0.0.1:8000"
    database_path: Path = Path("runtime/orchestrator.sqlite3")
    upload_directory: Path = Path("uploads")
    max_upload_bytes: int = Field(default=26_214_400, gt=0)

    openai_api_key: SecretStr | None = None
    openai_recap_model: str = "gpt-5.6-terra"
    openai_worker_model: str = "gpt-5.4-mini"
    openai_transcription_model: str = "gpt-4o-transcribe-diarize"
    openai_timeout_seconds: float = Field(default=120.0, gt=0)
    openai_max_retries: int = Field(default=2, ge=0, le=5)
    openai_max_requests_per_run: int = Field(default=5, ge=3, le=20)
    openai_max_output_tokens_per_run: int = Field(default=12_000, ge=1_000)
    openai_extractor_max_output_tokens: int = Field(default=6_500, ge=500)
    openai_recap_max_output_tokens: int = Field(default=2_500, ge=500)
    openai_verifier_max_output_tokens: int = Field(default=3_000, ge=500)
    openai_tracing_enabled: bool = False
    trace_include_sensitive_data: bool = False

    mcp_server_url: str | None = None
    mcp_auth_token: SecretStr | None = None
    mcp_calendar_tool: str = "create_calendar_event"
    mcp_task_tool: str = "create_task"
    mcp_lookup_tool: str = "find_action_by_idempotency_key"

    @field_validator("openai_api_key", "mcp_auth_token", mode="before")
    @classmethod
    def empty_secret_is_none(cls, value: object) -> object:
        if value == "":
            return None
        return value

    @field_validator("mcp_server_url", mode="before")
    @classmethod
    def empty_url_is_none(cls, value: object) -> object:
        if value == "":
            return None
        return value

    def require_openai_api_key(self) -> str:
        if self.openai_api_key is None:
            raise MissingOpenAIAPIKeyError
        return self.openai_api_key.get_secret_value()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
