import pytest

from meeting_action_orchestrator.config import Settings


def test_openai_defaults_use_stable_configurable_model_aliases() -> None:
    fields = Settings.model_fields

    assert fields["openai_worker_model"].default == "gpt-5.4-mini"
    assert fields["openai_recap_model"].default == "gpt-5.6-terra"
    assert fields["openai_transcription_model"].default == "gpt-4o-transcribe-diarize"
    assert fields["openai_max_retries"].default == 0


def test_upload_default_matches_transcription_api_boundary() -> None:
    expected_bytes = 25 * 1024 * 1024

    assert Settings.model_fields["max_upload_bytes"].default == expected_bytes


def test_delivery_targets_are_disabled_until_resources_are_configured() -> None:
    settings = Settings()

    assert settings.mcp_task_resource_id is None
    assert settings.mcp_calendar_resource_id is None
    assert settings.processing_batch_size == 1
    assert settings.delivery_batch_size == 20


def test_run_budget_must_cover_all_specialist_limits() -> None:
    with pytest.raises(ValueError, match="run output budget"):
        Settings(
            _env_file=None,
            openai_max_output_tokens_per_run=11_999,
        )


def test_remote_mcp_credentials_require_https() -> None:
    with pytest.raises(ValueError, match="Authenticated MCP"):
        Settings(
            _env_file=None,
            mcp_server_url="http://mcp.example.com/actions",
            mcp_auth_token="x" * 32,
        )


def test_production_mcp_requires_https() -> None:
    with pytest.raises(ValueError, match="Production MCP"):
        Settings(
            _env_file=None,
            app_env="production",
            mcp_server_url="http://mcp.example.com/actions",
        )

    settings = Settings(
        _env_file=None,
        app_env="production",
        mcp_server_url="http://127.0.0.1:9000/actions",
        mcp_auth_token="x" * 32,
    )

    assert settings.mcp_server_url == "http://127.0.0.1:9000/actions"
