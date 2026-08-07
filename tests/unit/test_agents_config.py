import pytest

from meeting_action_orchestrator.config import Settings


def test_openai_defaults_use_stable_configurable_model_aliases() -> None:
    fields = Settings.model_fields

    assert fields["openai_worker_model"].default == "gpt-5.4-mini"
    assert fields["openai_recap_model"].default == "gpt-5.6-terra"
    assert fields["openai_transcription_model"].default == "gpt-4o-transcribe-diarize"
    assert fields["openai_max_retries"].default == 0
    assert fields["openai_budget_policy_version"].default == 1
    assert fields["openai_extraction_preflight_request_limit"].default == 6
    assert fields["openai_extraction_provider_request_limit"].default == 6
    assert fields["openai_extraction_input_token_limit"].default == 800_000
    assert fields["openai_extraction_output_token_limit"].default == 24_000
    assert fields["openai_transcription_provider_request_limit"].default == 3
    assert fields["openai_transcription_audio_duration_ms_limit"].default == 21_600_000


def test_upload_default_matches_transcription_api_boundary() -> None:
    expected_bytes = 25 * 1024 * 1024

    assert Settings.model_fields["max_upload_bytes"].default == expected_bytes


def test_openai_timeout_fits_within_the_processing_lease() -> None:
    assert Settings(_env_file=None, openai_timeout_seconds=120).openai_timeout_seconds == 120

    with pytest.raises(ValueError, match="openai_timeout_seconds"):
        Settings(_env_file=None, openai_timeout_seconds=120.001)


@pytest.mark.parametrize(
    "field",
    [
        "openai_worker_model",
        "openai_recap_model",
        "openai_transcription_model",
    ],
)
def test_openai_model_names_are_normalized_and_bounded(field: str) -> None:
    settings = Settings(_env_file=None, **{field: "  model-alias  "})

    assert getattr(settings, field) == "model-alias"

    for invalid in ("   ", "m" * 201):
        with pytest.raises(ValueError, match="OpenAI model names"):
            Settings(_env_file=None, **{field: invalid})


def test_delivery_targets_are_disabled_until_resources_are_configured() -> None:
    settings = Settings()

    assert settings.mcp_task_resource_id is None
    assert settings.mcp_calendar_resource_id is None
    assert settings.processing_batch_size == 1
    assert settings.delivery_batch_size == 20
    assert settings.recording_cleanup_batch_size == 20
    assert settings.recording_cleanup_lease_seconds == 300
    assert settings.recording_orphan_scan_interval_seconds == 300
    assert settings.recording_orphan_grace_seconds == 86_400
    assert settings.recording_orphan_scan_batch_size == 100


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("recording_cleanup_batch_size", 101),
        ("recording_cleanup_lease_seconds", 29),
        ("recording_orphan_scan_interval_seconds", 86_401),
        ("recording_orphan_grace_seconds", 299),
        ("recording_orphan_scan_batch_size", 1_001),
    ],
)
def test_recording_maintenance_settings_are_bounded(field: str, value: int) -> None:
    with pytest.raises(ValueError, match=field):
        Settings(_env_file=None, **{field: value})


def test_run_budget_must_cover_all_specialist_limits() -> None:
    with pytest.raises(ValueError, match="run output budget"):
        Settings(
            _env_file=None,
            openai_max_output_tokens_per_run=11_999,
        )


def test_durable_output_budget_must_cover_one_specialist_run() -> None:
    with pytest.raises(ValueError, match="durable output budget"):
        Settings(
            _env_file=None,
            openai_extraction_output_token_limit=11_999,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("openai_budget_policy_version", 0),
        ("openai_extraction_preflight_request_limit", 2),
        ("openai_extraction_provider_request_limit", 2),
        ("openai_extraction_input_token_limit", 0),
        ("openai_transcription_provider_request_limit", 0),
        ("openai_transcription_audio_duration_ms_limit", 0),
    ],
)
def test_durable_provider_budget_settings_are_bounded(field: str, value: int) -> None:
    with pytest.raises(ValueError, match=field):
        Settings(_env_file=None, **{field: value})


def test_provider_tracing_cannot_bypass_budgeted_transport() -> None:
    with pytest.raises(ValueError, match="tracing is disabled"):
        Settings(_env_file=None, openai_tracing_enabled=True)


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
