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
