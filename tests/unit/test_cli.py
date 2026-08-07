from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

from meeting_action_orchestrator import cli
from meeting_action_orchestrator.config import Settings


def settings(root: Path) -> Settings:
    encoded_key = base64.urlsafe_b64encode(b"e" * 32).decode("ascii").rstrip("=")
    return Settings(
        _env_file=None,
        database_path=root / "runtime.sqlite3",
        upload_directory=root / "uploads",
        api_bearer_token="a" * 32,
        openai_api_key="test-openai-key",
        erasure_hmac_active_key_id="current",
        erasure_hmac_keys=json.dumps({"current": encoded_key}),
    )


def test_database_migrate_command(tmp_path: Path, monkeypatch: Any) -> None:
    configured = settings(tmp_path)
    monkeypatch.setattr(cli, "get_settings", lambda: configured)
    monkeypatch.setattr(cli, "configure_logging", lambda: None)

    assert cli.main(["database", "migrate"]) == 0
    assert (tmp_path / "runtime.sqlite3").is_file()


def test_serve_command_uses_configured_binding(tmp_path: Path, monkeypatch: Any) -> None:
    configured = settings(tmp_path)
    application = object()
    calls: list[tuple[object, dict[str, object]]] = []
    monkeypatch.setattr(cli, "get_settings", lambda: configured)
    monkeypatch.setattr(cli, "configure_logging", lambda: None)
    monkeypatch.setattr(cli, "create_application", lambda _value: application)
    monkeypatch.setattr(
        cli.uvicorn,
        "run",
        lambda app, **options: calls.append((app, options)),
    )

    assert cli.main(["serve"]) == 0
    assert calls == [
        (
            application,
            {
                "host": configured.app_host,
                "port": configured.app_port,
                "log_config": None,
                "access_log": False,
            },
        )
    ]


def test_erasure_keyring_verification_reports_only_the_key_count(
    tmp_path: Path,
    monkeypatch: Any,
    capsys: Any,
) -> None:
    configured = settings(tmp_path)
    monkeypatch.setattr(cli, "get_settings", lambda: configured)
    monkeypatch.setattr(cli, "configure_logging", lambda: None)

    assert cli.main(["erasure", "verify-keyring"]) == 0
    assert capsys.readouterr().out == "Erasure keyring verified: 1 key\n"
