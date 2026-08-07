from __future__ import annotations

from pathlib import Path
from typing import Any

from meeting_action_orchestrator import cli
from meeting_action_orchestrator.config import Settings


def settings(root: Path) -> Settings:
    return Settings(
        _env_file=None,
        database_path=root / "runtime.sqlite3",
        upload_directory=root / "uploads",
        api_bearer_token="a" * 32,
        openai_api_key="test-openai-key",
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
