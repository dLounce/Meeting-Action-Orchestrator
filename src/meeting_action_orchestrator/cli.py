from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

import uvicorn

from meeting_action_orchestrator.bootstrap import create_application
from meeting_action_orchestrator.config import Settings, get_settings
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.observability import configure_logging

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="meeting-orchestrator")
    commands = parser.add_subparsers(dest="command", required=True)
    database = commands.add_parser("database")
    database_commands = database.add_subparsers(dest="database_command", required=True)
    database_commands.add_parser("migrate")
    commands.add_parser("serve")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    configure_logging()
    settings = get_settings()
    if arguments.command == "database":
        return _migrate(settings)
    return _serve(settings)


def _migrate(settings: Settings) -> int:
    version = Database(settings.database_path).migrate()
    logger.info(
        "database migrated",
        extra={"fields": {"database_version": version}},
    )
    return 0


def _serve(settings: Settings) -> int:
    app = create_application(settings)
    uvicorn.run(
        app,
        host=settings.app_host,
        port=settings.app_port,
        log_config=None,
        access_log=True,
    )
    return 0
