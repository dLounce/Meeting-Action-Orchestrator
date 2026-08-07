from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence

import uvicorn

from meeting_action_orchestrator.application.meeting_erasure import ErasureKeyRegistry
from meeting_action_orchestrator.bootstrap import SystemClock, create_application
from meeting_action_orchestrator.config import Settings, get_settings
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.erasure_tokens import ErasureTokenKeyring
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork
from meeting_action_orchestrator.observability import configure_logging

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="meeting-orchestrator")
    commands = parser.add_subparsers(dest="command", required=True)
    database = commands.add_parser("database")
    database_commands = database.add_subparsers(dest="database_command", required=True)
    database_commands.add_parser("migrate")
    erasure = commands.add_parser("erasure")
    erasure_commands = erasure.add_subparsers(dest="erasure_command", required=True)
    erasure_commands.add_parser("verify-keyring")
    commands.add_parser("serve")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    configure_logging()
    settings = get_settings()
    if arguments.command == "database":
        return _migrate(settings)
    if arguments.command == "erasure":
        return _verify_erasure_keyring(settings)
    return _serve(settings)


def _migrate(settings: Settings) -> int:
    version = Database(settings.database_path).migrate()
    logger.info(
        "database migrated",
        extra={"fields": {"database_version": version}},
    )
    return 0


def _verify_erasure_keyring(settings: Settings) -> int:
    active_key_id, encoded_keys = settings.require_erasure_hmac_configuration()
    tokens = ErasureTokenKeyring.from_encoded(active_key_id, encoded_keys)
    database = Database(settings.database_path)
    database.migrate()

    def write_unit_of_work() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(database)

    def read_unit_of_work() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(database, immediate=False)

    registry = ErasureKeyRegistry(
        unit_of_work=write_unit_of_work,
        validation_unit_of_work=read_unit_of_work,
        tokens=tokens,
        clock=SystemClock(),
    )
    key_ids = registry.ensure_registered_sync()
    key_count = len(key_ids)
    unit = "key" if key_count == 1 else "keys"
    sys.stdout.write(f"Erasure keyring verified: {key_count} {unit}\n")
    return 0


def _serve(settings: Settings) -> int:
    app = create_application(settings)
    uvicorn.run(
        app,
        host=settings.app_host,
        port=settings.app_port,
        log_config=None,
        access_log=False,
    )
    return 0
