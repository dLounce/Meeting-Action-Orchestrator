from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from meeting_action_orchestrator.domain.enums import DeliveryOperationKind
from meeting_action_orchestrator.domain.models import DeliveryOperationBinding
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork
from tests.integration.test_processing_jobs import MEETING_ID, create_database

NOW = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)


def binding(
    *,
    actor_id: str = "owner",
    operation: DeliveryOperationKind = DeliveryOperationKind.RETRY,
) -> DeliveryOperationBinding:
    return DeliveryOperationBinding(
        request_key="delivery-operation-one",
        meeting_id=MEETING_ID,
        operation=operation,
        actor_id=actor_id,
        selection_fingerprint="a" * 64,
        created_at=NOW,
    )


def test_delivery_operation_binding_round_trips(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    original = binding()

    with SqliteUnitOfWork(database) as uow:
        uow.delivery_operations.add(original)
        uow.commit()
    with SqliteUnitOfWork(database) as uow:
        restored = uow.delivery_operations.get(original.request_key)

    assert restored == original


def test_delivery_operation_binding_rolls_back_with_transaction(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    original = binding()

    with pytest.raises(RuntimeError, match="stop"), SqliteUnitOfWork(database) as uow:
        uow.delivery_operations.add(original)
        raise RuntimeError("stop")
    with SqliteUnitOfWork(database) as uow:
        restored = uow.delivery_operations.get(original.request_key)

    assert restored is None


def test_request_key_is_globally_unique(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")

    with SqliteUnitOfWork(database) as uow:
        uow.delivery_operations.add(binding())
        uow.commit()
    with pytest.raises(sqlite3.IntegrityError), SqliteUnitOfWork(database) as uow:
        uow.delivery_operations.add(
            binding(
                actor_id="another-owner",
                operation=DeliveryOperationKind.RECONCILE,
            )
        )
