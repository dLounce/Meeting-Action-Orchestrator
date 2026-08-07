from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from meeting_action_orchestrator.domain.enums import (
    DeliveryOperationKind,
    DeliveryOperationStatus,
)
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
        updated_at=NOW,
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


def test_delivery_operation_claim_is_exclusive_and_completes_with_cas(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    original = binding()
    first_expiry = NOW + timedelta(seconds=30)
    renewed_expiry = NOW + timedelta(minutes=1)

    with SqliteUnitOfWork(database) as uow:
        uow.delivery_operations.add(original)
        first = uow.delivery_operations.claim(
            original.request_key,
            "worker-a",
            NOW,
            first_expiry,
        )
        uow.commit()
    with SqliteUnitOfWork(database) as uow:
        competing = uow.delivery_operations.claim(
            original.request_key,
            "worker-b",
            NOW,
            first_expiry,
        )
        assert first is not None
        renewed = uow.delivery_operations.renew(
            original.request_key,
            "worker-a",
            first.version,
            NOW + timedelta(seconds=1),
            renewed_expiry,
        )
        uow.commit()
    assert renewed is not None
    with SqliteUnitOfWork(database) as uow:
        completed = uow.delivery_operations.complete(
            original.request_key,
            "worker-a",
            renewed.version,
            NOW + timedelta(seconds=2),
        )
        uow.commit()
    with SqliteUnitOfWork(database, immediate=False) as uow:
        restored = uow.delivery_operations.get(original.request_key)

    assert competing is None
    assert completed is True
    assert restored is not None
    assert restored.status is DeliveryOperationStatus.COMPLETED
    assert restored.completed_at == NOW + timedelta(seconds=2)


def test_expired_delivery_operation_claim_is_reclaimable(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    original = binding()
    first_expiry = NOW + timedelta(seconds=30)

    with SqliteUnitOfWork(database) as uow:
        uow.delivery_operations.add(original)
        first = uow.delivery_operations.claim(
            original.request_key,
            "worker-a",
            NOW,
            first_expiry,
        )
        uow.commit()
    with SqliteUnitOfWork(database) as uow:
        reclaimed = uow.delivery_operations.claim(
            original.request_key,
            "worker-b",
            first_expiry,
            first_expiry + timedelta(seconds=30),
        )
        assert first is not None
        stale_release = uow.delivery_operations.release(
            original.request_key,
            "worker-a",
            first.version,
            first_expiry,
        )
        uow.commit()

    assert reclaimed is not None
    assert reclaimed.lease_owner == "worker-b"
    assert stale_release is False
