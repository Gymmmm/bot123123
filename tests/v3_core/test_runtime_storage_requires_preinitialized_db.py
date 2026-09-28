from __future__ import annotations

import sqlite3

import pytest

from v3_core.ingest.source_repository import SourceRepository
from v3_core.inventory.identity import IdentityService
from v3_core.publishing.delivery_state import PublicationDeliveryStateRepository
from v3_core.publishing.package_store import FrozenPackageStore
from v3_core.publishing.publication_instances import PublicationInstanceRepository
from v3_core.storage.inventory_reader import InventoryReader
from v3_core.storage.inventory_repository import InventoryRepository


@pytest.mark.parametrize(
    ("factory", "exercise"),
    [
        (
            lambda db: SourceRepository(db),
            lambda repo: repo.get_source_post(1),
        ),
        (
            lambda db: InventoryRepository(db),
            lambda repo: repo.canonical_facts("CAN_MISSING"),
        ),
        (
            lambda db: InventoryReader(db),
            lambda repo: repo.listing("l_1"),
        ),
        (
            lambda db: IdentityService(db),
            lambda repo: repo.allocate(canonical_record_id="CAN_MISSING", facts={}),
        ),
        (
            lambda db: FrozenPackageStore(db),
            lambda repo: repo.get("PKG_MISSING"),
        ),
        (
            lambda db: PublicationDeliveryStateRepository(db),
            lambda repo: repo.get("DLV_MISSING"),
        ),
        (
            lambda db: PublicationInstanceRepository(db),
            lambda repo: repo.get("PUB_MISSING"),
        ),
    ],
)
def test_runtime_storage_never_creates_missing_database(tmp_path, factory, exercise):
    db = tmp_path / "must-be-initialized-first.sqlite3"
    assert not db.exists()

    repository = factory(str(db))
    assert not db.exists()

    with pytest.raises((FileNotFoundError, sqlite3.OperationalError)):
        exercise(repository)

    assert not db.exists()
