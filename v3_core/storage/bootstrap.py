"""Explicit additive V3 storage initialization.

Normal runtime/preflight code must not call this implicitly.  This module exists
so database creation is an intentional operator action (``--initialize``) and
all additive V3 DDL can be audited in one place.
"""
from __future__ import annotations

from pathlib import Path
import sqlite3

from v3_core.ingest.source_repository import SOURCE_DDL
from v3_core.publishing.delivery_state import DDL as DELIVERY_DDL
from v3_core.publishing.package_store import DDL as PACKAGE_DDL
from v3_core.publishing.publication_instances import DDL as PUBLICATION_INSTANCE_DDL
from v3_core.storage.schema import DDL as INVENTORY_DDL


REQUIRED_V3_TABLES = frozenset(
    {
        "source_posts",
        "media_assets",
        "canonical_records",
        "canonical_overrides",
        "listings_v3",
        "listing_offers",
        "review_items",
        "publication_packages_v3",
        "publication_delivery_attempts_v3",
        "publication_instances",
    }
)

DDL_BLOCKS = (
    SOURCE_DDL,
    INVENTORY_DDL,
    PACKAGE_DDL,
    DELIVERY_DDL,
    PUBLICATION_INSTANCE_DDL,
)


def initialize_v3_storage(db_path: str | Path) -> Path:
    """Create only additive V3 tables/indexes and preserve all existing rows."""
    path = Path(db_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30)
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("BEGIN IMMEDIATE")
        for ddl in DDL_BLOCKS:
            conn.executescript(ddl)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return path


def existing_tables(db_path: str | Path) -> frozenset[str]:
    """Read schema names without creating a database or changing journal mode."""
    path = Path(db_path).expanduser().resolve()
    if not path.is_file():
        return frozenset()
    uri = f"file:{path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5)
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        return frozenset(str(row[0]) for row in rows)
    finally:
        conn.close()


__all__ = [
    "DDL_BLOCKS",
    "REQUIRED_V3_TABLES",
    "existing_tables",
    "initialize_v3_storage",
]
