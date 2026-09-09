from __future__ import annotations

import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from v3_core.storage.bootstrap import REQUIRED_V3_TABLES, existing_tables, initialize_v3_storage


def missing_runtime_tables(db_path: str | Path) -> frozenset[str]:
    return REQUIRED_V3_TABLES - existing_tables(db_path)


def backup_database(db_path: str | Path, backup_dir: str | Path) -> Path | None:
    source = Path(db_path).expanduser().resolve()
    if not source.is_file() or source.stat().st_size == 0:
        return None
    target_dir = Path(backup_dir).expanduser().resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = target_dir / f"{source.stem}-{stamp}.sqlite3"
    shutil.copy2(source, target)
    return target


def initialize_runtime_storage(db_path: str | Path, *, backup_dir: str | Path | None = None) -> Path:
    """Initialize only the native additive V3 schema.

    This deliberately does not create legacy ``drafts/listings/posts`` tables.
    V3 truth remains ``listings_v3`` + ``listing_offers`` +
    ``publication_instances``; aftercare tables are included by V3 bootstrap.
    """
    path = Path(db_path).expanduser().resolve()
    if backup_dir is not None:
        backup_database(path, backup_dir)
    initialize_v3_storage(path)
    missing = missing_runtime_tables(path)
    if missing:
        raise RuntimeError("V3 bootstrap incomplete: " + ", ".join(sorted(missing)))
    return path


def database_integrity(db_path: str | Path) -> tuple[bool, str]:
    path = Path(db_path).expanduser().resolve()
    if not path.is_file():
        return False, "database_missing"
    conn = sqlite3.connect(str(path), timeout=10)
    try:
        row = conn.execute("PRAGMA quick_check").fetchone()
        result = str(row[0] if row else "")
        return result == "ok", result or "quick_check_empty"
    finally:
        conn.close()


__all__ = [
    "backup_database",
    "database_integrity",
    "initialize_runtime_storage",
    "missing_runtime_tables",
]
