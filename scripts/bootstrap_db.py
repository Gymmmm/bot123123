#!/usr/bin/env python3
"""Create/upgrade a production SQLite database from an empty file safely.

Order matters:
1. legacy runtime compatibility tables still used by collector/parser/publisher;
2. user/admin bot legacy tables owned by db.Database;
3. additive V3 migrations and migration ledger.

The bootstrap is idempotent and never drops/rebuilds an existing database.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - dotenv is optional for explicit env deployments
    load_dotenv = None

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if load_dotenv is not None:
    load_dotenv(ROOT / ".env", override=False)

from db import Database
from qiaolian_v3.db.migration_runner import MigrationRunner


REQUIRED_LEGACY_TABLES = {
    "collect_sources",
    "source_posts",
    "drafts",
    "media_assets",
    "posts",
    "publish_logs",
}
REQUIRED_USER_TABLES = {
    "listings",
    "users",
    "favorites",
    "leads",
    "appointments",
    "tenant_bindings",
    "repair_tickets",
    "subscriptions",
}
REQUIRED_V3_TABLES = {
    "v3_sources",
    "v3_source_posts",
    "source_post_revisions",
    "canonical_records",
    "v3_listings",
    "listing_offers",
    "v3_publication_packages",
    "v3_channel_posts",
    "v3_sync_leases",
}


def _resolve_db_path() -> Path:
    raw = os.getenv("DB_PATH", str(ROOT / "data" / "qiaolian_dual_bot.db"))
    return Path(raw).expanduser().resolve()


def _apply_legacy_runtime_schema(conn: sqlite3.Connection) -> None:
    schema_path = ROOT / "schema_core.sql"
    if not schema_path.is_file():
        raise FileNotFoundError(f"missing production schema: {schema_path}")
    conn.executescript(schema_path.read_text(encoding="utf-8"))


def _verify(conn: sqlite3.Connection) -> None:
    tables = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    required = REQUIRED_LEGACY_TABLES | REQUIRED_USER_TABLES | REQUIRED_V3_TABLES
    missing = sorted(required - tables)
    if missing:
        raise RuntimeError("bootstrap_missing_tables:" + ",".join(missing))

    versions = {
        str(row[0])
        for row in conn.execute("SELECT version FROM schema_migrations").fetchall()
    }
    missing_versions = sorted({"001", "002"} - versions)
    if missing_versions:
        raise RuntimeError("bootstrap_missing_migrations:" + ",".join(missing_versions))


def bootstrap_database(db_path: Path | str | None = None) -> Path:
    path = Path(db_path).expanduser().resolve() if db_path is not None else _resolve_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    # Legacy collector/parser/publisher compatibility schema.
    with sqlite3.connect(path) as conn:
        _apply_legacy_runtime_schema(conn)

    # Existing user/admin runtime owns these additive tables/columns. Passing
    # the exact path avoids accidental initialization of config.DB_PATH.
    Database(path)

    # V3 remains the sole owner of V3 tables and its numbered migration ledger.
    with sqlite3.connect(path) as conn:
        applied = MigrationRunner(conn).migrate()
        _verify(conn)

    print(
        f"OK production bootstrap: {path} "
        f"v3_applied={','.join(applied) if applied else 'none'}"
    )
    return path


def main() -> int:
    try:
        bootstrap_database()
    except Exception as exc:
        print(f"BOOTSTRAP FAILED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
