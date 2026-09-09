"""Shared operational status for V3 runtime components.

This module is intentionally outside ingest/publishing business domains so all
runtime components can publish heartbeats and source-control state without
introducing cross-layer dependencies.
"""
from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import sqlite3
from typing import Any
from zoneinfo import ZoneInfo


DDL = """
CREATE TABLE IF NOT EXISTS v3_component_status (
    component TEXT PRIMARY KEY,
    state TEXT NOT NULL DEFAULT 'running',
    last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_event_at TEXT,
    last_error TEXT NOT NULL DEFAULT '',
    meta_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS collector_source_state_v3 (
    source_name TEXT PRIMARY KEY,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    last_collected_at TEXT,
    count_date TEXT NOT NULL DEFAULT '',
    today_count INTEGER NOT NULL DEFAULT 0,
    today_duplicates INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


class RuntimeStateRepository:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def _connect(self) -> sqlite3.Connection:
        uri = f"file:{self.db_path.as_posix()}?mode=rw"
        conn = sqlite3.connect(uri, uri=True, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def heartbeat(
        self,
        component: str,
        *,
        state: str = "running",
        event: bool = False,
        error: str = "",
        meta: dict[str, Any] | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO v3_component_status(component,state,last_seen_at,last_event_at,last_error,meta_json,updated_at)
                   VALUES (?,?,CURRENT_TIMESTAMP,CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END,?,?,CURRENT_TIMESTAMP)
                   ON CONFLICT(component) DO UPDATE SET
                     state=excluded.state,
                     last_seen_at=CURRENT_TIMESTAMP,
                     last_event_at=CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE v3_component_status.last_event_at END,
                     last_error=excluded.last_error,
                     meta_json=excluded.meta_json,
                     updated_at=CURRENT_TIMESTAMP""",
                (
                    str(component),
                    str(state),
                    int(bool(event)),
                    str(error or "")[:1000],
                    json.dumps(meta or {}, ensure_ascii=False, sort_keys=True),
                    int(bool(event)),
                ),
            )
            conn.commit()

    def component_rows(self) -> dict[str, dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM v3_component_status ORDER BY component").fetchall()
        return {str(row["component"]): dict(row) for row in rows}

    def source_enabled(self, source_name: str, *, default: bool = True) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT enabled FROM collector_source_state_v3 WHERE source_name=?",
                (str(source_name),),
            ).fetchone()
        return bool(int(row["enabled"])) if row is not None else bool(default)

    def set_source_enabled(self, source_name: str, enabled: bool) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO collector_source_state_v3(source_name,enabled,updated_at)
                   VALUES (?,?,CURRENT_TIMESTAMP)
                   ON CONFLICT(source_name) DO UPDATE SET enabled=excluded.enabled,updated_at=CURRENT_TIMESTAMP""",
                (str(source_name), int(bool(enabled))),
            )
            conn.commit()

    def ensure_source(self, source_name: str, *, enabled: bool = True) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO collector_source_state_v3(source_name,enabled) VALUES (?,?)",
                (str(source_name), int(bool(enabled))),
            )
            conn.commit()

    def touch_source(
        self,
        source_name: str,
        *,
        collected: bool = False,
        duplicate: bool = False,
        error: str = "",
        timezone_name: str = "Asia/Phnom_Penh",
    ) -> None:
        today = datetime.now(ZoneInfo(timezone_name)).date().isoformat()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT count_date,today_count,today_duplicates,enabled FROM collector_source_state_v3 WHERE source_name=?",
                (str(source_name),),
            ).fetchone()
            enabled = int(row["enabled"]) if row is not None else 1
            count = int(row["today_count"]) if row is not None and str(row["count_date"]) == today else 0
            duplicates = int(row["today_duplicates"]) if row is not None and str(row["count_date"]) == today else 0
            count += int(bool(collected))
            duplicates += int(bool(duplicate))
            conn.execute(
                """INSERT INTO collector_source_state_v3
                   (source_name,enabled,last_collected_at,count_date,today_count,today_duplicates,last_error,updated_at)
                   VALUES (?,?,CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END,?,?,?,?,CURRENT_TIMESTAMP)
                   ON CONFLICT(source_name) DO UPDATE SET
                     last_collected_at=CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE collector_source_state_v3.last_collected_at END,
                     count_date=excluded.count_date,
                     today_count=excluded.today_count,
                     today_duplicates=excluded.today_duplicates,
                     last_error=excluded.last_error,
                     updated_at=CURRENT_TIMESTAMP""",
                (
                    str(source_name), enabled, int(bool(collected)), today, count, duplicates,
                    str(error or "")[:1000], int(bool(collected)),
                ),
            )
            conn.commit()

    def source_rows(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM collector_source_state_v3 ORDER BY source_name").fetchall()
        return [dict(row) for row in rows]


__all__ = ["DDL", "RuntimeStateRepository"]
