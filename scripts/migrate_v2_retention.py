#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path

DEFAULT_DB = Path(__file__).resolve().parents[1] / "data" / "qiaolian_dual_bot.db"


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
    ).fetchone()
    return row is not None


def view_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='view' AND name=? LIMIT 1", (name,)
    ).fetchone()
    return row is not None


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == column for r in rows)


def ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    if not table_exists(conn, table):
        raise RuntimeError(f"missing table: {table}")
    if not column_exists(conn, table, column):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def ensure_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_renewal_status ON renewal_tracking(renewal_status);
        CREATE INDEX IF NOT EXISTS idx_renewal_user ON renewal_tracking(user_id);
        CREATE INDEX IF NOT EXISTS idx_reminder_type ON lease_reminder_logs(remind_type);
        CREATE INDEX IF NOT EXISTS idx_reminder_date ON lease_reminder_logs(remind_date);
        CREATE INDEX IF NOT EXISTS idx_analytics_variant ON publish_analytics(caption_variant);
        CREATE INDEX IF NOT EXISTS idx_analytics_area ON publish_analytics(area);
        CREATE INDEX IF NOT EXISTS idx_analytics_date ON publish_analytics(published_at);
        CREATE INDEX IF NOT EXISTS idx_leads_action ON leads(action);
        CREATE INDEX IF NOT EXISTS idx_leads_source ON leads(source);
        CREATE INDEX IF NOT EXISTS idx_leads_created ON leads(created_at);
        """
    )


def create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS renewal_tracking (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            binding_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            listing_id TEXT,
            renewal_status TEXT DEFAULT 'pending',
            user_response TEXT,
            advisor_notes TEXT,
            contacted_at TEXT,
            completed_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (binding_id) REFERENCES tenant_bindings(id)
        );

        CREATE TABLE IF NOT EXISTS lease_reminder_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            binding_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            remind_type TEXT NOT NULL,
            remind_date TEXT NOT NULL,
            sent_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (binding_id) REFERENCES tenant_bindings(id)
        );

        CREATE TABLE IF NOT EXISTS publish_analytics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            draft_id TEXT,
            post_id TEXT,
            message_id INTEGER,
            listing_id TEXT,
            area TEXT,
            property_type TEXT,
            monthly_rent REAL,
            caption_variant TEXT DEFAULT 'a',
            publish_hour INTEGER,
            publish_day_of_week INTEGER,
            published_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS system_config (
            key TEXT PRIMARY KEY,
            value TEXT,
            description TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


def patch_legacy_tables(conn: sqlite3.Connection) -> None:
    # Older deployments already had lease_reminder_logs with legacy columns:
    # lease_end_date/remind_for_date. We keep them and add the new ones.
    ensure_column(conn, "lease_reminder_logs", "remind_type", "TEXT")
    ensure_column(conn, "lease_reminder_logs", "remind_date", "TEXT")

    # Backfill newly added columns from legacy values when empty.
    conn.execute(
        """
        UPDATE lease_reminder_logs
        SET remind_date = COALESCE(NULLIF(remind_date, ''), remind_for_date, lease_end_date)
        WHERE remind_date IS NULL OR remind_date = ''
        """
    )
    conn.execute(
        """
        UPDATE lease_reminder_logs
        SET remind_type = COALESCE(NULLIF(remind_type, ''), 'legacy')
        WHERE remind_type IS NULL OR remind_type = ''
        """
    )


def create_views(conn: sqlite3.Connection) -> None:
    if view_exists(conn, "ab_test_performance"):
        conn.execute("DROP VIEW ab_test_performance")
    conn.execute(
        """
        CREATE VIEW ab_test_performance AS
        SELECT
            p.caption_variant,
            COUNT(DISTINCT p.listing_id) AS total_posts,
            COUNT(DISTINCT CASE WHEN l.action IN ('consult', 'consult_click') THEN l.user_id END) AS consult_count,
            COUNT(DISTINCT a.user_id) AS appointment_count,
            ROUND(
              CAST(COUNT(DISTINCT CASE WHEN l.action IN ('consult', 'consult_click') THEN l.user_id END) AS FLOAT)
              / NULLIF(COUNT(DISTINCT p.listing_id), 0) * 100,
              2
            ) AS consult_rate,
            ROUND(
              CAST(COUNT(DISTINCT a.user_id) AS FLOAT)
              / NULLIF(COUNT(DISTINCT p.listing_id), 0) * 100,
              2
            ) AS appointment_rate
        FROM publish_analytics p
        LEFT JOIN leads l ON p.listing_id = l.listing_id
        LEFT JOIN appointments a ON p.listing_id = a.listing_id
        WHERE p.published_at >= datetime('now', '-30 days')
        GROUP BY p.caption_variant
        """
    )

    if view_exists(conn, "renewal_conversion"):
        conn.execute("DROP VIEW renewal_conversion")
    conn.execute(
        """
        CREATE VIEW renewal_conversion AS
        SELECT
            strftime('%Y-%m', created_at) AS month,
            COUNT(*) AS total_reminders,
            SUM(CASE WHEN renewal_status = 'completed' THEN 1 ELSE 0 END) AS completed,
            ROUND(
              CAST(SUM(CASE WHEN renewal_status = 'completed' THEN 1 ELSE 0 END) AS FLOAT)
              / NULLIF(COUNT(*), 0) * 100,
              2
            ) AS conversion_rate
        FROM renewal_tracking
        WHERE created_at >= datetime('now', '-6 months')
        GROUP BY month
        ORDER BY month DESC
        """
    )


def seed_config(conn: sqlite3.Connection) -> None:
    rows = [
        (
            "caption_variant_weights",
            json.dumps({"a": 0.4, "b": 0.3, "c": 0.3}, ensure_ascii=False),
            "A/B测试文案分配权重",
        ),
        ("reminder_days", json.dumps([30, 7, 3], ensure_ascii=False), "提醒天数配置"),
        ("analytics_retention_days", "90", "数据分析保留天数"),
    ]
    conn.executemany(
        """
        INSERT INTO system_config(key, value, description, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET
            value=excluded.value,
            description=excluded.description,
            updated_at=CURRENT_TIMESTAMP
        """,
        rows,
    )


def migrate(db_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"db not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys=ON")

        # Existing table upgrades
        ensure_column(conn, "tenant_bindings", "monthly_rent", "REAL DEFAULT 0")
        ensure_column(conn, "tenant_bindings", "contract_start_date", "TEXT")
        ensure_column(conn, "tenant_bindings", "contract_end_date", "TEXT")
        ensure_column(conn, "tenant_bindings", "deposit_months", "INTEGER DEFAULT 2")
        ensure_column(conn, "tenant_bindings", "contract_notes", "TEXT")

        ensure_column(conn, "leads", "message_id", "INTEGER")
        ensure_column(conn, "leads", "caption_variant", "TEXT")
        ensure_column(conn, "leads", "conversion_value", "REAL DEFAULT 0")

        create_tables(conn)
        patch_legacy_tables(conn)
        ensure_indexes(conn)
        create_views(conn)
        seed_config(conn)

        # Backfill
        conn.execute(
            """
            UPDATE tenant_bindings
            SET contract_end_date = lease_end_date
            WHERE (contract_end_date IS NULL OR contract_end_date = '')
              AND lease_end_date IS NOT NULL
              AND lease_end_date != ''
            """
        )

        conn.commit()
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply retention/analytics migration (idempotent)")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="sqlite db path")
    args = parser.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    migrate(db_path)
    print(f"OK migrated: {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
