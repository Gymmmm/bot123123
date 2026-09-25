"""Durable V3 automatic publication orchestration.

This module does not introduce a second publication pipeline.  It schedules the
existing V3 review -> frozen package -> delivery coordinator chain and keeps
operator-facing state in additive SQLite tables.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
import json
import os
from pathlib import Path
import sqlite3
from typing import Any, Iterable
import uuid
from zoneinfo import ZoneInfo

from .admin_workflow import PublisherWorkflowService
from .delivery_state import DeliveryBlocked
from .package_store import FrozenPackage
from .telegram_adapter import TelegramChannelAdapter, deliver_approved_package


DDL = """
CREATE TABLE IF NOT EXISTS publisher_autopilot_settings_v3 (
    setting_key TEXT PRIMARY KEY,
    setting_value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS publisher_post_windows_v3 (
    window_id TEXT PRIMARY KEY,
    start_time TEXT NOT NULL,
    end_time TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_publisher_post_windows_v3_enabled
ON publisher_post_windows_v3(enabled,start_time,end_time);
CREATE TABLE IF NOT EXISTS publisher_auto_items_v3 (
    offer_id TEXT PRIMARY KEY,
    listing_id TEXT NOT NULL,
    review_id TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT 'queued',
    reason_code TEXT NOT NULL DEFAULT '',
    reason_text TEXT NOT NULL DEFAULT '',
    package_id TEXT NOT NULL DEFAULT '',
    channel_message_id TEXT NOT NULL DEFAULT '',
    origin TEXT NOT NULL DEFAULT 'collector',
    ignored INTEGER NOT NULL DEFAULT 0 CHECK(ignored IN (0,1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_publisher_auto_items_v3_state
ON publisher_auto_items_v3(state,ignored,updated_at);
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
CREATE TABLE IF NOT EXISTS publisher_autopilot_lock_v3 (
    lock_name TEXT PRIMARY KEY,
    owner TEXT NOT NULL DEFAULT '',
    locked_until TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


ERROR_LABELS = {
    "missing_rent": "缺少租金",
    "missing_location": "缺少项目或位置",
    "missing_listing_info": "房源信息不完整",
    "insufficient_media": "实拍图片不足",
    "unreadable_media": "图片无法读取",
    "duplicate_listing": "疑似重复房源",
    "already_published": "房源已经发布",
    "listing_not_publishable": "房源状态不可发布",
    "sale_store_only": "出售房源仅保存",
    "telegram_failed": "Telegram 发布失败",
    "telegram_unknown": "发送结果待确认，禁止重复发送",
    "canonical_error": "房源资料校验未通过",
    "admin_hold": "房源已暂停，等待管理员处理",
    "legacy_backlog": "旧库存已暂停自动投递",
}


@dataclass(frozen=True)
class AutoPublishConfig:
    enabled: bool
    min_media: int
    timezone_name: str
    interval_seconds: int


@dataclass(frozen=True)
class AutoPublishResult:
    status: str
    offer_id: str = ""
    listing_id: str = ""
    package_id: str = ""
    channel_message_id: str = ""
    reason_code: str = ""
    reason_text: str = ""


def _bool(raw: Any, default: bool = False) -> bool:
    value = str(raw if raw is not None else "").strip().lower()
    if not value:
        return bool(default)
    return value in {"1", "true", "yes", "on", "enabled"}


def _hhmm(value: str) -> str:
    clean = str(value or "").strip()
    try:
        parsed = datetime.strptime(clean, "%H:%M")
    except ValueError as exc:
        raise ValueError("time must use HH:MM") from exc
    return parsed.strftime("%H:%M")


class AutoPublishRepository:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def _connect(self) -> sqlite3.Connection:
        uri = f"file:{self.db_path.as_posix()}?mode=rw"
        conn = sqlite3.connect(uri, uri=True, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def ensure_defaults(self) -> None:
        defaults = {
            "enabled": os.getenv("V3_AUTO_PUBLISH_ENABLED", "true"),
            "min_media": os.getenv("V3_AUTO_PUBLISH_MIN_MEDIA", "4"),
            "timezone": os.getenv("V3_AUTO_PUBLISH_TIMEZONE", "Asia/Phnom_Penh"),
            "interval_seconds": os.getenv("V3_AUTO_PUBLISH_INTERVAL_SECONDS", "1800"),
        }
        default_windows = str(os.getenv("V3_AUTO_PUBLISH_WINDOWS", "09:00-21:00")).strip()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for key, value in defaults.items():
                conn.execute(
                    "INSERT OR IGNORE INTO publisher_autopilot_settings_v3(setting_key,setting_value) VALUES (?,?)",
                    (key, str(value)),
                )
            count = int(conn.execute("SELECT COUNT(*) FROM publisher_post_windows_v3").fetchone()[0])
            if count == 0:
                for part in default_windows.split(","):
                    if "-" not in part:
                        continue
                    start, end = [x.strip() for x in part.split("-", 1)]
                    try:
                        start, end = _hhmm(start), _hhmm(end)
                    except ValueError:
                        continue
                    conn.execute(
                        "INSERT INTO publisher_post_windows_v3(window_id,start_time,end_time,enabled) VALUES (?,?,?,1)",
                        ("WIN_" + uuid.uuid4().hex[:16], start, end),
                    )
            conn.commit()

    def config(self) -> AutoPublishConfig:
        with self._connect() as conn:
            rows = conn.execute("SELECT setting_key,setting_value FROM publisher_autopilot_settings_v3").fetchall()
        values = {str(row["setting_key"]): str(row["setting_value"]) for row in rows}
        return AutoPublishConfig(
            enabled=_bool(values.get("enabled"), True),
            min_media=max(1, int(values.get("min_media") or 4)),
            timezone_name=values.get("timezone") or "Asia/Phnom_Penh",
            interval_seconds=max(10, int(values.get("interval_seconds") or 1800)),
        )

    def set_enabled(self, enabled: bool) -> None:
        self.set_setting("enabled", "true" if enabled else "false")

    def set_setting(self, key: str, value: Any) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO publisher_autopilot_settings_v3(setting_key,setting_value,updated_at)
                   VALUES (?,?,CURRENT_TIMESTAMP)
                   ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value,updated_at=CURRENT_TIMESTAMP""",
                (str(key), str(value)),
            )
            conn.commit()

    def windows(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM publisher_post_windows_v3 WHERE enabled=1 ORDER BY start_time,end_time"
            ).fetchall()
        return [dict(row) for row in rows]

    def add_window(self, start: str, end: str) -> str:
        start, end = _hhmm(start), _hhmm(end)
        if start >= end:
            raise ValueError("发帖时段结束时间必须晚于开始时间")
        window_id = "WIN_" + uuid.uuid4().hex[:16]
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO publisher_post_windows_v3(window_id,start_time,end_time,enabled) VALUES (?,?,?,1)",
                (window_id, start, end),
            )
            conn.commit()
        return window_id

    def delete_window(self, window_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM publisher_post_windows_v3 WHERE window_id=?", (str(window_id),))
            conn.commit()

    def within_window(self, now: datetime | None = None) -> bool:
        cfg = self.config()
        local = now.astimezone(ZoneInfo(cfg.timezone_name)) if now else datetime.now(ZoneInfo(cfg.timezone_name))
        current = local.strftime("%H:%M")
        return any(str(row["start_time"]) <= current < str(row["end_time"]) for row in self.windows())

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
                     state=excluded.state,last_seen_at=CURRENT_TIMESTAMP,
                     last_event_at=CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE v3_component_status.last_event_at END,
                     last_error=excluded.last_error,meta_json=excluded.meta_json,updated_at=CURRENT_TIMESTAMP""",
                (
                    str(component), str(state), int(bool(event)), str(error or "")[:1000],
                    json.dumps(meta or {}, ensure_ascii=False, sort_keys=True), int(bool(event)),
                ),
            )
            conn.commit()

    def component_rows(self) -> dict[str, dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM v3_component_status").fetchall()
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
            dup = int(row["today_duplicates"]) if row is not None and str(row["count_date"]) == today else 0
            count += int(bool(collected))
            dup += int(bool(duplicate))
            conn.execute(
                """INSERT INTO collector_source_state_v3
                   (source_name,enabled,last_collected_at,count_date,today_count,today_duplicates,last_error,updated_at)
                   VALUES (?,?,CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END,?,?,?,?,CURRENT_TIMESTAMP)
                   ON CONFLICT(source_name) DO UPDATE SET
                     last_collected_at=CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE collector_source_state_v3.last_collected_at END,
                     count_date=excluded.count_date,today_count=excluded.today_count,today_duplicates=excluded.today_duplicates,
                     last_error=excluded.last_error,updated_at=CURRENT_TIMESTAMP""",
                (
                    str(source_name), enabled, int(bool(collected)), today, count, dup, str(error or "")[:1000],
                    int(bool(collected)),
                ),
            )
            conn.commit()

    def source_rows(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM collector_source_state_v3 ORDER BY source_name").fetchall()
        return [dict(row) for row in rows]

    def sync_candidates(self) -> int:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """SELECT r.review_id,r.listing_id,r.offer_id,o.offer_type
                   FROM review_items r JOIN listing_offers o ON o.offer_id=r.offer_id
                   WHERE o.offer_status='active' AND r.offer_id<>''"""
            ).fetchall()
            for row in rows:
                is_sale = str(row["offer_type"]) == "sale"
                reason_code = "sale_store_only" if is_sale else ""
                reason_text = ERROR_LABELS.get(reason_code, "")
                # New sale offers are archived out of the rent ops queue.
                state = "ignored" if is_sale else "queued"
                ignored = 1 if is_sale else 0
                conn.execute(
                    """INSERT INTO publisher_auto_items_v3
                       (offer_id,listing_id,review_id,state,reason_code,reason_text,ignored,origin)
                       VALUES (?,?,?,?,?,?,?,'collector')
                       ON CONFLICT(offer_id) DO UPDATE SET
                         listing_id=excluded.listing_id,review_id=excluded.review_id,
                         state=CASE
                           WHEN publisher_auto_items_v3.state='published' THEN 'published'
                           WHEN publisher_auto_items_v3.ignored=1 THEN publisher_auto_items_v3.state
                           WHEN publisher_auto_items_v3.state='held' THEN 'held'
                           WHEN publisher_auto_items_v3.state IN ('preview_ready','sending','unknown')
                             THEN publisher_auto_items_v3.state
                           WHEN excluded.reason_code='sale_store_only'
                                AND publisher_auto_items_v3.state='exception'
                                AND publisher_auto_items_v3.reason_code='sale_store_only'
                             THEN 'exception'
                           WHEN excluded.reason_code='sale_store_only' THEN 'ignored'
                           WHEN publisher_auto_items_v3.state='exception' THEN publisher_auto_items_v3.state
                           ELSE 'queued' END,
                         ignored=CASE
                           WHEN publisher_auto_items_v3.state='published' THEN publisher_auto_items_v3.ignored
                           WHEN publisher_auto_items_v3.ignored=1 THEN 1
                           WHEN excluded.reason_code='sale_store_only'
                                AND publisher_auto_items_v3.state='exception'
                                AND publisher_auto_items_v3.reason_code='sale_store_only'
                             THEN 0
                           WHEN excluded.reason_code='sale_store_only' THEN 1
                           ELSE publisher_auto_items_v3.ignored END,
                         reason_code=CASE WHEN excluded.reason_code<>'' THEN excluded.reason_code ELSE publisher_auto_items_v3.reason_code END,
                         reason_text=CASE WHEN excluded.reason_text<>'' THEN excluded.reason_text ELSE publisher_auto_items_v3.reason_text END,
                         updated_at=CURRENT_TIMESTAMP""",
                    (
                        str(row["offer_id"]), str(row["listing_id"]), str(row["review_id"]),
                        state, reason_code, reason_text, ignored,
                    ),
                )
            conn.commit()
        return len(rows)

    def next_item(self) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT a.*,r.review_status,o.offer_type,o.publication_policy,o.offer_status,
                          o.monthly_rent_usd,l.inventory_status,l.public_listing_id,l.project_name,
                          l.public_location_display,l.layout,l.property_type,l.canonical_record_id
                   FROM publisher_auto_items_v3 a
                   JOIN review_items r ON r.review_id=a.review_id
                   JOIN listing_offers o ON o.offer_id=a.offer_id
                   JOIN listings_v3 l ON l.listing_id=a.listing_id
                   WHERE a.state='queued' AND a.ignored=0
                   ORDER BY a.created_at ASC,a.offer_id ASC LIMIT 1"""
            ).fetchone()
        return dict(row) if row else None

    def set_item(
        self,
        offer_id: str,
        *,
        state: str,
        reason_code: str = "",
        reason_text: str = "",
        package_id: str = "",
        channel_message_id: str = "",
        origin: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """UPDATE publisher_auto_items_v3 SET state=?,reason_code=?,reason_text=?,
                   package_id=CASE WHEN ?<>'' THEN ? ELSE package_id END,
                   channel_message_id=CASE WHEN ?<>'' THEN ? ELSE channel_message_id END,
                   origin=COALESCE(?,origin),
                   published_at=CASE WHEN ?='published' THEN CURRENT_TIMESTAMP ELSE published_at END,
                   updated_at=CURRENT_TIMESTAMP WHERE offer_id=?""",
                (
                    str(state), str(reason_code), str(reason_text),
                    str(package_id), str(package_id), str(channel_message_id), str(channel_message_id),
                    origin, str(state), str(offer_id),
                ),
            )
            conn.commit()

    def requeue(self, offer_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """UPDATE publisher_auto_items_v3 SET state='queued',reason_code='',reason_text='',ignored=0,
                   updated_at=CURRENT_TIMESTAMP WHERE offer_id=?""",
                (str(offer_id),),
            )
            conn.commit()

    def mark_listing_pending_for_auto_publish(self, listing_id: str) -> None:
        """Make the first automatic public state explicitly await confirmation."""
        with self._connect() as conn:
            conn.execute(
                """UPDATE listings_v3
                   SET inventory_status='pending',updated_at=CURRENT_TIMESTAMP
                   WHERE listing_id=? AND inventory_status IN ('active','reserved')""",
                (str(listing_id),),
            )
            conn.commit()

    def ignore(self, offer_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE publisher_auto_items_v3 SET ignored=1,state='ignored',updated_at=CURRENT_TIMESTAMP WHERE offer_id=?",
                (str(offer_id),),
            )
            conn.commit()

    def mark_sale_store_only(self, offer_id: str, *, origin: str | None = None) -> None:
        """Archive a sale/store_only offer out of the rent ops exception queue.

        Sale data and sale-api visibility are unchanged; only Publisher queue
        classification is adjusted so operators are not asked to handle it.
        """
        with self._connect() as conn:
            conn.execute(
                """UPDATE publisher_auto_items_v3
                      SET state='ignored',
                          ignored=1,
                          reason_code='sale_store_only',
                          reason_text=?,
                          origin=COALESCE(?, origin),
                          updated_at=CURRENT_TIMESTAMP
                    WHERE offer_id=?""",
                (ERROR_LABELS["sale_store_only"], origin, str(offer_id)),
            )
            conn.commit()

    def ignore_by_reason(self, reason_code: str) -> int:
        """Bulk-ignore open exceptions for one reason (safe ops cleanup)."""
        code = str(reason_code or "").strip()
        if code != "sale_store_only":
            raise ValueError(f"bulk_ignore_not_allowed:{code}")
        with self._connect() as conn:
            cur = conn.execute(
                """UPDATE publisher_auto_items_v3
                      SET ignored=1,state='ignored',updated_at=CURRENT_TIMESTAMP
                    WHERE state='exception' AND ignored=0 AND reason_code=?""",
                (code,),
            )
            conn.commit()
            return int(cur.rowcount or 0)

    def exception_rows(
        self, *, limit: int = 20, reason_code: str | None = None
    ) -> list[dict[str, Any]]:
        code = str(reason_code or "").strip()
        where = "a.state='exception' AND a.ignored=0"
        params: list[Any] = []
        if code and code != "all":
            if code == "other":
                where += (
                    " AND a.reason_code NOT IN "
                    "('sale_store_only','missing_location','canonical_error')"
                )
            else:
                where += " AND a.reason_code=?"
                params.append(code)
        params.append(max(1, int(limit)))
        with self._connect() as conn:
            rows = conn.execute(
                f"""SELECT a.*,l.public_listing_id,l.display_title,l.project_name,o.monthly_rent_usd
                   FROM publisher_auto_items_v3 a
                   JOIN listings_v3 l ON l.listing_id=a.listing_id
                   JOIN listing_offers o ON o.offer_id=a.offer_id
                   WHERE {where}
                   ORDER BY a.updated_at DESC LIMIT ?""",
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def exception_counts_by_reason(self) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT reason_code, COUNT(*) AS c
                     FROM publisher_auto_items_v3
                    WHERE state='exception' AND ignored=0
                    GROUP BY reason_code"""
            ).fetchall()
        raw = {str(row["reason_code"] or ""): int(row["c"] or 0) for row in rows}
        counts = {
            "sale_store_only": int(raw.get("sale_store_only", 0)),
            "missing_location": int(raw.get("missing_location", 0)),
            "canonical_error": int(raw.get("canonical_error", 0)),
        }
        counts["other"] = sum(
            count
            for code, count in raw.items()
            if code not in {"sale_store_only", "missing_location", "canonical_error"}
        )
        counts["all"] = sum(raw.values())
        return counts

    def queue_count(self) -> int:
        with self._connect() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM publisher_auto_items_v3 WHERE state='queued' AND ignored=0").fetchone()[0])

    def held_count(self) -> int:
        with self._connect() as conn:
            return int(
                conn.execute(
                    "SELECT COUNT(*) FROM publisher_auto_items_v3 WHERE state='held' AND ignored=0"
                ).fetchone()[0]
            )

    def hold_queued_backlog(self, *, reason_code: str = "legacy_backlog") -> int:
        """Pause every currently queued auto item so ops can clear old inventory first.

        New collector inserts still land in ``queued`` and keep the normal autopilot path.
        """
        code = str(reason_code or "legacy_backlog").strip() or "legacy_backlog"
        text = ERROR_LABELS.get(code, "旧库存已暂停自动投递")
        with self._connect() as conn:
            cur = conn.execute(
                """UPDATE publisher_auto_items_v3
                      SET state='held',
                          reason_code=?,
                          reason_text=?,
                          updated_at=CURRENT_TIMESTAMP
                    WHERE state='queued' AND ignored=0""",
                (code, text),
            )
            conn.commit()
            return int(cur.rowcount or 0)

    def release_held_backlog(self, *, reason_code: str = "legacy_backlog") -> int:
        """Return held backlog items to the automatic queue."""
        code = str(reason_code or "legacy_backlog").strip() or "legacy_backlog"
        with self._connect() as conn:
            cur = conn.execute(
                """UPDATE publisher_auto_items_v3
                      SET state='queued',
                          reason_code='',
                          reason_text='',
                          updated_at=CURRENT_TIMESTAMP
                    WHERE state='held' AND ignored=0 AND reason_code=?""",
                (code,),
            )
            conn.commit()
            return int(cur.rowcount or 0)

    def held_rows(self, *, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT a.*,l.public_listing_id,l.display_title,l.project_name,o.monthly_rent_usd
                     FROM publisher_auto_items_v3 a
                     JOIN listings_v3 l ON l.listing_id=a.listing_id
                     JOIN listing_offers o ON o.offer_id=a.offer_id
                    WHERE a.state='held' AND a.ignored=0
                    ORDER BY a.created_at ASC, a.offer_id ASC
                    LIMIT ?""",
                (max(1, int(limit)),),
            ).fetchall()
        return [dict(row) for row in rows]

    def exception_count(self) -> int:
        with self._connect() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM publisher_auto_items_v3 WHERE state='exception' AND ignored=0").fetchone()[0])
    def acquire_lock(self, owner: str, *, ttl_seconds: int = 180) -> bool:
        now = datetime.now(timezone.utc)
        until = now + timedelta(seconds=max(30, int(ttl_seconds)))
        now_s = now.strftime("%Y-%m-%d %H:%M:%S")
        until_s = until.strftime("%Y-%m-%d %H:%M:%S")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT owner,locked_until FROM publisher_autopilot_lock_v3 WHERE lock_name='publish'").fetchone()
            if row is not None and str(row["locked_until"] or "") > now_s and str(row["owner"] or "") != str(owner):
                conn.rollback()
                return False
            conn.execute(
                """INSERT INTO publisher_autopilot_lock_v3(lock_name,owner,locked_until,updated_at)
                   VALUES ('publish',?,?,CURRENT_TIMESTAMP)
                   ON CONFLICT(lock_name) DO UPDATE SET owner=excluded.owner,locked_until=excluded.locked_until,updated_at=CURRENT_TIMESTAMP""",
                (str(owner), until_s),
            )
            conn.commit()
        return True

    def release_lock(self, owner: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE publisher_autopilot_lock_v3 SET owner='',locked_until=NULL,updated_at=CURRENT_TIMESTAMP WHERE lock_name='publish' AND owner=?",
                (str(owner),),
            )
            conn.commit()

    def interval_ready(self, interval_seconds: int) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT MAX(published_at) AS last_at FROM publisher_auto_items_v3 WHERE state='published'"
            ).fetchone()
        raw = str(row["last_at"] or "") if row else ""
        if not raw:
            return True
        try:
            last = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            return True
        return datetime.now(timezone.utc) - last >= timedelta(seconds=max(10, int(interval_seconds)))

    def publication_for_offer(self, offer_id: str, channel_chat_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT p.* FROM publication_instances p
                   WHERE p.offer_id=? AND p.platform='telegram' AND p.channel_chat_id=?
                     AND p.publish_status='published' ORDER BY p.published_at DESC LIMIT 1""",
                (str(offer_id), str(channel_chat_id)),
            ).fetchone()
        return dict(row) if row else None

    def unsafe_delivery_state(self, offer_id: str, channel_chat_id: str) -> str:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT state FROM publication_delivery_attempts_v3
                   WHERE offer_id=? AND channel_chat_id=? AND state IN ('sending','sent','unknown')
                   ORDER BY updated_at DESC LIMIT 1""",
                (str(offer_id), str(channel_chat_id)),
            ).fetchone()
        return str(row["state"]) if row else ""

    def approved_package(self, offer_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT package_id,canonical_facts_hash,status FROM publication_packages_v3
                   WHERE offer_id=? AND status IN ('approved','published') ORDER BY package_version DESC LIMIT 1""",
                (str(offer_id),),
            ).fetchone()
        return dict(row) if row else None

    def listing_rows(self, category: str, *, limit: int = 15) -> list[dict[str, Any]]:
        where = {
            "active": "l.inventory_status IN ('active','reserved')",
            "rented": "l.inventory_status='rented'",
            "offline": "l.inventory_status IN ('inactive','offline')",
            "recent": "p.publish_status='published'",
        }.get(str(category), "1=0")
        with self._connect() as conn:
            rows = conn.execute(
                f"""SELECT l.*,o.offer_id,o.monthly_rent_usd,p.channel_message_id,p.published_at
                    FROM listings_v3 l
                    LEFT JOIN listing_offers o ON o.listing_id=l.listing_id AND o.offer_type='rent' AND o.offer_status='active'
                    LEFT JOIN publication_instances p ON p.listing_id=l.listing_id AND p.platform='telegram'
                    WHERE {where}
                    ORDER BY COALESCE(p.published_at,l.updated_at) DESC LIMIT ?""",
                (max(1, int(limit)),),
            ).fetchall()
        return [dict(row) for row in rows]

    def set_listing_status(self, listing_id: str, status: str) -> None:
        clean = str(status).strip().lower()
        if clean not in {"active", "reserved", "rented", "inactive", "offline"}:
            raise ValueError("unsupported listing status")
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE listings_v3 SET inventory_status=?,updated_at=CURRENT_TIMESTAMP WHERE listing_id=?",
                (clean, str(listing_id)),
            )
            if cur.rowcount != 1:
                raise KeyError(listing_id)
            conn.commit()

    def today_stats(self) -> dict[str, int]:
        cfg = self.config()
        tz = ZoneInfo(cfg.timezone_name)
        start_local = datetime.combine(datetime.now(tz).date(), time.min, tzinfo=tz)
        end_local = start_local + timedelta(days=1)
        start = start_local.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        end = end_local.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with self._connect() as conn:
            q = lambda sql, params=(): int(conn.execute(sql, params).fetchone()[0])
            return {
                "collected": q("SELECT COUNT(*) FROM source_posts WHERE created_at>=? AND created_at<?", (start, end)),
                "parsed": q("SELECT COUNT(*) FROM canonical_records WHERE created_at>=? AND created_at<?", (start, end)),
                "auto_published": q("SELECT COUNT(*) FROM publisher_auto_items_v3 WHERE state='published' AND origin='collector' AND published_at>=? AND published_at<?", (start, end)),
                "manual_published": q("SELECT COUNT(*) FROM publisher_auto_items_v3 WHERE state='published' AND origin='manual' AND published_at>=? AND published_at<?", (start, end)),
                "duplicates": q("SELECT COALESCE(SUM(today_duplicates),0) FROM collector_source_state_v3 WHERE count_date=?", (start_local.date().isoformat(),)),
                "exceptions": q("SELECT COUNT(*) FROM publisher_auto_items_v3 WHERE state='exception' AND updated_at>=? AND updated_at<?", (start, end)),
                "publish_failed": q("SELECT COUNT(*) FROM publication_delivery_attempts_v3 WHERE state IN ('failed_before_send','unknown') AND updated_at>=? AND updated_at<?", (start, end)),
                "bookable": q("SELECT COUNT(*) FROM listings_v3 WHERE inventory_status IN ('active','reserved')"),
                "rented": q("SELECT COUNT(*) FROM listings_v3 WHERE inventory_status='rented'"),
            }


class AutoPublishService:
    def __init__(
        self,
        *,
        workflow: PublisherWorkflowService,
        repository: AutoPublishRepository,
        channel_chat_id: str,
    ):
        self.workflow = workflow
        self.repository = repository
        self.channel_chat_id = str(channel_chat_id or "").strip()
        if not self.channel_chat_id:
            raise ValueError("auto_publish_channel_chat_id_required")
        self.owner = "autopilot-" + uuid.uuid4().hex[:12]
        self.repository.ensure_defaults()

    @staticmethod
    def _critical_quality_flags(facts: dict[str, Any]) -> tuple[str, ...]:
        quality = facts.get("quality") if isinstance(facts.get("quality"), dict) else {}
        flags = [str(x).strip() for x in (quality.get("blocking_flags") or []) if str(x).strip()]
        # Backward-compatible with frozen quality_json that still lists
        # mixed_sale_rent_terms as blocking after rent was confirmed.
        try:
            rent = int(float(facts.get("monthly_rent_usd") or 0))
        except (TypeError, ValueError):
            rent = 0
        if rent > 0:
            flags = [flag for flag in flags if flag != "mixed_sale_rent_terms"]
        # Frozen snapshots may still hard-block these; demote when evidence exists.
        if str(facts.get("layout") or "").strip() or facts.get("bedrooms"):
            flags = [
                flag
                for flag in flags
                if flag not in {"unknown_property_type", "ambiguous_property_type"}
            ]
        if str(facts.get("public_location_key") or facts.get("public_location_display") or "").strip():
            flags = [flag for flag in flags if flag != "ambiguous_market_location"]
        return tuple(flags)

    def _strict_blockers(
        self,
        item: dict[str, Any],
        facts: dict[str, Any],
        media: Any,
        *,
        allow_pending: bool = False,
    ) -> list[str]:
        cfg = self.repository.config()
        blocking: list[str] = []
        if str(item.get("offer_type") or "") != "rent" or str(facts.get("deal_type") or "") == "sale":
            return ["sale_store_only"]
        try:
            rent = int(float(item.get("monthly_rent_usd") or 0))
        except (TypeError, ValueError):
            rent = 0
        if rent <= 0:
            blocking.append("missing_rent")
        project = str(item.get("project_name") or facts.get("project_name") or "").strip()
        location = str(item.get("public_location_display") or facts.get("public_location_display") or "").strip()
        if not project and not location:
            blocking.append("missing_location")
        if not str(item.get("layout") or facts.get("layout") or "").strip() and not facts.get("bedrooms"):
            blocking.append("missing_listing_info")
        if len(tuple(getattr(media, "gallery_paths", ()) or ())) < cfg.min_media:
            blocking.append("insufficient_media")
        cover = str(getattr(media, "cover_source_path", "") or "")
        if not cover or not Path(cover).is_file():
            blocking.append("unreadable_media")
        allowed_statuses = {"active", "reserved"}
        if allow_pending:
            allowed_statuses.add("pending")
        if str(item.get("inventory_status") or "") not in allowed_statuses:
            blocking.append("listing_not_publishable")
        if self._critical_quality_flags(facts):
            blocking.append("canonical_error")
        return list(dict.fromkeys(blocking))

    def _mark_exception(self, offer_id: str, code: str) -> AutoPublishResult:
        if code == "sale_store_only":
            self.repository.mark_sale_store_only(offer_id)
            text = ERROR_LABELS.get(code, code)
            return AutoPublishResult("store_only", offer_id=offer_id, reason_code=code, reason_text=text)
        text = ERROR_LABELS.get(code, code)
        self.repository.set_item(offer_id, state="exception", reason_code=code, reason_text=text)
        return AutoPublishResult("exception", offer_id=offer_id, reason_code=code, reason_text=text)

    async def process_one(self, *, bot: Any, force_offer_id: str = "", origin: str | None = None) -> AutoPublishResult:
        self.repository.sync_candidates()
        if force_offer_id:
            self.repository.requeue(force_offer_id)
            with self.repository._connect() as conn:
                row = conn.execute(
                    """SELECT a.*,r.review_status,o.offer_type,o.publication_policy,o.offer_status,
                              o.monthly_rent_usd,l.inventory_status,l.public_listing_id,l.project_name,
                              l.public_location_display,l.layout,l.property_type,l.canonical_record_id
                       FROM publisher_auto_items_v3 a JOIN review_items r ON r.review_id=a.review_id
                       JOIN listing_offers o ON o.offer_id=a.offer_id JOIN listings_v3 l ON l.listing_id=a.listing_id
                       WHERE a.offer_id=?""",
                    (str(force_offer_id),),
                ).fetchone()
            item = dict(row) if row else None
        else:
            item = self.repository.next_item()
        if item is None:
            return AutoPublishResult("idle")
        offer_id = str(item["offer_id"])
        listing_id = str(item["listing_id"])
        if origin:
            self.repository.set_item(offer_id, state="queued", origin=origin)
        existing = self.repository.publication_for_offer(offer_id, self.channel_chat_id)
        if existing:
            self.repository.set_item(
                offer_id, state="published", reason_code="", reason_text="",
                channel_message_id=str(existing.get("channel_message_id") or ""), origin=origin,
            )
            return AutoPublishResult(
                "already_published", offer_id=offer_id, listing_id=listing_id,
                channel_message_id=str(existing.get("channel_message_id") or ""),
            )
        unsafe = self.repository.unsafe_delivery_state(offer_id, self.channel_chat_id)
        if unsafe in {"sending", "sent", "unknown"}:
            return self._mark_exception(offer_id, "telegram_unknown")
        approved = self.repository.approved_package(offer_id)
        detail = self.workflow.review_detail(str(item["review_id"]))
        if str(detail.review.get("review_status") or "") in {"hold", "rejected"}:
            return self._mark_exception(offer_id, "admin_hold")
        facts = dict(detail.canonical.get("facts") or {})
        try:
            media = await asyncio.to_thread(self.workflow.review_media, review_id=str(item["review_id"]))
        except Exception:
            return self._mark_exception(offer_id, "unreadable_media")
        blockers = self._strict_blockers(item, facts, media, allow_pending=True)
        if blockers:
            return self._mark_exception(offer_id, blockers[0])
        if approved is None:
            self.repository.mark_listing_pending_for_auto_publish(listing_id)
            item["inventory_status"] = "pending"
        if str(detail.review.get("review_status") or "") != "approved":
            await asyncio.to_thread(
                self.workflow.approve_review,
                review_id=str(item["review_id"]),
                operator_user_id="system:auto_publish",
            )
        package: FrozenPackage
        if approved and str(approved.get("status")) == "published":
            existing = self.repository.publication_for_offer(offer_id, self.channel_chat_id)
            if existing:
                self.repository.set_item(
                    offer_id, state="published", channel_message_id=str(existing.get("channel_message_id") or ""), origin=origin,
                )
                return AutoPublishResult("already_published", offer_id=offer_id, listing_id=listing_id)
            return self._mark_exception(offer_id, "telegram_unknown")
        if approved and str(approved.get("status")) == "approved":
            package = self.workflow.package(str(approved["package_id"]))
            if package.canonical_facts_hash != str(detail.canonical.get("facts_hash") or ""):
                return self._mark_exception(offer_id, "canonical_error")
        else:
            package = await asyncio.to_thread(
                self.workflow.build_package_for_review,
                review_id=str(item["review_id"]),
                inventory_status_override="active",
            )
            package = await asyncio.to_thread(
                self.workflow.approve_package,
                package_id=package.package_id,
                approved_by="system:auto_publish",
            )
        self.repository.set_item(offer_id, state="sending", package_id=package.package_id, origin=origin)
        try:
            result = await deliver_approved_package(
                coordinator=self.workflow.delivery,
                adapter=TelegramChannelAdapter(bot),
                package_id=package.package_id,
                channel_chat_id=self.channel_chat_id,
                inventory_status_override="active",
            )
        except DeliveryBlocked as exc:
            code = "telegram_unknown" if "unknown" in str(exc).lower() or "sending" in str(exc).lower() or "sent" in str(exc).lower() else "telegram_failed"
            return self._mark_exception(offer_id, code)
        except Exception:
            return self._mark_exception(offer_id, "telegram_unknown")
        message_id = str(result.publication.channel_message_id)
        self.repository.set_listing_status(listing_id, "active")
        self.repository.set_item(
            offer_id, state="published", package_id=package.package_id,
            channel_message_id=message_id, origin=origin,
        )
        return AutoPublishResult(
            "published", offer_id=offer_id, listing_id=listing_id,
            package_id=package.package_id, channel_message_id=message_id,
        )

    async def scheduled_tick(self, context: Any) -> None:
        self.repository.heartbeat("publisher", state="running")
        self.repository.sync_candidates()
        cfg = self.repository.config()
        if not cfg.enabled or not self.repository.within_window():
            return
        if not self.repository.interval_ready(cfg.interval_seconds):
            return
        if not self.repository.acquire_lock(self.owner, ttl_seconds=max(180, cfg.interval_seconds)):
            return
        try:
            await self.process_one(bot=context.bot)
        finally:
            self.repository.release_lock(self.owner)


__all__ = [
    "AutoPublishConfig",
    "AutoPublishRepository",
    "AutoPublishResult",
    "AutoPublishService",
    "DDL",
    "ERROR_LABELS",
]
