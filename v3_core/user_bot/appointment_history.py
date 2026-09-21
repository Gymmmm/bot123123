"""Read-only V3 appointment history and public presentation.

The fixed-SHA User Bot renders recent appointments from its legacy appointment
and listing tables. V3 keeps the same compact list semantics but crosses the
storage boundary through ``appointments_v3`` and ``listings_v3`` only. Internal
listing ids are never exposed by the returned view models or rendered text.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from html import escape as he
from pathlib import Path
import sqlite3
from typing import Iterable
from zoneinfo import ZoneInfo

from v3_core.publishing.public_ids import normalize_public_id
from v3_core.status_labels import APPOINTMENT_STATUS_LABELS

from .appointments import APPOINTMENT_MODE_LABELS, APPOINTMENT_TIME_LABELS
from .listing_presenter import build_public_listing_details
from .public_inventory import PublicInventoryReader


@dataclass(frozen=True)
class AppointmentHistoryRecord:
    appointment_id: int
    public_listing_id: str
    viewing_mode: str
    appointment_date: str
    appointment_time: str
    status: str
    created_at: str


@dataclass(frozen=True)
class AppointmentHistoryItem:
    appointment_id: int
    public_listing_id: str
    subject: str
    viewing_mode: str
    appointment_date: str
    appointment_time: str
    status: str


@dataclass(frozen=True)
class AppointmentHistoryView:
    text: str
    items: tuple[AppointmentHistoryItem, ...]
    history_count: int


class SQLiteAppointmentHistoryReader:
    """Read ``appointments_v3`` without creating or mutating the database."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def _connect(self) -> sqlite3.Connection:
        if not self.db_path.is_file():
            raise FileNotFoundError(self.db_path)
        uri = f"file:{self.db_path.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def list_for_user(self, user_id: int, *, limit: int = 20) -> tuple[AppointmentHistoryRecord, ...]:
        cap = max(1, min(int(limit or 20), 100))
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT a.id,a.viewing_mode,a.appointment_date,a.appointment_time,
                          a.status,a.created_at,l.public_listing_id
                   FROM appointments_v3 a
                   JOIN listings_v3 l ON l.listing_id=a.listing_id
                   WHERE a.user_id=?
                   ORDER BY a.created_at DESC,a.id DESC
                   LIMIT ?""",
                (int(user_id), cap),
            ).fetchall()

        records: list[AppointmentHistoryRecord] = []
        for row in rows:
            public_id = normalize_public_id(row["public_listing_id"])
            if public_id is None:
                continue
            records.append(
                AppointmentHistoryRecord(
                    appointment_id=int(row["id"]),
                    public_listing_id=public_id,
                    viewing_mode=str(row["viewing_mode"] or "offline"),
                    appointment_date=str(row["appointment_date"] or ""),
                    appointment_time=str(row["appointment_time"] or ""),
                    status=str(row["status"] or "pending").strip().lower(),
                    created_at=str(row["created_at"] or ""),
                )
            )
        return tuple(records)


def _date_compact(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "待安排"
    parts = raw.replace("/", "-").split("-")
    if len(parts) >= 2 and all(part.isdigit() for part in parts[-2:]):
        return f"{int(parts[-2])}月{int(parts[-1])}日"
    return raw


def _time_compact(value: object) -> str:
    raw = str(value or "").strip()
    label = APPOINTMENT_TIME_LABELS.get(raw, raw or "待安排")
    return label.replace("-", "–").replace("至", "–").strip() or "待安排"


def _sort_key(record: AppointmentHistoryRecord) -> tuple[int, int, str]:
    bits = record.appointment_date.replace("/", "-").split("-")
    try:
        nums = [int(part) for part in bits if part.isdigit()]
        return (nums[-2], nums[-1], record.appointment_time)
    except (ValueError, IndexError):
        return (0, 0, "")


def _is_upcoming(record: AppointmentHistoryRecord, *, now: datetime) -> bool:
    if record.status not in {"pending", "confirmed"}:
        return False
    bits = record.appointment_date.replace("/", "-").split("-")
    try:
        nums = [int(part) for part in bits if part.isdigit()]
        month, day = nums[-2], nums[-1]
        return (month, day) >= (now.month, now.day)
    except (ValueError, IndexError):
        return True


def _subject(record: AppointmentHistoryRecord, inventory: PublicInventoryReader) -> str:
    view = inventory.resolve(record.public_listing_id)
    if view is None:
        return "这套房"
    details = build_public_listing_details(view)
    return details.subject or details.location or "这套房"


def _item(record: AppointmentHistoryRecord, inventory: PublicInventoryReader) -> AppointmentHistoryItem:
    return AppointmentHistoryItem(
        appointment_id=record.appointment_id,
        public_listing_id=record.public_listing_id,
        subject=_subject(record, inventory),
        viewing_mode=record.viewing_mode,
        appointment_date=record.appointment_date,
        appointment_time=record.appointment_time,
        status=record.status,
    )


def _lines(item: AppointmentHistoryItem) -> list[str]:
    status_icon, status_label = {"pending": ("🟡", "待顾问确认"), "confirmed": ("🟢", "看房已确认")}.get(item.status, ("🟡", "待顾问确认"))
    mode = APPOINTMENT_MODE_LABELS.get(item.viewing_mode, item.viewing_mode or "待确认")
    return [
        f"<b>{he(item.subject)}</b>",
        f"{he(_date_compact(item.appointment_date))} · {he(_time_compact(item.appointment_time))} · {he(mode)}",
        f"{status_icon} {he(status_label)}",
    ]


class AppointmentHistoryService:
    def __init__(
        self,
        *,
        reader: SQLiteAppointmentHistoryReader,
        inventory: PublicInventoryReader,
    ):
        self.reader = reader
        self.inventory = inventory

    def build(self, user_id: int, *, now: datetime | None = None) -> AppointmentHistoryView:
        records = self.reader.list_for_user(user_id, limit=20)
        if not records:
            return AppointmentHistoryView(
                text=(
                    "<b>我的预约</b>\n\n"
                    "目前没有待进行的看房预约。\n"
                    "您可以继续找房，看到合适的房源后直接预约。"
                ),
                items=(),
                history_count=0,
            )

        local_now = now or datetime.now(ZoneInfo("Asia/Phnom_Penh"))
        upcoming_records = tuple(record for record in records if _is_upcoming(record, now=local_now))
        history_records = tuple(record for record in records if record not in upcoming_records)
        if not upcoming_records:
            return AppointmentHistoryView(
                text=(
                    "<b>我的预约</b>\n\n"
                    "目前没有待进行的看房预约。\n"
                    "您可以继续找房，看到合适的房源后直接预约。"
                ),
                items=(),
                history_count=len(history_records),
            )
        items = tuple(_item(record, self.inventory) for record in upcoming_records[:2])

        parts = ["<b>我的预约</b>", ""]
        for index, item in enumerate(items):
            parts.extend(_lines(item))
            if index < len(items) - 1:
                parts.append("")
        if len(upcoming_records) > 2:
            parts.extend(["", "更多预约记录请联系中文顾问。"])
        return AppointmentHistoryView(
            text="\n".join(parts),
            items=items,
            history_count=len(history_records),
        )


__all__ = [
    "AppointmentHistoryItem",
    "AppointmentHistoryRecord",
    "AppointmentHistoryService",
    "AppointmentHistoryView",
    "SQLiteAppointmentHistoryReader",
]
