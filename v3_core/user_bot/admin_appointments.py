"""V3 administrator appointment console, isolated from customer navigation."""
from __future__ import annotations

from datetime import datetime
from html import escape
from pathlib import Path
import sqlite3
from typing import Any
from zoneinfo import ZoneInfo

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from v3_core.status_labels import APPOINTMENT_STATUS_LABELS, status_label

from .appointments import APPOINTMENT_MODE_LABELS, appointment_date_matches


class AdminAppointmentReader:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def _connect(self):
        uri = f"file:{self.db_path.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn

    def list_today(self, now: datetime | None = None) -> tuple[dict[str, Any], ...]:
        current = now or datetime.now(ZoneInfo("Asia/Phnom_Penh"))
        target = current.date()
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT a.*,l.public_listing_id,l.display_title,l.project_name
                   FROM appointments_v3 a
                   LEFT JOIN listings_v3 l ON l.listing_id=a.listing_id
                   ORDER BY a.appointment_time,a.id"""
            ).fetchall()
        return tuple(
            dict(row)
            for row in rows
            if appointment_date_matches(row["appointment_date"], target)
        )

    def get(self, appointment_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT a.*,l.public_listing_id,l.display_title,l.project_name
                   FROM appointments_v3 a
                   LEFT JOIN listings_v3 l ON l.listing_id=a.listing_id
                   WHERE a.id=? LIMIT 1""",
                (int(appointment_id),),
            ).fetchone()
        return dict(row) if row is not None else None


def admin_home_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("📅 今日预约", callback_data="adminq:appointments")]])


def _home_row():
    return [InlineKeyboardButton("⬅️ 返回咨询后台", callback_data="adminq:home")]


def build_admin_appointment_detail_text(row: dict[str, Any]) -> str:
    icon, label = status_label(
        APPOINTMENT_STATUS_LABELS,
        row.get("status"),
        ("🟡", "等待确认"),
    )
    mode_key = str(row.get("viewing_mode") or "offline").strip().lower()
    mode_label = APPOINTMENT_MODE_LABELS.get(mode_key, APPOINTMENT_MODE_LABELS["offline"])
    return (
        f"📅 <b>预约详情</b>\n\n{icon} {escape(label)}\n"
        f"客户：{escape(str(row.get('display_name') or row.get('username') or '未填写'))}\n"
        f"房源：{escape(str(row.get('public_listing_id') or '待生成'))}\n"
        f"时间：{escape(str(row.get('appointment_date') or '待安排'))} · {escape(str(row.get('appointment_time') or '待安排'))}\n"
        f"方式：{escape(mode_label)}\n"
        f"联系：{escape(str(row.get('contact_value') or '未填写'))}"
    )


async def show_admin_home(message: Any) -> None:
    await message.reply_text("🧑‍💼 <b>咨询后台</b>\n\n请选择要查看的内容。", parse_mode=ParseMode.HTML, reply_markup=admin_home_keyboard())


async def handle_admin_callback(update: Any, reader: AdminAppointmentReader) -> None:
    query = update.callback_query
    await query.answer()
    data = str(query.data or "")
    if data == "adminq:home":
        await query.edit_message_text("🧑‍💼 <b>咨询后台</b>\n\n请选择要查看的内容。", parse_mode=ParseMode.HTML, reply_markup=admin_home_keyboard())
        return
    if data == "adminq:appointments":
        rows = reader.list_today()
        buttons = []
        for row in rows:
            public_id = str(row.get("public_listing_id") or "待生成")
            name = str(row.get("display_name") or row.get("username") or "客户")
            tm = str(row.get("appointment_time") or "待安排")
            buttons.append([InlineKeyboardButton(f"{tm}｜{name}｜{public_id}"[:60], callback_data=f"adminq:appointment:{int(row['id'])}")])
        buttons.append(_home_row())
        text = "📅 <b>今日预约</b>" + ("\n\n请选择一条预约查看。" if rows else "\n\n今天暂无预约。")
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))
        return
    if data.startswith("adminq:appointment:"):
        raw_id = data.rsplit(":", 1)[-1]
        row = reader.get(int(raw_id)) if raw_id.isdigit() else None
        text = "预约记录已失效。" if row is None else build_admin_appointment_detail_text(row)
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回今日预约", callback_data="adminq:appointments")], _home_row()])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


__all__ = [
    "AdminAppointmentReader",
    "admin_home_keyboard",
    "build_admin_appointment_detail_text",
    "handle_admin_callback",
    "show_admin_home",
]
