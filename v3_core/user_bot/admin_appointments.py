"""V3 administrator appointment console, isolated from customer navigation."""
from __future__ import annotations

from datetime import datetime
from html import escape
import json
from pathlib import Path
import sqlite3
from typing import Any
from zoneinfo import ZoneInfo

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from v3_core.publishing.public_ids import normalize_public_id
from v3_core.status_labels import APPOINTMENT_STATUS_LABELS, status_label

from .appointments import APPOINTMENT_MODE_LABELS


class AdminAppointmentReader:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def _connect(self):
        uri = f"file:{self.db_path.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _public_row(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        if str(value.get("public_listing_id") or "").strip():
            value.pop("published_snapshot_json", None)
            return value
        raw = value.pop("published_snapshot_json", "")
        try:
            snapshot = json.loads(str(raw or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            snapshot = {}
        if isinstance(snapshot, dict):
            resolved = normalize_public_id(snapshot.get("public_listing_id"))
            if resolved:
                value["public_listing_id"] = resolved
        return value

    @staticmethod
    def _select_sql(where_clause: str) -> str:
        return f"""SELECT a.*,l.public_listing_id,l.display_title,l.project_name,
                          (SELECT pp.snapshot_json
                           FROM publication_instances pi
                           JOIN publication_packages_v3 pp ON pp.package_id=pi.package_id
                           WHERE pi.listing_id=a.listing_id
                             AND pi.platform='telegram'
                             AND pi.publish_status='published'
                           ORDER BY pi.updated_at DESC,pi.id DESC
                           LIMIT 1) AS published_snapshot_json
                   FROM appointments_v3 a
                   LEFT JOIN listings_v3 l ON l.listing_id=a.listing_id
                   WHERE {where_clause}"""

    def list_today(self, now: datetime | None = None) -> tuple[dict[str, Any], ...]:
        current = now or datetime.now(ZoneInfo("Asia/Phnom_Penh"))
        candidates = (
            current.strftime("%Y-%m-%d"),
            current.strftime("%m-%d"),
            f"{current.month}月{current.day}日",
        )
        with self._connect() as conn:
            rows = conn.execute(
                self._select_sql("a.appointment_date IN (?,?,?)")
                + " ORDER BY a.appointment_time,a.id",
                candidates,
            ).fetchall()
        return tuple(self._public_row(row) for row in rows)

    def get(self, appointment_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                self._select_sql("a.id=?") + " LIMIT 1",
                (int(appointment_id),),
            ).fetchone()
        return self._public_row(row) if row is not None else None


def admin_home_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("📅 今日预约", callback_data="adminq:appointments")]])


def _home_row():
    return [InlineKeyboardButton("⬅️ 返回咨询后台", callback_data="adminq:home")]


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
        if row is None:
            text = "预约记录已失效。"
        else:
            icon, label = status_label(APPOINTMENT_STATUS_LABELS, row.get("status"), ("🟡", "等待确认"))
            mode = APPOINTMENT_MODE_LABELS.get(
                str(row.get("viewing_mode") or "offline").strip().lower(),
                APPOINTMENT_MODE_LABELS["offline"],
            )
            text = (
                f"📅 <b>预约详情</b>\n\n{icon} {escape(label)}\n"
                f"客户：{escape(str(row.get('display_name') or row.get('username') or '未填写'))}\n"
                f"房源：{escape(str(row.get('public_listing_id') or '待生成'))}\n"
                f"时间：{escape(str(row.get('appointment_date') or '待安排'))} · {escape(str(row.get('appointment_time') or '待安排'))}\n"
                f"方式：{escape(mode)}\n"
                f"联系：{escape(str(row.get('contact_value') or '未填写'))}"
            )
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回今日预约", callback_data="adminq:appointments")], _home_row()])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)

