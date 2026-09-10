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

from .appointments import APPOINTMENT_MODE_LABELS, appointment_date_matches, display_date, display_time
from .callbacks import encode_listing_callback
from .telegram_navigation import advisor_handoff_url


_ADMIN_MUTABLE_STATUSES = frozenset({"pending", "assigned", "contacted", "confirmed"})
_ADMIN_ACTION_STATUS = {
    "confirm": "confirmed",
    "contacted": "contacted",
    "cancel": "cancelled",
}


class AdminAppointmentReader:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def _connect(self, *, readonly: bool = True):
        if readonly:
            uri = f"file:{self.db_path.as_posix()}?mode=ro"
            conn = sqlite3.connect(uri, uri=True, timeout=5)
        else:
            conn = sqlite3.connect(str(self.db_path), timeout=30)
            conn.execute("PRAGMA busy_timeout=30000")
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _public_row(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        direct = normalize_public_id(value.get("public_listing_id"))
        raw = value.pop("published_snapshot_json", "")
        if direct:
            value["public_listing_id"] = direct
            return value
        try:
            snapshot = json.loads(str(raw or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            snapshot = {}
        if isinstance(snapshot, dict):
            resolved = normalize_public_id(snapshot.get("public_listing_id"))
            if resolved:
                value["public_listing_id"] = resolved
            else:
                value["public_listing_id"] = ""
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
        target = current.date()
        with self._connect() as conn:
            rows = conn.execute(
                self._select_sql("1=1") + " ORDER BY a.appointment_time,a.id"
            ).fetchall()
        return tuple(
            self._public_row(row)
            for row in rows
            if appointment_date_matches(row["appointment_date"], target)
        )

    def list_pending(self, *, limit: int = 30) -> tuple[dict[str, Any], ...]:
        cap = max(1, min(int(limit or 30), 100))
        with self._connect() as conn:
            rows = conn.execute(
                self._select_sql("a.status IN ('pending','assigned','contacted')")
                + " ORDER BY a.created_at ASC,a.id ASC LIMIT ?",
                (cap,),
            ).fetchall()
        return tuple(self._public_row(row) for row in rows)

    def get(self, appointment_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                self._select_sql("a.id=?") + " LIMIT 1",
                (int(appointment_id),),
            ).fetchone()
        return self._public_row(row) if row is not None else None

    def update_status(self, appointment_id: int, status: str) -> tuple[dict[str, Any] | None, bool]:
        clean_status = str(status or "").strip().lower()
        if clean_status not in {"confirmed", "contacted", "cancelled"}:
            raise ValueError("unsupported_admin_appointment_status")
        current = self.get(int(appointment_id))
        if current is None:
            return None, False
        previous = str(current.get("status") or "pending").strip().lower()
        if previous == clean_status:
            return current, False
        if previous not in _ADMIN_MUTABLE_STATUSES:
            return current, False
        with self._connect(readonly=False) as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """UPDATE appointments_v3
                   SET status=?,updated_at=CURRENT_TIMESTAMP
                   WHERE id=? AND status=?""",
                (clean_status, int(appointment_id), previous),
            )
            conn.commit()
        changed = int(cursor.rowcount or 0) == 1
        return self.get(int(appointment_id)), changed

    def counts(self, now: datetime | None = None) -> tuple[int, int]:
        return len(self.list_today(now)), len(self.list_pending(limit=100))


def admin_home_keyboard(reader: AdminAppointmentReader | None = None) -> InlineKeyboardMarkup:
    today_count = pending_count = 0
    if reader is not None:
        today_count, pending_count = reader.counts()
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"🟡 待处理预约 ({pending_count})", callback_data="adminq:pending")],
            [InlineKeyboardButton(f"📅 今日预约 ({today_count})", callback_data="adminq:appointments")],
        ]
    )


def _home_row():
    return [InlineKeyboardButton("⬅️ 返回咨询后台", callback_data="adminq:home")]


def _contact_user_url(row: dict[str, Any]) -> str:
    username = str(row.get("username") or "").strip().lstrip("@")
    if username:
        return f"https://t.me/{username}"
    user_id = int(row.get("user_id") or 0)
    return f"tg://user?id={user_id}" if user_id > 0 else ""


def _appointment_action_rows(row: dict[str, Any]) -> list[list[InlineKeyboardButton]]:
    appointment_id = int(row.get("id") or 0)
    status = str(row.get("status") or "pending").strip().lower()
    rows: list[list[InlineKeyboardButton]] = []
    if status in _ADMIN_MUTABLE_STATUSES:
        if status != "confirmed":
            rows.append([InlineKeyboardButton("✅ 确认预约", callback_data=f"adminq:appointment:confirm:{appointment_id}")])
        if status != "contacted":
            rows.append([InlineKeyboardButton("☎️ 标记已联系", callback_data=f"adminq:appointment:contacted:{appointment_id}")])
        rows.append([InlineKeyboardButton("❌ 无法安排", callback_data=f"adminq:appointment:cancel:{appointment_id}")])
    user_url = _contact_user_url(row)
    if user_url:
        rows.append([InlineKeyboardButton("👤 联系用户", url=user_url)])
    return rows


def build_admin_appointment_detail_text(row: dict[str, Any]) -> str:
    icon, label = status_label(
        APPOINTMENT_STATUS_LABELS,
        row.get("status"),
        ("🟡", "等待确认"),
    )
    mode_key = str(row.get("viewing_mode") or "offline").strip().lower()
    mode_label = APPOINTMENT_MODE_LABELS.get(mode_key, APPOINTMENT_MODE_LABELS["offline"])
    public_id = str(row.get("public_listing_id") or "待生成")
    subject = str(row.get("display_title") or row.get("project_name") or "").strip()
    lines = [
        "📅 <b>预约详情</b>",
        "",
        f"{icon} <b>{escape(label)}</b>",
    ]
    if subject:
        lines.append(f"🏠 {escape(subject)}")
    lines.extend(
        [
            f"🆔 {escape(public_id)}",
            "",
            f"👤 {escape(str(row.get('display_name') or row.get('username') or '未填写'))}",
            f"📱 {escape(str(row.get('contact_value') or '未填写'))}",
            f"🗓 {escape(display_date(row.get('appointment_date')))} · {escape(display_time(row.get('appointment_time')))}",
            f"📍 {escape(mode_label)}",
        ]
    )
    return "\n".join(lines)


async def show_admin_home(message: Any, reader: AdminAppointmentReader | None = None) -> None:
    today_count = pending_count = 0
    if reader is not None:
        today_count, pending_count = reader.counts()
    await message.reply_text(
        "🧑‍💼 <b>咨询后台</b>\n\n"
        f"🟡 待处理预约：<b>{pending_count}</b>\n"
        f"📅 今日预约：<b>{today_count}</b>\n\n"
        "先处理待确认和待跟进预约。",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_home_keyboard(reader),
    )


async def _notify_user_status(
    context: Any,
    row: dict[str, Any],
    *,
    status: str,
    advisor_url: str = "",
    channel_url: str = "",
) -> None:
    if context is None or getattr(context, "bot", None) is None:
        return
    user_id = int(row.get("user_id") or 0)
    if user_id <= 0:
        return
    public_id = str(row.get("public_listing_id") or "").strip()
    subject = str(row.get("display_title") or row.get("project_name") or public_id or "这套房").strip()
    schedule = f"{display_date(row.get('appointment_date'))} · {display_time(row.get('appointment_time'))}"
    if status == "confirmed":
        text = (
            "✅ <b>预约已确认</b>\n\n"
            f"🏠 {escape(subject)}\n"
            f"🆔 {escape(public_id or '待生成')}\n"
            f"🗓 {escape(schedule)}\n\n"
            "请按约定时间看房。如需调整，可以联系中文顾问。"
        )
    elif status == "cancelled":
        text = (
            "⚠️ <b>这次预约暂时无法安排</b>\n\n"
            f"🏠 {escape(subject)}\n"
            f"🆔 {escape(public_id or '待生成')}\n"
            f"🗓 原预约：{escape(schedule)}\n\n"
            "可以重新选择时间，或联系中文顾问协助安排。"
        )
    else:
        return
    rows: list[list[InlineKeyboardButton]] = []
    if public_id:
        if status == "cancelled":
            rows.append([InlineKeyboardButton("📅 重新预约", callback_data=encode_listing_callback("book", public_id))])
        rows.append([InlineKeyboardButton("🏠 租赁详情", callback_data=encode_listing_callback("details", public_id))])
    clean_advisor = str(advisor_url or "").strip()
    if clean_advisor:
        rows.append([
            InlineKeyboardButton(
                "💬 联系中文顾问",
                url=advisor_handoff_url(clean_advisor, public_listing_id=public_id),
            )
        ])
    else:
        rows.append([InlineKeyboardButton("💬 联系中文顾问", callback_data="v3u:home:contact")])
    rows.append([InlineKeyboardButton("📅 查看我的预约", callback_data="v3u:home:appointments")])
    clean_channel = str(channel_url or "").strip()
    if clean_channel:
        rows.append([InlineKeyboardButton("📣 返回房源频道", url=clean_channel)])
    rows.append([InlineKeyboardButton("🏠 返回首页", callback_data="v3u:t:home")])
    await context.bot.send_message(
        chat_id=user_id,
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


async def _sync_after_status_change(
    row: dict[str, Any],
    *,
    availability: Any = None,
    channel_sync: Any = None,
) -> None:
    listing_id = str(row.get("listing_id") or "").strip()
    if not listing_id:
        return
    if availability is not None:
        try:
            availability.recompute(listing_id)
        except Exception:
            pass
    if channel_sync is not None:
        try:
            await channel_sync.sync(listing_id)
        except Exception:
            pass


def _list_markup(rows: tuple[dict[str, Any], ...], *, back_callback: str = "adminq:home") -> InlineKeyboardMarkup:
    buttons: list[list[InlineKeyboardButton]] = []
    for row in rows:
        public_id = str(row.get("public_listing_id") or "待生成")
        name = str(row.get("display_name") or row.get("username") or "客户")
        date_text = display_date(row.get("appointment_date"))
        tm = display_time(row.get("appointment_time"))
        buttons.append(
            [
                InlineKeyboardButton(
                    f"{date_text} {tm}｜{name}｜{public_id}"[:60],
                    callback_data=f"adminq:appointment:{int(row['id'])}",
                )
            ]
        )
    buttons.append([InlineKeyboardButton("⬅️ 返回咨询后台", callback_data=back_callback)])
    return InlineKeyboardMarkup(buttons)


async def handle_admin_callback(
    update: Any,
    reader: AdminAppointmentReader,
    *,
    context: Any = None,
    availability: Any = None,
    channel_sync: Any = None,
    advisor_url: str = "",
    channel_url: str = "",
) -> None:
    query = update.callback_query
    await query.answer()
    data = str(query.data or "")

    if data == "adminq:home":
        today_count, pending_count = reader.counts()
        await query.edit_message_text(
            "🧑‍💼 <b>咨询后台</b>\n\n"
            f"🟡 待处理预约：<b>{pending_count}</b>\n"
            f"📅 今日预约：<b>{today_count}</b>\n\n"
            "先处理待确认和待跟进预约。",
            parse_mode=ParseMode.HTML,
            reply_markup=admin_home_keyboard(reader),
        )
        return

    if data == "adminq:appointments":
        rows = reader.list_today()
        text = "📅 <b>今日预约</b>" + ("\n\n请选择一条预约处理。" if rows else "\n\n今天暂无预约。")
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=_list_markup(rows))
        return

    if data == "adminq:pending":
        rows = reader.list_pending()
        text = "🟡 <b>待处理预约</b>" + ("\n\n请选择一条预约处理。" if rows else "\n\n当前没有待处理预约。")
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=_list_markup(rows))
        return

    parts = data.split(":")
    if len(parts) == 4 and parts[:2] == ["adminq", "appointment"] and parts[2] in _ADMIN_ACTION_STATUS:
        action, raw_id = parts[2], parts[3]
        row = reader.get(int(raw_id)) if raw_id.isdigit() else None
        if row is None:
            await query.edit_message_text(
                "预约记录已失效。",
                reply_markup=InlineKeyboardMarkup([_home_row()]),
            )
            return
        target = _ADMIN_ACTION_STATUS[action]
        updated_row, changed = reader.update_status(int(raw_id), target)
        row = updated_row or row
        if changed:
            await _sync_after_status_change(row, availability=availability, channel_sync=channel_sync)
            await _notify_user_status(
                context,
                row,
                status=target,
                advisor_url=advisor_url,
                channel_url=channel_url,
            )
        text = build_admin_appointment_detail_text(row)
        if not changed:
            text += "\n\n状态没有重复变更。"
        markup_rows = _appointment_action_rows(row)
        markup_rows.extend(
            [
                [InlineKeyboardButton("⬅️ 返回待处理预约", callback_data="adminq:pending")],
                _home_row(),
            ]
        )
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(markup_rows))
        return

    if data.startswith("adminq:appointment:"):
        raw_id = data.rsplit(":", 1)[-1]
        row = reader.get(int(raw_id)) if raw_id.isdigit() else None
        if row is None:
            text = "预约记录已失效。"
            markup_rows = [_home_row()]
        else:
            text = build_admin_appointment_detail_text(row)
            markup_rows = _appointment_action_rows(row)
            markup_rows.extend(
                [
                    [InlineKeyboardButton("⬅️ 返回今日预约", callback_data="adminq:appointments")],
                    _home_row(),
                ]
            )
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(markup_rows))


__all__ = [
    "AdminAppointmentReader",
    "admin_home_keyboard",
    "build_admin_appointment_detail_text",
    "handle_admin_callback",
    "show_admin_home",
]
