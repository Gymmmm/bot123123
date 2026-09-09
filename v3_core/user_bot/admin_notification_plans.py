"""Pure admin-notification plans for V3 User Bot effects."""
from __future__ import annotations

from html import escape as he

from .admin_notifications import AdminNotification
from .appointment_submit_executor import AppointmentSubmitExecution
from .appointments import APPOINTMENT_MODE_LABELS, display_time
from .lead_service import LeadUser
from .public_appointment import PublicAppointmentDraft


CONTACT_SOURCE_LABELS = {
    "hub": "首页联系我们",
    "listing_callback": "房源咨询",
    "daily_broadcast": "每日广播咨询",
    "user_search": "找房咨询",
    "channel": "频道房源",
    "channel_deeplink": "频道房源",
    "search_result": "找房结果",
}


def display_contact_source(source: object) -> str:
    clean = str(source or "").strip().lower()
    return CONTACT_SOURCE_LABELS.get(clean, "用户咨询")


def user_mention_html(user: LeadUser) -> str:
    name = he(str(user.display_name or user.user_id))
    if int(user.user_id or 0) > 0:
        return f'<a href="tg://user?id={int(user.user_id)}">{name}</a>'
    return name


def user_contact_text(user: LeadUser) -> str:
    username = str(user.username or "").strip().lstrip("@")
    if username:
        return f"@{username}"
    return f"tg://user?id={int(user.user_id)}" if int(user.user_id or 0) > 0 else "-"


def general_contact_notification(
    user: LeadUser,
    *,
    source: str,
) -> AdminNotification:
    return AdminNotification(
        title="用户联系我们",
        lines=(
            f"用户：{user_mention_html(user)}",
            f"联系方式：{he(user_contact_text(user))}",
            f"入口：{he(display_contact_source(source))}",
        ),
    )


def appointment_notification(
    user: LeadUser,
    execution: AppointmentSubmitExecution,
    draft: PublicAppointmentDraft,
    *,
    subject: str = "",
    reply_markup=None,
) -> AdminNotification:
    updated = execution.submission.kind == "updated"
    title = f"📅 {'预约时间已修改' if updated else '新预约'} #{execution.submission.appointment_id}"
    mode_label = APPOINTMENT_MODE_LABELS.get(draft.mode, "实地看房")
    display_subject = str(subject or draft.public_listing_id).strip()
    return AdminNotification(
        title=title,
        lines=(
            f"🏠 <b>{he(display_subject)}</b>",
            f"🕐 {he(draft.date)} · {he(display_time(draft.time))}",
            f"📍 {he(mode_label)}",
            f"👤 客户｜{user_mention_html(user)}",
            f"💬 Telegram｜{he(user_contact_text(user))}",
            "<b>当前状态｜🟡 待确认</b>",
        ),
        show_bell=False,
        reply_markup=reply_markup,
    )


__all__ = [
    "CONTACT_SOURCE_LABELS",
    "appointment_notification",
    "display_contact_source",
    "general_contact_notification",
    "user_contact_text",
    "user_mention_html",
]
