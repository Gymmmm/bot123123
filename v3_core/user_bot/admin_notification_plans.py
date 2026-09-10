"""Pure admin-notification plans for V3 User Bot effects."""
from __future__ import annotations

from html import escape as he

from .admin_notifications import AdminNotification
from .appointment_submit_executor import AppointmentSubmitExecution
from .appointments import APPOINTMENT_MODE_LABELS, display_date, display_time
from .lead_service import LeadUser
from .public_appointment import PublicAppointmentDraft
from .source_display import source_display_label


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
        title="💬 新用户咨询",
        lines=(
            f"👤 {user_mention_html(user)}",
            f"📱 {he(user_contact_text(user))}",
            f"📍 来源｜{he(source_display_label(source))}",
        ),
        show_bell=False,
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
    title = "📅 预约时间已修改" if updated else "📅 新看房预约"
    mode_label = APPOINTMENT_MODE_LABELS.get(draft.mode, "实地看房")
    display_subject = str(subject or draft.public_listing_id).strip()
    return AdminNotification(
        title=title,
        lines=(
            f"🏠 <b>{he(display_subject)}</b>",
            f"🆔 {he(draft.public_listing_id)}",
            "",
            f"👤 {user_mention_html(user)}",
            f"📱 {he(user_contact_text(user))}",
            f"🗓 {he(display_date(draft.date))} · {he(display_time(draft.time))}",
            f"📍 {he(mode_label)}",
            "",
            "🟡 <b>待确认</b>",
        ),
        show_bell=False,
        reply_markup=reply_markup,
    )


__all__ = [
    "appointment_notification",
    "general_contact_notification",
    "user_contact_text",
    "user_mention_html",
]
