"""Public-only success view for a persisted V3 appointment."""
from __future__ import annotations

from html import escape as he

from .appointments import APPOINTMENT_MODE_LABELS, display_date, display_time
from .listing_presenter import build_public_listing_details
from .public_appointment import PublicAppointmentDraft
from .public_inventory import PublicInventoryReader
from .transition_views import TransitionChoice, TransitionView


def build_appointment_success_view(
    draft: PublicAppointmentDraft,
    inventory: PublicInventoryReader,
    *,
    submission_kind: str,
) -> TransitionView:
    """Render durable submission state without exposing an internal listing id."""
    subject = ""
    try:
        published = inventory.resolve(draft.public_listing_id)
        if published is not None:
            details = build_public_listing_details(published)
            subject = details.subject or details.location or ""
    except (FileNotFoundError, OSError, ValueError):
        subject = ""

    if str(submission_kind or "") == "updated":
        heading = "✅ <b>预约时间已修改</b>"
    elif draft.mode == "video":
        heading = "✅ <b>视频看房申请已提交</b>"
    else:
        heading = "✅ <b>预约申请已提交</b>"
    mode_label = APPOINTMENT_MODE_LABELS.get(draft.mode, "实地看房")
    lines = [heading, ""]
    if subject:
        lines.append(f"🏠 <b>{he(subject)}</b>")
    lines.extend(
        [
            f"🆔 {he(draft.public_listing_id)}",
            f"🗓 {he(display_date(draft.date))} · {he(display_time(draft.time))}",
            f"📍 {he(mode_label)}",
            "",
            "🟡 待顾问确认",
            "顾问确认后会通过 Telegram 通知您。",
        ]
    )
    if draft.mode == "video":
        lines.append("视频通话入口会在确认后按约定时间发送。")

    return TransitionView(
        kind="appointment_success",
        text="\n".join(lines),
        rows=(
            (TransitionChoice("📅 查看我的预约", "home", "appointments"),),
            (
                TransitionChoice(
                    "⬅️ 返回租赁详情",
                    "listing_details",
                    public_listing_id=draft.public_listing_id,
                ),
                TransitionChoice("🏠 返回首页", "home"),
            ),
        ),
    )


__all__ = ["build_appointment_success_view"]
