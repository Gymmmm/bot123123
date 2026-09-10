"""Issue #25 appointment confirmation view.

This module only renders public appointment state. Persistence remains owned by
the existing AppointmentSubmitExecutor and is invoked only by the explicit
submit callback.
"""
from __future__ import annotations

from html import escape as he

from .appointments import display_time
from .listing_presenter import build_public_listing_details
from .public_appointment import PublicAppointmentDraft
from .public_inventory import PublicInventoryReader
from .transition_views import TransitionChoice, TransitionView


def _date_display(value: object) -> str:
    raw = str(value or "").strip()
    bits = raw.replace("/", "-").split("-")
    if len(bits) >= 2 and all(part.isdigit() for part in bits[-2:]):
        return f"{int(bits[-2])}月{int(bits[-1])}日"
    return raw


def _time_display(value: object) -> str:
    raw = str(value or "").strip()
    return {
        "am": "上午",
        "pm": "下午",
        "evening": "晚上",
    }.get(raw, display_time(raw))


def build_appointment_confirmation_view(
    draft: PublicAppointmentDraft,
    inventory: PublicInventoryReader,
) -> TransitionView:
    """Render the locked date/time -> confirmation boundary from Issue #25."""
    if not draft.ready:
        raise ValueError("appointment_confirmation_requires_ready_draft")

    published = inventory.resolve(draft.public_listing_id)
    if published is None:
        raise ValueError("listing_not_publicly_published")
    if not published.bookable:
        raise ValueError("listing_not_bookable")

    details = build_public_listing_details(published)
    subject = details.subject or details.location or "这套房"
    price = (
        f"${int(details.monthly_rent_usd):,}/月"
        if details.monthly_rent_usd is not None and int(details.monthly_rent_usd) > 0
        else ""
    )
    is_video = draft.mode == "video"
    heading = "🎥 <b>确认视频看房</b>" if is_video else "📅 <b>确认看房预约</b>"
    mode_line = "🎥 视频看房" if is_video else "👀 实地看房"

    lines = [heading, "", f"🏠 <b>{he(subject)}</b>"]
    if price:
        lines.append(f"💰 <b>{he(price)}</b>")
    lines.extend(
        [
            "",
            f"📅 {he(_date_display(draft.date))}",
            f"🕐 {he(_time_display(draft.time))}",
            mode_line,
            "",
            "提交后，中文顾问会先确认房态和具体时间。",
        ]
    )

    return TransitionView(
        kind="appointment_confirmation",
        text="\n".join(lines),
        rows=(
            (TransitionChoice("✅ 提交预约", "appointment_submit"),),
            (TransitionChoice("⬅️ 修改时间", "appointment_back_time"),),
        ),
    )


__all__ = ["build_appointment_confirmation_view"]
