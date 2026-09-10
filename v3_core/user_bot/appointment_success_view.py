"""Public-only success view for a persisted V3 appointment."""
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
    return raw or "待安排"


def build_appointment_success_view(
    draft: PublicAppointmentDraft,
    inventory: PublicInventoryReader,
    *,
    submission_kind: str,
) -> TransitionView:
    """Render success without ever exposing the internal listing id."""
    subject = ""
    try:
        published = inventory.resolve(draft.public_listing_id)
        if published is not None:
            details = build_public_listing_details(published)
            subject = details.subject or details.location or ""
    except (FileNotFoundError, OSError, ValueError):
        # Persistence has already succeeded. A display-only lookup must never
        # turn that success into an internal-id leak or a false failure page.
        subject = ""

    if str(submission_kind or "") == "updated":
        heading = "✅ <b>预约时间已修改</b>"
    else:
        heading = "✅ <b>预约申请已提交</b>"

    short_time = {
        "am": "上午",
        "pm": "下午",
        "evening": "晚上",
    }.get(str(draft.time or "").strip(), display_time(draft.time))
    lines = [heading, ""]
    if subject:
        lines.append(f"🏠 <b>{he(subject)}</b>")
    lines.extend(
        [
            f"📅 {he(_date_display(draft.date))} · {he(short_time)}",
            f"🆔 {he(draft.public_listing_id)}",
            "",
            "中文顾问会确认房态和具体时间，",
            "之后通过 Telegram 联系你。",
            "",
            "房源信息已经带上，不用重复发送。",
        ]
    )

    return TransitionView(
        kind="appointment_success",
        text="\n".join(lines),
        rows=(
            (
                TransitionChoice("📅 我的预约", "home", value="appointments"),
                TransitionChoice("🏠 继续看房", "home"),
            ),
            (TransitionChoice("💬 联系中文顾问", "home", value="contact"),),
        ),
    )


__all__ = ["build_appointment_success_view"]
