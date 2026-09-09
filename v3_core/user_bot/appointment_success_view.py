"""Public-only success view for a persisted V3 appointment."""
from __future__ import annotations

from html import escape as he

from .appointments import APPOINTMENT_MODE_LABELS, display_time
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
            f"📅 {he(_date_display(draft.date))} · {he(display_time(draft.time))}",
            f"{'🎥' if draft.mode == 'video' else '👀'} {he(mode_label)}",
            f"🆔 {he(draft.public_listing_id)}",
            "",
        ]
    )
    if draft.mode == "video":
        lines.extend(
            [
                "顾问确认时间后，",
                "会在预约前通过 Telegram 发送视频通话入口。",
            ]
        )
    else:
        lines.extend(
            [
                "顾问会先确认最新房态和具体时间，",
                "确认后通过 Telegram 联系您。",
            ]
        )

    return TransitionView(
        kind="appointment_success",
        text="\n".join(lines),
        rows=(
            (
                TransitionChoice(
                    "⬅️ 返回房源",
                    "listing_details",
                    public_listing_id=draft.public_listing_id,
                ),
                TransitionChoice("🏠 返回首页", "home"),
            ),
        ),
    )


__all__ = ["build_appointment_success_view"]
