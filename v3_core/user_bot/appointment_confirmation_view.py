"""Appointment confirmation view for the V3 User Bot."""
from __future__ import annotations
from html import escape as he
from .appointments import display_time
from .listing_presenter import build_public_listing_details
from .public_appointment import PublicAppointmentDraft
from .public_inventory import PublicInventoryReader
from .transition_views import TransitionChoice, TransitionView

def _date_display(value: object) -> str:
    raw=str(value or "").strip()
    bits=raw.replace("/","-").split("-")
    if len(bits)>=2 and all(part.isdigit() for part in bits[-2:]):
        return f"{int(bits[-2])}月{int(bits[-1])}日"
    return raw

def _time_display(value: object) -> str:
    raw=str(value or "").strip()
    return {"am":"上午 09:00–12:00","pm":"下午 14:00–17:00"}.get(raw,display_time(raw))

def build_appointment_confirmation_view(draft: PublicAppointmentDraft, inventory: PublicInventoryReader) -> TransitionView:
    if not draft.ready:
        raise ValueError("appointment_confirmation_requires_ready_draft")
    published=inventory.resolve(draft.public_listing_id)
    if published is None:
        raise ValueError("listing_not_publicly_published")
    if not published.bookable:
        raise ValueError("listing_not_bookable")
    details=build_public_listing_details(published)
    subject=details.subject or details.location or "这套房"
    mode="视频代看" if draft.mode=="video" else "实地看房"
    lines=[
        "<b>确认预约</b>","",
        f"<b>{he(subject)}</b>",
        f"{he(mode)} · {he(_date_display(draft.date))} · {he(_time_display(draft.time))}",
        "",
    ]
    return TransitionView(
        kind="appointment_confirmation",
        text="\n".join(lines),
        rows=(
            (TransitionChoice("确认预约","appointment_submit"),),
            (TransitionChoice("修改日期","appointment_back_date"),TransitionChoice("修改时间","appointment_back_time")),
            (TransitionChoice("退出预约","appointment_exit"),),
        ),
    )

__all__=["build_appointment_confirmation_view"]
