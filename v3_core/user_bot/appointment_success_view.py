"""Public-only success view for a persisted V3 appointment."""
from __future__ import annotations
from html import escape as he
from .appointments import display_time
from .listing_presenter import booking_subject, build_public_listing_details
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

def build_appointment_success_view(draft: PublicAppointmentDraft, inventory: PublicInventoryReader, *, submission_kind: str = "created") -> TransitionView:
    published=inventory.resolve(draft.public_listing_id)
    if published is None:
        subject="这套房"
    else:
        details=build_public_listing_details(published)
        subject=booking_subject(details)
    mode="视频代看" if draft.mode=="video" else "实地看房"
    lines=[
        "✅ <b>预约已提交</b>",
        f"房源｜{he(subject)}",
        f"方式｜{he(mode)}",
        f"时间｜{he(_date_display(draft.date))} · {he(_time_display(draft.time))}",
        "侨联已收到预约，顾问会联系你确认具体看房安排。",
    ]
    return TransitionView(
        kind="appointment_success",
        text="\n".join(lines),
        rows=(
            (TransitionChoice("📋 我的预约","home","appointments"),TransitionChoice("💬 中文顾问","home","contact")),
            (TransitionChoice("🔍 继续找房","home","search"),),
        ),
    )

__all__=["build_appointment_success_view"]
