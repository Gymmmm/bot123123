"""Pure V3 appointment domain rules extracted from locked production behavior.

The public flow is intentionally small: a concrete bookable listing enters a
mode (offline by default, video optional), then date, then time, then immediate
submission.  Historical focus/confirm callbacks are compatibility concerns and
must not enlarge the new V3 state machine.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
import re
from typing import Literal


AppointmentMode = Literal["offline", "video"]
AppointmentStep = Literal["date", "time", "ready"]

APPOINTMENT_MODE_LABELS = {
    "offline": "实地看房",
    "video": "实时视频看房",
}
APPOINTMENT_TIME_LABELS = {
    "am": "上午 09:00–12:00",
    "pm": "下午 14:00–17:00",
    "evening": "晚上 17:00–19:00",
}
ACTIVE_APPOINTMENT_STATUSES = frozenset(
    {"pending", "assigned", "contacted", "confirmed"}
)
TERMINAL_APPOINTMENT_STATUSES = frozenset({"done", "cancelled"})


@dataclass(frozen=True)
class AppointmentDraft:
    listing_id: str
    mode: AppointmentMode = "offline"
    date: str = ""
    time: str = ""
    source: str = "user_bot"

    @property
    def step(self) -> AppointmentStep:
        if not self.date:
            return "date"
        if not self.time:
            return "time"
        return "ready"

    @property
    def ready(self) -> bool:
        return self.step == "ready"

    def with_mode(self, mode: object) -> "AppointmentDraft":
        clean = normalize_mode(mode)
        return replace(self, mode=clean)

    def with_date(self, value: object) -> "AppointmentDraft":
        clean = str(value or "").strip()
        if not clean:
            raise ValueError("appointment_date_required")
        return replace(self, date=clean, time="")

    def with_time(self, value: object) -> "AppointmentDraft":
        clean = str(value or "").strip()
        if not self.date:
            raise ValueError("appointment_date_required_before_time")
        if not clean:
            raise ValueError("appointment_time_required")
        return replace(self, time=clean)


def normalize_mode(value: object) -> AppointmentMode:
    clean = str(value or "offline").strip().lower()
    return clean if clean in APPOINTMENT_MODE_LABELS else "offline"  # type: ignore[return-value]


def normalize_custom_date(value: object) -> str:
    """Match the production custom-date normalization contract exactly."""
    raw = str(value or "").strip()
    compact = re.sub(r"\s+", "", raw)
    month = day = 0
    if re.fullmatch(r"\d{3,4}", compact):
        month = int(compact[:-2])
        day = int(compact[-2:])
    else:
        match = re.fullmatch(r"(?:\d{4}[-/.])?(\d{1,2})[-/.](\d{1,2})", compact)
        if not match:
            match = re.fullmatch(r"(\d{1,2})月(\d{1,2})日", compact)
        if match:
            month, day = int(match.group(1)), int(match.group(2))
    if month and day:
        try:
            datetime(2024, month, day)
        except ValueError:
            return ""
        return f"{month}月{day}日"
    if re.fullmatch(r"(?:下周|本周)?[一二三四五六日天]", compact):
        return compact
    return ""


def valid_custom_time(value: object) -> bool:
    clean = str(value or "").strip()[:40]
    return bool(
        re.fullmatch(
            r"(?:[01]?\d|2[0-3]):[0-5]\d|(?:上午|下午|傍晚|晚上)\s*\d{1,2}(?::[0-5]\d|点)?",
            clean,
        )
    )


def display_time(value: object) -> str:
    clean = str(value or "").strip()
    return APPOINTMENT_TIME_LABELS.get(clean, clean)


def duplicate_identity(
    *,
    user_id: int,
    listing_id: str,
    mode: object,
    date: object,
    time: object,
) -> tuple[int, str, AppointmentMode, str, str]:
    """Identity used by production to reuse an unfinished exact appointment."""
    return (
        int(user_id),
        str(listing_id or "").strip(),
        normalize_mode(mode),
        str(date or "").strip(),
        str(time or "").strip(),
    )


def editable_status(status: object) -> bool:
    return str(status or "").strip().lower() not in TERMINAL_APPOINTMENT_STATUSES


__all__ = [
    "ACTIVE_APPOINTMENT_STATUSES",
    "APPOINTMENT_MODE_LABELS",
    "APPOINTMENT_TIME_LABELS",
    "AppointmentDraft",
    "TERMINAL_APPOINTMENT_STATUSES",
    "display_time",
    "duplicate_identity",
    "editable_status",
    "normalize_custom_date",
    "normalize_mode",
    "valid_custom_time",
]
