"""Calendar interpretation at booking confirmation; no schema/data migration.

Legacy yearless rows are anchored to their creation date, never the viewing
clock. A date before creation is ambiguous except the existing three-day quick
picker's December/January rollover. Ambiguous rows remain visible for review.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

PHNOM_PENH = ZoneInfo("Asia/Phnom_Penh")


def local_now() -> datetime:
    return datetime.now(PHNOM_PENH)


def local_datetime(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        # Existing appointment timestamps were written as local wall time.
        return (
            parsed.replace(tzinfo=PHNOM_PENH)
            if parsed.tzinfo is None
            else parsed.astimezone(PHNOM_PENH)
        )
    except (ValueError, TypeError):
        return None


def _parts(value: object) -> tuple[int | None, int, int] | None:
    raw = re.sub(r"\s+", "", str(value or ""))
    match = re.fullmatch(r"(?:(\d{4})[-/.年])?(\d{1,2})[-/.月](\d{1,2})日?", raw)
    if match:
        year, month, day = match.groups()
        return int(year) if year else None, int(month), int(day)
    if re.fullmatch(r"\d{3,4}", raw):
        return None, int(raw[:-2]), int(raw[-2:])
    return None


def _relative(value: object, anchor: date) -> date | None:
    raw = re.sub(r"\s+", "", str(value or ""))
    if raw in {"今天", "明天", "后天"}:
        return anchor + timedelta(days={"今天": 0, "明天": 1, "后天": 2}[raw])
    match = re.fullmatch(r"(下周|本周)?([一二三四五六日天])", raw)
    if match:
        prefix, weekday = match.groups()
        index = "一二三四五六日".index("日" if weekday == "天" else weekday)
        monday = anchor - timedelta(days=anchor.weekday())
        target = monday + timedelta(days=index + (7 if prefix == "下周" else 0))
        if prefix is None and target < anchor:
            target += timedelta(days=7)
        return target
    return None


def booking_date(value: object, *, now: datetime | None = None) -> str:
    anchor = (
        now.astimezone(PHNOM_PENH) if now and now.tzinfo else now or local_now()
    ).date()
    relative = _relative(value, anchor)
    if relative is not None:
        return relative.isoformat()
    parts = _parts(value)
    if parts is None:
        raise ValueError("appointment_date_unrecognized")
    year, month, day = parts
    # New quick callbacks carry ISO. This also supports old December buttons.
    if year is None:
        year = anchor.year + (1 if anchor.month == 12 and month == 1 else 0)
    try:
        return date(year, month, day).isoformat()
    except ValueError as exc:
        raise ValueError("appointment_date_invalid") from exc


def legacy_date(
    value: object, created_at: str, *, viewing_date: date | None = None
) -> date | None:
    parts = _parts(value)
    if parts and parts[0] is not None:
        try:
            return date(*parts)
        except ValueError:
            return None
    created = local_datetime(created_at)
    if created is None:
        return None
    anchor = created.date()
    # Legacy relative strings have no selection timestamp (edits kept created_at).
    # They cannot safely be turned into completed appointments.
    if parts is None:
        return None
    _, month, day = parts
    try:
        year = anchor.year + (1 if anchor.month == 12 and month == 1 else 0)
        target = date(year, month, day)
        # Creation context can show a nearby upcoming old booking. It cannot
        # prove completion once a yearless row is past: old edits lost their
        # selection timestamp/year. Keep that row visible for confirmation.
        if 0 <= (target - anchor).days <= 2 and (
            viewing_date is None or target >= viewing_date
        ):
            return target
    except ValueError:
        pass
    return None


def booking_time(value: object) -> str:
    raw = str(value or "").strip()
    if raw in {"am", "pm", "evening"}:
        return raw  # Fixed existing Phnom Penh time windows, not an invented instant.
    if re.fullmatch(r"(?:[01]?\d|2[0-3]):[0-5]\d", raw):
        hour, minute = map(int, raw.split(":"))
        return f"{hour:02}:{minute:02}"
    match = re.fullmatch(r"(上午|下午|傍晚|晚上)\s*(\d{1,2})(?::([0-5]\d)|点)?", raw)
    if match:
        period, h, m = match.groups()
        hour, minute = int(h), int(m or 0)
        if (
            (period in {"晚上", "傍晚"} and hour in {0, 12})
            or (period == "上午" and hour > 12)
            or (period == "下午" and hour == 0)
        ):
            raise ValueError("appointment_time_requires_explicit_clock")
        if 0 <= hour <= 23:
            if period != "上午" and hour < 12:
                hour += 12
            return f"{hour:02}:{minute:02}"
    raise ValueError("appointment_time_invalid")


def same_confirmed_schedule(row, target_date: str, target_time: str) -> bool:
    raw_date, raw_time = str(row["appointment_date"] or ""), str(
        row["appointment_time"] or ""
    )
    interpreted = legacy_date(raw_date, str(row["created_at"] or ""))
    try:
        interpreted_time = booking_time(raw_time)
    except ValueError:
        interpreted_time = None
    return (
        raw_date == target_date
        or (interpreted is not None and interpreted.isoformat() == target_date)
    ) and (raw_time == target_time or interpreted_time == target_time)
