"""Telegram-neutral public listing responses for the V3 User Bot.

Merged listing view: one entry opens detail copy + a single photo flipper
(上一张 / 下一张), never a multi-photo media-group dump.
"""
from __future__ import annotations

from dataclasses import dataclass
from html import escape as he
from pathlib import Path
from typing import Literal

from v3_core.publishing.formatting import display_floor

from .adviser_notes import adviser_notes_for_view
from .listing_presenter import build_public_listing_details
from .public_inventory import PublishedListingView


InternalListingAction = Literal[
    "details",
    "photos",
    "book",
    "consult",
    "similar",
    "previous",
    "next",
    "change_search",
]


@dataclass(frozen=True)
class SemanticAction:
    label: str
    action: InternalListingAction
    target_public_listing_id: str = ""
    target_index: int | None = None


@dataclass(frozen=True)
class PublicDetailsResponse:
    text: str
    action_rows: tuple[tuple[SemanticAction, ...], ...]
    listing_summary: str = ""


@dataclass(frozen=True)
class PublicPhotosResponse:
    """Single-photo flipper payload (detail caption + one current frame)."""

    media_groups: tuple[tuple[str, ...], ...]
    text: str
    action_rows: tuple[tuple[SemanticAction, ...], ...]
    listing_summary: str = ""
    photo_path: str = ""
    photo_index: int = 0
    photo_total: int = 0

    @property
    def has_media(self) -> bool:
        return bool(self.photo_path) or bool(self.media_groups)


def _format_price(value: int | None) -> str:
    return f"${int(value):,}/月" if value is not None and int(value) > 0 else ""


def listing_summary_bits(
    *,
    project_name: object = "",
    layout: object = "",
    monthly_rent_usd: int | None = None,
    location: object = "",
) -> str:
    """Compact identity line for advisor handoff / consult prefill."""
    return "｜".join(
        part
        for part in (
            str(project_name or "").strip(),
            str(layout or "").strip(),
            _format_price(monthly_rent_usd),
            str(location or "").strip(),
        )
        if part
    )


def _format_size(value: float | None) -> str:
    if value is None or value <= 0:
        return ""
    numeric = int(value) if float(value).is_integer() else value
    return f"{numeric}㎡"


def _details_actions(
    *,
    bookable: bool,
    public_listing_id: str,
) -> tuple[tuple[SemanticAction, ...], ...]:
    """Text-only fallback actions (no separate 更多实拍 — merged into primary CTA)."""
    target = str(public_listing_id or "").strip()
    if bookable:
        return (
            (
                SemanticAction("📅 预约看房", "book", target),
                SemanticAction("💬 问这套房", "consult", target),
            ),
            (SemanticAction("🔍 看相近房源", "similar", target),),
        )
    return (
        (SemanticAction("💬 问这套房", "consult", target),),
        (SemanticAction("🔍 看相近房源", "similar", target),),
    )


def _photo_actions(
    *,
    bookable: bool,
    public_listing_id: str,
    photo_index: int = 0,
    photo_total: int = 0,
) -> tuple[tuple[SemanticAction, ...], ...]:
    target = str(public_listing_id or "").strip()
    rows: list[tuple[SemanticAction, ...]] = []
    total = max(0, int(photo_total or 0))
    index = max(0, int(photo_index or 0))
    if total > 1:
        prev_index = (index - 1) % total
        next_index = (index + 1) % total
        rows.append(
            (
                SemanticAction(
                    "⬅️ 上一张",
                    "photos",
                    target,
                    target_index=prev_index,
                ),
                SemanticAction(
                    "下一张 ➡️",
                    "photos",
                    target,
                    target_index=next_index,
                ),
            )
        )
    if bookable:
        rows.append(
            (
                SemanticAction("📅 预约看房", "book", target),
                SemanticAction("💬 问这套房", "consult", target),
            )
        )
    else:
        rows.append((SemanticAction("💬 问这套房", "consult", target),))
    rows.append((SemanticAction("🔍 看相近房源", "similar", target),))
    return tuple(rows)


def _detail_body_lines(view: PublishedListingView) -> list[str]:
    details = build_public_listing_details(view)
    price = _format_price(details.monthly_rent_usd)
    size = _format_size(details.size_sqm)
    floor = display_floor(details.floor)

    lines: list[str] = []
    if details.subject:
        lines.append(f"🏠 <b>{he(details.subject)}</b>")
    elif details.location:
        lines.append(f"🏠 <b>{he(details.location)}</b>")
    if price:
        lines.extend([f"💵 <b>{he(price)}</b>", ""])
    if details.location and details.location != details.project_name:
        lines.append(f"📍 {he(details.location)}")
    detail_parts: list[str] = []
    if size:
        detail_parts.append(size)
    if floor:
        detail_parts.append(floor)
    if detail_parts:
        lines.append(f"📐 {'｜'.join(he(item) for item in detail_parts)}")
    if details.lease_summary:
        lines.append(f"🔑 {he(details.lease_summary)}")
    lines.append(f"{details.status_icon} 房态：{he(details.status_label)}")
    if details.public_listing_id:
        lines.append(f"🆔 {he(details.public_listing_id)}")

    notes = adviser_notes_for_view(view, max_points=2, allow_empty=True).strip()
    if notes:
        safe_notes = "\n".join("• " + he(line) for line in notes.splitlines() if line.strip())
        lines.extend(["", "💬 <b>侨联说</b>", safe_notes])
    return lines


def build_details_response(view: PublishedListingView) -> PublicDetailsResponse:
    details = build_public_listing_details(view)
    lines = ["📋 <b>租赁详情</b>", ""] + _detail_body_lines(view)
    return PublicDetailsResponse(
        text="\n".join(lines),
        listing_summary=listing_summary_bits(
            project_name=details.project_name,
            layout=details.layout,
            monthly_rent_usd=details.monthly_rent_usd,
            location=details.location,
        ),
        action_rows=_details_actions(
            bookable=details.bookable,
            public_listing_id=details.public_listing_id,
        ),
    )


def _existing_gallery(paths: tuple[str, ...]) -> tuple[str, ...]:
    output: list[str] = []
    seen: set[str] = set()
    for raw in paths:
        path = str(raw or "").strip()
        if not path or path in seen:
            continue
        if Path(path).name.lower() in {"cover.jpg", "cover.jpeg", "cover.png"}:
            continue
        if not Path(path).is_file():
            continue
        output.append(path)
        seen.add(path)
    return tuple(output)


def _clip_caption(text: str, limit: int = 1024) -> str:
    clean = str(text or "").strip()
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 1)].rstrip() + "…"


def build_photos_response(
    view: PublishedListingView,
    *,
    offset: int = 0,
    page_size: int | None = None,
) -> PublicPhotosResponse:
    """Merged detail + one-photo flipper (上一张 / 下一张).

    ``offset`` is the 0-based photo index. ``page_size`` is ignored (kept for
    call-site compatibility after dropping progressive multi-photo pages).
    """
    del page_size  # progressive multi-photo pages removed
    details = build_public_listing_details(view)
    photos = _existing_gallery(details.gallery)
    total = len(photos)
    if total:
        index = max(0, int(offset or 0)) % total
        current = photos[index]
        groups = ((current,),)
        header = f"📋 <b>租赁详情</b> · 📸 {index + 1}/{total}"
    else:
        index = 0
        current = ""
        groups = ()
        header = "📋 <b>租赁详情</b>"

    body = _detail_body_lines(view)
    text = _clip_caption("\n".join([header, ""] + body))

    return PublicPhotosResponse(
        media_groups=groups,
        text=text,
        photo_path=current,
        photo_index=index,
        photo_total=total,
        listing_summary=listing_summary_bits(
            project_name=details.project_name,
            layout=details.layout,
            monthly_rent_usd=details.monthly_rent_usd,
            location=details.location,
        ),
        action_rows=_photo_actions(
            bookable=details.bookable,
            public_listing_id=details.public_listing_id,
            photo_index=index,
            photo_total=total,
        ),
    )


__all__ = [
    "InternalListingAction",
    "PublicDetailsResponse",
    "PublicPhotosResponse",
    "SemanticAction",
    "build_details_response",
    "build_photos_response",
    "listing_summary_bits",
]
