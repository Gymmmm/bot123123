"""Telegram-neutral public listing responses for the V3 User Bot."""
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
    media_groups: tuple[tuple[str, ...], ...]
    text: str
    action_rows: tuple[tuple[SemanticAction, ...], ...]
    listing_summary: str = ""

    @property
    def has_media(self) -> bool:
        return bool(self.media_groups)


def _format_price(value: int | None) -> str:
    return f"${int(value):,}/月" if value is not None and int(value) > 0 else ""


def listing_summary_bits(
    *,
    project_name: object = "",
    layout: object = "",
    monthly_rent_usd: int | None = None,
    location: object = "",
) -> str:
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
    target = str(public_listing_id or "").strip()
    book_row = (
        (SemanticAction("📅 预约看房", "book", target), SemanticAction("💬 问这套房", "consult", target))
        if bookable
        else (SemanticAction("💬 问这套房", "consult", target),)
    )
    return (
        book_row,
        (SemanticAction("📸 更多实拍", "photos", target),),
        (SemanticAction("✏️ 换个条件找", "change_search"),),
    )


def _photo_actions(
    *,
    bookable: bool,
    public_listing_id: str,
) -> tuple[tuple[SemanticAction, ...], ...]:
    target = str(public_listing_id or "").strip()
    book_row = (
        (SemanticAction("📅 预约看房", "book", target), SemanticAction("💬 问这套房", "consult", target))
        if bookable
        else (SemanticAction("💬 问这套房", "consult", target),)
    )
    return (
        book_row,
        (SemanticAction("📋 租赁详情", "details", target),),
    )


def build_details_response(view: PublishedListingView) -> PublicDetailsResponse:
    details = build_public_listing_details(view)
    price = _format_price(details.monthly_rent_usd)
    size = _format_size(details.size_sqm)
    floor = display_floor(details.floor)

    lines = ["📋 <b>租赁详情</b>", ""]
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

    notes = adviser_notes_for_view(view, max_points=2, allow_empty=True)
    if notes.strip():
        lines.extend(["", "💬 <b>侨联说</b>", he(notes)])

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


def build_photos_response(view: PublishedListingView) -> PublicPhotosResponse:
    details = build_public_listing_details(view)
    photos = _existing_gallery(details.gallery)
    groups = tuple(tuple(photos[offset : offset + 10]) for offset in range(0, len(photos), 10))
    if groups:
        text = "📸 <b>这些是这套房目前留下的现场实拍。</b>"
    else:
        text = "📸 <b>这套房目前没有更多实拍。</b>"
    return PublicPhotosResponse(
        media_groups=groups,
        text=text,
        listing_summary=listing_summary_bits(
            project_name=details.project_name,
            layout=details.layout,
            monthly_rent_usd=details.monthly_rent_usd,
            location=details.location,
        ),
        action_rows=_photo_actions(
            bookable=details.bookable,
            public_listing_id=details.public_listing_id,
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
