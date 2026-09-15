"""Telegram-neutral search result cards for published V3 rent inventory.

Card copy consumes only ``PublishedListingView`` objects. Public facts and cover
selection come from the frozen publication package; only current availability
comes from live inventory. No legacy listing/media lookup is permitted here.
"""
from __future__ import annotations

from dataclasses import dataclass
from html import escape as he
from pathlib import Path
from typing import Iterable

from v3_core.publishing.formatting import display_floor

from .listing_presenter import build_public_listing_details
from .listing_responses import SemanticAction
from .public_inventory import PublishedListingView


@dataclass(frozen=True)
class SearchCardResponse:
    public_listing_id: str
    text: str
    photo_path: str
    action_rows: tuple[tuple[SemanticAction, ...], ...]
    index: int
    total: int


def _format_price(value: int | None) -> str:
    return f"${int(value):,}/月" if value is not None and int(value) > 0 else "价格待确认"


def _format_size(value: float | None) -> str:
    if value is None or value <= 0:
        return ""
    numeric = int(value) if float(value).is_integer() else value
    return f"{numeric}㎡"


def _frozen_cover(view: PublishedListingView) -> str:
    candidate = str(view.package.get("cover_path") or "").strip()
    return candidate if candidate and Path(candidate).is_file() else ""


def _card_actions(
    views: tuple[PublishedListingView, ...],
    *,
    index: int,
    bookable: bool,
) -> tuple[tuple[SemanticAction, ...], ...]:
    current = views[index]
    current_public_id = current.public_listing_id
    rows: list[tuple[SemanticAction, ...]] = []
    total = len(views)

    if total > 1:
        previous_index = (index - 1) % total
        next_index = (index + 1) % total
        rows.append(
            (
                SemanticAction(
                    "⬅️ 上一套",
                    "previous",
                    target_public_listing_id=views[previous_index].public_listing_id,
                    target_index=previous_index,
                ),
                SemanticAction(
                    "下一套 ➡️",
                    "next",
                    target_public_listing_id=views[next_index].public_listing_id,
                    target_index=next_index,
                ),
            )
        )

    rows.append(
        (
            SemanticAction(
                "🏠 租赁详情",
                "details",
                target_public_listing_id=current_public_id,
            ),
            SemanticAction(
                "📸 更多实拍",
                "photos",
                target_public_listing_id=current_public_id,
            ),
        )
    )
    if bookable:
        rows.append(
            (
                SemanticAction(
                    "📅 预约看房",
                    "book",
                    target_public_listing_id=current_public_id,
                ),
            )
        )
    rows.append(
        (
            SemanticAction(
                "💬 联系中文顾问",
                "consult",
                target_public_listing_id=current_public_id,
            ),
        )
    )
    rows.append((SemanticAction("✏️ 调整条件", "change_search"),))
    return tuple(rows)


def build_search_card(
    views: Iterable[PublishedListingView],
    index: int,
) -> SearchCardResponse:
    items = tuple(views)
    if not items:
        raise ValueError("search_card_requires_results")
    position = int(index) % len(items)
    view = items[position]
    details = build_public_listing_details(view)

    project = details.project_name or details.location or "金边"
    layout = details.layout or details.property_type or "房源"
    price = _format_price(details.monthly_rent_usd)
    size = _format_size(details.size_sqm)
    floor = display_floor(details.floor)

    lines = [
        f"🏠 <b>{he(project)}｜{he(layout)}</b>",
        f"💰 <b>{he(price)}</b>",
        "",
    ]
    if details.location:
        lines.append(f"📍 {he(details.location)}")
    house_bits = [value for value in (size, floor) if value]
    if house_bits:
        lines.append(f"📐 {he(' · '.join(house_bits))}")
    lines.extend(
        [
            "",
            f"{details.status_icon} {he(details.status_label)}",
            f"第 {position + 1}/{len(items)} 套",
        ]
    )

    return SearchCardResponse(
        public_listing_id=details.public_listing_id,
        text="\n".join(lines),
        photo_path=_frozen_cover(view),
        action_rows=_card_actions(items, index=position, bookable=details.bookable),
        index=position,
        total=len(items),
    )


def build_search_cards(
    views: Iterable[PublishedListingView],
) -> tuple[SearchCardResponse, ...]:
    items = tuple(views)
    return tuple(build_search_card(items, index) for index in range(len(items)))


__all__ = [
    "SearchCardResponse",
    "build_search_card",
    "build_search_cards",
]
