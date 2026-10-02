"""Telegram-neutral search result cards for published V3 rent inventory."""
from __future__ import annotations
from dataclasses import dataclass
from html import escape as he
from pathlib import Path
from typing import Iterable
from v3_core.inventory.listing_taxonomy import location_display_overlaps_project
from v3_core.publishing.formatting import display_floor
from .listing_presenter import build_public_listing_details
from .listing_responses import SemanticAction, _frozen_cover_path
from .public_inventory import PublishedListingView

@dataclass(frozen=True)
class SearchCardResponse:
    public_listing_id: str
    text: str
    photo_path: str
    action_rows: tuple[tuple[SemanticAction, ...], ...]
    index: int
    total: int

def _frozen_cover(view: PublishedListingView) -> str:
    """Prefer rendered cover; fall back to first readable gallery frame."""
    cover = _frozen_cover_path(view)
    if cover:
        return cover
    for raw in getattr(view, "gallery", ()) or ():
        path = str(raw or "").strip()
        if path and Path(path).expanduser().is_file():
            return str(Path(path).expanduser().resolve())
    return ""

def _card_actions(views: tuple[PublishedListingView, ...], *, index: int, bookable: bool) -> tuple[tuple[SemanticAction, ...], ...]:
    current=views[index]
    target=current.public_listing_id
    rows=[]
    total=len(views)
    if total > 1:
        prev_i=(index-1)%total
        next_i=(index+1)%total
        rows.append((
            SemanticAction("上一套","previous",views[prev_i].public_listing_id,prev_i),
            SemanticAction("下一套","next",views[next_i].public_listing_id,next_i),
        ))
    if bookable:
        rows.append((SemanticAction("📷 查看实拍","photos",target),SemanticAction("📅 在线预约","book",target)))
    else:
        rows.append((SemanticAction("📷 查看实拍","photos",target),))
    rows.append((SemanticAction("💬 咨询这套","consult",target),SemanticAction("🔍 找相似","similar",target)))
    rows.append((SemanticAction("🔄 调整条件","change_search"),))
    return tuple(rows)

def build_search_card(views: Iterable[PublishedListingView], index: int) -> SearchCardResponse:
    items=tuple(views)
    if not items:
        raise ValueError("search_card_requires_results")
    position=int(index)%len(items)
    view=items[position]
    details=build_public_listing_details(view)
    floor=display_floor(details.floor)
    project=str(details.project_name or "").strip()
    area=str(details.location or "").strip()
    layout=str(details.layout or "").strip()
    rent=(
        f"${int(details.monthly_rent_usd):,}/月"
        if details.monthly_rent_usd is not None and int(details.monthly_rent_usd)>0
        else ""
    )
    headline = "｜".join(v for v in (project or area, layout) if v) or "房源"
    lines=[f"{he(headline)}"]
    if rent:
        lines.append(f"💵 {he(rent)}")
    if project and area and not location_display_overlaps_project(project, area):
        lines.append(f"📍 {he(area)}")
    status_line = _status_line_for(details)
    if status_line:
        lines.append(status_line)
    return SearchCardResponse(
        public_listing_id=details.public_listing_id,
        text="\n".join(lines),
        photo_path=_frozen_cover(view),
        action_rows=_card_actions(items,index=position,bookable=details.bookable),
        index=position,
        total=len(items),
    )


_STATUS_LABELS = {
    "active": "🟢 当前可预约",
    "reserved": "🟡 已有预约，仍可预约",
    "pending": "🔵 房态待确认",
    "rented": "🔴 已租出",
    "leased": "🔴 已租出",
    "inactive": "⚫ 已下架",
    "offline": "⚫ 已下架",
}


def _status_line_for(details) -> str:
    """Locked five-state status badge (Section 3)."""
    label = _STATUS_LABELS.get(str(getattr(details, "inventory_status", "") or "").strip().lower())
    if label:
        return label
    fallback = str(getattr(details, "status_label", "") or "").strip()
    icon = str(getattr(details, "status_icon", "") or "").strip()
    if icon and fallback:
        return f"{icon} {he(fallback)}"
    return ""

def build_search_cards(views: Iterable[PublishedListingView]) -> tuple[SearchCardResponse, ...]:
    items=tuple(views)
    return tuple(build_search_card(items,index) for index in range(len(items)))

__all__=["SearchCardResponse","build_search_card","build_search_cards"]