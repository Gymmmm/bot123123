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
from .search_query import SearchCriteria
from .similar_differences import listing_differences

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

def _has_photos(view: PublishedListingView) -> bool:
    return any(str(raw or "").strip() for raw in (getattr(view, "gallery", ()) or ()))


def _card_actions(views: tuple[PublishedListingView, ...], *, index: int, bookable: bool) -> tuple[tuple[SemanticAction, ...], ...]:
    """V4 result card: 看实拍 goes straight to the album, no detour via details."""
    current=views[index]
    target=current.public_listing_id
    rows=[]
    first=[]
    if _has_photos(current):
        first.append(SemanticAction("📸 看实拍","photos",target))
    first.append(SemanticAction("🏠 房源详情","details",target))
    rows.append(tuple(first))
    if bookable:
        rows.append((SemanticAction("📅 预约看房","book",target),SemanticAction("💬 咨询这套","consult",target)))
    else:
        rows.append((SemanticAction("💬 咨询这套","consult",target),))
    total=len(views)
    if total > 1:
        prev_i=(index-1)%total
        next_i=(index+1)%total
        rows.append((
            SemanticAction("⬅️ 上一套","previous",views[prev_i].public_listing_id,prev_i),
            SemanticAction("下一套 ➡️","next",views[next_i].public_listing_id,next_i),
        ))
    rows.append((SemanticAction("🔄 调整条件","change_search"),))
    return tuple(rows)


def card_header(index: int, total: int, *, similar: bool = False) -> str:
    title = "🏘️ <b>这几套也值得看看</b>" if similar else "🏠 <b>找到这些房源</b>"
    return f"{title} · {index + 1}/{total}" if total > 1 else title


def build_search_card(
    views: Iterable[PublishedListingView],
    index: int,
    *,
    criteria: SearchCriteria | None = None,
    similar: bool = False,
) -> SearchCardResponse:
    items=tuple(views)
    if not items:
        raise ValueError("search_card_requires_results")
    position=int(index)%len(items)
    view=items[position]
    details=build_public_listing_details(view)
    project=str(details.project_name or "").strip()
    area=str(details.location or "").strip()
    layout=str(details.layout or "").strip()
    rent=(
        f"${int(details.monthly_rent_usd):,}/月"
        if details.monthly_rent_usd is not None and int(details.monthly_rent_usd)>0
        else ""
    )
    headline = "｜".join(v for v in (project or area, layout) if v) or "房源"
    lines=[card_header(position, len(items), similar=similar), "", f"{he(headline)}"]
    if rent:
        lines.append(f"💵 {he(rent)}")
    if project and area and not location_display_overlaps_project(project, area):
        lines.append(f"📍 {he(area)}")
    status_line = _status_line_for(details)
    if status_line:
        lines.append(status_line)
    differences = listing_differences(
        view,
        criteria,
        rent=int(details.monthly_rent_usd) if details.monthly_rent_usd else None,
        location=area,
        layout=layout,
    )
    if differences:
        lines.append("")
        lines.extend(he(item) for item in differences)
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

def build_search_cards(
    views: Iterable[PublishedListingView],
    *,
    criteria: SearchCriteria | None = None,
    similar: bool = False,
) -> tuple[SearchCardResponse, ...]:
    items=tuple(views)
    return tuple(build_search_card(items,index,criteria=criteria,similar=similar) for index in range(len(items)))

__all__=["SearchCardResponse","build_search_card","build_search_cards","card_header"]
