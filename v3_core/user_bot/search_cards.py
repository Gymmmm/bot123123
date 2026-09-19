"""Telegram-neutral search result cards for published V3 rent inventory."""
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

def _frozen_cover(view: PublishedListingView) -> str:
    candidate=str(view.package.get("cover_path") or "").strip()
    if candidate and Path(candidate).is_file():
        return candidate
    for raw in getattr(view,"gallery",()):
        path=str(raw or "").strip()
        if path and Path(path).is_file():
            return path
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
        rows.append((SemanticAction("房源详情","details",target),SemanticAction("预约看房","book",target)))
    else:
        rows.append((SemanticAction("房源详情","details",target),))
    rows.append((SemanticAction("换条件","change_search"),))
    return tuple(rows)

def build_search_card(views: Iterable[PublishedListingView], index: int) -> SearchCardResponse:
    items=tuple(views)
    if not items:
        raise ValueError("search_card_requires_results")
    position=int(index)%len(items)
    view=items[position]
    details=build_public_listing_details(view)
    location=str(details.location or "").strip()
    floor=display_floor(details.floor)
    bits=[v for v in (location,floor) if v]
    lines=[]
    if bits:
        lines.append(he(" · ".join(bits)))
    lines.append(f"{details.status_icon} {he(details.status_label)} · {position+1} / {len(items)}")
    return SearchCardResponse(
        public_listing_id=details.public_listing_id,
        text="\n".join(lines),
        photo_path=_frozen_cover(view),
        action_rows=_card_actions(items,index=position,bookable=details.bookable),
        index=position,
        total=len(items),
    )

def build_search_cards(views: Iterable[PublishedListingView]) -> tuple[SearchCardResponse, ...]:
    items=tuple(views)
    return tuple(build_search_card(items,index) for index in range(len(items)))

__all__=["SearchCardResponse","build_search_card","build_search_cards"]
