"""Telegram-neutral search result cards for published V3 rent inventory."""
from __future__ import annotations
from dataclasses import dataclass
from html import escape as he
from pathlib import Path
import os
import re
from typing import Iterable
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
    current = views[index]
    target = current.public_listing_id
    rows: list[tuple[SemanticAction, ...]] = []
    total = len(views)
    if total > 1:
        prev_i = (index - 1) % total
        next_i = (index + 1) % total
        rows.append((
            SemanticAction("⬅️ 上一套", "previous", views[prev_i].public_listing_id, prev_i),
            SemanticAction("下一套 ➡️", "next", views[next_i].public_listing_id, next_i),
        ))
    if bookable:
        rows.append((
            SemanticAction("📷 房源详情", "details", target),
            SemanticAction("📅 预约看房", "book", target),
        ))
        rows.append((
            SemanticAction("🔄 调整条件", "change_search"),
            SemanticAction("💬 中文顾问", "consult", target),
        ))
    else:
        rows.append((
            SemanticAction("🔎 看相近房源", "similar", target),
            SemanticAction("💬 中文顾问", "consult", target),
        ))
        rows.append((SemanticAction("🔄 调整条件", "change_search"),))
    return tuple(rows)


def _advisor_username() -> str:
    raw = str(os.getenv("ADVISOR_TG") or os.getenv("SUPPORT_USERNAME") or "").strip()
    raw = re.sub(r"^https?://t\.me/", "", raw, flags=re.I)
    return raw.lstrip("@").split("?", 1)[0].strip("/")


def _first_payment(view: PublishedListingView, rent: int | None) -> int | None:
    if rent is None or rent <= 0:
        return None
    facts = getattr(view, "snapshot", {}).get("canonical_facts")
    if not isinstance(facts, dict):
        return None
    try:
        deposit = int(facts.get("deposit_months") or 0)
        prepay = int(facts.get("prepay_months") or 0)
    except (TypeError, ValueError):
        return None
    months = deposit + prepay
    if months <= 0:
        frozen_offer = getattr(view, "frozen_offer", {})
        payment = str(frozen_offer.get("payment_terms") or frozen_offer.get("deposit_terms") or "")
        match = re.search(r"押\s*(\d+)\s*付\s*(\d+)", payment)
        if match:
            months = int(match.group(1)) + int(match.group(2))
    return rent * months if months > 0 else None


def _snapshot_fact(view: PublishedListingView, key: str) -> str:
    facts = getattr(view, "snapshot", {}).get("canonical_facts")
    if not isinstance(facts, dict):
        return ""
    return str(facts.get(key) or "").strip()

def build_search_card(views: Iterable[PublishedListingView], index: int) -> SearchCardResponse:
    items = tuple(views)
    if not items:
        raise ValueError("search_card_requires_results")
    position = int(index) % len(items)
    view = items[position]
    details = build_public_listing_details(view)
    area = str(getattr(details, "location", "") or "").strip()
    layout = str(getattr(details, "layout", "") or "").strip()
    rent_value = getattr(details, "monthly_rent_usd", None)
    rent = f"${int(rent_value):,}/月" if rent_value is not None and int(rent_value) > 0 else ""
    payment = str(getattr(details, "deposit_terms", "") or "").strip()
    first_payment = _first_payment(view, rent_value)
    size = ""
    if getattr(details, "size_sqm", None) is not None and getattr(details, "size_sqm", None) > 0:
        numeric = int(getattr(details, "size_sqm", None)) if float(getattr(details, "size_sqm", None)).is_integer() else getattr(details, "size_sqm", None)
        size = str(numeric)
    floor = str(getattr(details, "floor", "") or "").strip().replace("楼", "")
    total_floor = _snapshot_fact(view, "total_floor").replace("楼", "")
    lease = str(getattr(details, "contract_term", "") or "").strip()
    furniture = _snapshot_fact(view, "furniture") or _snapshot_fact(view, "decoration")
    management = str(getattr(details, "management_fee", "") or "").strip()
    advisor = _advisor_username()

    title = " · ".join(v for v in (area, layout, rent) if v) or "房源"
    lines = [f"💰 <b>{he(title)}</b>"]
    first = f"{he(payment)}｜预计首付 <b>${int(first_payment):,}</b>" if first_payment else he(payment)
    if first:
        lines.extend(["", first])
    property_bits = []
    if size:
        property_bits.append(f"📐 {he(size)}㎡")
    if floor and total_floor:
        property_bits.append(f"{he(floor)}层/共{he(total_floor)}层")
    elif floor:
        property_bits.append(f"{he(floor)}层")
    if lease:
        property_bits.append(he(lease))
    if property_bits:
        lines.append("｜".join(property_bits))
    furnishing_bits = [v for v in (furniture, management) if v]
    if furnishing_bits:
        lines.append("🛋 " + "｜".join(he(v) for v in furnishing_bits))
    if area:
        lines.append(f"📍 {he(area)}")
    lines.extend([
        "",
        f"{details.status_icon} {he(details.status_label)}｜{position + 1}/{len(items)}",
        "",
        "📸 实拍房源｜中文顾问",
        "📹 没时间到现场？可约视频实拍代看",
        f"还有不清楚的，直接回复问我，或联系顾问 @{he(advisor)}" if advisor else "还有不清楚的，直接回复问我，或联系顾问",
    ])
    return SearchCardResponse(
        public_listing_id=getattr(details, "public_listing_id", ""),
        text="\n".join(lines),
        photo_path=_frozen_cover(view),
        action_rows=_card_actions(items, index=position, bookable=bool(getattr(details, "bookable", False))),
        index=position,
        total=len(items),
    )

def build_search_cards(views: Iterable[PublishedListingView]) -> tuple[SearchCardResponse, ...]:
    items=tuple(views)
    return tuple(build_search_card(items,index) for index in range(len(items)))

__all__=["SearchCardResponse","build_search_card","build_search_cards"]