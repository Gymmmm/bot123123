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

InternalListingAction=Literal["details","photos","book","consult","similar","previous","next","change_search"]

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
    def has_media(self)->bool:
        return bool(self.media_groups)

def _format_price(value: int|None)->str:
    return ("$"+f"{int(value):,}/月") if value is not None and int(value)>0 else ""

def listing_summary_bits(*,project_name:object="",layout:object="",monthly_rent_usd:int|None=None,location:object="")->str:
    return "｜".join(part for part in (str(project_name or "").strip(),str(layout or "").strip(),_format_price(monthly_rent_usd),str(location or "").strip()) if part)

def _format_size(value:float|None)->str:
    if value is None or value<=0:
        return ""
    numeric=int(value) if float(value).is_integer() else value
    return f"{numeric}㎡"

def _frozen_cover(view:PublishedListingView)->str:
    candidate=str(getattr(view,"package",{}).get("cover_path") or "").strip()
    if candidate and Path(candidate).is_file():
        return candidate
    for raw in getattr(view,"gallery",()):
        path=str(raw or "").strip()
        if path and Path(path).is_file():
            return path
    return ""

def _existing_gallery(view:PublishedListingView)->tuple[str,...]:
    cover=_frozen_cover(view)
    output=[]
    seen=set()
    for raw in getattr(view,"gallery",()):
        path=str(raw or "").strip()
        if not path or path in seen or path==cover:
            continue
        if not Path(path).is_file():
            continue
        output.append(path)
        seen.add(path)
    return tuple(output)

def _media_groups(paths:tuple[str,...])->tuple[tuple[str,...],...]:
    n=len(paths)
    if n==0:
        return ()
    if n==1:
        return ((paths[0],),)
    groups=[]
    offset=0
    while n-offset>10:
        remaining=n-offset
        size=9 if remaining==11 else 10
        groups.append(tuple(paths[offset:offset+size]))
        offset+=size
    groups.append(tuple(paths[offset:]))
    return tuple(groups)

def _details_actions(*,bookable:bool,public_listing_id:str,has_gallery:bool)->tuple[tuple[SemanticAction,...],...]:
    target=str(public_listing_id or "").strip()
    rows=[]
    first=[]
    if has_gallery:
        first.append(SemanticAction("更多实拍","photos",target))
    if bookable:
        first.append(SemanticAction("预约看房","book",target))
    if first:
        rows.append(tuple(first))
    rows.append((SemanticAction("咨询这套","consult",target),))
    return tuple(rows)

def _photo_actions(*,bookable:bool,public_listing_id:str)->tuple[tuple[SemanticAction,...],...]:
    target=str(public_listing_id or "").strip()
    row=[]
    if bookable:
        row.append(SemanticAction("预约看房","book",target))
    row.append(SemanticAction("咨询这套","consult",target))
    return (tuple(row),)

def build_details_response(view:PublishedListingView)->PublicDetailsResponse:
    details=build_public_listing_details(view)
    price=_format_price(details.monthly_rent_usd)
    published_price=_format_price(details.published_monthly_rent_usd)
    size=_format_size(details.size_sqm)
    floor=display_floor(details.floor)
    subject=" · ".join(v for v in (details.project_name,details.layout) if v) or details.location or "这套房"
    lines=[f"<b>{he(subject)}</b>"]
    if price:
        if published_price and published_price != price:
            lines.append(f"<b>{he(price)}</b>（发布价 {he(published_price)}）　{details.status_icon} {he(details.status_label)}")
        else:
            lines.append(f"<b>{he(price)}</b>　{details.status_icon} {he(details.status_label)}")
    else:
        lines.append(f"{details.status_icon} {he(details.status_label)}")
    lines.append("")
    if details.location:
        lines.append(he(details.location))
    specs=[value for value in (floor,size) if value]
    if specs:
        lines.append(he(" · ".join(specs)))
    if details.lease_summary:
        lines.append(he(details.lease_summary))
    notes=adviser_notes_for_view(view,allow_empty=True)
    if notes.strip():
        lines.extend(["","<b>侨联说</b>"])
        if not details.bookable:
            lines.append("<i>当前状态已更新，以下为发布时的房源说明。</i>")
        lines.append(f"<blockquote>{he(notes)}</blockquote>")
    if details.public_listing_id:
        lines.extend(["",he(details.public_listing_id)])
    gallery=_existing_gallery(view)
    return PublicDetailsResponse(
        text="\n".join(lines),
        listing_summary=listing_summary_bits(project_name=details.project_name,layout=details.layout,monthly_rent_usd=details.monthly_rent_usd,location=details.location),
        action_rows=_details_actions(bookable=details.bookable,public_listing_id=details.public_listing_id,has_gallery=bool(gallery)),
    )

def build_photos_response(view:PublishedListingView)->PublicPhotosResponse:
    details=build_public_listing_details(view)
    photos=_existing_gallery(view)
    project=details.project_name or details.subject or "这套房"
    text=f"<b>{he(project)} · 更多实拍</b>" if photos else "<b>暂无更多实拍图片</b>"
    return PublicPhotosResponse(
        media_groups=_media_groups(photos),
        text=text,
        listing_summary=listing_summary_bits(project_name=details.project_name,layout=details.layout,monthly_rent_usd=details.monthly_rent_usd,location=details.location),
        action_rows=_photo_actions(bookable=details.bookable,public_listing_id=details.public_listing_id) if photos else (),
    )

__all__=["InternalListingAction","PublicDetailsResponse","PublicPhotosResponse","SemanticAction","build_details_response","build_photos_response","listing_summary_bits"]
