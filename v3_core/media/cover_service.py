"""Resolve V3 inventory facts into one final rendered cover.

The service owns cover output naming and the inventory -> renderer contract.
It does not read drafts or mutate publication packages. ``renderer`` is
injectable so contracts can be tested without launching Chromium.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from v3_core.publishing.formatting import display_floor, display_layout, display_property_type
from v3_core.storage.inventory_reader import InventoryReader
from .cover_renderer import CoverRenderData, render_cover
from .cover_styles import normalize_cover_style, recommended_cover_style
from .service import PreparedSourceMedia


@dataclass(frozen=True)
class RenderedCover:
    listing_id: str
    offer_id: str
    style: str
    source_image: str
    output_path: str
    data: CoverRenderData


Renderer = Callable[..., str]


class CoverRenderService:
    def __init__(
        self,
        *,
        reader: InventoryReader,
        output_dir: str | Path,
        renderer: Renderer = render_cover,
    ):
        self.reader = reader
        self.output_dir = Path(output_dir).expanduser().resolve()
        self.renderer = renderer

    @staticmethod
    def _price(offer: dict) -> str:
        if str(offer.get("offer_type") or "") == "rent":
            value = offer.get("monthly_rent_usd")
        else:
            value = offer.get("sale_price_usd")
        if value in (None, ""):
            return ""
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return str(value)
        return str(int(numeric)) if numeric.is_integer() else str(numeric)

    def render(
        self,
        *,
        listing_id: str,
        offer_id: str,
        media: PreparedSourceMedia,
        style: str | None = None,
        output_path: str | Path | None = None,
    ) -> RenderedCover:
        listing = self.reader.listing(listing_id)
        offer = self.reader.offer(offer_id)
        if str(offer.get("listing_id") or "") != str(listing_id):
            raise ValueError("cover_offer_listing_mismatch")
        canonical = self.reader.canonical(str(listing["canonical_record_id"]))
        facts = dict(canonical["facts"])
        selected_style = style or recommended_cover_style(
            listing.get("property_type"),
            listing.get("property_subtype"),
            listing.get("display_title"),
            facts.get("property_type"),
            facts.get("property_subtype"),
        )
        normalized_style = normalize_cover_style(selected_style, allow_video=False)
        public_id = str(listing.get("public_listing_id") or "").strip()
        if not public_id:
            raise ValueError("cover_requires_public_listing_id")
        if not Path(media.cover_source_path).is_file():
            raise FileNotFoundError(f"cover_source_not_found:{media.cover_source_path}")

        property_type = display_property_type(listing.get("property_type") or "")
        layout = display_layout(listing.get("layout") or "", property_type)
        floor = display_floor(listing.get("floor") or "", property_type)
        data = CoverRenderData(
            public_listing_id=public_id,
            project=str(listing.get("project_name") or ""),
            project_alias=str(listing.get("project_alias") or ""),
            property_type=property_type,
            deal_type=str(offer.get("offer_type") or facts.get("deal_type") or "rent"),
            layout=layout,
            area=str(
                listing.get("public_location_display")
                or listing.get("canonical_area_display")
                or ""
            ),
            size=str(listing.get("size_sqm") or ""),
            floor=floor,
            price=self._price(offer),
            # Channel covers stay factual and minimal. Marketing/adviser copy
            # belongs in the rental-detail view, never on the cover itself.
            highlight_1="",
            highlight_2="",
            highlight_3="",
        )
        target = (
            Path(output_path).expanduser().resolve()
            if output_path
            else self.output_dir / f"{public_id}_{normalized_style}.png"
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        rendered = self.renderer(
            style=normalized_style,
            source_image=str(Path(media.cover_source_path).resolve()),
            output_path=str(target),
            data=data,
        )
        rendered_path = Path(rendered).expanduser().resolve()
        if not rendered_path.is_file():
            raise RuntimeError("cover_renderer_did_not_create_output")
        return RenderedCover(
            listing_id=str(listing_id),
            offer_id=str(offer_id),
            style=normalized_style,
            source_image=str(Path(media.cover_source_path).resolve()),
            output_path=str(rendered_path),
            data=data,
        )


__all__ = ["CoverRenderService", "RenderedCover"]
