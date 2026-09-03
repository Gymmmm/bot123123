from __future__ import annotations

import hashlib
import html
import json
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont, ImageOps

from qiaolian_dual.canonical_fact_projection import package_gate, package_snapshot, validate_facts
from qiaolian_dual.canonical_facts import draft_projection
from qiaolian_dual.canonical_listing_materializer import (
    ensure_canonical_projection_schema, materialize_listing, canonical_projection_hash,
)
from qiaolian_dual.publishability_contract import evaluate_publishability
from qiaolian_dual.utils_formatting import _display_layout, _display_floor
from qiaolian_dual.cover_styles import (
    ACCEPTED_COVER_STYLE_KEYS,
    FINAL_COVER_STYLES,
    cover_template_path,
    normalize_cover_style,
)

# Temporary restore shim: keep classify + locked caption while full package module is reattached.
ROOT = Path(__file__).resolve().parent
PACKAGE_ROOT = ROOT / "media" / "publication_packages"
COVER_TEMPLATE_MAP = {key: cover_template_path(key) for key in (*FINAL_COVER_STYLES, "video_vertical")}
PACKAGE_ADDITIVE_COLUMNS = {
    "discussion_text": "TEXT NOT NULL DEFAULT ''",
    "source_identity_json": "TEXT",
    "source_identity_hash": "TEXT",
    "source_identity_migrated_at": "TEXT",
    "public_token": "TEXT",
    "canonical_facts_hash": "TEXT",
    "canonical_facts_schema": "TEXT",
    "publication_location_level": "TEXT",
    "canonical_projection_hash": "TEXT",
    "canonical_provenance_json": "TEXT",
    "quality_json": "TEXT",
}
DDL = """
CREATE TABLE IF NOT EXISTS publication_packages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  package_id TEXT NOT NULL,
  draft_id TEXT NOT NULL,
  property_id TEXT NOT NULL,
  package_version INTEGER NOT NULL,
  source_type TEXT NOT NULL,
  listing_type TEXT NOT NULL,
  media_type TEXT NOT NULL,
  cover_template TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'package_ready',
  cover_path TEXT NOT NULL,
  main_images_json TEXT NOT NULL,
  discussion_images_json TEXT NOT NULL,
  post_text TEXT NOT NULL,
  discussion_text TEXT NOT NULL DEFAULT '',
  fee_text TEXT NOT NULL DEFAULT '',
  advice_text TEXT NOT NULL DEFAULT '',
  snapshot_json TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  UNIQUE(draft_id, package_version)
);
"""

def now_utc() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _canonical_cover_template(value: str) -> str:
    return normalize_cover_style(value, allow_video=True)

def classify(*, source_type: str, source_name: str, property_type: str,
             project: str, media_type: str = "image", price: Any = None,
             highlights: Any = None, is_special: bool = False) -> dict[str, str]:
    media = str(media_type or "image").lower()
    source = f"{source_type or ''} {source_name or ''}".lower()
    listing = f"{property_type or ''} {project or ''}".lower()
    normalized_source = "wechat" if ("wechat" in source or "微信" in source) else "telegram"
    if "video" in media:
        return {"source_type": normalized_source, "listing_type": "video", "media_type": "video", "cover_template": "video_vertical"}
    is_villa = "别墅" in listing or "villa" in listing
    is_townhouse = any(token in listing for token in ("排屋", "联排", "townhouse"))
    listing_type = "villa" if is_villa else ("townhouse" if is_townhouse else "apartment")
    try:
        numeric_price = float(re.sub(r"[^0-9.]", "", str(price or "0") or "0") or 0)
    except (TypeError, ValueError):
        numeric_price = 0
    _ = (highlights, is_special)
    cover_template = "black_gold" if (is_villa or is_townhouse or numeric_price >= 1200) else "classic_blue"
    return {"source_type": normalized_source, "listing_type": listing_type, "media_type": "image", "cover_template": cover_template}

def ensure_schema(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executescript(DDL)
        ensure_canonical_projection_schema(conn)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(publication_packages)")}
        for name, sql_type in PACKAGE_ADDITIVE_COLUMNS.items():
            if name not in cols:
                conn.execute(f"ALTER TABLE publication_packages ADD COLUMN {name} {sql_type}")

def format_button_post_text(
    d: dict, listing_id: str, tag_lines: list[str], caption_variant: str = "a"
) -> str:
    """Locked 4-line channel caption. A/B/C variants no longer change public copy."""
    from qiaolian_dual.channel_post import format_channel_listing_post
    _ = caption_variant
    payload = dict(d or {})
    raw_status = str(payload.get("status") or "").strip().lower()
    if raw_status in {"", "pending", "draft"}:
        payload["status"] = "active"
    extra = [str(line).strip() for line in (tag_lines or []) if str(line).strip().startswith("#")]
    return format_channel_listing_post(
        payload,
        listing_id=str(listing_id or payload.get("listing_id") or ""),
        status=payload.get("status"),
        extra_tags=extra,
    )
