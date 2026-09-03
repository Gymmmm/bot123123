"""侨联房源发布包：加工在审核前完成，批准后冻结，发布只读成品。"""
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

ROOT = Path(__file__).resolve().parent
PACKAGE_ROOT = ROOT / "media" / "publication_packages"
COVER_TEMPLATE_MAP = {
    key: cover_template_path(key)
    for key in (*FINAL_COVER_STYLES, "video_vertical")
}

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
  source_identity_json TEXT,
  source_identity_hash TEXT,
  source_identity_migrated_at TEXT,
  public_token TEXT,
  approved_by TEXT,
  approved_at TEXT,
  published_at TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(draft_id, package_version)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_publication_packages_one_approved
ON publication_packages(draft_id) WHERE status='approved';
CREATE UNIQUE INDEX IF NOT EXISTS idx_publication_packages_package_id
ON publication_packages(package_id);
CREATE INDEX IF NOT EXISTS idx_publication_packages_status
ON publication_packages(status, id);
"""
