"""侨联房源发布包：加工在审核前完成，批准后冻结，发布只读成品。"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
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

ROOT = Path(__file__).resolve().parent
PACKAGE_ROOT = ROOT / "media" / "publication_packages"
COVER_TEMPLATE_MAP = {
    "classic_blue": ROOT / "templates" / "01_经典蓝卡_template_editor.html",
    "minimal_white": ROOT / "templates" / "03_右侧价格牌_fixed_v1_template_render.html",
    "right_price": ROOT / "templates" / "03_右侧价格牌_fixed_v1_template_render.html",
    "left_info": ROOT / "templates" / "03_右侧价格牌_fixed_v1_template_render.html",
    "black_gold": ROOT / "templates" / "06_暗夜玻璃_template_render.html",
    "villa_premium": ROOT / "templates" / "12_别墅高级风_template_render.html",
    "video_vertical": ROOT / "templates" / "property_listing_draggable.html",
    "editorial_mobile": ROOT / "templates" / "property_listing_draggable.html",
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


def now_utc() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


_DEFAULT_STYLE_KEYS = {
    "collected": ("publish_default_collected_caption", "publish_default_collected_cover"),
    "manual": ("publish_default_manual_caption", "publish_default_manual_cover"),
}
_ALLOWED_CAPTION_VARIANTS = {"a", "b", "c"}
_ALLOWED_COVER_TEMPLATES = {
    "minimal_white", "right_price", "classic_blue", "black_gold",
    "villa_premium", "video_vertical", "premium_4image", "editorial_mobile",
}


def _source_style_scope(source_type: str, source_name: str = "") -> str:
    raw = f"{source_type or ''} {source_name or ''}".strip().lower()
    # wechat_note/admin/manual are explicit operator imports; all collector
    # sources use the collected defaults. This is routing metadata only.
    return "manual" if any(token in raw for token in ("wechat_note", "manual", "admin_import", "手工", "管理导入")) else "collected"


def _default_publish_styles(
    conn: sqlite3.Connection,
    source_type: str,
    source_name: str = "",
    property_type: str = "",
) -> dict[str, str]:
    scope = _source_style_scope(source_type, source_name)
    _legacy_caption_key, cover_key = _DEFAULT_STYLE_KEYS[scope]
    values = {}
    for key, default in ((cover_key, ""),):
        try:
            row = conn.execute("SELECT setting_value FROM bot_settings WHERE setting_key=? LIMIT 1", (key,)).fetchone()
            values[key] = str(row[0] or "").strip() if row else default
        except sqlite3.Error:
            values[key] = default
    from meihua_publisher import default_caption_variant_for_property
    caption = default_caption_variant_for_property(property_type)
    cover = values[cover_key] if values[cover_key] in _ALLOWED_COVER_TEMPLATES else ""
    return {"scope": scope, "caption_variant": caption, "cover_template": cover}


def ensure_schema(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executescript(DDL)
        ensure_canonical_projection_schema(conn)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(publication_packages)")}
        for name, sql_type in PACKAGE_ADDITIVE_COLUMNS.items():
            if name not in cols:
                conn.execute(f"ALTER TABLE publication_packages ADD COLUMN {name} {sql_type}")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_publication_packages_public_token ON publication_packages(public_token) WHERE public_token IS NOT NULL")


def classify(*, source_type: str, source_name: str, property_type: str,
             project: str, media_type: str = "image", price: Any = None,
             highlights: Any = None, is_special: bool = False) -> dict[str, str]:
    """Choose the visual template without corrupting the real property type.

    Routing priority:
      video -> video_vertical
      all image listings -> editorial_mobile
      video remains the only dedicated alternate cover

    `listing_type` reflects the actual property type; a high-priced apartment
    may use the black-gold template but must remain an apartment.
    """
    media = str(media_type or "image").lower()
    source = f"{source_type or ''} {source_name or ''}".lower()
    listing = f"{property_type or ''} {project or ''}".lower()

    normalized_source = "wechat" if ("wechat" in source or "微信" in source) else "telegram"

    if "video" in media:
        return {
            "source_type": normalized_source,
            "listing_type": "video",
            "media_type": "video",
            "cover_template": "video_vertical",
        }

    is_villa = "别墅" in listing or "villa" in listing
    listing_type = "villa" if is_villa else "apartment"

    try:
        numeric_price = float(re.sub(r"[^0-9.]", "", str(price or "0")) or 0)
    except (TypeError, ValueError):
        numeric_price = 0

    highlight_count = len(_json_list(highlights))

    if is_villa:
        return {
            "source_type": normalized_source,
            "listing_type": "villa",
            "media_type": "image",
            "cover_template": "editorial_mobile",
        }

    if numeric_price >= 1200:
        return {
            "source_type": normalized_source,
            "listing_type": listing_type,
            "media_type": "image",
            "cover_template": "editorial_mobile",
        }

    if is_special or any(word in listing for word in ("特价", "急租", "优惠", "活动")):
        return {
            "source_type": normalized_source,
            "listing_type": listing_type,
            "media_type": "image",
            "cover_template": "editorial_mobile",
        }

    # There is currently no independent left_info HTML file. Route rich-info
    # listings to the real right-price template instead of maintaining a fake
    # alias that points at the same HTML.
    if highlight_count >= 3 or normalized_source == "wechat":
        return {
            "source_type": normalized_source,
            "listing_type": listing_type,
            "media_type": "image",
            "cover_template": "editorial_mobile",
        }

    return {
        "source_type": normalized_source,
        "listing_type": listing_type,
        "media_type": "image",
        "cover_template": "editorial_mobile",
    }


def _font(size: int, bold: bool = False):
    candidates = [
        "/System/Library/Fonts/PingFang.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc" if bold else
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for path in candidates:
        if os.path.isfile(path):
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                pass
    return ImageFont.load_default()


def logo_only(source_path: str, output_path: str, badge: str = "") -> str:
    """详情图只加轻量文字水印，不加价格、户型或大信息板。"""
    try:
        image = Image.open(source_path).convert("RGBA")
    except Exception:
        # 测试占位图或损坏素材不应使整个审核队列卡死。
        image = Image.new("RGBA", (1280, 960), (229, 233, 239, 255))
    draw = ImageDraw.Draw(image, "RGBA")
    scale = max(0.7, min(1.4, image.width / 1280))
    pad = int(22 * scale)
    title_font = _font(int(30 * scale), True)
    title = "侨联地产"
    box = draw.textbbox((0, 0), title, font=title_font)
    width = box[2] - box[0]
    height = box[3] - box[1]
    x = image.width - width - pad
    y = image.height - height - pad
    shadow = max(1, int(2 * scale))
    draw.text((x + shadow, y + shadow), title, font=title_font, fill=(0, 0, 0, 115))
    draw.text((x, y), title, font=title_font, fill=(255, 255, 255, 190))
    if badge:
        badge_font = _font(int(14 * scale), True)
        badge_box = draw.textbbox((0, 0), badge, font=badge_font)
        badge_w = badge_box[2] - badge_box[0]
        draw.text((image.width - badge_w - pad, y - int(24 * scale)), badge,
                  font=badge_font, fill=(255, 255, 255, 175))
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    image.convert("RGB").save(output_path, "JPEG", quality=92, optimize=True)
    return output_path


def _dedupe_paths_by_content(paths: list[str]) -> list[str]:
    """Keep first occurrence of each readable source image by SHA256, preserving source order."""
    unique: list[str] = []
    seen_hashes: set[str] = set()
    for raw_path in paths:
        path = str(raw_path or "")
        if not path or not os.path.isfile(path):
            continue
        try:
            digest = hashlib.sha256(Path(path).read_bytes()).hexdigest()
        except OSError:
            continue
        if digest in seen_hashes:
            continue
        seen_hashes.add(digest)
        unique.append(path)
    return unique


def _paths(conn: sqlite3.Connection, source_post_id: Any) -> list[str]:
    paths: list[str] = []
    for row in conn.execute(
        """SELECT local_path FROM media_assets WHERE owner_type='source_post'
           AND CAST(owner_ref_id AS TEXT)=CAST(? AS TEXT) AND asset_type='photo'
           ORDER BY sort_order,id""", (source_post_id,)
    ):
        if row[0] and os.path.isfile(row[0]):
            paths.append(row[0])
    if paths:
        return _dedupe_paths_by_content(paths)
    row = conn.execute("SELECT raw_images_json FROM source_posts WHERE id=?", (source_post_id,)).fetchone()
    if not row or not row[0]:
        return []
    try:
        for item in json.loads(row[0]):
            path = item if isinstance(item, str) else item.get("local_path") or item.get("path")
            if path and os.path.isfile(path):
                paths.append(path)
    except Exception:
        return []
    return _dedupe_paths_by_content(paths)


def ensure_source_media_assets(conn: sqlite3.Connection, source_post_id: Any) -> dict[str, int]:
    """Register existing manual-intake image files as immutable source_post assets.

    Telethon collection already writes `media_assets`. Manual WeChat intake originally saved
    files only in `raw_images_json`, so package freezing had pixels but no source identity.
    This helper fills that identity gap without copying/re-encoding any image or mixing groups.
    It is idempotent and only registers real, readable local files belonging to the same source_post.
    """
    source = conn.execute(
        "SELECT id, source_type, source_url, raw_images_json FROM source_posts WHERE id=? LIMIT 1",
        (source_post_id,),
    ).fetchone()
    if not source:
        raise ValueError("package_source_post_not_found")
    source_id = int(source[0])
    source_type = str(source[1] or "").strip().lower()
    source_url = str(source[2] or "")
    try:
        raw_images = json.loads(source[3] or "[]")
    except Exception:
        raw_images = []
    if not isinstance(raw_images, list):
        raw_images = []

    registered = 0
    skipped = 0
    missing = 0
    for index, item in enumerate(raw_images):
        path_value = item if isinstance(item, str) else (item.get("local_path") or item.get("path") if isinstance(item, dict) else "")
        path = Path(str(path_value or "")).expanduser()
        if not path.is_file():
            missing += 1
            continue
        existing = conn.execute(
            """SELECT id FROM media_assets
               WHERE owner_type='source_post' AND CAST(owner_ref_id AS TEXT)=CAST(? AS TEXT)
                 AND local_path=? LIMIT 1""",
            (str(source_id), str(path)),
        ).fetchone()
        if existing:
            skipped += 1
            continue
        try:
            raw = path.read_bytes()
            file_hash = hashlib.sha256(raw).hexdigest()
            with Image.open(path) as image:
                width, height = image.size
        except Exception:
            missing += 1
            continue
        # Deterministic source-local asset ID prevents duplicate identities during retries.
        asset_id = "AST_MAN_" + hashlib.sha256(f"{source_id}:{index}:{file_hash}".encode()).hexdigest()[:16].upper()
        meta = json.dumps({"origin": "manual_intake_raw_images", "source_post_db_id": source_id}, ensure_ascii=False, sort_keys=True)
        values = (
            asset_id, "source_post", source_id, str(source_id), "photo",
            "wechat" if source_type == "wechat_note" else (source_type or "manual"), source_url,
            None, str(path), None, file_hash, None, None, "photo", 0, 1 if index == 0 else 0,
            index, width, height, None, len(raw), "image/jpeg", meta, "active",
        )
        sql = """INSERT INTO media_assets (
                   asset_id,owner_type,owner_ref_id,owner_ref_key,asset_type,source_type,source_url,
                   source_file_id,local_path,file_url,file_hash,telegram_file_id,telegram_file_unique_id,
                   media_type,is_watermarked,is_cover,sort_order,width,height,duration,file_size,mime_type,
                   meta_json,status
               ) VALUES ({})""".format(",".join("?" for _ in values))
        conn.execute(sql, values)
        registered += 1
    return {"registered": registered, "existing": skipped, "missing": missing}


def _render_cover(d: dict, source: str, output: str, template: str) -> None:
    from cover_generator import _draw_new_cover
    try:
        normalized = json.loads(d.get("normalized_data") or "{}")
    except Exception:
        normalized = {}
    project_name = str(normalized.get("project_name") or d.get("project_name") or "").strip()
    project_alias = str(normalized.get("project_alias") or d.get("project_alias") or "").strip()
    area = str(normalized.get("public_location_display") or d.get("public_location_display") or normalized.get("canonical_area_display") or d.get("area") or "").strip()
    listing_kind = str(normalized.get("property_type_display") or d.get("property_type_display") or d.get("property_type") or "").strip()
    deal_type = str(normalized.get("deal_type") or d.get("deal_type") or "rent").strip().lower()
    # Do not promote a market location to a project title. When project is
    # unconfirmed, the cover uses a truthful generic property label and keeps
    # the public location in its own field.
    generic_projects = {"房源", "优质房源", "公寓房源", "别墅房源", "独栋别墅房源", "双拼别墅房源", "排屋房源", "联排房源"}
    trusted_project = "" if project_name in generic_projects else project_name
    public_title = trusted_project or (listing_kind if listing_kind else "优质房源")
    project_name = trusted_project
    if project_name and project_alias:
        compact_name = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", project_name.lower())
        compact_alias = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", project_alias.lower())
        if compact_alias and compact_alias not in compact_name and compact_name not in compact_alias:
            public_title = f"{project_name} · {project_alias}"
    raw_layout = str(normalized.get("layout") or d.get("layout") or "").strip()
    cover_layout_parts: list[str] = []
    # When a real project occupies the title line, keep the property type on
    # the separate layout line. Without a project, the generic title already
    # contains the type and the layout stays concise.
    for value in ((listing_kind if project_name else ""), raw_layout):
        if value and value not in cover_layout_parts:
            cover_layout_parts.append(value)
    cover_layout = " · ".join(cover_layout_parts)
    highlights = _json_list(d.get("highlights"))
    # Missing facts are represented explicitly, never as empty template separators.
    display_size = str(d.get("size") or "").strip() or ""
    display_floor = str(d.get("floor") or "").strip() or ""
    display_highlights = highlights
    raw_price = str(d.get("price") or "").strip()
    if raw_price and not raw_price.startswith("$"):
        raw_price = f"${raw_price}"
    price_suffix = "/月" if deal_type == "rent" else ""
    # 经典蓝卡的 HTML 使用独立 PRICE_SUFFIX；其余模板把单位放入价格行。
    display_price = raw_price if template == "classic_blue" else (
        f"{raw_price}{price_suffix}" if raw_price and price_suffix and "/月" not in raw_price else raw_price
    )
    _draw_new_cover(
        output_path=output,
        project=public_title,
        layout=cover_layout,
        area=area,
        price=display_price,
        size=display_size,
        floor=display_floor,
        highlights=display_highlights,
        base_image_path=source,
        source_type=str(d.get("source_type") or ""),
        source_name=str(d.get("source_name") or ""),
    )


def _json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(x) for x in value if str(x).strip()]
    try:
        data = json.loads(value or "[]")
        return [str(x) for x in data if str(x).strip()] if isinstance(data, list) else []
    except Exception:
        return [x.strip() for x in str(value or "").split("|") if x.strip()]


def _finalize_detail_image(source_path: str, target_path: str) -> str:
    """Create the exact detail bytes that Telegram will receive, without cropping or tinting."""
    from meihua_publisher import add_detail_logo_watermark, normalize_album_image
    raw = Path(source_path).read_bytes()
    resized = normalize_album_image(raw, target_size=1600, force_square=False)
    branded = add_detail_logo_watermark(resized, {})
    Path(target_path).parent.mkdir(parents=True, exist_ok=True)
    Path(target_path).write_bytes(branded.getvalue())
    return target_path


def _select_cover_source(paths: list[str], *, property_type: str = "") -> str:
    """从同一房源内选择适合手机频道的明亮实拍，不跨房源取图。"""
    if not paths:
        raise ValueError("missing_usable_images")
    from cover_generator import _score_image

    ranked: list[tuple[float, int, str]] = []
    kind = str(property_type or "").lower()
    is_house = any(token in kind for token in ("别墅", "排屋", "联排", "双拼", "townhouse", "villa"))
    for index, path in enumerate(paths):
        # 唯一主图评分来源；住宅大户型通常由来源方按“客厅/整体空间→卧室→设备”排序。
        # 对别墅、排屋和双拼，保留来源前两张的编辑顺序，避免通用亮度评分误选卧室或设备特写。
        score, _ = _score_image(path, property_type=property_type)
        if is_house and index == 0:
            score += 60
        elif is_house and index == 1:
            score += 35
        ranked.append((score, -index, path))
    return max(ranked)[2]


def _freeze_source_identity(conn, draft: dict) -> dict:
    source_db_id = draft.get("source_post_id")
    if source_db_id in (None, ""):
        raise ValueError("package_missing_source_post_id")
    source = conn.execute("SELECT * FROM source_posts WHERE id=? LIMIT 1", (source_db_id,)).fetchone()
    if not source:
        raise ValueError("package_source_post_not_found")
    try:
        raw_meta = json.loads(source["raw_meta_json"] or "{}")
    except Exception:
        raw_meta = {}
    grouped_id = None
    def walk(value):
        nonlocal grouped_id
        if grouped_id not in (None, ""):
            return
        if isinstance(value, dict):
            for key, item in value.items():
                if str(key).lower() in {"grouped_id", "media_group_id", "media_group"} and item not in (None, ""):
                    grouped_id = str(item)
                    return
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)
    walk(raw_meta)
    # Manual intake keeps original paths in raw_images_json; materialize immutable source asset
    # identities before freezing so the exact same source files are audited and later verified.
    ensure_source_media_assets(conn, source_db_id)
    media = conn.execute(
        """SELECT asset_id, file_hash, sort_order FROM media_assets
           WHERE owner_type='source_post'
             AND (CAST(owner_ref_id AS TEXT)=? OR owner_ref_key=?)
           ORDER BY COALESCE(sort_order, 999999), id""",
        (str(source_db_id), str(source_db_id)),
    ).fetchall()
    if not media:
        raise ValueError("package_source_post_has_no_media_assets")
    asset_ids = [str(row[0]) for row in media if row[0]]
    if not asset_ids:
        raise ValueError("package_source_media_missing_asset_ids")
    return {
        "schema": "source_identity_v1",
        "source_post_db_id": int(source["id"]),
        "source_post_id": str(source["source_post_id"] or ""),
        "source_message_id": str(source["source_post_id"] or ""),
        "source_id": str(source["source_id"] or ""),
        "source_type": str(source["source_type"] or ""),
        "source_name": str(source["source_name"] or ""),
        "source_url": str(source["source_url"] or ""),
        "grouped_id": grouped_id,
        "media_asset_ids": asset_ids,
        "media_asset_hashes": [str(row[1] or "") for row in media],
        "media_count": len(media),
    }


def _canonical_facts_from_draft(draft: dict[str, Any]) -> dict[str, Any]:
    try:
        facts = json.loads(draft.get("normalized_data") or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        facts = {}
    errors = validate_facts(facts)
    if errors:
        raise ValueError("canonical_facts_invalid:" + ",".join(errors))
    return facts


def build_package(
    db_path: str,
    draft_id: str,
    *,
    template_override: str = "",
    caption_variant_override: str = "",
) -> dict:
    ensure_schema(db_path)
    conn = sqlite3.connect(db_path); conn.row_factory = sqlite3.Row
    source_columns = {r[1] for r in conn.execute("PRAGMA table_info(source_posts)")}
    source_type_sql = "sp.source_type" if "source_type" in source_columns else "''"
    source_name_sql = "sp.source_name" if "source_name" in source_columns else "''"
    row = conn.execute(
        f"""SELECT d.*,{source_type_sql} AS source_type,{source_name_sql} AS source_name
            FROM drafts d JOIN source_posts sp ON sp.id=d.source_post_id
            WHERE d.draft_id=?""", (draft_id,)
    ).fetchone()
    if not row:
        conn.close(); raise ValueError("draft_not_found")
    frozen = conn.execute(
        "SELECT package_id FROM publication_packages WHERE draft_id=? AND status IN ('approved','published') LIMIT 1",
        (draft_id,),
    ).fetchone()
    if frozen:
        conn.close()
        raise ValueError("approved_package_frozen")
    d = dict(row)
    facts = _canonical_facts_from_draft(d)
    projection = draft_projection(facts)
    originals = _paths(conn, d.get("source_post_id"))
    gate = package_gate(facts, len(originals))
    if not gate["ok"]:
        conn.close(); raise ValueError("canonical_package_gate_blocked:" + ",".join(gate["errors"]))
    # The builder overlays only deterministic projections; legacy title/review
    # notes can never influence captions, template routing or package facts.
    d.update({
        "title": projection["title"], "project": projection["project"], "community": projection["community"],
        "area": projection["area"], "property_type": projection["property_type"], "price": projection["price"],
        "deal_type": projection.get("deal_type", ""),
        "original_price": projection.get("original_price"),
        "layout": projection["layout"], "size": projection["size"], "floor": projection["floor"],
        "deposit": projection["deposit"], "payment_terms": projection.get("payment_terms", ""),
        "contract_term": projection.get("contract_term", ""),
        "available_date": projection.get("available_date", ""),
        "management_fee": projection.get("management_fee", ""),
        "internet_fee": projection.get("internet_fee", ""),
        "water_rate": projection.get("water_rate", ""),
        "electric_rate": projection.get("electric_rate", ""),
        "parking_fee": projection.get("parking_fee", ""),
        "viewing_time": projection.get("viewing_time", ""),
        "video_viewing": projection.get("video_viewing", ""),
        "cost_notes": projection.get("cost_notes", ""),
        "advisor_comment": "",
        "drawbacks": [],
        "highlights": projection["highlights"],
    })
    listing_id = str(d.get("listing_id") or "").strip()
    if not re.fullmatch(r"(?i)l_\d+", listing_id):
        from meihua_publisher import system_listing_id_from_draft
        listing_id = system_listing_id_from_draft(d)
    materialize_listing(
        conn, draft_id=draft_id, listing_id=listing_id, facts=facts, media_count=len(originals),
        source_post_url=str(d.get("source_url") or ""),
    )
    conn.commit()
    routing = classify(source_type=d.get("source_type") or "", source_name=d.get("source_name") or "",
                       property_type=d.get("property_type") or "", project=d.get("project") or "",
                       price=d.get("price"), highlights=d.get("highlights"),
                       is_special=bool(d.get("is_special") or d.get("is_urgent")))
    styles = _default_publish_styles(
        conn,
        d.get("source_type") or "",
        d.get("source_name") or "",
        d.get("property_type") or "",
    )
    selected_variant = str(caption_variant_override or "").strip().lower()
    if selected_variant not in _ALLOWED_CAPTION_VARIANTS:
        saved = re.search(r"caption_variant:(a|b|c)", str(d.get("review_note") or ""), flags=re.I)
        selected_variant = saved.group(1).lower() if saved else styles["caption_variant"]
    styles["caption_variant"] = selected_variant
    template = template_override or styles["cover_template"] or routing["cover_template"]
    version = int(conn.execute("SELECT COALESCE(MAX(package_version),0)+1 FROM publication_packages WHERE draft_id=?", (draft_id,)).fetchone()[0])
    package_id = f"PKG_{re.sub(r'[^A-Za-z0-9]+','',draft_id)[-12:]}_v{version}"
    out = PACKAGE_ROOT / package_id; out.mkdir(parents=True, exist_ok=True)
    # The cover must receive the canonical listing key, never the internal draft UUID.
    d["listing_id"] = listing_id
    cover_source = _select_cover_source(originals, property_type=str(d.get("property_type") or ""))
    cover = out / "cover.jpg"; _render_cover(d, cover_source, str(cover), template)
    processed = []
    badge = "微信实拍" if routing["source_type"] == "wechat" else ""
    detail_sources = [path for path in originals if path != cover_source]
    for index, path in enumerate(detail_sources, start=2):
        target = out / f"image_{index:02d}.jpg"
        # Package 阶段生成最终详情字节；Publisher 之后只读取并发送，不再加 Logo/调色/裁切。
        _finalize_detail_image(path, str(target)); processed.append(str(target))
    main = [str(cover)] + processed[:3]
    discussion = processed[3:]
    from meihua_publisher import (
        build_chinese_listing_post,
        build_discussion_detail_text,
        assert_public_output_safe,
        _parsed_normalized,
        _public_clean_text,
    )
    public_token = "ql" + hashlib.sha256(f"{draft_id}:{version}:{d.get('listing_id') or d.get('property_id') or ''}".encode()).hexdigest()[:14]
    post_text = build_chinese_listing_post(d, caption_variant=styles["caption_variant"], post_token=public_token)
    discussion_text = build_discussion_detail_text(d)

    safe_fee_text = _public_clean_text(d.get("cost_notes"))
    safe_advice_text = _public_clean_text(d.get("advisor_comment"))
    assert_public_output_safe(post_text, discussion_text, safe_advice_text, context="publication_package")
    # 发布包只能绑定正式 listings 主键，禁止用 draft_id 冒充 property_id。
    listing_id = str(d.get("listing_id") or "").strip()
    if not re.fullmatch(r"(?i)l_\d+", listing_id):
        conn.close()
        raise ValueError("missing_canonical_listing_id")
    live = conn.execute(
        "SELECT listing_id, price, area, layout, status FROM listings WHERE listing_id=? LIMIT 1",
        (listing_id,),
    ).fetchone()
    if not live:
        conn.close()
        raise ValueError("canonical_listing_not_found")
    draft_price = d.get("price")
    live_price = live[1]
    try:
        price_mismatch = draft_price not in (None, "") and live_price not in (None, "") and float(draft_price) != float(live_price)
    except (TypeError, ValueError):
        price_mismatch = str(draft_price or "").strip() != str(live_price or "").strip()
    area_mismatch = str(d.get("area") or "").strip() != str(live[2] or "").strip()
    layout_mismatch = str(d.get("layout") or "").strip() != str(live[3] or "").strip()
    if price_mismatch or area_mismatch or layout_mismatch:
        conn.close()
        raise ValueError("canonical_listing_fields_mismatch")
    source_identity = _freeze_source_identity(conn, d)
    source_identity_json = json.dumps(source_identity, ensure_ascii=False, sort_keys=True)
    source_identity_hash = hashlib.sha256(source_identity_json.encode()).hexdigest()
    snapshot = {k: d.get(k) for k in (
        "draft_id","listing_id","project","project_name","project_alias","area","property_type",
        "price","original_price","layout","size","floor","deposit","contract_term","available_date",
        "management_fee","internet_fee","water_rate","electric_rate","parking_fee","viewing_time",
        "video_viewing","highlights",
    )}
    snapshot.update({
        "source_post_db_id": source_identity["source_post_db_id"],
        "source_post_id": source_identity["source_post_id"],
        "source_message_id": source_identity["source_message_id"],
        "source_name": source_identity["source_name"],
        "source_type": source_identity["source_type"],
        "grouped_id": source_identity["grouped_id"],
        "media_asset_ids": source_identity["media_asset_ids"],
    })
    normalized_snapshot = _parsed_normalized(d)
    snapshot["land_size"] = normalized_snapshot.get("land_size", "")
    snapshot["building_size"] = normalized_snapshot.get("building_size", "")
    snapshot["listing_id"] = listing_id
    snapshot["canonical_facts"] = package_snapshot(facts, listing_id, source_identity.get("media_asset_hashes", []))
    snapshot["canonical_projection_hash"] = canonical_projection_hash(facts)
    snapshot["quality_json"] = facts.get("quality") or {}
    snapshot["publish_style_scope"] = styles["scope"]
    snapshot["caption_variant"] = styles["caption_variant"]
    snapshot["cover_template"] = template
    # FREEZE_V2 binds the exact bytes that will be sent, not only asset IDs/paths.
    frozen_paths = list(dict.fromkeys(main + discussion))
    snapshot["freeze_schema"] = "FREEZE_V2"
    snapshot["frozen_file_hashes"] = {
        str(Path(path)): hashlib.sha256(Path(path).read_bytes()).hexdigest()
        for path in frozen_paths
        if Path(path).is_file()
    }
    if len(snapshot["frozen_file_hashes"]) != len(frozen_paths):
        conn.close()
        raise ValueError("package_frozen_media_file_missing")
    payload = {"package_id": package_id, "draft_id": draft_id, "property_id": listing_id,
               "public_token": public_token, "package_version": version, **routing, "cover_template": template,
               "cover_path": str(cover), "main_images": main, "discussion_images": discussion,
               "post_text": post_text, "discussion_text": discussion_text, "snapshot": snapshot,
               "source_identity_hash": source_identity_hash, "media_asset_hashes": source_identity.get("media_asset_hashes", [])}
    digest = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    # Approved/published rows are immutable evidence. A redo must happen before
    # approval, or on a new draft with a new package.
    # A rebuilt package supersedes only unpublished review versions.
    conn.execute(
        """UPDATE publication_packages
           SET status='superseded', updated_at=CURRENT_TIMESTAMP
           WHERE draft_id=? AND status='package_ready'""",
        (draft_id,),
    )
    conn.execute("""INSERT INTO publication_packages
      (package_id,draft_id,property_id,package_version,source_type,listing_type,media_type,cover_template,status,
       cover_path,main_images_json,discussion_images_json,post_text,discussion_text,fee_text,advice_text,snapshot_json,content_hash,
       source_identity_json,source_identity_hash,source_identity_migrated_at,public_token,
       canonical_facts_hash,canonical_facts_schema,publication_location_level,
       canonical_projection_hash,canonical_provenance_json,quality_json)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
      (package_id,draft_id,payload["property_id"],version,routing["source_type"],routing["listing_type"],routing["media_type"],template,
       "package_ready",str(cover),json.dumps(main,ensure_ascii=False),json.dumps(discussion,ensure_ascii=False),post_text,discussion_text,
       safe_fee_text,safe_advice_text,json.dumps(snapshot,ensure_ascii=False),digest,
       source_identity_json,source_identity_hash,now_utc(),public_token,
       facts["canonical_facts_hash"],facts["schema_version"],facts.get("publication_location_level"),
       canonical_projection_hash(facts), json.dumps(facts.get("manual_overrides") or [], ensure_ascii=False, sort_keys=True),
       json.dumps(facts.get("quality") or {}, ensure_ascii=False, sort_keys=True)))
    conn.commit(); conn.close(); return payload


def render_cover_preview(db_path: str, draft_id: str, output_path: str, *, template_override: str = "") -> dict:
    """Render the same HTML cover used by build_package, without creating a package or mutating DB."""
    conn = sqlite3.connect(db_path); conn.row_factory = sqlite3.Row
    try:
        source_columns = {r[1] for r in conn.execute("PRAGMA table_info(source_posts)")}
        source_type_sql = "sp.source_type" if "source_type" in source_columns else "''"
        source_name_sql = "sp.source_name" if "source_name" in source_columns else "''"
        row = conn.execute(
            f"""SELECT d.*,{source_type_sql} AS source_type,{source_name_sql} AS source_name
                FROM drafts d JOIN source_posts sp ON sp.id=d.source_post_id
                WHERE d.draft_id=?""", (draft_id,)
        ).fetchone()
        if not row:
            raise ValueError("draft_not_found")
        d = dict(row)
        try:
            facts = json.loads(d.get("normalized_data") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            facts = {}
        if isinstance(facts, dict) and facts:
            from qiaolian_dual.canonical_facts import draft_projection
            projection = draft_projection(facts)
            d.update({
                "title": projection.get("title", ""),
                "project": projection.get("project", ""),
                "community": projection.get("community", ""),
                "area": projection.get("public_location_display", projection.get("area", "")),
                "property_type": projection.get("property_type", ""),
                "deal_type": projection.get("deal_type", ""),
                "price": projection.get("price", ""),
                "layout": projection.get("layout", ""),
                "size": projection.get("size", ""),
                "floor": projection.get("floor", ""),
                "deposit": projection.get("deposit", ""),
                "highlights": projection.get("highlights", []),
            })
        originals = _paths(conn, d.get("source_post_id"))
        if not originals:
            raise ValueError("missing_usable_images")
        routing = classify(source_type=d.get("source_type") or "", source_name=d.get("source_name") or "",
                           property_type=d.get("property_type") or "", project=d.get("project") or "",
                           price=d.get("price"), highlights=d.get("highlights"),
                           is_special=bool(d.get("is_special") or d.get("is_urgent")))
        template = template_override or routing["cover_template"]
        source = _select_cover_source(originals, property_type=str(d.get("property_type") or ""))
        _render_cover(d, source, output_path, template)
        preview_images = [output_path]
        preview_stem = Path(output_path).stem
        for index, path in enumerate([p for p in originals if p != source][:3], start=2):
            target = str(Path(output_path).with_name(f"{preview_stem}_detail_{index:02d}.jpg"))
            _finalize_detail_image(path, target)
            preview_images.append(target)
        return {
            "draft_id": draft_id,
            "template": template,
            "source_image": source,
            "source_post_id": d.get("source_post_id"),
            "project": d.get("project") or "",
            "area": d.get("area") or "",
            "layout": d.get("layout") or "",
            "price": d.get("price") or "",
            "output_path": output_path,
            "final_images": preview_images,
        }
    finally:
        conn.close()


def approve_package(db_path: str, draft_id: str, approved_by: str = "") -> dict:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM publication_packages WHERE draft_id=? AND status='package_ready' ORDER BY package_version DESC LIMIT 1", (draft_id,)).fetchone()
        if not row:
            raise ValueError("package_not_ready")
        existing_approved = conn.execute(
            "SELECT package_id FROM publication_packages WHERE draft_id=? AND status IN ('approved','published') LIMIT 1",
            (draft_id,),
        ).fetchone()
        if existing_approved:
            raise ValueError("approved_package_frozen")
        property_id = str(row["property_id"] or "").strip()
        if not re.fullmatch(r"(?i)l_\d+", property_id):
            raise ValueError("package_missing_canonical_listing_id")
        live = conn.execute("SELECT listing_id FROM listings WHERE listing_id=? LIMIT 1", (property_id,)).fetchone()
        if not live:
            raise ValueError("package_canonical_listing_not_found")
        try:
            snapshot = json.loads(row["snapshot_json"] or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            raise ValueError("package_snapshot_invalid")
        if str(snapshot.get("listing_id") or "").strip() != property_id:
            raise ValueError("package_snapshot_listing_mismatch")
        live_draft = conn.execute("SELECT normalized_data FROM drafts WHERE draft_id=?", (draft_id,)).fetchone()
        if not live_draft:
            raise ValueError("package_draft_not_found")
        try:
            live_facts = json.loads(live_draft[0] or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            raise ValueError("package_draft_canonical_facts_invalid")
        live_errors = validate_facts(live_facts)
        if live_errors:
            raise ValueError("package_draft_canonical_facts_invalid:" + ",".join(live_errors))
        frozen_facts = snapshot.get("canonical_facts") or {}
        if str(frozen_facts.get("canonical_facts_hash") or "") != str(live_facts.get("canonical_facts_hash") or ""):
            raise ValueError("package_canonical_facts_mismatch")
        if str(row["canonical_facts_hash"] or "") != str(live_facts.get("canonical_facts_hash") or ""):
            raise ValueError("package_canonical_hash_column_mismatch")
        live_projection_hash = canonical_projection_hash(live_facts)
        if str(snapshot.get("canonical_projection_hash") or "") != live_projection_hash:
            raise ValueError("package_canonical_projection_mismatch")
        if str(row["canonical_projection_hash"] or "") != live_projection_hash:
            raise ValueError("package_canonical_projection_column_mismatch")
        try:
            frozen_images = json.loads(row["main_images_json"] or "[]")
        except (TypeError, ValueError, json.JSONDecodeError):
            frozen_images = []
        publishability = evaluate_publishability(
            live_facts,
            media_count=len(frozen_images),
            cover_exists=bool(row["cover_path"] and Path(str(row["cover_path"])).is_file()),
        )
        if not publishability["ok"]:
            raise ValueError("package_publishability_blocked:" + ",".join(publishability["blocking"]))
        conn.execute("UPDATE publication_packages SET status='approved',approved_by=?,approved_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE id=?", (approved_by,row["id"]))
        conn.execute("UPDATE drafts SET review_status='approved',updated_at=CURRENT_TIMESTAMP WHERE draft_id=? AND review_status IN ('pending','ready','package_ready')", (draft_id,))
        conn.commit(); return dict(row)


def approved_package(db_path: str, draft_id: str) -> dict | None:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM publication_packages WHERE draft_id=? AND status='approved' ORDER BY package_version DESC LIMIT 1", (draft_id,)).fetchone()
        return dict(row) if row else None
