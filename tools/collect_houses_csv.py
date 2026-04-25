#!/usr/bin/env python3
"""
Collect listing-like rows from SQLite and export a publish-ready houses.csv.

Default output is data/houses.csv, which can be consumed by tools/publish_houses_csv.py.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "qiaolian_dual_bot.db"
DEFAULT_OUT = ROOT / "data" / "houses.csv"

CSV_FIELDS = [
    "title",
    "area",
    "type",
    "price",
    "image_cover",
    "image2",
    "image3",
    "image4",
    "feature1",
    "feature2",
    "feature3",
    "brand",
    "caption",
    "contact",
    "project",
    "layout",
    "size",
    "floor",
    "available_date",
    "highlights",
    "drawbacks",
    "cost_notes",
    "payment_terms",
    "contract_term",
    "furniture",
    "hashtags",
    "tags",
    "bg_image",
    "listing_id",
    "draft_id",
    "source_post_id",
    "source_name",
    "source_url",
]

RENT_SIGNALS = (
    "出租",
    "租房",
    "月租",
    "押",
    "公寓",
    "别墅",
    "排屋",
    "看房",
    "studio",
    "apartment",
    "villa",
)

NON_RENT_SIGNALS = (
    "机车",
    "摩托",
    "川崎",
    "雅马哈",
    "本田",
    "喷漆",
    "现车",
    "二手车",
    "卖车",
    "买车",
    "车辆",
    "招聘",
    "手机",
    "ipad",
    "待审核",
    "test listing",
    "verification",
)

SALE_SIGNALS = (
    "sale",
    "for sale",
    "出售",
    "出售信息",
    "售房",
    "售楼",
)

GENERIC_TITLE_VALUES = {
    "",
    "公寓",
    "apartment",
    "出租",
    "租房",
    "房源",
    "房源推荐",
    "整租",
    "短租",
    "待审核",
}

STATUS_PRIORITY = {
    "ready": 0,
    "published": 1,
    "pending": 2,
}

DISPLAY_NOISE_TOKENS = ("啊雷莎", "阿雷莎", "🇨🇳", "🌵")
GENERIC_PROJECT_VALUES = {
    "",
    "公寓",
    "别墅",
    "排屋",
    "住宅",
    "社区",
    "小区",
    "金边",
}


def _safe_text(v: object) -> str:
    return str(v or "").strip()


def _clean_display_text(v: object) -> str:
    text = _safe_text(v)
    if not text:
        return ""
    for token in DISPLAY_NOISE_TOKENS:
        text = text.replace(token, " ")
    text = re.sub(r"^\s*\d{3,4}(?!米)", "", text)
    text = re.sub(r"[#⭐️✨🏠🏡🏢🔥📍💰✅📝☎️]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" -｜|·•,，")
    return text


def _clean_project_text(v: object) -> str:
    text = _clean_display_text(v)
    if text in GENERIC_PROJECT_VALUES:
        return ""
    return text


def _parse_list(raw: object) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    text = str(raw).strip()
    if not text:
        return []
    try:
        val = json.loads(text)
        if isinstance(val, list):
            return [str(x).strip() for x in val if str(x).strip()]
    except Exception:
        pass
    parts = [p.strip() for p in re.split(r"[|｜；;、,\n]+", text) if p.strip()]
    return parts


def _norm_price(raw: object) -> str:
    s = _safe_text(raw)
    if not s:
        return ""
    if re.search(r"-\s*\d", s):
        return ""
    digits = re.sub(r"[^\d]", "", s)
    if not digits:
        return ""
    try:
        value = int(digits)
    except ValueError:
        return ""
    if value <= 0:
        return ""
    return f"${value}/月"


def _parse_json_dict(raw: object) -> dict:
    if isinstance(raw, dict):
        return raw
    text = _safe_text(raw)
    if not text:
        return {}
    try:
        value = json.loads(text)
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _normalize_payment_terms(raw: object) -> str:
    text = _safe_text(raw)
    if not text:
        return ""
    m = re.search(
        r"押\s*([一二三四五六七八九十两0-9]{1,3})(?:个?月)?\s*[，,/、\s]*付\s*([一二三四五六七八九十两0-9]{1,3})(?:个?月)?",
        text,
    )
    if m:
        return f"押{m.group(1)}付{m.group(2)}"
    m_dep = re.search(r"(?:deposit|押金)\s*[:：]?\s*([0-9]+(?:\.\d+)?)", text, flags=re.I)
    if m_dep:
        n = m_dep.group(1).rstrip("0").rstrip(".")
        return f"押{n}月"
    m_cn = re.search(r"(押[一二三四五六七八九十两0-9][^，。；;\s]{0,8})", text)
    if m_cn:
        return m_cn.group(1)
    return ""


def _normalize_contract_term(raw: object) -> str:
    text = _safe_text(raw)
    if not text:
        return ""
    m_direct = re.search(r"^([一二三四五六七八九十两0-9]{1,3})\s*(年|个月|月)$", text)
    if m_direct:
        return f"{m_direct.group(1)}{m_direct.group(2)}"
    m = re.search(
        r"(?:合同|租期|lease|contract|term|min(?:imum)?\s*lease)\s*(?:期限|期|:|：)?\s*([一二三四五六七八九十两0-9]{1,3})\s*(年|个月|月|month|months|year|years|yr|yrs)",
        text,
        flags=re.I,
    )
    if not m:
        m = re.search(r"([0-9]{1,2})\s*(year|years|yr|yrs|month|months|mo)\s*(?:lease|contract|term)?", text, flags=re.I)
    if not m:
        m = re.search(r"([一二三四五六七八九十两]{1,3})\s*年\s*(?:起租|合同|租期)?", text)
        if m:
            return f"{m.group(1)}年"
        return ""
    num = _safe_text(m.group(1))
    unit = _safe_text(m.group(2)).lower()
    if unit in {"year", "years", "yr", "yrs"}:
        unit = "年"
    elif unit in {"month", "months", "mo"}:
        unit = "个月"
    return f"{num}{unit}" if num and unit else ""


def _price_value(raw: object) -> int | None:
    s = _safe_text(raw)
    if not s:
        return None
    if re.search(r"-\s*\d", s):
        return None
    digits = re.sub(r"[^\d]", "", s)
    if not digits:
        return None
    try:
        value = int(digits)
    except ValueError:
        return None
    return value if value > 0 else None


def _pick_features(highlights: list[str]) -> tuple[str, str, str]:
    vals = highlights[:3]
    while len(vals) < 3:
        vals.append("")
    return vals[0], vals[1], vals[2]


def _is_generic_title(title: str) -> bool:
    cleaned = re.sub(r"[\s|｜,，·•\-_/]+", "", str(title or "").strip().lower())
    return cleaned in GENERIC_TITLE_VALUES


def _has_layout_signal(*texts: str) -> bool:
    blob = " ".join(str(t or "") for t in texts)
    return bool(
        re.search(
            r"([1-9]\s*房)|([1-9]\s*bed)|(\bstudio\b)|(\bloft\b)|(\b[1-9]\s*br\b)|([1-9]\s*室)",
            blob,
            flags=re.IGNORECASE,
        )
    )


def _normalized_token(raw: str) -> str:
    token = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", str(raw or "").lower())
    return token.strip()


def _is_listing_like(
    title: str,
    project: str,
    property_type: str,
    layout: str,
    area: str,
    raw_text: str,
    price_raw: object,
) -> bool:
    blob = " ".join([title, project, property_type, layout, area, raw_text]).lower()
    has_rent_signal = any(s.lower() in blob for s in RENT_SIGNALS)
    has_sale_signal = any(s.lower() in blob for s in SALE_SIGNALS)
    if has_sale_signal and not has_rent_signal:
        return False
    if any(s.lower() in blob for s in NON_RENT_SIGNALS):
        return False

    price_val = _price_value(price_raw)
    if price_val is not None and price_val > 20000:
        # 通常是售卖总价，不适合租赁频道自动发布。
        return False
    has_valid_price = price_val is not None and 150 <= price_val <= 12000
    has_layout = _has_layout_signal(title, project, layout, property_type, raw_text)
    has_area = len(_normalized_token(area)) >= 2

    # 信息太弱时直接丢弃（比如只有“公寓”）。
    if not (has_valid_price or has_layout or has_rent_signal):
        return False
    if _is_generic_title(title) and not (has_valid_price and has_layout):
        return False
    if not has_area and _is_generic_title(project):
        return False
    return True


def _is_image_path(path: str) -> bool:
    p = _safe_text(path).lower()
    return p.endswith((".jpg", ".jpeg", ".png", ".webp"))


def _fetch_media_paths(conn: sqlite3.Connection, draft_id_num: int, source_post_id: int) -> tuple[str, list[str]]:
    draft_cover = conn.execute(
        """
        SELECT local_path
        FROM media_assets
        WHERE owner_type='draft' AND owner_ref_id=? AND status='active'
        ORDER BY is_cover DESC, sort_order ASC, id ASC
        LIMIT 1
        """,
        (draft_id_num,),
    ).fetchone()
    source_rows = conn.execute(
        """
        SELECT local_path
        FROM media_assets
        WHERE owner_type='source_post' AND owner_ref_id=? AND status='active'
        ORDER BY sort_order ASC, id ASC
        """,
        (source_post_id,),
    ).fetchall()
    source_paths = [
        _safe_text(r["local_path"])
        for r in source_rows
        if _safe_text(r["local_path"]) and _is_image_path(_safe_text(r["local_path"]))
    ]
    cover = _safe_text(draft_cover["local_path"]) if draft_cover else ""
    if cover and not _is_image_path(cover):
        cover = ""
    if not cover and source_paths:
        cover = source_paths[0]
    rest = [p for p in source_paths if p and p != cover]
    return cover, rest


def _join_tags(area: str, layout: str, property_type: str) -> str:
    tags = ["#金边租房", "#侨联实拍"]
    for raw in (area, layout, property_type):
        t = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", _safe_text(raw))
        if not t:
            continue
        tag = f"#{t}"
        if tag not in tags:
            tags.append(tag)
        if len(tags) >= 5:
            break
    return " ".join(tags)


def _compose_title(title: str, project: str, area: str, layout: str, property_type: str, price: str) -> str:
    preferred = _clean_project_text(title) or _clean_project_text(project)
    if preferred and not _is_generic_title(preferred):
        return preferred[:64]

    chunks: list[str] = []
    area_s = _safe_text(area)
    layout_s = _safe_text(layout)
    ptype_s = _safe_text(property_type)
    price_s = _safe_text(price)

    if area_s:
        chunks.append(area_s)
    if layout_s:
        chunks.append(layout_s)
    elif ptype_s:
        chunks.append(ptype_s)
    if price_s:
        chunks.append(price_s)
    if not chunks:
        chunks = [_clean_project_text(project) or "金边精选房源"]
    return " | ".join(chunks)[:64]


def _canonical_property_type(property_type: str, title: str, project: str, raw_text: str) -> str:
    blob = " ".join([property_type, title, project, raw_text])
    lower = blob.lower()
    if any(token in blob for token in ("独栋", "双拼", "泳池独栋")) or "villa" in lower or "别墅" in blob:
        return "别墅"
    if "排屋" in blob or "townhouse" in lower:
        return "排屋"
    if "服务式" in blob or "serviced apartment" in lower:
        return "服务式公寓"
    return _clean_project_text(property_type) or "公寓"


def _fingerprint(project: str, area: str, layout: str, property_type: str, price: str) -> str:
    return "|".join(
        [
            _normalized_token(project),
            _normalized_token(area),
            _normalized_token(layout or property_type),
            _normalized_token(price),
        ]
    )


def _pick_listing_id(raw_listing_id: str, draft_id_num: int, source_post_id: int) -> str:
    val = _safe_text(raw_listing_id)
    if val:
        if val.startswith("QJ-") and val[3:].isdigit():
            return f"l_{val[3:]}"
        return val
    if draft_id_num > 0:
        return f"l_{draft_id_num}"
    if source_post_id > 0:
        return f"sp_{source_post_id}"
    return f"l_{int(time.time())}"


def export_csv(
    db_path: Path,
    out_csv: Path,
    *,
    statuses: list[str],
    limit: int,
    require_images: bool,
    dedupe_source_post: bool,
    dedupe_fingerprint: bool,
    contact: str,
    brand: str,
    allow_no_price: bool,
) -> tuple[int, int]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ",".join("?" for _ in statuses)
        rows = conn.execute(
            f"""
            SELECT
                d.id AS draft_id_num,
                d.draft_id,
                d.listing_id,
                d.source_post_id,
                d.title,
                d.project,
                d.area,
                d.property_type,
                d.price,
                d.layout,
                d.size,
                d.floor,
                d.deposit,
                d.available_date,
                d.highlights,
                d.drawbacks,
                d.cost_notes,
                d.normalized_data,
                d.extracted_data,
                d.review_status,
                d.updated_at,
                d.queue_score,
                sp.source_name,
                sp.source_url,
                sp.raw_text
            FROM drafts d
            LEFT JOIN source_posts sp ON sp.id = d.source_post_id
            WHERE d.review_status IN ({placeholders})
            ORDER BY d.updated_at DESC, d.id DESC
            LIMIT ?
            """,
            (*statuses, max(limit * 4, limit)),
        ).fetchall()
        prioritized = sorted(
            rows,
            key=lambda r: (
                STATUS_PRIORITY.get(_safe_text(r["review_status"]).lower(), 9),
                -(float(r["queue_score"] or 0)),
                -int(r["draft_id_num"] or 0),
            ),
        )

        out_csv.parent.mkdir(parents=True, exist_ok=True)
        written = 0
        scanned = 0
        seen_source_posts: set[int] = set()
        seen_fingerprints: set[str] = set()
        with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
            for row in prioritized:
                if written >= limit:
                    break
                scanned += 1
                title = _clean_display_text(row["title"])
                project = _clean_project_text(row["project"])
                property_type = _safe_text(row["property_type"])
                area = _safe_text(row["area"]) or "金边"
                layout = _safe_text(row["layout"])
                raw_text = _safe_text(row["raw_text"])
                if not _is_listing_like(title, project, property_type, layout, area, raw_text, row["price"]):
                    continue

                source_post_id = int(row["source_post_id"] or 0)
                if dedupe_source_post and source_post_id > 0:
                    if source_post_id in seen_source_posts:
                        continue
                draft_id_num = int(row["draft_id_num"] or 0)
                cover, extras = _fetch_media_paths(conn, draft_id_num, source_post_id)
                if require_images and not cover:
                    continue

                highlights = _parse_list(row["highlights"])
                drawbacks = _parse_list(row["drawbacks"])
                f1, f2, f3 = _pick_features(highlights)
                ptype = _canonical_property_type(property_type, title, project, raw_text)
                price = _norm_price(row["price"])
                if not price:
                    price = "价格私聊"
                if (not allow_no_price) and ("私聊" in price):
                    continue
                canonical_project = _clean_project_text(row["project"]) or title or area
                canonical_title = _compose_title(title, canonical_project, area, layout, ptype, price)
                row_fp = _fingerprint(canonical_project or canonical_title, area, layout, ptype, price)
                if dedupe_fingerprint and row_fp in seen_fingerprints:
                    continue
                normalized = _parse_json_dict(row["normalized_data"])
                extracted = _parse_json_dict(row["extracted_data"])
                payment_terms = _normalize_payment_terms(
                    normalized.get("payment_terms")
                    or extracted.get("payment_terms")
                    or normalized.get("deposit")
                    or extracted.get("deposit")
                    or row["deposit"]
                    or row["cost_notes"]
                    or raw_text
                )
                contract_term = _normalize_contract_term(
                    normalized.get("contract_term")
                    or extracted.get("contract_term")
                    or row["cost_notes"]
                    or raw_text
                )

                rec = {
                    "title": canonical_title,
                    "area": area,
                    "type": ptype,
                    "price": price,
                    "image_cover": cover,
                    "image2": extras[0] if len(extras) > 0 else "",
                    "image3": extras[1] if len(extras) > 1 else "",
                    "image4": extras[2] if len(extras) > 2 else "",
                    "feature1": f1,
                    "feature2": f2,
                    "feature3": f3,
                    "brand": brand,
                    "caption": "",
                    "contact": contact,
                    "listing_id": _pick_listing_id(_safe_text(row["listing_id"]), draft_id_num, source_post_id),
                    "draft_id": _safe_text(row["draft_id"]),
                    "source_post_id": str(source_post_id),
                    "source_name": _safe_text(row["source_name"]),
                    "source_url": _safe_text(row["source_url"]),
                    "project": canonical_project or canonical_title,
                    "layout": layout or ptype,
                    "size": _safe_text(row["size"]),
                    "floor": _safe_text(row["floor"]),
                    "available_date": _safe_text(row["available_date"]),
                    "highlights": "｜".join(highlights[:3]),
                    "drawbacks": "｜".join(drawbacks[:2]),
                    "cost_notes": _safe_text(row["cost_notes"]),
                    "payment_terms": payment_terms,
                    "contract_term": contract_term,
                    "furniture": "",
                    "hashtags": _join_tags(area, layout, ptype),
                    "tags": _join_tags(area, layout, ptype),
                    "bg_image": extras[0] if len(extras) > 0 else cover,
                }
                writer.writerow(rec)
                written += 1
                if dedupe_source_post and source_post_id > 0:
                    seen_source_posts.add(source_post_id)
                if dedupe_fingerprint:
                    seen_fingerprints.add(row_fp)
        return written, scanned
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Collect listing rows from DB into houses.csv")
    ap.add_argument("--db", default=os.getenv("DB_PATH", str(DEFAULT_DB)))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--statuses", default="ready,published,pending", help="Comma-separated draft statuses")
    ap.add_argument("--limit", type=int, default=80)
    ap.add_argument("--allow-no-images", action="store_true")
    ap.add_argument("--allow-no-price", action="store_true")
    ap.add_argument("--contact", default=os.getenv("QIAOLIAN_CONTACT", "@pengqingw"))
    ap.add_argument("--brand", default=os.getenv("QIAOLIAN_BRAND", "侨联地产"))
    ap.add_argument(
        "--dedupe-source-post",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Keep only latest row per source_post_id.",
    )
    ap.add_argument(
        "--dedupe-fingerprint",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Drop near-duplicate rows by project/area/layout/price fingerprint.",
    )
    args = ap.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    out_csv = Path(args.out).expanduser().resolve()
    statuses = [s.strip() for s in str(args.statuses).split(",") if s.strip()]
    if not statuses:
        statuses = ["pending", "ready", "published"]

    if not db_path.is_file():
        raise FileNotFoundError(f"db not found: {db_path}")

    written, scanned = export_csv(
        db_path,
        out_csv,
        statuses=statuses,
        limit=max(1, int(args.limit)),
        require_images=not args.allow_no_images,
        dedupe_source_post=bool(args.dedupe_source_post),
        dedupe_fingerprint=bool(args.dedupe_fingerprint),
        contact=str(args.contact or "@pengqingw").strip(),
        brand=str(args.brand or "侨联地产").strip(),
        allow_no_price=bool(args.allow_no_price),
    )
    print(f"houses.csv generated: {out_csv}")
    print(f"rows_written={written} rows_scanned={scanned}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
