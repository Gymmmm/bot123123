#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""微信笔记直导入口：粘贴原文 -> 自动抽字段 -> 写入 source_posts + excel_* 表。"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import time
import uuid
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "qiaolian_dual_bot.db"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _clean(v: str) -> str:
    return str(v or "").strip()


def _pick(patterns: list[str], text: str) -> str:
    for p in patterns:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            return _clean(m.group(1))
    return ""


def parse_wechat_note(raw_text: str) -> dict:
    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    title = lines[0] if lines else "微信笔记房源"

    area = _pick(
        [
            r"(?:位置|地址|区域)[:：]\s*([^\n]+)",
            r"(BKK1|BKK2|BKK3|俄罗斯市场|洪森大道|钻石岛|森速|富力城|炳发城)",
        ],
        raw_text,
    )
    layout = _pick(
        [
            r"(?:户型|房型)[:：]\s*([^\n]+)",
            r"(\d+\s*房(?:\+\d+\s*保姆房)?[^\n]{0,20})",
        ],
        raw_text,
    )
    price = _pick(
        [
            r"(?:租金|月租|价格)[:：]\s*\$?\s*([0-9][0-9,]{2,})",
            r"\$([0-9][0-9,]{2,})\s*(?:/月|每月)?",
        ],
        raw_text,
    ).replace(",", "")
    payment_terms = _pick(
        [
            r"(?:押金|押付|付款)[:：]\s*(押[^\n]+)",
            r"(押\s*[一二三四五六七八九十两0-9]+\s*付\s*[一二三四五六七八九十两0-9]+)",
        ],
        raw_text,
    )
    contract_term = _pick(
        [
            r"(?:合同|租期)[:：]\s*([0-9一二三四五六七八九十两]+\s*(?:年|个月|月))",
            r"([0-9]+\s*(?:year|years|month|months))",
        ],
        raw_text,
    )
    size = _pick(
        [
            r"(?:面积|建面|建筑面积)[:：]\s*([0-9xX×米平方米m²M² ]+)",
        ],
        raw_text,
    )
    furnishing = _pick(
        [
            r"(?:配套|家具|配置)[:：]\s*([^\n]+)",
        ],
        raw_text,
    )
    contact = _pick(
        [
            r"(?:飞机|telegram|tg)[:：]\s*(@[A-Za-z0-9_]+)",
            r"(?:微信|wechat)[:：]\s*([A-Za-z0-9_]+)",
            r"(?:电话|phone)[:：]\s*([+0-9]{6,})",
        ],
        raw_text,
    )

    prop_type = "公寓"
    low = raw_text.lower()
    if "别墅" in raw_text or "villa" in low:
        prop_type = "别墅"
    elif "排屋" in raw_text or "townhouse" in low:
        prop_type = "排屋"
    elif "商铺" in raw_text or "shophouse" in low or "店铺" in raw_text:
        prop_type = "商铺"

    highlights = []
    # 优先把 furnishing 字段作为第一条亮点（如"全新家具齐全+网络+打扫"）
    if furnishing:
        short = furnishing.replace("+", "、").replace("，", "、").split("、")[0].strip()[:12]
        if short:
            highlights.append(short)
    for key in (
        "南北通透", "通透", "视野", "采光", "景观", "全新",
        "拎包", "近商圈", "交通便利", "泳池", "安保", "车位",
        "安静", "高层", "江景", "城景",
    ):
        if key in raw_text and key not in highlights:
            highlights.append(key)
        if len(highlights) >= 3:
            break
    highlights = highlights[:3]

    return {
        "title": title,
        "area": area,
        "layout": layout,
        "property_type": prop_type,
        "price": int(price) if price.isdigit() else None,
        "payment_terms": payment_terms,
        "contract_term": contract_term,
        "size": size,
        "furnishing": furnishing,
        "contact": contact,
        "highlights": highlights,
    }


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def main() -> int:
    parser = argparse.ArgumentParser(description="Import WeChat note into qiaolian pipeline.")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="sqlite db path")
    parser.add_argument("--text-file", default="", help="wechat note text file path")
    parser.add_argument("--source-name", default="wechat_note_manual", help="source name label")
    parser.add_argument("--images", default="", help="image paths joined by |")
    parser.add_argument("--cover-w", type=int, default=800)
    parser.add_argument("--cover-h", type=int, default=600)
    parser.add_argument("--cover-kind", default="right_price_fixed")
    args = parser.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    if not db_path.exists():
        print(f"DB not found: {db_path}")
        return 2

    if args.text_file:
        text_path = Path(args.text_file).expanduser().resolve()
        if not text_path.exists():
            print(f"text file not found: {text_path}")
            return 2
        raw_text = text_path.read_text(encoding="utf-8")
    else:
        print("请粘贴微信笔记，结束后按 Ctrl+D:")
        raw_text = ""
        try:
            while True:
                raw_text += input() + "\n"
        except EOFError:
            pass

    raw_text = _clean(raw_text)
    if not raw_text:
        print("empty note")
        return 1

    parsed = parse_wechat_note(raw_text)
    now = _now()
    source_post_id = f"wechat_{int(time.time() * 1000)}"
    # dedupe_hash 只基于内容，不含时间戳，防止同一文本重复导入
    dedupe_hash = hashlib.sha1(
        f"{args.source_name}|{raw_text}".encode("utf-8", errors="ignore")
    ).hexdigest()

    images = [_clean(x) for x in args.images.split("|") if _clean(x)]
    raw_meta = {
        "source": "wechat_note_bridge",
        **parsed,
    }

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        # 内容去重：同一 source_name + 内容不重复导入
        existing = conn.execute(
            "SELECT id FROM source_posts WHERE dedupe_hash = ? LIMIT 1",
            (dedupe_hash,),
        ).fetchone()
        if existing:
            print(f"duplicate: already imported as source_post id={existing['id']}, skipping.")
            conn.close()
            return 0

        cur = conn.execute(
            """
            INSERT INTO source_posts (
                source_type, source_name, source_post_id, source_url, source_author,
                raw_text, raw_images_json, raw_videos_json, raw_contact, raw_meta_json,
                dedupe_hash, parse_status, fetched_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, '[]', ?, ?, ?, 'pending', ?, ?, ?)
            """,
            (
                "wechat_note",
                args.source_name,
                source_post_id,
                "",
                "wechat_manual",
                raw_text,
                json.dumps(images, ensure_ascii=False),
                parsed.get("contact", ""),
                json.dumps(raw_meta, ensure_ascii=False),
                dedupe_hash,
                now,
                now,
                now,
            ),
        )
        source_row_id = int(cur.lastrowid)

        if _table_exists(conn, "excel_intake_batches"):
            batch_id = f"BATCH_WECHAT_{int(time.time())}"
            conn.execute(
                """
                INSERT OR IGNORE INTO excel_intake_batches (
                    batch_id, source_name, source_file, source_type,
                    imported_rows, valid_rows, invalid_rows, status,
                    operator_user_id, notes, created_at, updated_at
                ) VALUES (?, ?, ?, 'excel_intake', 0, 0, 0, 'imported', '', '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (batch_id, args.source_name, "stdin/wechat_note"),
            )

            row_id = f"ROW_{uuid.uuid4()}"
            conn.execute(
                """
                INSERT INTO excel_listing_rows (
                    row_id, batch_id, source_row_no, listing_id, title, area, property_type, layout,
                    monthly_rent, payment_terms, contract_term, contact, raw_row_json,
                    image_cover, image2, image3, image4,
                    desired_cover_w, desired_cover_h, desired_cover_kind,
                    ingestion_status, validation_errors, normalized_data,
                    source_post_id, draft_id, publish_status, created_at, updated_at
                ) VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'imported', '', ?, ?, '', 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (
                    row_id,
                    batch_id,
                    source_post_id,
                    parsed.get("title", ""),
                    parsed.get("area", ""),
                    parsed.get("property_type", ""),
                    parsed.get("layout", ""),
                    parsed.get("price"),
                    parsed.get("payment_terms", ""),
                    parsed.get("contract_term", ""),
                    parsed.get("contact", ""),
                    json.dumps(raw_meta, ensure_ascii=False),
                    images[0] if len(images) > 0 else "",
                    images[1] if len(images) > 1 else "",
                    images[2] if len(images) > 2 else "",
                    images[3] if len(images) > 3 else "",
                    args.cover_w,
                    args.cover_h,
                    args.cover_kind,
                    json.dumps(parsed, ensure_ascii=False),
                    source_row_id,
                ),
            )
            conn.execute(
                """
                UPDATE excel_intake_batches
                SET imported_rows = imported_rows + 1,
                    valid_rows = valid_rows + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE batch_id=?
                """,
                (batch_id,),
            )
        conn.commit()
    finally:
        conn.close()

    print("OK imported from wechat note")
    print(f"source_post_id={source_post_id}")
    print(f"title={parsed.get('title','')}")
    print(f"area={parsed.get('area','')}")
    print(f"price={parsed.get('price')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
