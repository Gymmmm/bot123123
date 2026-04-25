#!/usr/bin/env python3
"""Safely remove obvious garbage drafts from the publish flow.

Default mode is dry-run. Use --execute to soft-delete candidates by moving them
to review_status='rejected'. Published/rejected drafts are never touched.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path


DB_DEFAULT = "data/qiaolian_dual_bot.db"

BLACKLIST_PATTERN = re.compile(
    r"(sale price|business for sale|shop for sale|transfer|shop transfer|take over|urgent sale|"
    r"转让|顶让|出售|急售|诚售|土地卖|店面转|生意好|回国转|"
    r"លក់|ផ្ទេរ|លក់បន្ទាន់|លក់ហាង|ផ្ទេរហាង)",
    re.IGNORECASE,
)
USD_AMOUNT_PATTERN = re.compile(r"\$\s?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)")
SALE_PRICE_LIMIT = 15000


def _load_json_obj(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _quality_flags(row: sqlite3.Row) -> set[str]:
    data = {}
    data.update(_load_json_obj(row["extracted_data"]))
    data.update(_load_json_obj(row["normalized_data"]))
    raw_flags = data.get("quality_flags") or data.get("flags") or []
    if not isinstance(raw_flags, list):
        return set()
    return {str(flag) for flag in raw_flags}


def _has_sale_price(text: str) -> bool:
    for price_str in USD_AMOUNT_PATTERN.findall(text or ""):
        try:
            if float(price_str.replace(",", "")) > SALE_PRICE_LIMIT:
                return True
        except ValueError:
            continue
    return False


def _candidate_reasons(row: sqlite3.Row) -> list[str]:
    text = row["raw_text"] or ""
    note = row["review_note"] or ""
    flags = _quality_flags(row)

    reasons: list[str] = []
    if BLACKLIST_PATTERN.search(text) or _has_sale_price(text):
        reasons.append("commercial_sale")
    if "commercial_waste" in flags or "non_rental_source" in flags:
        reasons.append("commercial_sale")
    if "media:broken" in note or "missing_real_media" in note:
        reasons.append("media_broken")
    invalid_price = "invalid_price" in note or row["price"] in (None, 0, "")
    invalid_layout = "invalid_layout" in note or not str(row["layout"] or "").strip()
    if invalid_price or invalid_layout:
        reasons.append("invalid_core")
    if not text.strip():
        reasons.append("empty_source")

    # Keep the criteria intentionally narrow:
    # - obvious sale/transfer/business posts
    # - broken media plus invalid essentials
    # - empty source plus invalid essentials
    if "commercial_sale" in reasons:
        return reasons
    if "media_broken" in reasons and "invalid_core" in reasons:
        return reasons
    if "empty_source" in reasons and "invalid_core" in reasons:
        return reasons
    return []


def fetch_candidates(conn: sqlite3.Connection, limit: int | None = None) -> list[sqlite3.Row]:
    sql = """
        SELECT d.id, d.draft_id, d.review_status, d.review_note, d.price, d.layout,
               d.extracted_data, d.normalized_data, d.created_at,
               sp.source_name, sp.source_url, sp.raw_text
        FROM drafts d
        LEFT JOIN source_posts sp ON sp.id = d.source_post_id
        WHERE COALESCE(d.review_status,'') NOT IN ('published', 'rejected')
        ORDER BY d.id ASC
    """
    rows = conn.execute(sql).fetchall()
    out: list[sqlite3.Row] = []
    for row in rows:
        reasons = _candidate_reasons(row)
        if not reasons:
            continue
        out.append(row)
        if limit and len(out) >= limit:
            break
    return out


def ensure_backup(db_path: Path) -> Path:
    backup_dir = db_path.parent / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = backup_dir / f"{db_path.name}.before_garbage_cleanup_{stamp}"
    shutil.copy2(db_path, backup)
    return backup


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=DB_DEFAULT)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--examples", type=int, default=20)
    args = parser.parse_args()

    db_path = Path(args.db)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    candidates = fetch_candidates(conn, args.limit)

    reason_counts: dict[str, int] = {}
    for row in candidates:
        for reason in _candidate_reasons(row):
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

    print(f"mode={'execute' if args.execute else 'dry-run'}")
    print(f"candidates={len(candidates)}")
    print("reason_counts=" + json.dumps(reason_counts, ensure_ascii=False, sort_keys=True))
    for row in candidates[: args.examples]:
        reasons = ",".join(_candidate_reasons(row))
        print(
            f"- {row['draft_id']} status={row['review_status']} price={row['price']} "
            f"layout={row['layout'] or '-'} source={row['source_name'] or '-'} reasons={reasons}"
        )

    if not args.execute:
        conn.close()
        return 0

    backup = ensure_backup(db_path)
    changed = 0
    for row in candidates:
        reasons = ",".join(_candidate_reasons(row))
        note = (row["review_note"] or "").strip()
        marker = f"cleanup:garbage:{reasons}"
        new_note = note if marker in note else (f"{note} | {marker}".strip(" |") if note else marker)
        cur = conn.execute(
            """
            UPDATE drafts
            SET review_status='rejected',
                review_note=?,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=?
              AND COALESCE(review_status,'') NOT IN ('published', 'rejected')
            """,
            (new_note, row["id"]),
        )
        changed += cur.rowcount or 0
    conn.commit()
    conn.close()
    print(f"backup={backup}")
    print(f"rejected={changed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
