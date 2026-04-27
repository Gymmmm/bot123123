#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""房源采集录入工具（只写 source_posts 现有字段）"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

REQUIRED_COLUMNS = (
    "source_type",
    "source_name",
    "source_post_id",
    "source_url",
    "source_author",
    "raw_text",
    "raw_images_json",
    "raw_videos_json",
    "raw_contact",
    "raw_meta_json",
    "dedupe_hash",
    "parse_status",
    "fetched_at",
    "created_at",
    "updated_at",
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HOUSE_CSV_CANDIDATES = (
    ROOT / "data" / "houses.csv",
    ROOT / "reports" / "zufang555_full.csv",
    ROOT / "reports_zufang555_full.csv",
)


def _default_db_path() -> Path:
    return ROOT / "data" / "qiaolian_dual_bot.db"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _safe_int(value: Any, default: int) -> int:
    text = _clean(value)
    if not text:
        return default
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return default
    try:
        return int(digits)
    except ValueError:
        return default


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def _schema_guard(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(source_posts)").fetchall()
    cols = {r["name"] for r in rows}
    missing = [c for c in REQUIRED_COLUMNS if c not in cols]
    if missing:
        raise RuntimeError(f"source_posts 缺少字段: {', '.join(missing)}")


def _pick(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        if key in row and _clean(row.get(key)):
            return _clean(row.get(key))
    return ""


def _parse_images_from_row(row: dict[str, Any]) -> list[str]:
    images: list[str] = []

    raw_images_json = _pick(row, "raw_images_json")
    if raw_images_json:
        try:
            parsed = json.loads(raw_images_json)
            if isinstance(parsed, list):
                for item in parsed:
                    item_s = _clean(item)
                    if item_s:
                        images.append(item_s)
        except json.JSONDecodeError:
            pass

    if not images:
        merged = _pick(row, "images", "图片", "photo_paths", "photos")
        if merged:
            parts = []
            for chunk in merged.replace("\n", "|").split("|"):
                for sub in chunk.split(","):
                    v = _clean(sub)
                    if v:
                        parts.append(v)
            images.extend(parts)

    for key in ("image_cover", "image1", "image2", "image3", "image4"):
        value = _pick(row, key)
        if value:
            images.append(value)

    deduped = []
    seen = set()
    for img in images:
        if img not in seen:
            seen.add(img)
            deduped.append(img)
    return deduped


def _build_raw_text_from_fields(data: dict[str, str]) -> str:
    title = _clean(data.get("title"))
    area = _clean(data.get("area"))
    prop_type = _clean(data.get("type"))
    price = _clean(data.get("price"))
    highlights = [_clean(data.get("feature1")), _clean(data.get("feature2")), _clean(data.get("feature3"))]
    payment_terms = _clean(data.get("payment_terms"))
    contract_term = _clean(data.get("contract_term"))
    extra = _clean(data.get("description"))

    lines: list[str] = []
    if title:
        lines.append(title)
    if area or prop_type:
        lines.append(" ".join([x for x in [f"📍 {area}" if area else "", f"｜{prop_type}" if prop_type else ""]]).strip())
    if price:
        lines.append(f"💰 ${price}/月")
    if any(highlights):
        lines.append("✨ " + "｜".join([x for x in highlights if x]))
    if payment_terms:
        lines.append(f"💳 付款方式：{payment_terms}")
    if contract_term:
        lines.append(f"📄 合同期：{contract_term}")
    if extra:
        lines.append(extra)
    return "\n".join([line for line in lines if line.strip()])


def _insert_source_post(conn: sqlite3.Connection, payload: dict[str, Any]) -> int:
    row = (
        payload["source_type"],
        payload["source_name"],
        payload["source_post_id"],
        payload.get("source_url"),
        payload.get("source_author"),
        payload["raw_text"],
        _json_dumps(payload.get("raw_images_json", [])),
        _json_dumps(payload.get("raw_videos_json", [])),
        payload.get("raw_contact"),
        _json_dumps(payload.get("raw_meta_json", {})),
        payload["dedupe_hash"],
        "pending",
        payload["fetched_at"],
        payload["created_at"],
        payload["updated_at"],
    )
    cur = conn.execute(
        """
        INSERT INTO source_posts (
            source_type,
            source_name,
            source_post_id,
            source_url,
            source_author,
            raw_text,
            raw_images_json,
            raw_videos_json,
            raw_contact,
            raw_meta_json,
            dedupe_hash,
            parse_status,
            fetched_at,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        row,
    )
    return int(cur.lastrowid)


def _ensure_excel_batch(conn: sqlite3.Connection, *, batch_id: str, source_name: str, source_file: str) -> None:
    if not _table_exists(conn, "excel_intake_batches"):
        return
    conn.execute(
        """
        INSERT INTO excel_intake_batches (
            batch_id, source_name, source_file, source_type, status, created_at, updated_at
        ) VALUES (?, ?, ?, 'excel_intake', 'imported', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(batch_id) DO UPDATE SET
            source_name=excluded.source_name,
            source_file=excluded.source_file,
            updated_at=CURRENT_TIMESTAMP
        """,
        (batch_id, source_name, source_file),
    )


def _insert_cover_render_job(
    conn: sqlite3.Connection,
    *,
    row_id: str,
    desired_w: int,
    desired_h: int,
    desired_kind: str,
) -> None:
    """为 excel_listing_rows 记录创建对应的 cover_render_jobs 任务。"""
    if not _table_exists(conn, "cover_render_jobs"):
        return
    job_id = f"JOB_{uuid.uuid4()}"
    conn.execute(
        """
        INSERT INTO cover_render_jobs (
            job_id, row_id, desired_w, desired_h, desired_kind,
            render_status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (job_id, row_id, desired_w, desired_h, desired_kind),
    )


def _insert_excel_row(
    conn: sqlite3.Connection,
    *,
    batch_id: str,
    source_row_no: int,
    row: dict[str, Any],
    payload: dict[str, Any],
    source_post_id: int,
) -> None:
    if not _table_exists(conn, "excel_listing_rows"):
        return

    row_id = f"ROW_{uuid.uuid4()}"
    listing_id = _pick(row, "listing_id", "draft_id", "source_post_id")
    title = _pick(row, "title", "listing_title", "project", "标题")
    area = _pick(row, "area", "区域")
    property_type = _pick(row, "type", "property_type", "户型", "房型")
    layout = _pick(row, "layout", "type", "property_type", "户型")
    monthly_rent_raw = _pick(row, "price", "monthly_rent", "monthly_rent_usd", "月租", "rent")
    monthly_rent_digits = "".join(ch for ch in monthly_rent_raw if ch.isdigit())
    monthly_rent = int(monthly_rent_digits) if monthly_rent_digits else None
    payment_terms = _pick(row, "payment_terms", "押付")
    contract_term = _pick(row, "contract_term", "合同期")
    contact = _pick(row, "contact", "联系人", "电话")
    image_cover = _pick(row, "image_cover")
    image2 = _pick(row, "image2")
    image3 = _pick(row, "image3")
    image4 = _pick(row, "image4")
    desired_w = _safe_int(_pick(row, "cover_w", "desired_cover_w"), 800)
    desired_h = _safe_int(_pick(row, "cover_h", "desired_cover_h"), 600)
    desired_kind = _pick(row, "cover_kind", "desired_cover_kind") or "right_price_fixed"

    conn.execute(
        """
        INSERT INTO excel_listing_rows (
            row_id, batch_id, source_row_no, listing_id, title, area, property_type, layout,
            monthly_rent, payment_terms, contract_term, contact, raw_row_json,
            image_cover, image2, image3, image4,
            desired_cover_w, desired_cover_h, desired_cover_kind,
            ingestion_status, normalized_data, source_post_id, publish_status,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'imported', ?, ?, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (
            row_id,
            batch_id,
            source_row_no,
            listing_id,
            title,
            area,
            property_type,
            layout,
            monthly_rent,
            payment_terms,
            contract_term,
            contact,
            _json_dumps(row),
            image_cover,
            image2,
            image3,
            image4,
            desired_w,
            desired_h,
            desired_kind,
            _json_dumps(payload.get("raw_meta_json", {})),
            source_post_id,
        ),
    )
    _insert_cover_render_job(
        conn,
        row_id=row_id,
        desired_w=desired_w,
        desired_h=desired_h,
        desired_kind=desired_kind,
    )


def _build_payload_manual(data: dict[str, str], images: list[str]) -> dict[str, Any]:
    now = _now()
    source_post_id = data.get("source_post_id") or f"manual_{int(time.time() * 1000)}"
    raw_text = _build_raw_text_from_fields(data)
    dedupe_seed = "|".join(
        [
            "manual_intake",
            data.get("source_name", "property_intake_manual"),
            source_post_id,
            raw_text,
            _json_dumps(images),
        ]
    )
    return {
        "source_type": "manual_intake",
        "source_name": data.get("source_name", "property_intake_manual"),
        "source_post_id": source_post_id,
        "source_url": data.get("source_url"),
        "source_author": data.get("source_author", "manual"),
        "raw_text": raw_text,
        "raw_images_json": images,
        "raw_videos_json": [],
        "raw_contact": data.get("contact", ""),
        "raw_meta_json": {
            "title": data.get("title", ""),
            "area": data.get("area", ""),
            "type": data.get("type", ""),
            "price": data.get("price", ""),
            "feature1": data.get("feature1", ""),
            "feature2": data.get("feature2", ""),
            "feature3": data.get("feature3", ""),
            "payment_terms": data.get("payment_terms", ""),
            "contract_term": data.get("contract_term", ""),
            "source": "property_intake_manual",
        },
        "dedupe_hash": _sha1(dedupe_seed),
        "parse_status": "pending",
        "fetched_at": now,
        "created_at": now,
        "updated_at": now,
    }


def _build_payload_csv(row: dict[str, Any], csv_name: str, row_num: int) -> dict[str, Any]:
    now = _now()
    title = _pick(row, "title", "listing_title", "project", "标题")
    area = _pick(row, "area", "区域")
    prop_type = _pick(row, "type", "property_type", "户型", "房型")
    price = _pick(row, "price", "monthly_rent", "monthly_rent_usd", "月租", "rent")
    feature1 = _pick(row, "feature1", "亮点1")
    feature2 = _pick(row, "feature2", "亮点2")
    feature3 = _pick(row, "feature3", "亮点3")
    payment_terms = _pick(row, "payment_terms", "押付")
    contract_term = _pick(row, "contract_term", "合同期")
    description = _pick(row, "description", "caption", "文案", "highlights", "cost_notes")
    source_post_id = _pick(row, "source_post_id", "listing_id", "draft_id") or f"csv_{int(time.time() * 1000)}_{row_num}"
    source_type = _pick(row, "source_type") or "csv_intake"
    source_name = _pick(row, "source_name") or f"property_intake_csv:{csv_name}"
    source_author = _pick(row, "source_author", "author", "publisher") or "csv"
    source_url = _pick(row, "source_url", "url", "link")
    images = _parse_images_from_row(row)
    videos_json = _pick(row, "raw_videos_json")
    raw_videos = []
    if videos_json:
        try:
            parsed_videos = json.loads(videos_json)
            if isinstance(parsed_videos, list):
                raw_videos = parsed_videos
        except json.JSONDecodeError:
            raw_videos = []

    raw_text = _pick(row, "raw_text")
    if not raw_text:
        raw_text = _build_raw_text_from_fields(
            {
                "title": title,
                "area": area,
                "type": prop_type,
                "price": price,
                "feature1": feature1,
                "feature2": feature2,
                "feature3": feature3,
                "payment_terms": payment_terms,
                "contract_term": contract_term,
                "description": description,
            }
        )

    if not raw_text:
        raise ValueError("缺少可用文案（raw_text/title/caption 至少一个）")

    raw_meta_json = _pick(row, "raw_meta_json")
    if raw_meta_json:
        try:
            raw_meta = json.loads(raw_meta_json)
            if not isinstance(raw_meta, dict):
                raw_meta = {"raw_meta_text": raw_meta_json}
        except json.JSONDecodeError:
            raw_meta = {"raw_meta_text": raw_meta_json}
    else:
        raw_meta = {}

    raw_meta.update(
        {
            "title": title,
            "area": area,
            "type": prop_type,
            "price": price,
            "feature1": feature1,
            "feature2": feature2,
            "feature3": feature3,
            "payment_terms": payment_terms,
            "contract_term": contract_term,
            "csv_row_num": row_num,
            "csv_file": csv_name,
        }
    )

    dedupe_hash = _pick(row, "dedupe_hash")
    if not dedupe_hash:
        dedupe_seed = "|".join([source_type, source_name, source_post_id, raw_text, _json_dumps(images)])
        dedupe_hash = _sha1(dedupe_seed)

    return {
        "source_type": source_type,
        "source_name": source_name,
        "source_post_id": source_post_id,
        "source_url": source_url,
        "source_author": source_author,
        "raw_text": raw_text,
        "raw_images_json": images,
        "raw_videos_json": raw_videos,
        "raw_contact": _pick(row, "raw_contact", "contact", "联系方式", "agent_contact"),
        "raw_meta_json": raw_meta,
        "dedupe_hash": dedupe_hash,
        "parse_status": "pending",
        "fetched_at": now,
        "created_at": now,
        "updated_at": now,
    }


def interactive_add_property(db_path: Path) -> int:
    print("\n============================================================")
    print("房源交互录入 (写入 source_posts，parse_status=pending)")
    print("============================================================\n")

    data: dict[str, str] = {}
    data["source_name"] = _clean(input("source_name [property_intake_manual]: ")) or "property_intake_manual"
    data["source_author"] = _clean(input("source_author [manual]: ")) or "manual"
    data["source_url"] = _clean(input("source_url (可空): "))
    data["source_post_id"] = _clean(input("source_post_id (可空，留空自动生成): "))

    print("\n[房源信息]")
    data["title"] = _clean(input("标题 title: "))
    data["area"] = _clean(input("区域 area: "))
    data["type"] = _clean(input("户型 type: "))
    data["price"] = _clean(input("价格 price: "))
    data["feature1"] = _clean(input("卖点 feature1: "))
    data["feature2"] = _clean(input("卖点 feature2: "))
    data["feature3"] = _clean(input("卖点 feature3: "))
    data["payment_terms"] = _clean(input("押几付几 payment_terms: "))
    data["contract_term"] = _clean(input("合同期 contract_term: "))
    data["description"] = _clean(input("补充描述 description: "))
    data["contact"] = _clean(input("联系方式 contact: "))

    print("\n[图片路径] 每行一条，直接回车结束:")
    images: list[str] = []
    while True:
        img = _clean(input("image path: "))
        if not img:
            break
        p = Path(img).expanduser()
        if p.exists():
            resolved = str(p.resolve())
            images.append(resolved)
            print(f"  + 已添加: {resolved}")
        else:
            print(f"  ! 文件不存在，将按原样保存: {img}")
            images.append(img)

    if not data["title"] and not data["description"]:
        raise RuntimeError("标题和描述不能同时为空，至少提供一个")

    payload = _build_payload_manual(data, images)
    preview_text = payload["raw_text"][:160].replace("\n", " / ")
    print("\n[预览]")
    print(f"source_name: {payload['source_name']}")
    print(f"source_post_id: {payload['source_post_id']}")
    print(f"images: {len(images)}")
    print(f"raw_text: {preview_text}")

    confirm = _clean(input("\n确认写入? (y/n): ")).lower()
    if confirm != "y":
        print("已取消写入")
        return 0

    with _connect(db_path) as conn:
        _schema_guard(conn)
        rid = _insert_source_post(conn, payload)
        conn.commit()
    print(f"\n✅ 写入成功 source_posts.id={rid}, parse_status=pending")
    return rid


def batch_import_csv(db_path: Path, csv_file: Path) -> tuple[int, int, list[str]]:
    success = 0
    skipped = 0
    errors: list[str] = []

    batch_id = f"BATCH_{int(time.time())}"
    with _connect(db_path) as conn:
        _schema_guard(conn)
        _ensure_excel_batch(
            conn,
            batch_id=batch_id,
            source_name=f"property_intake_csv:{csv_file.stem}",
            source_file=str(csv_file),
        )
        with csv_file.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            if not reader.fieldnames:
                raise RuntimeError("CSV 为空或缺少表头")
            for row_num, row in enumerate(reader, start=2):
                try:
                    payload = _build_payload_csv(row, csv_file.name, row_num)
                    rid = _insert_source_post(conn, payload)
                    _insert_excel_row(
                        conn,
                        batch_id=batch_id,
                        source_row_no=row_num,
                        row=row,
                        payload=payload,
                        source_post_id=rid,
                    )
                    success += 1
                    print(f"✓ 第{row_num}行写入成功 id={rid} source_post_id={payload['source_post_id']}")
                except sqlite3.IntegrityError as exc:
                    skipped += 1
                    reason = f"第{row_num}行跳过（唯一键冲突）: {exc}"
                    errors.append(reason)
                    print(f"! {reason}")
                except Exception as exc:  # noqa: BLE001
                    reason = f"第{row_num}行失败: {exc}"
                    errors.append(reason)
                    print(f"✗ {reason}")
        if _table_exists(conn, "excel_intake_batches"):
            conn.execute(
                """
                UPDATE excel_intake_batches
                SET imported_rows=?, valid_rows=?, invalid_rows=?, updated_at=CURRENT_TIMESTAMP
                WHERE batch_id=?
                """,
                (success + skipped, success, max(len(errors) - skipped, 0), batch_id),
            )
        conn.commit()
    return success, skipped, errors


def _resolve_current_house_csv() -> Path:
    existing = [p for p in DEFAULT_HOUSE_CSV_CANDIDATES if p.exists()]
    if existing:
        latest = max(existing, key=lambda p: p.stat().st_mtime)
        return latest.resolve()
    tried = "\n".join([f"  - {p}" for p in DEFAULT_HOUSE_CSV_CANDIDATES])
    raise FileNotFoundError(f"未找到当前 house CSV，请检查以下路径:\n{tried}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Property intake tool for source_posts")
    parser.add_argument("--db", default=str(_default_db_path()), help="sqlite db path")
    parser.add_argument("--csv", dest="csv_file", help="csv file for batch import")
    parser.add_argument(
        "--house-csv",
        nargs="?",
        const="current",
        help="import current house csv (default: auto detect under data/reports)",
    )
    args = parser.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    if not db_path.exists():
        print(f"❌ 数据库不存在: {db_path}")
        return 2

    try:
        csv_arg = args.csv_file
        if args.house_csv is not None:
            csv_arg = args.house_csv

        if csv_arg:
            if str(csv_arg).strip().lower() in {"current", "house", "houses", "now"}:
                csv_path = _resolve_current_house_csv()
            else:
                csv_path = Path(str(csv_arg)).expanduser().resolve()
            if not csv_path.exists():
                print(f"❌ CSV 文件不存在: {csv_path}")
                return 2
            print(f"开始导入 CSV: {csv_path}")
            success, skipped, errors = batch_import_csv(db_path, csv_path)
            print("\n================ 导入结果 ================")
            print(f"成功: {success}")
            print(f"跳过(重复): {skipped}")
            print(f"失败: {len(errors) - skipped if len(errors) >= skipped else 0}")
            print(f"总提示: {len(errors)}")
            if errors:
                print("最近提示:")
                for line in errors[-5:]:
                    print(f"  - {line}")
            print("==========================================")
            return 0 if (len(errors) == skipped) else 1

        interactive_add_property(db_path)
        return 0
    except KeyboardInterrupt:
        print("\n已取消")
        return 130
    except Exception as exc:  # noqa: BLE001
        print(f"\n❌ 执行失败: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
