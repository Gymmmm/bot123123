"""
db.py · 侨联地产频道发布系统 · 数据库操作
"""

import sqlite3
import json
import os
from datetime import datetime

DB_PATH = os.getenv("DB_PATH", "data/qiaolian_dual_bot.db")
_SCHEMA  = os.path.join(os.path.dirname(__file__), "schema.sql")


# ── 连接 ──────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """首次启动建表，幂等。"""
    conn = get_conn()
    with open(_SCHEMA, encoding="utf-8") as f:
        conn.executescript(f.read())
    conn.commit()
    conn.close()
    print(f"[DB] 数据库就绪：{DB_PATH}")


# ── Listings CRUD ─────────────────────────────────────────

_LISTING_FIELDS = [
    "type", "area", "project", "title", "price", "layout", "size",
    "deposit", "contract_term", "available_date", "tags",
    "highlights", "cost_notes", "advisor_comment", "drawbacks",
    "images", "cover_image",
]


def create_listing(data: dict) -> str:
    """新建房源，返回 listing_id。"""
    conn = get_conn()
    lid = data.get("listing_id") or _gen_id()
    row = {k: data.get(k, "") for k in _LISTING_FIELDS}
    # images / file_ids 保证是 JSON 字符串
    if isinstance(row.get("images"), list):
        row["images"] = json.dumps(row["images"], ensure_ascii=False)
    conn.execute(
        """
        INSERT INTO listings (listing_id, type, area, project, title, price,
            layout, size, deposit, contract_term, available_date, tags,
            highlights, cost_notes, advisor_comment, drawbacks, images,
            cover_image, status)
        VALUES (:listing_id, :type, :area, :project, :title, :price,
            :layout, :size, :deposit, :contract_term, :available_date, :tags,
            :highlights, :cost_notes, :advisor_comment, :drawbacks, :images,
            :cover_image, 'draft')
        """,
        {"listing_id": lid, **row},
    )
    conn.commit()
    conn.close()
    return lid


def update_listing(listing_id: str, data: dict):
    """更新房源字段（只更新 data 里有的字段）。"""
    allowed = set(_LISTING_FIELDS)
    fields  = [k for k in data if k in allowed]
    if not fields:
        return
    if isinstance(data.get("images"), list):
        data["images"] = json.dumps(data["images"], ensure_ascii=False)
    set_clause = ", ".join(f"{f} = :{f}" for f in fields)
    conn = get_conn()
    conn.execute(
        f"UPDATE listings SET {set_clause}, updated_at = datetime('now','localtime') "
        f"WHERE listing_id = :listing_id",
        {**data, "listing_id": listing_id},
    )
    conn.commit()
    conn.close()


def get_listing(listing_id: str) -> dict | None:
    conn = get_conn()
    row  = conn.execute(
        "SELECT * FROM listings WHERE listing_id = ?", (listing_id,)
    ).fetchone()
    conn.close()
    return _row(row)


def list_listings(status: str | None = None) -> list[dict]:
    conn = get_conn()
    if status:
        rows = conn.execute(
            "SELECT * FROM listings WHERE status = ? ORDER BY created_at DESC", (status,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM listings ORDER BY created_at DESC"
        ).fetchall()
    conn.close()
    return [_row(r) for r in rows]


def set_listing_status(listing_id: str, status: str):
    conn = get_conn()
    conn.execute(
        "UPDATE listings SET status = ?, updated_at = datetime('now','localtime') "
        "WHERE listing_id = ?",
        (status, listing_id),
    )
    conn.commit()
    conn.close()


def update_listing_price(listing_id: str, new_price: str):
    conn = get_conn()
    conn.execute(
        "UPDATE listings SET price = ?, updated_at = datetime('now','localtime') "
        "WHERE listing_id = ?",
        (new_price, listing_id),
    )
    conn.commit()
    conn.close()


# ── Channel Posts ─────────────────────────────────────────

def save_channel_post(
    listing_id: str,
    channel_id: str,
    media_group_id: str,
    media_message_ids: list[int],
    button_message_id: int,
    file_ids: list[str],
):
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO channel_posts
            (listing_id, channel_id, media_group_id, media_message_ids,
             button_message_id, file_ids, status, published_at)
        VALUES (?, ?, ?, ?, ?, ?, 'published', datetime('now','localtime'))
        """,
        (
            listing_id,
            channel_id,
            media_group_id,
            json.dumps(media_message_ids),
            button_message_id,
            json.dumps(file_ids, ensure_ascii=False),
        ),
    )
    conn.commit()
    conn.close()


def get_latest_channel_post(listing_id: str) -> dict | None:
    conn = get_conn()
    row  = conn.execute(
        "SELECT * FROM channel_posts WHERE listing_id = ? "
        "ORDER BY published_at DESC LIMIT 1",
        (listing_id,),
    ).fetchone()
    conn.close()
    return _row(row)


def update_channel_post_status(listing_id: str, status: str):
    conn = get_conn()
    conn.execute(
        "UPDATE channel_posts SET status = ? WHERE listing_id = ?",
        (status, listing_id),
    )
    conn.commit()
    conn.close()


def update_file_ids(listing_id: str, file_ids: list[str]):
    """回写 TG file_id，下次重发直接复用无需再上传。"""
    conn = get_conn()
    conn.execute(
        "UPDATE channel_posts SET file_ids = ? "
        "WHERE listing_id = ? AND status = 'published'",
        (json.dumps(file_ids, ensure_ascii=False), listing_id),
    )
    # 同时也存进 listings.images 方便后续读取
    conn.execute(
        "UPDATE listings SET images = ?, updated_at = datetime('now','localtime') "
        "WHERE listing_id = ?",
        (json.dumps(file_ids, ensure_ascii=False), listing_id),
    )
    conn.commit()
    conn.close()


# ── 工具函数 ──────────────────────────────────────────────

def _row(row) -> dict | None:
    if row is None:
        return None
    d = dict(row)
    # 自动解析 JSON 字段
    for key in ("images", "file_ids", "media_message_ids"):
        if key in d and isinstance(d[key], str):
            try:
                d[key] = json.loads(d[key])
            except Exception:
                d[key] = []
    return d


def _gen_id() -> str:
    ts  = datetime.now().strftime("%y%m%d%H%M%S")
    return f"L{ts}"
