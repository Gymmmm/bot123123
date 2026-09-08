from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Any, Optional


# --- 数据标准化辅助函数 ---
def normalize_price(value: Any) -> Optional[int]:
    """清洗价格字符串，转换为纯整数。"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)

    s = str(value).strip()
    # 移除货币符号、单位、逗号等非数字字符
    s = re.sub(r'[\$,€¥£/月A-Za-z]', '', s)
    s = s.replace(',', '')

    try:
        return int(float(s))
    except ValueError:
        return None


def standardize_json_array_field(value: Any) -> str:
    """将输入转换为 JSON 数组字符串。支持逗号分隔的字符串。"""
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False)
    elif isinstance(value, str):
        # 如果是逗号分隔的字符串，则拆分成列表
        if ',' in value:
            return json.dumps([item.strip() for item in value.split(',') if item.strip()], ensure_ascii=False)
        # 如果是单个字符串，尝试解析为JSON，否则包装成列表
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return json.dumps(parsed, ensure_ascii=False)
            else:
                return json.dumps([value], ensure_ascii=False)
        except (json.JSONDecodeError, TypeError):
            return json.dumps([value], ensure_ascii=False)
    elif value is None:
        return "[]"
    else:
        return json.dumps([str(value)], ensure_ascii=False)


def standardize_json_object_field(value: Any) -> str:
    """将输入转换为 JSON 对象字符串。"""
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    elif isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return json.dumps(parsed, ensure_ascii=False)
            else:
                # 如果是字符串但解析后不是字典，则包装成字典
                return json.dumps({"value": value}, ensure_ascii=False)
        except (json.JSONDecodeError, TypeError):
            # 如果是字符串但不是合法JSON，则包装成字典
            return json.dumps({"value": value}, ensure_ascii=False)
    elif value is None:
        return "{}"
    else:
        return json.dumps({"value": str(value)}, ensure_ascii=False)


# --- 数据模型定义 ---
@dataclass
class ListingDraft:
    listing_id: str = ""
    title: str = ""
    type: str = ""  # 统一为 'type'，与数据库 'listings' 表对齐
    area: str = ""
    project: str = ""
    price: int = 0
    layout: str = ""
    size: str = ""
    tags: list[str] = field(default_factory=list)
    highlights: list[str] = field(default_factory=list)
    cost_notes: str = ""
    advisor_comment: str = ""
    deposit: str = ""
    available_date: str = ""
    drawbacks: list[str] = field(default_factory=list)
    images: list[str] = field(default_factory=list)
    cover_image: str = ""
    status: str = "draft"


# --- 数据库操作类 ---
class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self) -> None:
        # 保持数据库结构不变，由外部迁移脚本处理
        pass

    def next_listing_id(self) -> str:
        """生成统一房源编号：l_房源ID。"""
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT listing_id FROM drafts WHERE listing_id LIKE 'l_%'
                    UNION ALL
                    SELECT listing_id FROM listings WHERE listing_id LIKE 'l_%'
                    """
                ).fetchall()
            numbers = []
            for row in rows:
                raw = str(row["listing_id"] or "")
                if raw.startswith("l_") and raw[2:].isdigit():
                    numbers.append(int(raw[2:]))
            if numbers:
                return f"l_{max(numbers) + 1}"
        except sqlite3.Error:
            pass
        return f"l_{int(time.time())}"

    def create_draft(self, draft_data: dict[str, Any]) -> int:
        """创建草稿并返回自增 ID"""
        # 严格对齐线上 drafts 表字段
        fields = [
            "draft_id", "source_post_id", "listing_id", "title", "project", "community",
            "area", "property_type", "price", "layout", "size", "floor", "deposit",
            "available_date", "highlights", "drawbacks", "advisor_comment", "cost_notes",
            "extracted_data", "normalized_data", "review_status", "review_note",
            "operator_user_id", "cover_asset_id", "approved_at", "published_at"
        ]

        db_values = (
            draft_data.get("draft_id"),
            draft_data.get("source_post_id"),
            draft_data.get("listing_id"),
            draft_data.get("title"),
            draft_data.get("project"),
            draft_data.get("community"),
            draft_data.get("area"),
            draft_data.get("type"),
            normalize_price(draft_data.get("price")),
            draft_data.get("layout"),
            draft_data.get("size"),
            draft_data.get("floor"),
            draft_data.get("deposit"),
            draft_data.get("available_date"),
            standardize_json_array_field(draft_data.get("highlights")),
            standardize_json_array_field(draft_data.get("drawbacks")),
            draft_data.get("advisor_comment"),
            draft_data.get("cost_notes"),
            standardize_json_object_field(draft_data.get("extracted_data")),
            standardize_json_object_field(draft_data.get("normalized_data")),
            draft_data.get("review_status", "pending"),
            draft_data.get("review_note"),
            draft_data.get("operator_user_id"),
            draft_data.get("cover_asset_id"),
            draft_data.get("approved_at"),
            draft_data.get("published_at"),
        )

        placeholders = ", ".join(["?"] * len(fields))
        query = f'INSERT INTO drafts ({", ".join(fields)}) VALUES ({placeholders})'

        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, db_values)
            conn.commit()
            return cursor.lastrowid

    def save_media_asset(self, asset_data: dict[str, Any]) -> int:
        """统一管理媒体资产"""
        # 严格对齐线上 media_assets 表字段
        fields = [
            "asset_id", "owner_type", "owner_ref_id", "owner_ref_key", "asset_type",
            "source_type", "source_url", "source_file_id", "local_path", "file_url",
            "file_hash", "telegram_file_id", "telegram_file_unique_id", "media_type",
            "is_watermarked", "is_cover", "sort_order", "width", "height",
            "duration", "file_size", "mime_type", "meta_json", "status"
        ]

        db_values = (
            asset_data.get("asset_id"),
            asset_data.get("owner_type"),
            asset_data.get("owner_ref_id"),
            asset_data.get("owner_ref_key"),
            asset_data.get("asset_type"),
            asset_data.get("source_type"),
            asset_data.get("source_url"),
            asset_data.get("source_file_id"),
            asset_data.get("local_path"),
            asset_data.get("file_url"),
            asset_data.get("file_hash"),
            asset_data.get("telegram_file_id"),
            asset_data.get("telegram_file_unique_id"),
            asset_data.get("media_type"),
            asset_data.get("is_watermarked", 0),
            asset_data.get("is_cover", 0),
            asset_data.get("sort_order", 0),
            asset_data.get("width"),
            asset_data.get("height"),
            asset_data.get("duration"),
            asset_data.get("file_size"),
            asset_data.get("mime_type"),
            standardize_json_object_field(asset_data.get("meta_json")),
            asset_data.get("status", "active"),
        )
        placeholders = ", ".join(["?"] * len(fields))
        query = f'INSERT INTO media_assets ({", ".join(fields)}) VALUES ({placeholders})'

        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, db_values)
            conn.commit()
            return cursor.lastrowid
