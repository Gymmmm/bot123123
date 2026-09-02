#!/usr/bin/env python3
"""封面兼容层：选图、登记资产，视觉统一交给 HTML 正式渲染器。"""
from __future__ import annotations

import colorsys
import hashlib
import json
import logging
import os
from pathlib import Path
import sqlite3
from typing import Optional, Sequence
import uuid

from PIL import Image, ImageFilter, ImageOps

from html_cover_renderer import render_html_cover
from qiaolian_dual.cover_styles import cover_template_path, normalize_cover_style


log = logging.getLogger("cover_generator")
BASE_DIR = Path(__file__).resolve().parent
COVER_DIR = BASE_DIR / "media" / "covers"
DB_PATH_DEFAULT = os.getenv("DB_PATH", "data/qiaolian_dual_bot.db")
_SERVER_MEDIA_ROOT = "/opt/qiaolian_dual_bots"


def _remap_server_path(path: str) -> str:
    """把数据库里的生产绝对路径映射为当前项目路径。"""
    value = str(path or "")
    prefix = _SERVER_MEDIA_ROOT + "/"
    if value.startswith(prefix):
        return str(BASE_DIR / value[len(prefix):])
    return value


def _score_image(img_path: str, *, property_type: str = "") -> tuple[float, str]:
    """为同一房源组内的实拍图评分；越适合 4:3 封面分数越高。"""
    try:
        with Image.open(img_path) as source:
            image = ImageOps.exif_transpose(source).convert("RGB")
    except Exception as exc:
        return -999.0, f"无法打开:{exc}"

    width, height = image.size
    if width <= 0 or height <= 0:
        return -999.0, "图片尺寸无效"

    score = 0.0
    reasons: list[str] = []
    aspect = width / height
    if aspect >= 1.3:
        score += 30
        reasons.append("横图")
    elif aspect >= 1.0:
        score += 12
        reasons.append("方图")
    else:
        score -= 15
        reasons.append("竖图")

    pixels = width * height
    if pixels >= 1920 * 1080:
        score += 25
        reasons.append("高清")
    elif pixels >= 1280 * 720:
        score += 15
        reasons.append("清晰")
    elif pixels >= 640 * 480:
        score += 5
    else:
        score -= 10
        reasons.append("尺寸小")

    thumb = image.resize((64, 64), Image.Resampling.LANCZOS)
    luminance = list(thumb.convert("L").getdata())
    brightness = sum(luminance) / len(luminance)
    if 55 <= brightness <= 195:
        score += 20
        reasons.append("曝光正常")
    elif brightness < 35:
        score -= 25
        reasons.append("过暗")
    elif brightness > 215:
        score -= 12
        reasons.append("过亮")
    else:
        score += 5

    rgb = list(thumb.getdata())
    saturation = sum(colorsys.rgb_to_hsv(r / 255, g / 255, b / 255)[1] for r, g, b in rgb) / len(rgb)
    if saturation > 0.22:
        score += 15
        reasons.append("色彩完整")
    elif saturation < 0.07:
        score -= 18
        reasons.append("疑似截图")

    edges = list(thumb.filter(ImageFilter.FIND_EDGES).convert("L").getdata())
    edge_density = sum(1 for value in edges if value > 25) / len(edges)
    if edge_density > 0.12:
        score += 10
        reasons.append("空间信息丰富")
    elif edge_density < 0.04:
        score -= 18
        reasons.append("内容过少")

    ratio_gap = abs(aspect - 4 / 3)
    if ratio_gap < 0.12:
        score += 10
        reasons.append("接近4:3")
    elif ratio_gap < 0.35:
        score += 4

    kind = str(property_type or "").lower()
    if any(token in kind for token in ("别墅", "villa", "排屋", "townhouse")) and brightness >= 145 and saturation >= 0.20:
        score += 12
        reasons.append("适合低密住宅")
    return score, " | ".join(reasons)


def choose_best_cover_image(
    images: Sequence[str], *, property_type: str = ""
) -> tuple[Optional[str], int, str]:
    """严格只在传入的同组图片中选择封面。"""
    candidates: list[tuple[float, int, str, str]] = []
    for index, raw_path in enumerate(images or []):
        if not isinstance(raw_path, str) or raw_path.startswith(("http://", "https://")):
            continue
        path = _remap_server_path(raw_path)
        if "dummy" in path.lower() or not Path(path).is_file():
            continue
        score, reason = _score_image(path, property_type=property_type)
        candidates.append((score, index, path, reason))
    if not candidates:
        return None, -1, "无可用本地实拍图"
    score, index, path, reason = max(candidates, key=lambda item: item[0])
    if score < -10:
        return None, -1, f"图片质量不足(最高分={score:.1f})"
    return path, index, f"第{index + 1}张（共{len(images)}张）| 得分={score:.1f} | {reason}"


def generate_house_cover(
    output_path: str,
    project: str = "",
    property_type: str = "",
    area: str = "",
    size: str = "",
    floor: str = "",
    price=None,
    layout: str = "",
    highlights: Optional[Sequence[str]] = None,
    base_image_path: Optional[str] = None,
    source_type: str = "",
    source_name: str = "",
    style: str = "classic_blue",
) -> str:
    """兼容旧调用；所有样式均由正式 HTML 模板生成。"""
    _ = source_type, source_name
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    fallback: Optional[Path] = None
    source = Path(_remap_server_path(base_image_path or ""))
    if not source.is_file():
        fallback = output.with_name(f".{output.stem}_fallback.png")
        Image.new("RGB", (1600, 1200), (18, 47, 102)).save(fallback)
        source = fallback
    values = list(highlights or [])[:3]
    values.extend([""] * (3 - len(values)))
    try:
        render_html_cover(
            template_path=str(cover_template_path(normalize_cover_style(style))),
            source_image=str(source),
            output_path=str(output),
            data={
                "project": project,
                "property_type": property_type,
                "area": area,
                "size": size,
                "floor": floor,
                "price": price,
                "layout": layout,
                "h1": values[0],
                "h2": values[1],
                "h3": values[2],
            },
        )
    finally:
        if fallback:
            fallback.unlink(missing_ok=True)
    return str(output)


class CoverGenerator:
    """保留旧服务接口，生成结果与正式发布包使用同一渲染路径。"""

    def __init__(self, db_path: str = DB_PATH_DEFAULT):
        path = Path(db_path).expanduser()
        self.db_path = str(path if path.is_absolute() else (BASE_DIR / path).resolve())
        COVER_DIR.mkdir(parents=True, exist_ok=True)

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _calc_hash(path: str) -> str:
        digest = hashlib.sha256()
        with open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _get_source_post_images(self, source_post_id) -> list[str]:
        """按原顺序读取同一 source_post 的本地实拍，绝不跨组。"""
        if not source_post_id:
            return []
        with self._get_conn() as conn:
            rows = conn.execute(
                """SELECT local_path FROM media_assets
                   WHERE owner_type='source_post' AND owner_ref_id=?
                     AND asset_type='photo' AND local_path IS NOT NULL AND local_path != ''
                     AND COALESCE(status,'active')='active'
                   ORDER BY sort_order ASC, id ASC""",
                (str(source_post_id),),
            ).fetchall()
            paths = [
                _remap_server_path(row["local_path"])
                for row in rows
                if Path(_remap_server_path(row["local_path"])).is_file()
            ]
            if paths:
                return paths
            row = conn.execute(
                "SELECT raw_images_json FROM source_posts WHERE id=?", (source_post_id,)
            ).fetchone()
        if not row or not row["raw_images_json"]:
            return []
        try:
            raw_images = json.loads(row["raw_images_json"])
        except (TypeError, ValueError, json.JSONDecodeError):
            return []
        result: list[str] = []
        for item in raw_images if isinstance(raw_images, list) else []:
            raw_path = item if isinstance(item, str) else (item.get("local_path") or item.get("path") or "") if isinstance(item, dict) else ""
            path = _remap_server_path(str(raw_path).strip())
            if path and not path.startswith(("http://", "https://")) and Path(path).is_file():
                result.append(path)
        return result

    def generate_for_draft(self, draft_id: str, base_image_path: str = "") -> tuple[Optional[int], Optional[str]]:
        """生成审核封面并登记为 draft 的当前封面资产。"""
        _ = base_image_path  # 正式路径始终按发布包的同组选图规则生成。
        with self._get_conn() as conn:
            draft = conn.execute(
                "SELECT id, source_post_id FROM drafts WHERE draft_id=?", (draft_id,)
            ).fetchone()
        if not draft:
            log.warning("Draft %s not found", draft_id)
            return None, None

        output = COVER_DIR / f"cover_{draft_id}.png"
        try:
            from publication_package import render_cover_preview

            rendered = render_cover_preview(self.db_path, draft_id, str(output))
        except Exception:
            log.exception("Failed to render cover for %s", draft_id)
            return None, None

        with Image.open(output) as image:
            width, height = image.size
        asset_key = f"AST_{uuid.uuid4()}"
        metadata = json.dumps(
            {
                "generated_from_draft_id": draft_id,
                "source_post_id": draft["source_post_id"],
                "base_image": rendered.get("source_image") or "",
                "cover_template": rendered.get("template") or "classic_blue",
                "renderer": "html_cover_renderer",
            },
            ensure_ascii=False,
        )
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM media_assets WHERE owner_type='draft' AND owner_ref_id=? AND is_cover=1",
                (draft["id"],),
            )
            cursor.execute(
                """INSERT INTO media_assets (
                    asset_id, owner_type, owner_ref_id, owner_ref_key,
                    asset_type, source_type, local_path, file_url, file_hash,
                    media_type, is_watermarked, is_cover, sort_order,
                    width, height, file_size, mime_type, meta_json, status,
                    created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
                (
                    asset_key,
                    "draft",
                    draft["id"],
                    draft_id,
                    "image",
                    "generated",
                    str(output),
                    f"/media/covers/{output.name}",
                    self._calc_hash(str(output)),
                    "photo",
                    1,
                    1,
                    0,
                    width,
                    height,
                    output.stat().st_size,
                    "image/png",
                    metadata,
                    "active",
                ),
            )
            media_asset_db_id = int(cursor.lastrowid)
            cursor.execute(
                "UPDATE drafts SET cover_asset_id=?, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
                (media_asset_db_id, draft_id),
            )
            conn.commit()
        return media_asset_db_id, str(output)


__all__ = [
    "CoverGenerator",
    "_score_image",
    "choose_best_cover_image",
    "generate_house_cover",
]
