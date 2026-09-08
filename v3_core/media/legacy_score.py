"""Legacy cover-score fallback extracted from fixed production SHA.

Used only when OpenCV metrics are unavailable.  This is the `_score_image`
behavior from `cover_generator.py` at 8e4605cf, isolated from draft/DB/rendering
code.
"""
from __future__ import annotations

import colorsys
from PIL import Image, ImageFilter, ImageOps


def score_image(img_path: str, *, property_type: str = "") -> tuple[float, str]:
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
    saturation = sum(
        colorsys.rgb_to_hsv(r / 255, g / 255, b / 255)[1]
        for r, g, b in rgb
    ) / len(rgb)
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


__all__ = ["score_image"]
