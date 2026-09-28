"""Left-hero + right-3 stacked rental collage for photos first screen.

Matches the product mock: one large living-room frame, three stacked room
shots, white footer with 侨联地产 / 出租房源. Pure Pillow — no Chromium.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import tempfile

from PIL import Image, ImageDraw, ImageFont


# Canvas roughly matching the mock aspect (wide listing card).
CANVAS_W = 1280
CANVAS_H = 860
GAP = 14
RADIUS = 22
FOOTER_H = 96
PAD = 18

_SERIF = "/usr/share/fonts/opentype/noto/NotoSerifCJK-Bold.ttc"
_SANS = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
_FALLBACK = "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc"

# Fallback fonts to try when system fonts are not available
_FONT_CANDIDATES = [
    "/System/Library/Fonts/PingFang.ttc",  # macOS
    "/System/Library/Fonts/STHeiti Light.ttc",  # macOS
    "/Library/Fonts/Arial Unicode.ttf",  # macOS
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",  # Linux
    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",  # Linux
]


def _font(path: str, size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    for candidate in (path, _FALLBACK, *_FONT_CANDIDATES):
        try:
            if candidate and Path(candidate).is_file():
                return ImageFont.truetype(candidate, size=size)
        except (OSError, IOError):
            continue
    return ImageFont.load_default()


def _cover_fit(path: Path, width: int, height: int) -> Image.Image:
    img = Image.open(path).convert("RGB")
    src_w, src_h = img.size
    if src_w <= 0 or src_h <= 0:
        return Image.new("RGB", (width, height), (230, 228, 222))
    scale = max(width / src_w, height / src_h)
    new_w = max(1, int(src_w * scale + 0.5))
    new_h = max(1, int(src_h * scale + 0.5))
    resized = img.resize((new_w, new_h), Image.Resampling.LANCZOS)
    left = max(0, (new_w - width) // 2)
    top = max(0, (new_h - height) // 2)
    return resized.crop((left, top, left + width, top + height))


def _rounded(img: Image.Image, radius: int) -> Image.Image:
    if radius <= 0:
        return img
    mask = Image.new("L", img.size, 0)
    draw = ImageDraw.Draw(mask)
    draw.rounded_rectangle((0, 0, img.size[0], img.size[1]), radius=radius, fill=255)
    out = Image.new("RGB", img.size, (255, 255, 255))
    out.paste(img, (0, 0), mask)
    return out


def _cache_dir() -> Path:
    for candidate in (
        Path("/opt/qiaolian_v3/runtime/media/photos_collage_preview"),
        Path.home() / ".cache" / "qiaolian_photos_collage",
        Path("/tmp/qiaolian_photos_collage"),
    ):
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            # Verify it's actually writable
            test_file = candidate / ".write_test"
            test_file.touch()
            test_file.unlink()
            return candidate
        except (OSError, PermissionError):
            continue
    raise RuntimeError("No writable cache directory for collage")


def render_side_stack_collage(
    photo_paths: list[str] | tuple[str, ...],
    *,
    public_listing_id: str = "",
    out_path: str | Path | None = None,
) -> str:
    """Render left-large + right-3 collage. Returns absolute file path.

    Production lock: collector only keeps listings with ≥4 usable photos, so
    this renderer requires 4 readable frames (hero + 3 distinct thumbs). No
    duplicate-thumb padding.
    """
    readable = [Path(p) for p in photo_paths if p and Path(p).is_file()]
    if len(readable) < 4:
        raise ValueError("side_stack_collage_needs_at_least_four_photos")

    hero = readable[0]
    thumbs = list(readable[1:4])

    digest = hashlib.sha1(
        "|".join(
            [
                str(public_listing_id or ""),
                str(hero.resolve()),
                *[str(p.resolve()) for p in thumbs],
                f"{CANVAS_W}x{CANVAS_H}",
            ]
        ).encode("utf-8")
    ).hexdigest()[:16]
    target = Path(out_path) if out_path else _cache_dir() / f"side_{digest}.jpg"
    if target.is_file() and out_path is None:
        return str(target.resolve())

    canvas = Image.new("RGB", (CANVAS_W, CANVAS_H), (255, 255, 255))

    grid_top = PAD
    grid_bottom = CANVAS_H - FOOTER_H - PAD
    grid_h = grid_bottom - grid_top
    grid_left = PAD
    grid_right = CANVAS_W - PAD
    grid_w = grid_right - grid_left

    # ~68% width for hero, rest for stacked thumbs.
    thumb_col_w = int(grid_w * 0.30)
    hero_w = grid_w - GAP - thumb_col_w
    thumb_h = (grid_h - GAP * 2) // 3

    hero_img = _rounded(_cover_fit(hero, hero_w, grid_h), RADIUS)
    canvas.paste(hero_img, (grid_left, grid_top))

    tx = grid_left + hero_w + GAP
    for index, path in enumerate(thumbs[:3]):
        ty = grid_top + index * (thumb_h + GAP)
        this_h = thumb_h if index < 2 else (grid_bottom - ty)
        thumb = _rounded(_cover_fit(path, thumb_col_w, max(this_h, 1)), RADIUS)
        canvas.paste(thumb, (tx, ty))

    draw = ImageDraw.Draw(canvas)
    brand = _font(_SERIF, 42)
    english = _font(_SANS, 16)
    tag = _font(_SANS, 28)

    footer_y = CANVAS_H - FOOTER_H
    draw.text((PAD + 4, footer_y + 18), "侨联地产", font=brand, fill=(28, 26, 22))
    draw.text(
        (PAD + 6, footer_y + 64),
        "OVERSEAS UNITED REAL ESTATE",
        font=english,
        fill=(120, 116, 108),
    )

    divider_x = CANVAS_W - PAD - 220
    draw.line(
        [(divider_x, footer_y + 28), (divider_x, footer_y + 70)],
        fill=(210, 206, 198),
        width=2,
    )
    draw.text((divider_x + 18, footer_y + 34), "出租房源", font=tag, fill=(55, 52, 46))

    target.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(target, format="JPEG", quality=90, optimize=True)
    return str(target.resolve())


__all__ = ["render_side_stack_collage"]


def render_gallery_page(
    photo_paths: tuple[str, ...], *, public_listing_id: str, page_index: int
) -> str:
    """Render up to four real photos into one cached Telegram frame."""
    if not photo_paths or len(photo_paths) > 4:
        raise ValueError("gallery_page_requires_one_to_four_photos")
    paths = tuple(Path(raw).resolve(strict=True) for raw in photo_paths)
    signature = "|".join(
        f"{path}:{path.stat().st_size}:{path.stat().st_mtime_ns}" for path in paths
    )
    digest = hashlib.sha256(
        f"gallery-v1|{public_listing_id}|{page_index}|{signature}".encode()
    ).hexdigest()[:24]
    target = _cache_dir() / f"gallery_{digest}.jpg"
    if target.is_file():
        return str(target)

    width, height, gap = 1200, 1600, 8
    canvas = Image.new("RGB", (width, height), "white")
    if len(paths) == 1:
        boxes = [(0, 0, width, height)]
    elif len(paths) == 2:
        boxes = [(0, 0, (width-gap)//2, height), ((width+gap)//2, 0, width, height)]
    elif len(paths) == 3:
        left = (width-gap)//2
        boxes = [(0, 0, left, height), (left+gap, 0, width, (height-gap)//2),
                 (left+gap, (height+gap)//2, width, height)]
    else:
        mid_x, mid_y = (width-gap)//2, (height-gap)//2
        boxes = [(0, 0, mid_x, mid_y), (mid_x+gap, 0, width, mid_y),
                 (0, mid_y+gap, mid_x, height), (mid_x+gap, mid_y+gap, width, height)]
    for path, (x1, y1, x2, y2) in zip(paths, boxes):
        canvas.paste(_cover_fit(path, x2-x1, y2-y1), (x1, y1))
    brand_path = Path(__file__).resolve().parents[1] / "media" / "assets" / "qiaolian_new_logo_20260929.jpeg"
    if brand_path.is_file():
        with Image.open(brand_path) as brand_source:
            brand = brand_source.convert("RGB")
            brand.thumbnail((330, 115), Image.Resampling.LANCZOS)
            x, y = width - brand.width - 20, 20
            canvas.paste(brand, (x, y))
    fd, temp_name = tempfile.mkstemp(prefix="gallery_", suffix=".jpg", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            canvas.save(stream, format="JPEG", quality=86, optimize=True)
        os.replace(temp_name, target)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)
    return str(target)


__all__.append("render_gallery_page")
