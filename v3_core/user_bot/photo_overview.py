"""Pillow thumbnail overview for the public listing photo entry."""
from __future__ import annotations

from pathlib import Path
from PIL import Image, ImageOps

CANVAS = (1080, 1350)
GAP = 8
MARGIN = 0
MAX_PREVIEW = 8


def render_photo_overview(paths: tuple[str, ...], *, output_path: str | Path) -> str:
    candidates = [Path(p) for p in paths if p and Path(p).is_file()][:MAX_PREVIEW]
    readable = []
    for path in candidates:
        try:
            with Image.open(path) as probe:
                probe.verify()
            readable.append(path)
        except Exception:
            continue
    if not readable:
        return ""
    out = Path(output_path).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    canvas = Image.new("RGB", CANVAS, "white")
    cols = 2
    rows = (len(readable) + 1) // 2
    cell_w = (CANVAS[0] - GAP) // 2
    cell_h = (CANVAS[1] - GAP * (rows - 1)) // rows
    for idx, path in enumerate(readable):
        row, col = divmod(idx, cols)
        x = col * (cell_w + GAP)
        y = row * (cell_h + GAP)
        with Image.open(path) as src:
            tile = ImageOps.contain(src.convert("RGB"), (cell_w, cell_h), Image.Resampling.LANCZOS)
        # Letterbox instead of stretching/cropping the source.
        box = Image.new("RGB", (cell_w, cell_h), "white")
        box.paste(tile, ((cell_w - tile.width) // 2, (cell_h - tile.height) // 2))
        # Odd final frame spans the row while preserving its own aspect ratio.
        if idx == len(readable) - 1 and len(readable) % 2 == 1:
            span_w = CANVAS[0]
            with Image.open(path) as src:
                tile = ImageOps.contain(src.convert("RGB"), (span_w, cell_h), Image.Resampling.LANCZOS)
            box = Image.new("RGB", (span_w, cell_h), "white")
            box.paste(tile, ((span_w - tile.width) // 2, (cell_h - tile.height) // 2))
            canvas.paste(box, (0, y))
        else:
            canvas.paste(box, (x, y))
    canvas.save(out, format="JPEG", quality=90, optimize=True)
    return str(out)


__all__ = ["MAX_PREVIEW", "render_photo_overview"]