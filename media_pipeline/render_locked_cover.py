#!/usr/bin/env python3
"""Render the locked 1600x1200 Qiaolian cover with Noto Sans SC/CJK."""
from __future__ import annotations

import argparse
import base64
import html as html_lib
import mimetypes
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent
TEMPLATE = ROOT / "qiaolian_cover_locked_sc.html"
WIDTH, HEIGHT = 1600, 1200
FONT_REGULAR = Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc")
FONT_BOLD = Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc")


def to_data_uri(path: Path) -> str:
    mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    return f"data:{mime};base64,{base64.b64encode(path.read_bytes()).decode('ascii')}"


def _embed_fonts(source: str) -> str:
    if not FONT_REGULAR.exists() or not FONT_BOLD.exists():
        raise FileNotFoundError("Noto Sans CJK fonts are required on the render host")
    source = source.replace('url("./fonts/NotoSansSC-Bold.otf")', f'url("{to_data_uri(FONT_BOLD)}") format("truetype")')
    source = source.replace('url("./fonts/NotoSansSC-Regular.otf")', f'url("{to_data_uri(FONT_REGULAR)}") format("truetype")')
    return source


def price_line(price: str) -> str:
    p = (price or "").strip()
    if not p:
        return ""
    if not p.startswith("$") and p not in {"面议", "租金面议"}:
        p = f"${p}"
    if p.endswith("/月") or p in {"面议", "租金面议"}:
        return p
    return f"{p}/月"


def render(*, bg: Path, output: Path, project: str, layout: str, area: str, size: str, floor: str, highlights: str, price: str) -> Path:
    source = _embed_fonts(TEMPLATE.read_text(encoding="utf-8"))
    repl = {
        "{{BG_SRC}}": to_data_uri(bg),
        "{{PROJECT}}": html_lib.escape(project),
        "{{LAYOUT}}": html_lib.escape(layout),
        "{{AREA}}": html_lib.escape(area),
        "{{SIZE}}": html_lib.escape(size),
        "{{FLOOR}}": html_lib.escape(floor),
        "{{HIGHLIGHTS}}": html_lib.escape(highlights),
        "{{PRICE_LINE}}": html_lib.escape(price_line(price)),
    }
    for key, value in repl.items():
        source = source.replace(key, value)
    output.parent.mkdir(parents=True, exist_ok=True)
    work = output.with_suffix(".html")
    work.write_text(source, encoding="utf-8")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": WIDTH, "height": HEIGHT}, device_scale_factor=1)
        page.goto(work.as_uri(), wait_until="load")
        page.evaluate("document.fonts.ready")
        page.locator(".poster").screenshot(path=str(output), type="jpeg", quality=92)
        browser.close()
    work.unlink(missing_ok=True)
    return output


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bg", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    for name in ("project", "layout", "area", "size", "floor", "highlights", "price"):
        ap.add_argument(f"--{name}", required=True)
    args = ap.parse_args()
    print(render(bg=args.bg, output=args.out, project=args.project, layout=args.layout, area=args.area, size=args.size, floor=args.floor, highlights=args.highlights, price=args.price))


if __name__ == "__main__":
    main()
