#!/usr/bin/env python3
"""
Render listing poster HTML templates to image via headless Chromium.

Templates:
    - right_price_fixed: `templates/03_右侧价格牌_fixed_v1_template_render.html`
    - dark_glass: `templates/06_暗夜玻璃_template_render.html`
    - villa_premium: `templates/12_别墅高级风_template_render.html`

Legacy kinds are still accepted as aliases and will fall back to `right_price_fixed`.

Why separate *_render.html:
  Editor HTML uses transform:scale + side panel, which makes deterministic rasterization harder.
  Render templates are fixed 1600x1200 canvases.
"""

from __future__ import annotations

import argparse
import base64
import html
import mimetypes
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from PIL import Image, ImageOps


ROOT = Path(__file__).resolve().parents[1]
RIGHT_PRICE_FIXED_TEMPLATE = ROOT / "templates" / "03_右侧价格牌_fixed_v1_template_render.html"
DARK_GLASS_TEMPLATE = ROOT / "templates" / "06_暗夜玻璃_template_render.html"
VILLA_PREMIUM_TEMPLATE = ROOT / "templates" / "12_别墅高级风_template_render.html"

KIND_ALIASES = {
    "blue_card": "right_price_fixed",
    "white_bar": "right_price_fixed",
    "right_price": "right_price_fixed",
    "magazine_white": "right_price_fixed",
    "metro_panel": "right_price_fixed",
    "lite_strip": "right_price_fixed",
    "portrait_luxe": "right_price_fixed",
}


def _canonical_kind(kind: str) -> str:
    picked = str(kind or "").strip().lower() or "right_price_fixed"
    return KIND_ALIASES.get(picked, picked)


def _find_chromium() -> str | None:
    for name in ("chromium-browser", "chromium", "google-chrome", "google-chrome-stable"):
        p = shutil.which(name)
        if p:
            return p
    return None


def _bg_src(bg_url: str | None, bg_local: str | None, *, default_url: str) -> str:
    if bg_local:
        p = Path(bg_local).expanduser().resolve()
        if not p.is_file():
            raise FileNotFoundError(f"bg local file not found: {p}")
        mime = mimetypes.guess_type(str(p))[0] or "image/jpeg"
        b64 = base64.b64encode(p.read_bytes()).decode("ascii")
        return f"data:{mime};base64,{b64}"
    if bg_url:
        return html.escape(bg_url, quote=True)
    # Default: remote placeholder (requires network at render time)
    return html.escape(default_url, quote=True)


def _parse_kind(argv: list[str]) -> str:
    kind = "right_price_fixed"
    for i, a in enumerate(argv):
        if a == "--kind" and i + 1 < len(argv):
            kind = str(argv[i + 1]).strip().lower() or kind
            break
        if a.startswith("--kind="):
            kind = str(a.split("=", 1)[1]).strip().lower() or kind
            break
    return _canonical_kind(kind)


def _pick_writable_dir(candidates: list[Path]) -> Path:
    for p in candidates:
        try:
            p.mkdir(parents=True, exist_ok=True)
            probe = p / ".write_probe"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            return p
        except Exception:
            continue
    raise PermissionError("No writable temp directory for renderer.")


def _price_line(price: str) -> str:
    p = str(price or "").strip()
    if not p:
        return ""
    if p.endswith("/月"):
        return p
    # If looks like a numeric monthly rent, normalize to "$1234/月"
    m = re.match(r"^\s*\$?\s*([0-9][0-9,]*)\s*$", p.replace(",", ""))
    if m:
        n = m.group(1).replace(",", "")
        return f"${n}/月"
    if p.startswith("$") and re.search(r"[0-9]", p) and "月" not in p:
        return f"{p}/月" if not p.endswith("/") else f"{p}月"
    return p


def main() -> int:
    argv = sys.argv[1:]
    kind = _parse_kind(argv)

    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="Output image path (.png/.jpg/.jpeg)")
    ap.add_argument(
        "--kind",
        choices=(
            "right_price_fixed",
            "dark_glass",
            "villa_premium",
            "right_price",
            "blue_card",
            "white_bar",
            "magazine_white",
            "metro_panel",
            "lite_strip",
            "portrait_luxe",
        ),
        default=kind,
        help="Which built-in render template to use (ignored if --template is provided). Deprecated kinds fall back to right_price_fixed.",
    )
    ap.add_argument(
        "--template",
        default="",
        help="Override render HTML template path (advanced). If set, --kind is ignored.",
    )
    ap.add_argument("--w", type=int, default=0, help="Viewport width. 0 means auto by template kind.")
    ap.add_argument("--h", type=int, default=0, help="Viewport height. 0 means auto by template kind.")
    ap.add_argument(
        "--dpr",
        type=float,
        default=1.0,
        help="Device scale factor. NOTE: snap Chromium may refuse writing some paths; /root is safest.",
    )
    ap.add_argument(
        "--jpeg-quality",
        type=int,
        default=90,
        help="If --out ends with .jpg/.jpeg, re-encode with this JPEG quality after rasterize.",
    )

    canonical_kind = _canonical_kind(kind)

    if canonical_kind == "right_price_fixed":
        default_project = "太子幸福"
        default_layout = "Studio"
        default_area = "桑园"
        default_size = "38㎡"
        default_floor = "26楼"
        default_price = "$480/月"
        default_h1 = "采光好"
        default_h2 = "近商场"
        default_h3 = "安静"
    elif canonical_kind == "dark_glass":
        default_project = "天际ONE"
        default_layout = "Studio"
        default_area = "钻石岛"
        default_size = "41㎡"
        default_floor = "29楼"
        default_price = "$680/月"
        default_h1 = "夜景开阔"
        default_h2 = "高层静音"
        default_h3 = "智能门锁"
    elif canonical_kind == "villa_premium":
        default_project = "集茂独栋别墅"
        default_layout = "5房别墅"
        default_area = "洪森大道"
        default_size = "320㎡"
        default_floor = "3楼"
        default_price = "$2500/月"
        default_h1 = "家具家电齐全"
        default_h2 = "拎包入住"
        default_h3 = "实拍可核验"
    else:
        default_project = "富力城"
        default_layout = "1房1卫"
        default_area = "BKK1"
        default_size = "45㎡"
        default_floor = "8楼"
        default_price = "$680"
        default_h1 = "家具基本全新"
        default_h2 = "小区泳池"
        default_h3 = "健身房"

    ap.add_argument("--project", default=default_project)
    ap.add_argument("--ref", default="QC0315")
    ap.add_argument("--layout", default=default_layout)
    ap.add_argument("--area", default=default_area)
    ap.add_argument("--size", default=default_size)
    ap.add_argument("--floor", default=default_floor)
    ap.add_argument("--price", default=default_price)
    ap.add_argument("--payment", default="押1付1")
    ap.add_argument("--h1", default=default_h1)
    ap.add_argument("--h2", default=default_h2)
    ap.add_argument("--h3", default=default_h3)

    g = ap.add_mutually_exclusive_group()
    g.add_argument("--bg-url", default=None, help="Background image URL (https://...)")
    g.add_argument("--bg-local", default=None, help="Background image local path")

    args = ap.parse_args(argv)
    args.kind = _canonical_kind(args.kind)

    explicit_template = str(args.template or "").strip()
    if explicit_template:
        tpl = Path(explicit_template).expanduser().resolve()
    else:
        tpl = {
            "right_price_fixed": RIGHT_PRICE_FIXED_TEMPLATE,
            "dark_glass": DARK_GLASS_TEMPLATE,
            "villa_premium": VILLA_PREMIUM_TEMPLATE,
        }[args.kind]
    if not tpl.is_file():
        print(f"template not found: {tpl}", file=sys.stderr)
        return 2

    if args.kind == "right_price_fixed":
        default_bg = "https://images.unsplash.com/photo-1484154218962-a197022b5858?q=80&w=1800&auto=format&fit=crop"
    elif args.kind == "dark_glass":
        default_bg = "https://images.unsplash.com/photo-1494526585095-c41746248156?q=80&w=1800&auto=format&fit=crop"
    elif args.kind == "villa_premium":
        default_bg = "https://images.unsplash.com/photo-1600607687644-c7f34b5f7ef5?q=80&w=1800&auto=format&fit=crop"
    else:
        default_bg = "https://images.unsplash.com/photo-1505693416388-ac5ce068fe85?q=80&w=1800&auto=format&fit=crop"
    render_w, render_h = 1600, 1200

    if args.w > 0 and args.h > 0:
        out_w, out_h = int(args.w), int(args.h)
    else:
        out_w, out_h = render_w, render_h
    bg_src = _bg_src(args.bg_url, args.bg_local, default_url=default_bg)

    s = tpl.read_text(encoding="utf-8")
    repl: dict[str, str] = {
        "{{BG_SRC}}": bg_src,
        "{{PROJECT}}": html.escape(str(args.project), quote=False),
        "{{REF}}": html.escape(str(args.ref), quote=False),
        "{{LAYOUT}}": html.escape(str(args.layout), quote=False),
        "{{SIZE}}": html.escape(str(args.size), quote=False),
        "{{FLOOR}}": html.escape(str(args.floor), quote=False),
        "{{PAYMENT}}": html.escape(str(args.payment), quote=False),
        "{{H1}}": html.escape(str(args.h1), quote=False),
        "{{H2}}": html.escape(str(args.h2), quote=False),
        "{{H3}}": html.escape(str(args.h3), quote=False),
    }
    if "{{PRICE}}" in s:
        repl["{{PRICE}}"] = html.escape(str(args.price), quote=False)
    if "{{AREA}}" in s:
        repl["{{AREA}}"] = html.escape(str(args.area), quote=False)
    if "{{PRICE_LINE}}" in s:
        repl["{{PRICE_LINE}}"] = html.escape(_price_line(str(args.price)), quote=False)
    if "{{REF}}" not in s:
        repl.pop("{{REF}}", None)
    if "{{PAYMENT}}" not in s:
        repl.pop("{{PAYMENT}}", None)

    for k, v in repl.items():
        if k in s:
            s = s.replace(k, v)

    if "{{" in s and "}}" in s:
        # crude but effective: catch any leftover template tokens
        tail = s[s.find("{{") : s.find("{{") + 80].replace("\n", " ")
        print(f"template still contains placeholders near: {tail}", file=sys.stderr)
        return 3

    out_path = Path(args.out).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Chromium 临时截图目录需当前进程可写，不能硬编码到 /root（systemd 普通用户会失败）。
    tmp_env = str(os.getenv("QIAOLIAN_RENDER_TMP", "")).strip()
    home = Path.home()
    candidates: list[Path] = []
    if tmp_env:
        candidates.append(Path(tmp_env).expanduser())
    candidates.extend(
        [
            home / "snap/chromium/common/qiaolian_raster_tmp",
            home / "qiaolian_raster_tmp",
            Path("/tmp/qiaolian/raster_tmp"),
            Path("/tmp/qiaolian_raster_tmp"),
            Path("/opt/qiaolian_dual_bots/media/renders/runtime/raster_tmp"),
            out_path.parent / "raster_tmp",
        ]
    )
    raster_root = _pick_writable_dir(candidates)
    raster_path = raster_root / (out_path.name + ".raster.png")
    raster_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_html = raster_path.with_suffix(".render.html")
    tmp_html.write_text(s, encoding="utf-8")

    chromium = _find_chromium()
    if not chromium:
        print("No chromium binary found (chromium-browser/chromium/chrome).", file=sys.stderr)
        return 4

    url = tmp_html.as_uri()
    # Some Chromium builds return a content viewport shorter than requested window size.
    # Add a small capture padding, then crop back to target viewport to avoid bottom clipping.
    viewport_pad = int(str(os.getenv("QIAOLIAN_RENDER_VIEWPORT_PAD", "120")).strip() or "120")
    capture_w, capture_h = int(render_w), int(render_h + max(0, viewport_pad))

    cmd = [
        chromium,
        "--headless=new",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--hide-scrollbars",
        f"--window-size={capture_w},{capture_h}",
        f"--force-device-scale-factor={float(args.dpr)}",
        # Give remote images/fonts a moment to settle
        "--virtual-time-budget=8000",
        f"--screenshot={str(raster_path)}",
        url,
    ]
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError:
        print("chromium screenshot failed", file=sys.stderr)
        return 5

    if not raster_path.is_file():
        print(f"expected raster missing: {raster_path}", file=sys.stderr)
        return 6

    rendered = Image.open(raster_path)
    render_size = (int(render_w), int(render_h))
    if rendered.size != render_size:
        # Chromium (especially snap builds) may return a shorter screenshot than requested.
        # Fit back to template canvas first to avoid top-left cropping artifacts.
        if rendered.width >= render_size[0] and rendered.height >= render_size[1]:
            rendered = rendered.crop((0, 0, render_size[0], render_size[1]))
        else:
            rendered = ImageOps.fit(
                rendered,
                render_size,
                method=Image.Resampling.LANCZOS,
                centering=(0.5, 0.5),
            )

    out_size = (int(out_w), int(out_h))
    if rendered.size != out_size:
        rendered = rendered.resize(out_size, Image.Resampling.LANCZOS)

    suf = out_path.suffix.lower()
    if suf in (".jpg", ".jpeg"):
        out_img = rendered.convert("RGB")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_img.save(out_path, "JPEG", quality=int(args.jpeg_quality), optimize=True)
    else:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if rendered.mode not in ("RGB", "RGBA"):
            rendered = rendered.convert("RGBA")
        rendered.save(out_path)

    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
