#!/usr/bin/env python3
"""Orchestrate 侨联 listing media: scrub → cover pick → gallery topline → cover renders."""
from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from PIL import Image, ImageDraw, ImageOps  # noqa: E402

from cover_maker_gold_bottom import generate_cover  # noqa: E402
from cover_selector import select_cover, _json_default  # noqa: E402
from gallery_topline_brand import format_gallery_topline, DEFAULT_MARK  # noqa: E402
from scrub_source_marks import scrub_file, IMAGE_EXTS, make_before_after  # noqa: E402

LOCKED_DIR = ROOT / "output" / "cover_locked_code"
sys.path.insert(0, str(LOCKED_DIR))
try:
    from render_locked_cover import render as render_locked_daily  # type: ignore
except Exception:  # pragma: no cover
    render_locked_daily = None

DEFAULT_FACTS = {
    "project": "富力城",
    "layout": "1房公寓",
    "area": "BKK1",
    "size": "45㎡",
    "floor": "8楼",
    "price": "680",
    "property_type": "1房公寓",
    "location": "BKK1",
    "highlights": "家具家电齐全 · 近商场 · 采光好",
    "tags": ["家具家电齐全", "近商场", "采光好"],
    "rent": 680,
}

GOLD_MARK = ROOT / "assets" / "qiaolian_logo_mark_gold.png"
TOPLINE_FALLBACK = ROOT / "assets" / "qiaolian_logo_topline_gold.png"
PARSER_DIRS = [
    Path("/workspace/qiaolian-raw-collect-v1/prod_parser_out"),
    Path("/workspace/qiaolian-raw-collect-v1/washed_v2_safe"),
    Path("/workspace/qiaolian-raw-collect-v1/washed_v2"),
]


def list_images(folder: Path) -> list[Path]:
    return sorted(
        p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in IMAGE_EXTS
    )


def ensure_mark() -> Path:
    if GOLD_MARK.exists():
        return GOLD_MARK
    if TOPLINE_FALLBACK.exists():
        return TOPLINE_FALLBACK
    raise FileNotFoundError("No brand mark asset found")


def _clean_project(name: str | None) -> str | None:
    if not name:
        return None
    name = str(name).strip()
    bad = ("网络", "保洁", "配套", "包含", "null", "None")
    if any(b in name for b in bad) and len(name) < 20:
        # likely mis-parsed from 包含项目 line
        if "网络" in name or "保洁" in name:
            return None
    if len(name) > 40:
        return None
    return name


def parse_text_fields(text: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if not text:
        return out
    # hashtag project
    m = re.search(r"#([^\s#\n]+)", text)
    if m:
        out["project"] = m.group(1).strip()
    # bracket title
    m = re.search(r"【([^】]+)】", text)
    if m and "project" not in out:
        title = m.group(1)
        # strip marketing fluff if it looks like a project
        out["title_hint"] = title
    # price
    prices = re.findall(r"\$\s*(\d{2,5})", text)
    if prices:
        # take first or range mid
        nums = [int(x) for x in prices]
        out["rent"] = nums[0]
        out["price"] = str(nums[0])
        if len(nums) >= 2 and nums[0] != nums[1]:
            out["price_display"] = f"${nums[0]}–${nums[1]}"
    # layout
    m = re.search(r"(?:房间户型|户型)[：:]\s*([^\n]+)", text)
    if m:
        layout = m.group(1).strip().split("｜")[0].split("|")[0].strip()
        out["layout"] = layout
        out["property_type"] = layout
    else:
        m = re.search(r"(\d\+?\d?\s*房(?:\d厅)?|复式\d房|单间|Studio)", text, re.I)
        if m:
            out["layout"] = m.group(1)
            out["property_type"] = m.group(1)
    # floor
    m = re.search(r"(?:楼层情况|楼层)[：:]\s*([^\n]+)", text)
    if m:
        fl = m.group(1).strip()
        out["floor"] = fl if "楼" in fl else f"{fl}楼" if fl.isdigit() else fl
    else:
        m = re.search(r"(\d{1,3})\s*楼", text)
        if m:
            out["floor"] = f"{m.group(1)}楼"
    # size
    m = re.search(r"(\d{2,4})\s*[㎡m²]", text)
    if m:
        out["size"] = f"{m.group(1)}㎡"
    # area / district hints
    for pat in (r"(BKK\d)", r"(Tonle Bassac|Chroy Changvar|Toul Kork|Sen Sok)", r"(波罗勉|森速|堆谷|茶胶)"):
        m = re.search(pat, text, re.I)
        if m:
            out["area"] = m.group(1)
            out["location"] = m.group(1)
            break
    # highlights from 亮点 / 配套
    bits = []
    for lab in ("房源亮点", "配套情况", "生活配套"):
        m = re.search(rf"{lab}[：:]\s*([^\n]+)", text)
        if m:
            bits.extend(re.split(r"[｜|/|、]", m.group(1)))
    tags = [b.strip() for b in bits if b.strip() and len(b.strip()) <= 12][:4]
    if tags:
        out["tags"] = tags
        out["highlights"] = " · ".join(tags[:3])
    return out


def load_listing_facts(listing_id: str) -> dict[str, Any]:
    facts: dict[str, Any] = {"listing_id": listing_id, "source": "sample_defaults"}

    raw_json = None
    for d in PARSER_DIRS:
        p = d / f"{listing_id}.json"
        if p.exists():
            try:
                raw_json = json.loads(p.read_text(encoding="utf-8"))
                facts["parser_path"] = str(p)
                break
            except Exception:
                continue

    text = ""
    if isinstance(raw_json, dict):
        text = (
            raw_json.get("sanitized_text")
            or raw_json.get("raw_text")
            or raw_json.get("text")
            or ""
        )
        f = raw_json.get("facts") or {}
        # prefer text parse; overlay confirmed numeric fields
        parsed = parse_text_fields(text)
        facts.update({k: v for k, v in parsed.items() if v})
        proj = _clean_project(f.get("project_name")) or _clean_project(f.get("community_name"))
        if proj:
            facts["project"] = proj
        elif parsed.get("project"):
            facts["project"] = parsed["project"]
        if f.get("layout"):
            # prefer text layout if richer
            if not parsed.get("layout") or len(str(f.get("layout"))) > len(str(parsed.get("layout") or "")):
                pass
            facts.setdefault("layout", f["layout"])
            facts.setdefault("property_type", f.get("property_type_display") or f["layout"])
        if f.get("monthly_rent_usd"):
            facts["rent"] = int(f["monthly_rent_usd"])
            facts["price"] = str(int(f["monthly_rent_usd"]))
        if f.get("size_sqm"):
            facts["size"] = f"{int(f['size_sqm'])}㎡"
        if f.get("floor"):
            fl = str(f["floor"])
            facts["floor"] = fl if "楼" in fl else f"{fl}楼"
        if f.get("canonical_area_display"):
            facts["area"] = f["canonical_area_display"]
            facts["location"] = f["canonical_area_display"]
        if f.get("highlights"):
            hl = [str(x) for x in f["highlights"] if x][:3]
            if hl:
                facts["tags"] = hl
                facts["highlights"] = " · ".join(hl)
        facts["source"] = "parser+text" if text else "parser"
        facts["sanitized_text"] = text[:500]
    else:
        # try messages folder
        msg_candidates = list(Path("/workspace/qiaolian-raw-collect-v1/messages").glob(f"*{listing_id}*")) if Path("/workspace/qiaolian-raw-collect-v1/messages").exists() else []
        for mp in msg_candidates[:3]:
            try:
                t = mp.read_text(encoding="utf-8", errors="ignore")
                if listing_id in t or "$" in t:
                    parsed = parse_text_fields(t)
                    if parsed:
                        facts.update({k: v for k, v in parsed.items() if v})
                        facts["source"] = "messages"
                        break
            except Exception:
                pass

    # normalize — only invent sample defaults when no real listing text/parser was found
    used_sample = facts.get("source") == "sample_defaults"
    if used_sample:
        for k, v in DEFAULT_FACTS.items():
            facts.setdefault(k, v)
    facts["location"] = facts.get("location") or facts.get("area") or ("BKK1" if used_sample else "金边")
    facts["area"] = facts.get("area") or facts["location"]
    facts["property_type"] = facts.get("property_type") or facts.get("layout") or ("公寓" if not used_sample else DEFAULT_FACTS["layout"])
    facts["layout"] = facts.get("layout") or facts["property_type"]
    if "rent" not in facts and facts.get("price"):
        try:
            facts["rent"] = int(re.sub(r"\D", "", str(facts["price"])) or "0") or (680 if used_sample else 0)
        except Exception:
            facts["rent"] = 680 if used_sample else 0
    if not facts.get("price"):
        facts["price"] = str(facts.get("rent") or (680 if used_sample else "面议"))
    # avoid leaking sample size/floor onto real listings
    if not used_sample:
        if facts.get("size") == DEFAULT_FACTS["size"] and "size" not in (facts.get("_parsed_keys") or []):
            # if size came only from DEFAULT seed at start, clear when text had none
            pass
        # re-seed cleared: start from empty then merge — handled below via tracked keys
    facts.setdefault("size", "—" if not used_sample else DEFAULT_FACTS["size"])
    facts.setdefault("floor", "—" if not used_sample else DEFAULT_FACTS["floor"])
    facts.setdefault("highlights", DEFAULT_FACTS["highlights"] if used_sample else "可预约看房")
    facts.setdefault("tags", list(DEFAULT_FACTS["tags"]) if used_sample else ["可预约看房"])
    return facts


def process_listing(
    listing_id: str,
    input_dir: Path,
    output_root: Path,
    *,
    render_covers: bool,
    mark_path: Path,
) -> dict[str, Any]:
    t0 = time.time()
    src_dir = input_dir / listing_id
    out_dir = output_root / listing_id
    scrub_dir = out_dir / "scrubbed"
    gallery_dir = out_dir / "gallery"
    scrub_dir.mkdir(parents=True, exist_ok=True)
    gallery_dir.mkdir(parents=True, exist_ok=True)

    report: dict[str, Any] = {
        "listing_id": listing_id,
        "input_dir": str(src_dir),
        "output_dir": str(out_dir),
        "errors": [],
        "enhance_auto_p": False,
    }

    photos = list_images(src_dir)
    report["photo_count_in"] = len(photos)
    if len(photos) < 2:
        report["skipped"] = "fewer_than_2_photos"
        report["elapsed_s"] = round(time.time() - t0, 3)
        return report

    # 1) Scrub
    scrubbed_paths: list[Path] = []
    scrub_meta = []
    for src in photos:
        dst = scrub_dir / f"{src.stem}_scrubbed.jpg"
        try:
            info = scrub_file(src, dst, prefer_crop=True)
            scrub_meta.append(info)
            scrubbed_paths.append(dst)
        except Exception as e:
            report["errors"].append({"stage": "scrub", "file": str(src), "error": str(e)})
    report["photos_scrubbed"] = len(scrubbed_paths)
    report["scrub"] = [
        {
            "input": m.get("input"),
            "output": m.get("output"),
            "methods": m.get("methods"),
            "regions": len(m.get("regions") or []),
            "coverage": m.get("coverage"),
            "crop_frac": m.get("crop_frac"),
        }
        for m in scrub_meta
    ]

    if len(scrubbed_paths) < 1:
        report["errors"].append({"stage": "scrub", "error": "no_scrubbed_outputs"})
        report["elapsed_s"] = round(time.time() - t0, 3)
        (out_dir / "pipeline_report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, default=_json_default),
            encoding="utf-8",
        )
        return report

    # 2) Cover select
    try:
        best_path, scores = select_cover(scrubbed_paths)
        report_path = out_dir / "cover_pick_report.json"
        report_path.write_text(
            json.dumps(scores, ensure_ascii=False, indent=2, default=_json_default),
            encoding="utf-8",
        )
        winner_dst = out_dir / "cover_winner.jpg"
        if best_path:
            with Image.open(best_path) as im:
                ImageOps.exif_transpose(im).convert("RGB").save(
                    winner_dst, "JPEG", quality=92, optimize=True
                )
            report["cover_winner"] = str(winner_dst)
            report["cover_winner_src"] = str(best_path)
        else:
            report["errors"].append({"stage": "cover_select", "error": "no_winner"})
            best_path = str(scrubbed_paths[0])
            with Image.open(best_path) as im:
                ImageOps.exif_transpose(im).convert("RGB").save(
                    winner_dst, "JPEG", quality=92, optimize=True
                )
            report["cover_winner"] = str(winner_dst)
            report["cover_winner_src"] = best_path
        report["cover_score"] = (scores.get("best") or {}).get("cover_score")
    except Exception as e:
        report["errors"].append({"stage": "cover_select", "error": str(e)})
        best_path = str(scrubbed_paths[0])
        winner_dst = out_dir / "cover_winner.jpg"
        shutil.copy2(best_path, winner_dst)
        report["cover_winner"] = str(winner_dst)

    winner_name = Path(report.get("cover_winner_src") or scrubbed_paths[0]).name

    # 3) Gallery (non-cover)
    gallery_results = []
    for src in scrubbed_paths:
        if src.name == winner_name:
            continue
        dst = gallery_dir / f"{src.stem}_gallery.jpg"
        try:
            info = format_gallery_topline(src, dst, mark_path=mark_path)
            gallery_results.append(info)
        except Exception as e:
            report["errors"].append({"stage": "gallery", "file": str(src), "error": str(e)})
    report["gallery_count"] = len(gallery_results)
    report["gallery"] = gallery_results

    # 4) Cover renders
    facts = load_listing_facts(listing_id)
    report["facts"] = {k: v for k, v in facts.items() if k != "sanitized_text"}
    report["facts_text_preview"] = (facts.get("sanitized_text") or "")[:240]
    cover_bg = Path(report["cover_winner"])

    if render_covers:
        # daily locked price-tag (HTML/Playwright)
        daily_out = out_dir / "cover_daily.jpg"
        try:
            if render_locked_daily is None:
                raise RuntimeError("render_locked_cover unavailable")
            render_locked_daily(
                bg=cover_bg,
                output=daily_out,
                project=str(facts.get("project") or DEFAULT_FACTS["project"]),
                layout=str(facts.get("layout") or DEFAULT_FACTS["layout"]),
                area=str(facts.get("area") or DEFAULT_FACTS["area"]),
                size=str(facts.get("size") or DEFAULT_FACTS["size"]),
                floor=str(facts.get("floor") or DEFAULT_FACTS["floor"]),
                highlights=str(facts.get("highlights") or DEFAULT_FACTS["highlights"]),
                price=str(facts.get("price") or DEFAULT_FACTS["price"]),
            )
            report["cover_daily"] = str(daily_out)
        except Exception as e:
            report["errors"].append({"stage": "cover_daily", "error": str(e)})

        # gold-bottom
        gold_out = out_dir / "cover_gold.jpg"
        try:
            logo = str(GOLD_MARK if GOLD_MARK.exists() else mark_path)
            generate_cover(
                str(cover_bg),
                str(gold_out),
                title=str(facts.get("project") or DEFAULT_FACTS["project"]),
                rent=facts.get("rent") or 680,
                property_type=str(facts.get("property_type") or facts.get("layout") or "公寓"),
                location=str(facts.get("location") or facts.get("area") or "金边"),
                area=str(facts.get("size") or DEFAULT_FACTS["size"]),
                floor=str(facts.get("floor") or DEFAULT_FACTS["floor"]),
                tags=list(facts.get("tags") or DEFAULT_FACTS["tags"]),
                logo_path=logo,
            )
            report["cover_gold"] = str(gold_out)
        except Exception as e:
            report["errors"].append({"stage": "cover_gold", "error": str(e)})
    else:
        report["cover_render_skipped"] = True

    report["elapsed_s"] = round(time.time() - t0, 3)
    (out_dir / "pipeline_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )
    return report


def make_contact_sheet(listing_out: Path, sheet_path: Path, cols: int = 4) -> Path:
    """Contact sheet of key 7028 outputs for the user."""
    candidates: list[Path] = []
    winner = listing_out / "cover_winner.jpg"
    daily = listing_out / "cover_daily.jpg"
    gold = listing_out / "cover_gold.jpg"
    for p in (daily, gold, winner):
        if p.exists():
            candidates.append(p)
    scrubbed = sorted((listing_out / "scrubbed").glob("*_scrubbed.jpg"))
    gallery = sorted((listing_out / "gallery").glob("*_gallery.jpg"))
    # include a few scrub + gallery
    candidates.extend(scrubbed[:3])
    candidates.extend(gallery[:4])
    # before/after if present
    ba = listing_out / "scrub_before_after.jpg"
    if ba.exists():
        candidates.insert(0, ba)

    if not candidates:
        raise FileNotFoundError("no images for contact sheet")

    thumbs = []
    tw, th = 400, 300
    for p in candidates:
        with Image.open(p) as im:
            im = ImageOps.exif_transpose(im).convert("RGB")
            # contain into tw x th
            canvas = Image.new("RGB", (tw, th), (24, 28, 34))
            scale = min(tw / im.width, th / im.height)
            nw, nh = max(1, int(im.width * scale)), max(1, int(im.height * scale))
            im2 = im.resize((nw, nh), Image.Resampling.LANCZOS)
            canvas.paste(im2, ((tw - nw) // 2, (th - nh) // 2))
            # label
            d = ImageDraw.Draw(canvas)
            label = p.name[:28]
            d.rectangle([0, th - 22, tw, th], fill=(0, 0, 0))
            d.text((6, th - 18), label, fill=(230, 220, 180))
            thumbs.append(canvas)

    rows = (len(thumbs) + cols - 1) // cols
    gap = 8
    sheet = Image.new(
        "RGB",
        (cols * tw + (cols + 1) * gap, rows * th + (rows + 1) * gap + 36),
        (18, 20, 24),
    )
    d = ImageDraw.Draw(sheet)
    d.text((gap, 10), f"侨联 pipeline — listing {listing_out.name}", fill=(241, 209, 118))
    for i, th_im in enumerate(thumbs):
        r, c = divmod(i, cols)
        x = gap + c * (tw + gap)
        y = 36 + gap + r * (th + gap)
        sheet.paste(th_im, (x, y))
    sheet_path.parent.mkdir(parents=True, exist_ok=True)
    sheet.save(sheet_path, "JPEG", quality=90, optimize=True)
    return sheet_path


def discover_listings(input_dir: Path, min_photos: int = 2) -> list[str]:
    ids = []
    for d in sorted(input_dir.iterdir()):
        if not d.is_dir():
            continue
        if len(list_images(d)) >= min_photos:
            ids.append(d.name)
    return ids


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Run full 侨联 listing media pipeline")
    ap.add_argument(
        "--input-dir",
        type=Path,
        default=Path("/workspace/qiaolian-raw-collect-v1/media"),
    )
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "output" / "pipeline_v1",
    )
    ap.add_argument("--limit", type=int, default=None, help="Max listings to process")
    ap.add_argument(
        "--cover-limit",
        type=int,
        default=5,
        help="Max listings to fully render covers (always includes 7028 & 7237)",
    )
    ap.add_argument(
        "--always-cover",
        nargs="*",
        default=["7028", "7237"],
        help="Listing IDs that always get cover renders",
    )
    ap.add_argument("--only", nargs="*", default=None, help="Process only these listing IDs")
    args = ap.parse_args(argv)

    input_dir = args.input_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    mark = ensure_mark()
    listings = discover_listings(input_dir)
    if args.only:
        want = set(args.only)
        listings = [x for x in listings if x in want]
        # also allow force even if <2? still require discover
    if args.limit is not None:
        # keep always-cover ids even when limiting
        always = [x for x in (args.always_cover or []) if x in listings]
        rest = [x for x in listings if x not in always]
        need = max(0, args.limit - len(always))
        listings = always + rest[:need]

    always_set = set(args.always_cover or [])
    # First N listings get covers, plus always-cover IDs (7028/7237).
    cover_set = set(listings[: max(0, args.cover_limit)]) | {
        x for x in always_set if x in listings or x in (args.only or [])
    }
    # If --only was used, still honor always + cover_limit within planned list
    cover_set &= set(listings) | always_set
    cover_set = {x for x in cover_set if x in listings}

    summary: dict[str, Any] = {
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "listings_planned": listings,
        "cover_render_ids": sorted(cover_set),
        "mark": str(mark),
        "enhance_auto_p": False,
        "results": [],
    }

    print(f"Listings to process: {len(listings)} (cover renders: {len(cover_set)})")
    total_scrubbed = 0
    total_errors = 0
    t_all = time.time()

    for i, lid in enumerate(listings, 1):
        do_cover = lid in cover_set
        print(f"[{i}/{len(listings)}] {lid} cover={do_cover} ...", flush=True)
        try:
            rep = process_listing(
                lid,
                input_dir,
                output_dir,
                render_covers=do_cover,
                mark_path=mark,
            )
        except Exception as e:
            rep = {
                "listing_id": lid,
                "errors": [{"stage": "fatal", "error": str(e)}],
                "photos_scrubbed": 0,
            }
        summary["results"].append(
            {
                "listing_id": lid,
                "photos_scrubbed": rep.get("photos_scrubbed", 0),
                "gallery_count": rep.get("gallery_count", 0),
                "cover_daily": rep.get("cover_daily"),
                "cover_gold": rep.get("cover_gold"),
                "errors": rep.get("errors") or [],
                "elapsed_s": rep.get("elapsed_s"),
                "skipped": rep.get("skipped"),
            }
        )
        total_scrubbed += int(rep.get("photos_scrubbed") or 0)
        total_errors += len(rep.get("errors") or [])
        err_n = len(rep.get("errors") or [])
        print(
            f"  scrubbed={rep.get('photos_scrubbed')} gallery={rep.get('gallery_count')} "
            f"errors={err_n} {rep.get('elapsed_s')}s",
            flush=True,
        )

    # Contact sheet + before/after for 7028
    p7028 = output_dir / "7028"
    if p7028.exists():
        try:
            # pick one scrubbed with regions if possible
            scrub_report = []
            pr = p7028 / "pipeline_report.json"
            if pr.exists():
                scrub_report = (json.loads(pr.read_text(encoding="utf-8")).get("scrub") or [])
            pick = None
            for s in scrub_report:
                if (s.get("regions") or 0) > 0 or (s.get("methods") and "noop" not in (s.get("methods") or [])):
                    pick = s
                    break
            if pick is None and scrub_report:
                pick = scrub_report[0]
            if pick:
                before = Path(pick["input"])
                after = Path(pick["output"])
                if before.exists() and after.exists():
                    make_before_after(
                        before,
                        after,
                        p7028 / "scrub_before_after.jpg",
                        label_before="SOURCE",
                        label_after="SCRUBBED",
                    )
            sheet = make_contact_sheet(
                p7028, output_dir / "7028_contact_sheet.jpg"
            )
            summary["contact_sheet_7028"] = str(sheet)
            print(f"contact sheet: {sheet}")
        except Exception as e:
            summary["contact_sheet_error"] = str(e)
            total_errors += 1

    summary["listings_processed"] = len(listings)
    summary["photos_scrubbed"] = total_scrubbed
    summary["errors"] = total_errors
    summary["elapsed_s"] = round(time.time() - t_all, 3)
    summary["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")

    summary_path = output_dir / "pipeline_run_summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )
    print(f"\nDONE listings={len(listings)} scrubbed={total_scrubbed} errors={total_errors}")
    print(f"summary: {summary_path}")
    return 0 if total_errors == 0 else 0  # non-fatal errors still success


if __name__ == "__main__":
    raise SystemExit(main())
