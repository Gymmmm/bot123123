#!/usr/bin/env python3
"""Qiaolian media pipeline v2: source scrub -> cover pick -> locked cover + white-frame details."""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from PIL import Image, ImageOps
from cover_selector import select_cover, _json_default
from gallery_whiteframe import render_detail, DEFAULT_LOGO
from render_locked_cover import render as render_locked_cover
from scrub_source_marks import scrub_file, IMAGE_EXTS

PARSER_DIRS = [
    Path("/workspace/qiaolian-raw-collect-v1/prod_parser_out"),
    Path("/workspace/qiaolian-raw-collect-v1/washed_v2_safe"),
    Path("/workspace/qiaolian-raw-collect-v1/washed_v2"),
]
MAX_SCRUB_COVERAGE = 0.08


def list_images(folder: Path) -> list[Path]:
    return sorted(p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in IMAGE_EXTS)


def _facts_from_obj(obj: dict[str, Any]) -> dict[str, Any]:
    f = obj.get("facts") if isinstance(obj.get("facts"), dict) else obj
    out: dict[str, Any] = {}
    project = f.get("project_name") or f.get("community_name") or f.get("project")
    layout = f.get("layout") or f.get("property_type_display") or f.get("property_type")
    area = f.get("canonical_area_display") or f.get("area") or f.get("location")
    rent = f.get("monthly_rent_usd") or f.get("rent") or f.get("price")
    if project: out["project"] = str(project)
    if layout: out["layout"] = str(layout)
    if area: out["area"] = str(area)
    if rent is not None: out["price"] = str(rent).replace("$", "").replace("/月", "").strip()
    if f.get("size_sqm") is not None: out["size"] = f"{int(float(f['size_sqm']))}㎡"
    elif f.get("size"): out["size"] = str(f["size"])
    if f.get("floor") is not None:
        floor = str(f["floor"])
        out["floor"] = floor if "楼" in floor else f"{floor}楼"
    highlights = f.get("highlights") or f.get("tags")
    if isinstance(highlights, list): out["highlights"] = " · ".join(map(str, highlights[:3]))
    elif highlights: out["highlights"] = str(highlights)
    return out


def load_listing_facts(listing_id: str, explicit: Path | None) -> dict[str, str]:
    candidates = [explicit] if explicit else []
    candidates += [d / f"{listing_id}.json" for d in PARSER_DIRS]
    for p in candidates:
        if not p or not p.exists():
            continue
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
            facts = _facts_from_obj(obj)
            if facts.get("project") and facts.get("layout") and facts.get("area") and facts.get("price"):
                facts.setdefault("size", "—")
                facts.setdefault("floor", "—")
                facts.setdefault("highlights", "可预约看房")
                facts["source_path"] = str(p)
                return facts
        except Exception:
            continue
    raise RuntimeError(f"missing_real_listing_facts:{listing_id}; refusing sample/default cover data")


def safe_scrub(src: Path, dst: Path) -> dict[str, Any]:
    info = scrub_file(src, dst, prefer_crop=False)
    coverage = float(info.get("coverage") or 0)
    if coverage > MAX_SCRUB_COVERAGE:
        dst.unlink(missing_ok=True)
        return {**info, "accepted": False, "reason": "scrub_coverage_too_high"}
    return {**info, "accepted": True}


def process_listing(listing_id: str, input_root: Path, output_root: Path, facts_json: Path | None) -> dict[str, Any]:
    started = time.time()
    src_dir = input_root / listing_id
    out_dir = output_root / listing_id
    scrub_dir = out_dir / "scrubbed"
    detail_dir = out_dir / "details"
    scrub_dir.mkdir(parents=True, exist_ok=True)
    detail_dir.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {"listing_id": listing_id, "errors": []}

    photos = list_images(src_dir)
    report["photo_count_in"] = len(photos)
    if len(photos) < 2:
        raise RuntimeError("fewer_than_2_photos")

    accepted: list[Path] = []
    scrub_meta: list[dict[str, Any]] = []
    for src in photos:
        dst = scrub_dir / f"{src.stem}_clean.jpg"
        try:
            meta = safe_scrub(src, dst)
            scrub_meta.append(meta)
            if meta.get("accepted"):
                accepted.append(dst)
        except Exception as exc:
            report["errors"].append({"stage": "scrub", "file": str(src), "error": str(exc)})
    report["scrub"] = scrub_meta
    report["photos_accepted"] = len(accepted)
    report["photos_rejected"] = len(photos) - len(accepted)
    if len(accepted) < 2:
        raise RuntimeError("not_enough_safe_photos_after_scrub")

    winner, scores = select_cover(accepted)
    if not winner:
        winner = str(accepted[0])
    winner = Path(winner)
    report["cover_score"] = (scores.get("best") or {}).get("cover_score")
    report["cover_winner_src"] = str(winner)
    (out_dir / "cover_pick_report.json").write_text(json.dumps(scores, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")

    clean_winner = out_dir / "cover_winner.jpg"
    with Image.open(winner) as im:
        ImageOps.exif_transpose(im).convert("RGB").save(clean_winner, "JPEG", quality=94, optimize=True)

    facts = load_listing_facts(listing_id, facts_json)
    report["facts"] = facts
    cover = out_dir / "cover.jpg"
    render_locked_cover(bg=clean_winner, output=cover, project=facts["project"], layout=facts["layout"], area=facts["area"], size=facts["size"], floor=facts["floor"], highlights=facts["highlights"], price=facts["price"])
    report["cover"] = str(cover)

    details = []
    n = 0
    for src in accepted:
        if src.resolve() == winner.resolve():
            continue
        n += 1
        dst = detail_dir / f"{n:02d}_{src.stem}.jpg"
        details.append(render_detail(src, dst, logo_path=DEFAULT_LOGO))
    report["detail_count"] = len(details)
    report["details"] = details
    report["output_contract"] = [str(cover)] + [x["output"] for x in details]
    report["elapsed_s"] = round(time.time() - started, 3)
    (out_dir / "pipeline_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def main() -> int:
    ap = argparse.ArgumentParser(description="Qiaolian listing media pipeline v2")
    ap.add_argument("--input-dir", type=Path, required=True)
    ap.add_argument("--output-dir", type=Path, required=True)
    ap.add_argument("--only", nargs="+")
    ap.add_argument("--facts-json", type=Path, default=None, help="Explicit canonical/parser facts JSON for a single listing")
    args = ap.parse_args()
    ids = args.only or sorted(p.name for p in args.input_dir.iterdir() if p.is_dir())
    results = []
    failures = []
    for listing_id in ids:
        try:
            result = process_listing(listing_id, args.input_dir, args.output_dir, args.facts_json)
            results.append(result)
            print(f"OK {listing_id}: cover=1 details={result['detail_count']} rejected={result['photos_rejected']}")
        except Exception as exc:
            failures.append({"listing_id": listing_id, "error": str(exc)})
            print(f"FAIL {listing_id}: {exc}", file=sys.stderr)
    summary = {"ok": len(results), "failed": len(failures), "results": results, "failures": failures}
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "pipeline_v2_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
