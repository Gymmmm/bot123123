from __future__ import annotations

from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw

from v3_core.media.photo_formatter import enhance_property_photo
from v3_core.media.ranker import _overexposure_penalty, _overexposure_stats
from v3_core.media.source_scrub import detect_source_marks, scrub_file


def test_enhance_crushes_highlights_more_than_it_lifts_midtones():
    # Bright window + mid wall: highlights should come down, midtones stay close.
    src = Image.new("RGB", (200, 200), (120, 120, 120))
    draw = ImageDraw.Draw(src)
    draw.rectangle([20, 20, 180, 90], fill=(250, 250, 250))
    out = enhance_property_photo(src)
    src_arr = np.asarray(src)
    out_arr = np.asarray(out)
    # Window region mean should drop.
    assert float(out_arr[30:80, 40:160].mean()) < float(src_arr[30:80, 40:160].mean()) - 8
    # Mid wall should not be crushed into mud.
    assert float(out_arr[140:180, 40:160].mean()) > 95


def test_overexposure_penalty_soft_rejects_blown_indoor_windows():
    stats = {"overall": 0.22, "upper": 0.25}
    penalty, soft = _overexposure_penalty(stats, room_label="bedroom")
    assert soft is True
    assert penalty > 20


def test_overexposure_stats_on_synthetic_blowout():
    img = np.zeros((100, 100, 3), dtype=np.uint8)
    img[:40, :, :] = 255
    stats = _overexposure_stats(img)
    assert stats["overall"] > 0.3
    assert stats["upper"] > 0.5


def test_center_watermark_detected_and_inpainted(tmp_path: Path):
    src = tmp_path / "wm.jpg"
    img = Image.new("RGB", (1000, 700), (70, 95, 70))
    draw = ImageDraw.Draw(img)
    # Facade block
    draw.rectangle([120, 80, 880, 620], fill=(160, 155, 140))
    # Translucent-style bright watermark bands across the middle (URL-like)
    for y, x0, x1 in ((310, 200, 780), (330, 240, 720), (350, 260, 700)):
        draw.rectangle([x0, y, x1, y + 14], fill=(220, 220, 220))
    img.save(src, quality=95)

    bgr = np.array(img)[:, :, ::-1].copy()
    det = detect_source_marks(bgr)
    kinds = {r.get("kind") for r in det["regions"]}
    assert "center_watermark" in kinds

    dst = tmp_path / "scrubbed.jpg"
    info = scrub_file(src, dst, prefer_crop=True)
    assert Path(dst).is_file()
    methods = info.get("methods") or []
    assert "inpaint_telea" in methods or any(str(m).startswith("crop_bottom") for m in methods)
