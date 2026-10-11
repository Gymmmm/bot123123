import math

import numpy as np
import pytest

from v3_core.media import straighten

cv2 = pytest.importorskip("cv2")


def _facade(lean_deg: float = 6.0, size=(900, 1200)):
    """Synthetic facade: dark vertical pillars all leaning by ``lean_deg``."""
    h, w = size
    img = np.full((h, w, 3), 225, np.uint8)
    lean = math.tan(math.radians(lean_deg))
    for x in range(120, w - 80, 110):
        top = (int(x + lean * (-h / 2)), 0)
        bottom = (int(x + lean * (h / 2)), h - 1)
        cv2.line(img, top, bottom, (40, 40, 40), 9)
    return img


def test_straighten_levels_tilted_verticals_without_black_corners():
    img = _facade(6.0)
    result = straighten.straighten_array(img)
    assert result is not None
    out, info = result
    assert abs(info["rotation_deg"]) > 3
    assert info["residual_after_deg"] <= 1.0
    gray = cv2.cvtColor(out, cv2.COLOR_BGR2GRAY)
    after = straighten.mean_abs_lean_deg(gray)
    assert after is not None and after < 1.5
    assert info["crop_kept"] >= 0.6
    # No empty corners from the warp.
    for y, x in ((0, 0), (0, -1), (-1, 0), (-1, -1)):
        assert gray[y, x] > 100


def test_strong_parallel_lean_is_fully_levelled():
    # The exterior path is no longer clamped at 8 degrees: a 12 degree roll is levelled.
    result = straighten.straighten_array(_facade(12.0))
    assert result is not None
    assert result[1]["residual_after_deg"] <= 1.0
    assert abs(result[1]["rotation_deg"]) > 8.0


def _converging(edge_deg: float, size=(1000, 1200)):
    """Synthetic low-angle shot: pillars converge upwards, outer ones lean ~edge_deg."""
    h, w = size
    img = np.full((h, w, 3), 225, np.uint8)
    shrink = math.tan(math.radians(edge_deg)) * h / (w * 0.45)
    for xb in range(80, w - 60, 70):
        xt = w / 2 + (xb - w / 2) * (1 - shrink)
        cv2.line(img, (int(xt), 0), (int(xb), h - 1), (40, 40, 40), 7)
    for y in range(80, h, 140):
        cv2.line(img, (0, y), (w, y), (120, 120, 120), 3)
    return img


def test_strong_keystone_is_fully_corrected_within_distortion_caps():
    status, out, m = straighten.correct_array(_converging(12.0), mode="exterior")
    assert status == "corrected" and out is not None
    assert m["convergence_deg"] >= 25          # ~30 degrees total convergence
    assert m["residual_before_deg"] > 10
    assert m["residual_after_deg"] <= straighten.TARGET_RESIDUAL_DEG
    assert m["keystone_strength"] == 1.0 and not m["partial"]
    assert m["aspect_change"] <= straighten.EXTERIOR_MAX_ASPECT_CHANGE
    assert m["edge_aspect_change"] <= straighten.EXTERIOR_MAX_EDGE_ASPECT_CHANGE
    assert m["crop_kept"] >= straighten.EXTERIOR_MIN_CROP_KEPT
    gray = cv2.cvtColor(out, cv2.COLOR_BGR2GRAY)
    for y, x in ((0, 0), (0, -1), (-1, 0), (-1, -1)):
        assert gray[y, x] > 100                # no empty warp corners


def test_beyond_the_cap_applies_the_max_partial_correction_instead_of_skipping():
    status, out, m = straighten.correct_array(_converging(25.0), mode="exterior")
    assert status == "corrected" and out is not None
    assert m["partial"] and m["keystone_strength"] < 1.0
    assert m["residual_after_deg"] < m["residual_before_deg"] - 5
    assert m["aspect_change"] <= straighten.EXTERIOR_MAX_ASPECT_CHANGE
    assert m["edge_aspect_change"] <= straighten.EXTERIOR_MAX_EDGE_ASPECT_CHANGE


def test_distortion_cap_limits_strength_and_reverts_when_even_minimum_breaks_it(monkeypatch):
    img = _converging(12.0)
    monkeypatch.setattr(straighten, "EXTERIOR_MAX_ASPECT_CHANGE", 0.05)
    status, _, m = straighten.correct_array(img, mode="exterior")
    assert status == "corrected" and m["partial"] and m["aspect_change"] <= 0.05
    monkeypatch.setattr(straighten, "EXTERIOR_MAX_ASPECT_CHANGE", 0.001)
    status, out, _ = straighten.correct_array(img, mode="exterior")
    assert status == "reverted:aspect_change" and out is None


def test_featureless_photo_returns_none_and_file_api_falls_back(tmp_path):
    flat = np.full((600, 800, 3), 128, np.uint8)
    assert straighten.straighten_array(flat) is None
    src = tmp_path / "flat.jpg"
    cv2.imwrite(str(src), flat)
    assert straighten.straighten_file(src, tmp_path / "out.jpg") is None
    assert straighten.straighten_file(tmp_path / "missing.jpg", tmp_path / "out2.jpg") is None


def test_without_opencv_straighten_is_a_noop(tmp_path, monkeypatch):
    monkeypatch.setattr(straighten, "cv2", None)
    assert straighten.straighten_file(tmp_path / "a.jpg", tmp_path / "b.jpg") is None
