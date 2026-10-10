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
    assert abs(info["roll_deg"]) > 3
    gray = cv2.cvtColor(out, cv2.COLOR_BGR2GRAY)
    after = straighten.mean_abs_lean_deg(gray)
    assert after is not None and after < 1.5
    assert info["crop_kept"] >= 0.6
    # No empty corners from the warp.
    for y, x in ((0, 0), (0, -1), (-1, 0), (-1, -1)):
        assert gray[y, x] > 100


def test_roll_is_clamped_to_eight_degrees():
    result = straighten.straighten_array(_facade(12.0))
    assert result is not None
    assert abs(result[1]["roll_deg"]) <= 8.0 + 1e-6


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
