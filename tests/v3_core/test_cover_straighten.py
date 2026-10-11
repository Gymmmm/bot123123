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


def _converging(edge_deg: float, size=(1000, 1200)):
    """Synthetic upward shot: pillars converge upwards, outer ones lean ~edge_deg."""
    h, w = size
    img = np.full((h, w, 3), 225, np.uint8)
    shrink = math.tan(math.radians(edge_deg)) * h / (w * 0.45)
    for xb in range(80, w - 60, 70):
        xt = w / 2 + (xb - w / 2) * (1 - shrink)
        cv2.line(img, (int(xt), 0), (int(xb), h - 1), (40, 40, 40), 7)
    for y in range(80, h, 140):
        cv2.line(img, (0, y), (w, y), (120, 120, 120), 3)
    return img


def _reference_gentle(img):
    """The Gym-approved 03:41 scratch algorithm (/workspace/cover_type/straighten.py), verbatim math."""
    H, W = img.shape[:2]
    segs = straighten._vertical_segments(cv2.cvtColor(img, cv2.COLOR_BGR2GRAY))
    xs = np.array([(s[0] - W / 2) / W for s in segs]); ls = np.array([s[2] for s in segs]); w = np.array([s[3] for s in segs])
    keep = np.ones(len(segs), bool)
    for _ in range(4):
        A = np.stack([np.ones(keep.sum()), xs[keep]], 1) * w[keep, None]
        p, q = np.linalg.lstsq(A, ls[keep] * w[keep], rcond=None)[0]
        res = np.abs(ls - (p + q * xs))
        keep = res <= max(0.02, np.percentile(res[keep], 75) * 1.5)
        if keep.sum() < 4:
            break
    if abs(math.degrees(math.atan(p))) > 8.0:
        p = math.tan(math.radians(math.copysign(8.0, math.degrees(math.atan(p)))))
    qk = q * 0.85
    src, dst = [], []
    for fx in (0.2, 0.8):
        for y in (0, H):
            src.append((fx * W + (p + qk * (fx - 0.5)) * (y - H / 2), y)); dst.append((fx * W, y))
    M = cv2.getPerspectiveTransform(np.float32(src), np.float32(dst))
    warped = cv2.warpPerspective(img, M, (W, H), flags=cv2.INTER_LANCZOS4, borderMode=cv2.BORDER_CONSTANT)
    mask = cv2.warpPerspective(np.full((H, W), 255, np.uint8), M, (W, H), flags=cv2.INTER_NEAREST)
    c = cv2.perspectiveTransform(np.float32([[[0, 0], [W, 0], [W, H], [0, H]]]), M)[0]
    l = max(c[0][0], c[3][0]); r = min(c[1][0], c[2][0]); t = max(c[0][1], c[1][1]); b = min(c[2][1], c[3][1])
    l, t = int(math.ceil(max(0, l))) + 2, int(math.ceil(max(0, t))) + 2
    r, b = int(min(W, r)) - 2, int(min(H, b)) - 2
    for _ in range(60):
        if mask[t:b, l:r].min() == 255:
            break
        l += 3; r -= 3; t += 3; b -= 3
    return warped[t:b, l:r]


@pytest.mark.parametrize("img", [_facade(6.0), _converging(10.0)], ids=["roll", "keystone"])
def test_exterior_is_the_approved_gentle_algorithm_pixel_for_pixel(img):
    status, out, m = straighten.correct_array(img, mode="exterior")
    assert status == "corrected" and m["method"] == "gentle"
    ref = _reference_gentle(img)
    assert out.shape == ref.shape and np.array_equal(out, ref)


def test_gentle_levels_a_small_tilt_without_black_corners():
    status, out, info = straighten.correct_array(_facade(6.0), mode="exterior")
    assert status == "corrected"
    assert 5.0 < abs(info["roll_deg"]) < 7.0
    assert info["residual_after_deg"] <= 1.0 and info["hero_ok"]
    assert info["crop_kept"] >= 0.7
    gray = cv2.cvtColor(out, cv2.COLOR_BGR2GRAY)
    for y, x in ((0, 0), (0, -1), (-1, 0), (-1, -1)):
        assert gray[y, x] > 100


def test_roll_is_clamped_to_eight_degrees_and_leftover_tilt_is_not_hero():
    status, _, info = straighten.correct_array(_facade(12.0), mode="exterior")
    assert status == "corrected"
    assert abs(abs(info["roll_deg"]) - 8.0) < 1e-6           # ±8° clamp, never stronger
    assert info["residual_after_deg"] > straighten.HERO_MAX_RESIDUAL_DEG
    assert info["hero_ok"] is False


def test_keystone_is_85_percent_not_full():
    status, _, info = straighten.correct_array(_converging(10.0), mode="exterior")
    assert status == "corrected"
    assert info["keystone_strength"] == 0.85
    assert abs(info["keystone"] - 0.85 * info["keystone_q"]) < 1e-3
    assert 0.3 < info["residual_after_deg"] < info["residual_before_deg"]   # gentle: some lean is left
    assert info["hero_ok"] is True


def test_close_upward_shot_is_flagged_not_over_corrected():
    status, _, info = straighten.correct_array(_converging(24.0), mode="exterior")
    assert info["severe_upshot"] is True and info["hero_ok"] is False
    assert info.get("keystone_strength", 0.85) == 0.85                     # never a 100 % second pass


def test_distortion_is_only_a_guard(monkeypatch):
    img = _converging(10.0)
    status, out, info = straighten.correct_array(img, mode="exterior")
    assert status == "corrected" and info["aspect_change"] > 0               # measured
    monkeypatch.setattr(straighten, "GUARD_ASPECT_CHANGE", 0.001)
    status, out, _ = straighten.correct_array(img, mode="exterior")
    assert status == "reverted:aspect_change" and out is None              # clearly broken → original


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
