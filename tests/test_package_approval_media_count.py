from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
V2 = ROOT / "v2"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(V2) not in sys.path:
    sys.path.insert(0, str(V2))

from qiaolian_publisher_v2.release_contract_patch import _effective_package_media_count


def _row(*, main, discussion, identity=None):
    return {
        "main_images_json": json.dumps(main),
        "discussion_images_json": json.dumps(discussion),
        "source_identity_json": json.dumps(identity or {}),
    }


def test_single_cover_with_four_gallery_photos_is_approvable():
    row = _row(
        main=["cover.jpg"],
        discussion=["p1.jpg", "p2.jpg", "p3.jpg", "p4.jpg"],
        identity={"media_count": 4},
    )
    assert _effective_package_media_count(row) == 4


def test_partial_gallery_uses_frozen_source_identity_fallback():
    row = _row(
        main=["cover.jpg"],
        discussion=["p1.jpg", "p2.jpg", "p3.jpg"],
        identity={"media_count": 4},
    )
    assert _effective_package_media_count(row) == 4


def test_true_media_total_below_four_stays_blocked_count():
    row = _row(
        main=["cover.jpg"],
        discussion=["p1.jpg", "p2.jpg", "p3.jpg"],
        identity={"media_count": 3},
    )
    assert _effective_package_media_count(row) == 3


def test_legacy_multi_image_package_without_identity_keeps_old_counting():
    row = {
        "main_images_json": json.dumps(["p1.jpg", "p2.jpg", "p3.jpg", "p4.jpg"]),
        "discussion_images_json": json.dumps([]),
        "source_identity_json": None,
    }
    assert _effective_package_media_count(row) == 4


def test_duplicate_legacy_paths_are_not_double_counted():
    row = {
        "main_images_json": json.dumps(["p1.jpg", "p2.jpg"]),
        "discussion_images_json": json.dumps(["p2.jpg", "p3.jpg"]),
        "source_identity_json": None,
    }
    assert _effective_package_media_count(row) == 3
