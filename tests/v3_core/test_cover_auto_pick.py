from __future__ import annotations

from pathlib import Path

from v3_core.media import media_selection
from v3_core.media.ranker import (
    _cover_room_bonus,
    _cover_text_penalty,
    _has_publishable_resolution,
    _room_cover_tier,
    rank_photo_paths,
)


def test_resolution_gate_accepts_phone_portrait_but_rejects_thumbnails():
    assert _has_publishable_resolution(540, 1064)
    assert _has_publishable_resolution(960, 720)
    assert not _has_publishable_resolution(320, 1200)
    assert not _has_publishable_resolution(480, 480)


def test_room_cover_tier_orders_living_above_toilet():
    assert _room_cover_tier("living") > _room_cover_tier("exterior")
    assert _room_cover_tier("exterior") > _room_cover_tier("kitchen")
    assert _room_cover_tier("kitchen") > _room_cover_tier("bedroom")
    assert _room_cover_tier("bedroom") > _room_cover_tier("")
    assert _room_cover_tier("") > _room_cover_tier("toilet")


def test_room_cover_tier_villa_prefers_exterior():
    assert _room_cover_tier("exterior", preference="exterior") > _room_cover_tier(
        "living", preference="exterior"
    )
    assert _room_cover_tier("living", preference="exterior") > _room_cover_tier(
        "kitchen", preference="exterior"
    )


def test_cover_room_bonus_rewards_living_and_penalizes_toilet():
    living = _cover_room_bonus({"label": "living", "living": 0.8})
    toilet = _cover_room_bonus({"label": "toilet", "toilet": 0.8})
    assert living > 15
    assert toilet < -30


def test_cover_room_bonus_villa_prefers_exterior():
    exterior = _cover_room_bonus({"label": "exterior", "exterior": 0.8}, preference="exterior")
    living = _cover_room_bonus({"label": "living", "living": 0.8}, preference="exterior")
    assert exterior > living


def test_cover_text_penalty_flags_bottom_contact_band():
    penalty, soft = _cover_text_penalty({"bottom_text": 0.6, "text_heavy": 0.6})
    assert soft is True
    assert penalty >= 20


def test_rank_sort_prefers_living_over_sharper_toilet(monkeypatch, tmp_path: Path):
    living = tmp_path / "living.jpg"
    toilet = tmp_path / "toilet.jpg"
    living.write_bytes(b"LIV")
    toilet.write_bytes(b"TOI")

    def fake_score(path: Path, source_order: int, *, cover_preference: object = "living"):
        name = path.name
        if name.startswith("living"):
            return {
                "file": str(path),
                "path": str(path),
                "filename": name,
                "source_order": source_order,
                "score": 60.0,
                "reject": False,
                "soft_reject": False,
                "room_label": "living",
                "room_tier": 5,
                "orientation": 90.0,
                "sharpness": 40.0,
                "exposure": 70.0,
                "resolution": 70.0,
                "ratio": 1.33,
            }
        return {
            "file": str(path),
            "path": str(path),
            "filename": name,
            "source_order": source_order,
            "score": 95.0,
            "reject": False,
            "soft_reject": True,
            "room_label": "toilet",
            "room_tier": -2,
            "orientation": 100.0,
            "sharpness": 99.0,
            "exposure": 90.0,
            "resolution": 90.0,
            "ratio": 1.33,
        }

    monkeypatch.setattr("v3_core.media.ranker._score_one", fake_score)
    ranked = rank_photo_paths([toilet, living])
    assert Path(ranked[0]["file"]).name == "living.jpg"


def test_auto_cover_skips_toilet_and_soft_reject(tmp_path, monkeypatch):
    toilet = tmp_path / "toilet.jpg"
    texty = tmp_path / "texty.jpg"
    living = tmp_path / "living.jpg"
    for path, payload in ((toilet, b"T"), (texty, b"X"), (living, b"L")):
        path.write_bytes(payload)

    monkeypatch.setattr(media_selection, "_dhash", lambda path: None)
    monkeypatch.setattr(
        media_selection,
        "rank_photo_paths",
        lambda paths, cover_preference="living": [
            {
                "file": str(toilet.resolve()),
                "reject": False,
                "soft_reject": False,
                "room_label": "toilet",
                "score": 99,
            },
            {
                "file": str(texty.resolve()),
                "reject": False,
                "soft_reject": True,
                "room_label": "bedroom",
                "score": 90,
            },
            {
                "file": str(living.resolve()),
                "reject": False,
                "soft_reject": False,
                "room_label": "living",
                "score": 70,
            },
        ],
    )
    result = media_selection.select_publication_media([toilet, texty, living])
    # 主图/封面 = living；相册按打分序，厕所仍可留在相册但不做封面。
    assert result["cover_path"] == str(living.resolve())
    assert result["cover_preference"] == "living"
    assert result["gallery_paths"] == [
        str(toilet.resolve()),
        str(texty.resolve()),
        str(living.resolve()),
    ]


def test_apartment_cover_requires_clean_landscape_living_panorama(tmp_path, monkeypatch):
    detail = tmp_path / "tv-wall-detail.jpg"
    panorama = tmp_path / "living-panorama.jpg"
    for path, payload in ((detail, b"D"), (panorama, b"P")):
        path.write_bytes(payload)
    monkeypatch.setattr(media_selection, "_dhash", lambda path: None)
    monkeypatch.setattr(
        media_selection,
        "rank_photo_paths",
        lambda paths, cover_preference="living": [
            {
                "file": str(detail.resolve()), "reject": False, "soft_reject": False,
                "room_label": "living", "ratio": 0.9,
                "room": {"living": 0.9, "bed": 0.0, "toilet": 0.0},
                "text": {"text_heavy": 0.0},
            },
            {
                "file": str(panorama.resolve()), "reject": False, "soft_reject": False,
                "room_label": "living", "ratio": 1.45,
                "room": {"living": 0.8, "bed": 0.1, "toilet": 0.0},
                "text": {"text_heavy": 0.1},
            },
        ],
    )
    result = media_selection.select_publication_media([detail, panorama])
    assert result["cover_path"] == str(panorama.resolve())
    assert result["policy"] == "apartment_panorama_hard_gate_manual_review_fallback"


def test_apartment_auto_publish_stops_when_no_panorama_exists(tmp_path, monkeypatch):
    import pytest

    bedroom = tmp_path / "bedroom.jpg"
    bedroom.write_bytes(b"B")
    monkeypatch.setattr(media_selection, "_dhash", lambda path: None)
    monkeypatch.setattr(
        media_selection,
        "rank_photo_paths",
        lambda paths, cover_preference="living": [{
            "file": str(bedroom.resolve()), "reject": False, "soft_reject": False,
            "room_label": "bedroom", "ratio": 1.4,
            "room": {"living": 0.1, "bed": 0.8, "toilet": 0.0},
            "text": {"text_heavy": 0.0},
        }],
    )
    with pytest.raises(ValueError, match="apartment_cover_requires_panorama_review"):
        media_selection.select_publication_media([bedroom])


def test_auto_cover_villa_prefers_exterior(tmp_path, monkeypatch):
    living = tmp_path / "living.jpg"
    exterior = tmp_path / "exterior.jpg"
    bedroom = tmp_path / "bedroom.jpg"
    for path, payload in ((living, b"L"), (exterior, b"E"), (bedroom, b"B")):
        path.write_bytes(payload)

    monkeypatch.setattr(media_selection, "_dhash", lambda path: None)
    monkeypatch.setattr(
        media_selection,
        "rank_photo_paths",
        lambda paths, cover_preference="living": [
            {
                "file": str(living.resolve()),
                "reject": False,
                "soft_reject": False,
                "room_label": "living",
                "score": 95,
            },
            {
                "file": str(exterior.resolve()),
                "reject": False,
                "soft_reject": False,
                "room_label": "exterior",
                "score": 80,
            },
            {
                "file": str(bedroom.resolve()),
                "reject": False,
                "soft_reject": False,
                "room_label": "bedroom",
                "score": 70,
            },
        ],
    )
    result = media_selection.select_publication_media(
        [living, exterior, bedroom],
        cover_preference="exterior",
    )
    assert result["cover_path"] == str(exterior.resolve())
    assert result["cover_preference"] == "exterior"


def test_auto_cover_villa_rejects_soft_exterior_instead_of_falling_back(tmp_path, monkeypatch):
    import pytest

    living = tmp_path / "living.jpg"
    exterior = tmp_path / "exterior.jpg"
    for path, payload in ((living, b"L"), (exterior, b"E")):
        path.write_bytes(payload)

    monkeypatch.setattr(media_selection, "_dhash", lambda path: None)
    monkeypatch.setattr(
        media_selection,
        "rank_photo_paths",
        lambda paths, cover_preference="living": [
            {
                "file": str(exterior.resolve()),
                "reject": False,
                "soft_reject": True,
                "room_label": "exterior",
                "score": 64,
            },
            {
                "file": str(living.resolve()),
                "reject": False,
                "soft_reject": False,
                "room_label": "living",
                "score": 90,
            },
        ],
    )
    with pytest.raises(ValueError, match="villa_cover_requires_exterior_review"):
        media_selection.select_publication_media(
            [living, exterior],
            cover_preference="exterior",
        )


def test_villa_auto_publish_stops_when_no_clean_exterior_exists(tmp_path, monkeypatch):
    import pytest

    living = tmp_path / "living.jpg"
    kitchen = tmp_path / "kitchen.jpg"
    for path, payload in ((living, b"L"), (kitchen, b"K")):
        path.write_bytes(payload)
    monkeypatch.setattr(media_selection, "_dhash", lambda path: None)
    monkeypatch.setattr(
        media_selection,
        "rank_photo_paths",
        lambda paths, cover_preference="living": [
            {
                "file": str(living.resolve()), "reject": False, "soft_reject": False,
                "room_label": "living", "score": 95,
            },
            {
                "file": str(kitchen.resolve()), "reject": False, "soft_reject": False,
                "room_label": "kitchen", "score": 90,
            },
        ],
    )
    with pytest.raises(ValueError, match="villa_cover_requires_exterior_review"):
        media_selection.select_publication_media(
            [living, kitchen],
            cover_preference="exterior",
        )
