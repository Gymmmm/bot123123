from __future__ import annotations

from pathlib import Path

from v3_core.media import media_selection
from v3_core.media.ranker import (
    _cover_room_bonus,
    _cover_text_penalty,
    _room_cover_tier,
    rank_photo_paths,
)


def test_room_cover_tier_orders_living_above_toilet():
    assert _room_cover_tier("living") > _room_cover_tier("exterior")
    assert _room_cover_tier("exterior") > _room_cover_tier("kitchen")
    assert _room_cover_tier("kitchen") > _room_cover_tier("bedroom")
    assert _room_cover_tier("bedroom") > _room_cover_tier("")
    assert _room_cover_tier("") > _room_cover_tier("toilet")


def test_cover_room_bonus_rewards_living_and_penalizes_toilet():
    living = _cover_room_bonus({"label": "living", "living": 0.8})
    toilet = _cover_room_bonus({"label": "toilet", "toilet": 0.8})
    assert living > 15
    assert toilet < -30


def test_cover_text_penalty_flags_bottom_contact_band():
    penalty, soft = _cover_text_penalty({"bottom_text": 0.6, "text_heavy": 0.6})
    assert soft is True
    assert penalty >= 20


def test_rank_sort_prefers_living_over_sharper_toilet(monkeypatch, tmp_path: Path):
    living = tmp_path / "living.jpg"
    toilet = tmp_path / "toilet.jpg"
    living.write_bytes(b"LIV")
    toilet.write_bytes(b"TOI")

    def fake_score(path: Path, source_order: int):
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
        lambda paths: [
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
    assert result["cover_path"] == str(living.resolve())
    assert result["gallery_paths"][0] == str(living.resolve())
    assert str(toilet.resolve()) in result["gallery_paths"]
    assert result["gallery_paths"].index(str(living.resolve())) < result["gallery_paths"].index(
        str(toilet.resolve())
    )
