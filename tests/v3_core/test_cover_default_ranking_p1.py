from __future__ import annotations

from pathlib import Path

import cv2
import numpy as np
from PIL import Image

import v3_core.media.media_selection as media_selection
from v3_core.media.ranker import _cover_scene_metrics


def _files(tmp_path: Path, names: list[str]) -> list[str]:
    paths = []
    for index, name in enumerate(names):
        path = tmp_path / f"{name}.jpg"
        Image.new("RGB", (960, 720), (80 + index * 15, 120, 150)).save(path)
        paths.append(str(path.resolve()))
    return paths


def _ranker_for(priorities: dict[str, tuple[int, float]]):
    def fake_rank(paths):
        rows = []
        for index, raw in enumerate(paths, start=1):
            path = Path(raw).resolve()
            priority, quality = priorities[path.stem]
            rows.append({
                "file": str(path),
                "path": str(path),
                "filename": path.name,
                "source_order": index,
                "reject": False,
                "cover_scene_priority": priority,
                "space": quality,
                "score": quality,
                "orientation": 100.0,
                "sharpness": quality,
                "exposure": quality,
                "resolution": 100.0,
                "ratio": 1.333,
            })
        rows.sort(
            key=lambda item: (
                item["cover_scene_priority"],
                item["space"],
                item["score"],
                -item["source_order"],
            ),
            reverse=True,
        )
        return rows
    return fake_rank


def _select(tmp_path: Path, monkeypatch, names, priorities, manual=None):
    paths = _files(tmp_path, names)
    monkeypatch.setattr(media_selection, "rank_photo_paths", _ranker_for(priorities))
    monkeypatch.setattr(media_selection, "_hamming", lambda a, b: 999)
    manual_path = paths[names.index(manual)] if manual else None
    selected = media_selection.select_publication_media(paths, manual_cover_path=manual_path)
    return Path(selected["cover_path"]).stem


def test_default_cover_prefers_living_room_over_skyline_bedroom_and_bathroom(tmp_path, monkeypatch):
    chosen = _select(
        tmp_path,
        monkeypatch,
        ["skyline", "living", "bedroom", "bathroom"],
        {"skyline": (0, 95), "living": (4, 80), "bedroom": (3, 85), "bathroom": (2, 90)},
    )
    assert chosen == "living"


def test_default_cover_prefers_bedroom_over_bathroom_and_balcony(tmp_path, monkeypatch):
    chosen = _select(
        tmp_path,
        monkeypatch,
        ["bathroom", "bedroom", "balcony"],
        {"bathroom": (2, 95), "bedroom": (3, 75), "balcony": (1, 99)},
    )
    assert chosen == "bedroom"


def test_default_cover_prefers_building_exterior_when_only_outdoor_options_exist(tmp_path, monkeypatch):
    chosen = _select(
        tmp_path,
        monkeypatch,
        ["balcony", "city", "building"],
        {"balcony": (1, 92), "city": (0, 99), "building": (2, 70)},
    )
    assert chosen == "building"


def test_default_cover_allows_best_quality_view_when_only_views_exist(tmp_path, monkeypatch):
    chosen = _select(
        tmp_path,
        monkeypatch,
        ["view_a", "view_b", "view_c"],
        {"view_a": (0, 60), "view_b": (0, 90), "view_c": (0, 75)},
    )
    assert chosen == "view_b"


def test_manual_view_selection_always_beats_automatic_interior_ranking(tmp_path, monkeypatch):
    chosen = _select(
        tmp_path,
        monkeypatch,
        ["living", "skyline"],
        {"living": (4, 90), "skyline": (0, 60)},
        manual="skyline",
    )
    assert chosen == "skyline"


def test_local_scene_heuristic_demotes_balcony_skyline_below_room():
    room = np.full((720, 960, 3), (190, 190, 190), dtype=np.uint8)
    cv2.rectangle(room, (80, 350), (880, 700), (175, 175, 175), -1)
    cv2.rectangle(room, (120, 220), (430, 470), (100, 120, 145), -1)
    cv2.line(room, (100, 600), (860, 600), (60, 60, 60), 8)

    view = np.full((720, 960, 3), (45, 75, 120), dtype=np.uint8)
    view[:260, :, :] = (235, 185, 120)
    for x in range(0, 960, 55):
        cv2.rectangle(view, (x, 230), (x + 35, 520), (120, 120, 120), -1)
    for x in range(40, 940, 85):
        cv2.line(view, (x, 300), (x, 690), (20, 20, 20), 7)
    cv2.line(view, (0, 430), (960, 430), (20, 20, 20), 10)

    room_metrics = _cover_scene_metrics(room)
    view_metrics = _cover_scene_metrics(view)
    assert room_metrics["cover_scene_priority"] > view_metrics["cover_scene_priority"]
