from pathlib import Path

import media_selection


def test_media_selection_preserves_gallery_order_filters_rejects_and_honours_manual(tmp_path, monkeypatch):
    a = tmp_path / "a.jpg"; a.write_bytes(b"A")
    dup = tmp_path / "dup.jpg"; dup.write_bytes(b"A")
    b = tmp_path / "b.jpg"; b.write_bytes(b"B")
    bad = tmp_path / "bad.jpg"; bad.write_bytes(b"BAD")

    monkeypatch.setattr(media_selection, "_dhash", lambda path: None)
    monkeypatch.setattr(media_selection, "rank_photo_paths", lambda paths: [
        {"file": str(b.resolve()), "reject": False, "score": 99},
        {"file": str(a.resolve()), "reject": False, "score": 80},
        {"file": str(bad.resolve()), "reject": True, "score": 1},
    ])

    result = media_selection.select_publication_media([a, dup, b, bad], manual_cover_path=a)
    assert result["gallery_paths"] == [str(a.resolve()), str(b.resolve())]
    assert result["cover_path"] == str(a.resolve())
    assert result["duplicates"][0]["kind"] == "exact"
    assert str(bad.resolve()) in result["rejected_paths"]


def test_media_selection_uses_ranked_best_when_no_manual_cover(tmp_path, monkeypatch):
    a = tmp_path / "a.jpg"; a.write_bytes(b"A")
    b = tmp_path / "b.jpg"; b.write_bytes(b"B")
    monkeypatch.setattr(media_selection, "_dhash", lambda path: None)
    monkeypatch.setattr(media_selection, "rank_photo_paths", lambda paths: [
        {"file": str(b.resolve()), "reject": False, "score": 99},
        {"file": str(a.resolve()), "reject": False, "score": 80},
    ])
    result = media_selection.select_publication_media([a, b])
    assert result["gallery_paths"] == [str(a.resolve()), str(b.resolve())]
    assert result["cover_path"] == str(b.resolve())
