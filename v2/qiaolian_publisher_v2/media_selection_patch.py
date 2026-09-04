from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from media_selection import select_publication_media

_INSTALLED = False
_MANUAL_COVER_PATH = ""
_LAST_SELECTION: dict[str, Any] = {}


def _source_paths(conn: sqlite3.Connection, source_post_id: Any) -> list[str]:
    global _MANUAL_COVER_PATH
    _MANUAL_COVER_PATH = ""
    rows = conn.execute(
        """SELECT local_path,is_cover FROM media_assets
           WHERE owner_type='source_post'
             AND (CAST(owner_ref_id AS TEXT)=CAST(? AS TEXT) OR owner_ref_key=CAST(? AS TEXT))
             AND asset_type='photo' AND status='active'
           ORDER BY COALESCE(sort_order,999999), id""",
        (source_post_id, source_post_id),
    ).fetchall()
    paths: list[str] = []
    for row in rows:
        path = str(row[0] or "")
        if path and Path(path).is_file() and path not in paths:
            paths.append(path)
            if int(row[1] or 0) and not _MANUAL_COVER_PATH:
                _MANUAL_COVER_PATH = path
    if paths:
        return paths

    row = conn.execute("SELECT raw_images_json FROM source_posts WHERE id=?", (source_post_id,)).fetchone()
    if not row or not row[0]:
        return []
    try:
        raw_images = json.loads(row[0])
    except Exception:
        return []
    for item in raw_images if isinstance(raw_images, list) else []:
        path = item if isinstance(item, str) else (item.get("local_path") or item.get("path") if isinstance(item, dict) else "")
        if path and Path(str(path)).is_file() and str(path) not in paths:
            paths.append(str(path))
    return paths


def install_media_selection_patch() -> None:
    """Install the unified media contract after the existing cover-picker hook."""
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True

    import publication_package as package

    def patched_paths(conn: sqlite3.Connection, source_post_id: Any) -> list[str]:
        global _LAST_SELECTION
        raw_paths = _source_paths(conn, source_post_id)
        if not raw_paths:
            _LAST_SELECTION = {}
            return []
        _LAST_SELECTION = select_publication_media(raw_paths, manual_cover_path=_MANUAL_COVER_PATH)
        return list(_LAST_SELECTION["gallery_paths"])

    def patched_select_cover_source(paths: list[str], *, property_type: str = "") -> str:
        _ = property_type
        if _LAST_SELECTION and list(_LAST_SELECTION.get("gallery_paths") or []) == list(paths):
            cover = str(_LAST_SELECTION.get("cover_path") or "")
            if cover and cover in paths:
                return cover
        return str(select_publication_media(paths, manual_cover_path=_MANUAL_COVER_PATH)["cover_path"])

    package._paths = patched_paths
    package._select_cover_source = patched_select_cover_source


__all__ = ["install_media_selection_patch"]
