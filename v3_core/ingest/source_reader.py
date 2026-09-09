"""Read-only access to immutable source evidence and source media."""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from typing import Any


class SourceReader:
    def __init__(self, db_path: str):
        self.db_path = str(Path(db_path).expanduser().resolve())

    def _connect(self) -> sqlite3.Connection:
        path = Path(self.db_path)
        if not path.is_file():
            raise FileNotFoundError(path)
        uri = f"file:{path.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def source_post(self, source_post_id: int | str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM source_posts WHERE id=?",
                (int(source_post_id),),
            ).fetchone()
        if row is None:
            raise KeyError(source_post_id)
        data = {key: row[key] for key in row.keys()}
        for key, fallback in (("raw_images_json", []), ("raw_videos_json", []), ("raw_meta_json", {})):
            try:
                data[key.removesuffix("_json")] = json.loads(str(data.get(key) or ""))
            except (TypeError, ValueError, json.JSONDecodeError):
                data[key.removesuffix("_json")] = fallback
        return data

    def source_image_paths(self, source_post_id: int | str) -> list[str]:
        """Return source-order image paths without mutating or re-encoding them."""
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT local_path FROM media_assets
                   WHERE owner_type='source_post'
                     AND CAST(owner_ref_id AS TEXT)=CAST(? AS TEXT)
                     AND asset_type='photo'
                     AND status='active'
                   ORDER BY sort_order,id""",
                (str(source_post_id),),
            ).fetchall()
        paths: list[str] = []
        for row in rows:
            path = str(row["local_path"] or "").strip()
            if path and Path(path).is_file() and path not in paths:
                paths.append(path)
        if paths:
            return paths

        source = self.source_post(source_post_id)
        for item in source.get("raw_images") or []:
            if isinstance(item, str):
                path = item
            elif isinstance(item, dict):
                path = str(item.get("local_path") or item.get("path") or "")
            else:
                path = ""
            path = str(path or "").strip()
            if path and Path(path).is_file() and path not in paths:
                paths.append(path)
        return paths

    def source_identity(self, source_post_id: int | str) -> dict[str, Any]:
        source = self.source_post(source_post_id)
        return {
            "source_post_db_id": int(source["id"]),
            "source_type": str(source.get("source_type") or ""),
            "source_name": str(source.get("source_name") or ""),
            "source_post_id": str(source.get("source_post_id") or ""),
            "source_url": str(source.get("source_url") or ""),
            "dedupe_hash": str(source.get("dedupe_hash") or ""),
        }


__all__ = ["SourceReader"]
