"""Persistent operator-managed Telegram source overlay.

The production ``sources.json`` remains the base configuration and therefore
keeps all existing collector policy untouched.  Telegram administrators may add
or remove source identities through a small JSON overlay stored beside the
shared SQLite database.  Collector filtering, minimum-media rules, parser,
dedupe and publication gates are deliberately not configurable here.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
from typing import Any


_USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{3,}$")
_NUMERIC_RE = re.compile(r"^-?\d{5,}$")


def normalize_entity(value: object) -> str:
    text = str(value or "").strip()
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if text.lower().startswith(prefix):
            text = text[len(prefix):].split("?", 1)[0].split("/", 1)[0]
            break
    text = text.strip().lstrip("@")
    if _USERNAME_RE.fullmatch(text):
        return text
    if _NUMERIC_RE.fullmatch(text):
        return text
    raise ValueError("请发送频道 @username、t.me/username 或频道数字 ID")


def default_overlay_path(db_path: str | Path) -> Path:
    configured = str(os.getenv("V3_ADMIN_SOURCES_JSON") or "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(db_path).expanduser().resolve().parent / "collector_sources_admin.json"


class AdminSourceRegistry:
    def __init__(self, *, base_path: str | Path, db_path: str | Path, overlay_path: str | Path | None = None):
        self.base_path = Path(base_path).expanduser().resolve()
        self.overlay_path = Path(overlay_path).expanduser().resolve() if overlay_path else default_overlay_path(db_path)

    @staticmethod
    def _normalized_row(raw: dict[str, Any], index: int) -> dict[str, Any] | None:
        if raw.get("is_enabled") is False:
            return None
        entity = raw.get("entity_id") or raw.get("entity")
        if entity in (None, ""):
            return None
        try:
            entity_id = normalize_entity(entity)
        except ValueError:
            return None
        source_name = str(raw.get("source_name") or entity_id).strip().lstrip("@")
        if not source_name:
            return None
        row = dict(raw)
        row["source_name"] = source_name
        row["entity_id"] = entity_id
        row.setdefault("source_type", "telegram_channel")
        row.setdefault("source_db_id", row.get("source_id", index + 1))
        return row

    def _base_rows(self) -> list[dict[str, Any]]:
        if not self.base_path.is_file():
            return []
        decoded = json.loads(self.base_path.read_text(encoding="utf-8"))
        if not isinstance(decoded, list):
            raise ValueError("sources.json 必须是 JSON 数组")
        result: list[dict[str, Any]] = []
        for index, raw in enumerate(decoded):
            if isinstance(raw, dict):
                row = self._normalized_row(raw, index)
                if row:
                    result.append(row)
        return result

    def _overlay(self) -> dict[str, Any]:
        if not self.overlay_path.is_file():
            return {"added": [], "removed": []}
        try:
            decoded = json.loads(self.overlay_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            return {"added": [], "removed": []}
        if not isinstance(decoded, dict):
            return {"added": [], "removed": []}
        return {
            "added": [dict(x) for x in decoded.get("added", []) if isinstance(x, dict)],
            "removed": [str(x) for x in decoded.get("removed", []) if str(x).strip()],
        }

    def _write_overlay(self, value: dict[str, Any]) -> None:
        self.overlay_path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.overlay_path.with_suffix(self.overlay_path.suffix + ".tmp")
        temp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        temp.replace(self.overlay_path)

    def rows(self) -> list[dict[str, Any]]:
        overlay = self._overlay()
        removed = {str(x) for x in overlay["removed"]}
        merged: dict[str, dict[str, Any]] = {}
        for index, row in enumerate(self._base_rows()):
            if str(row["source_name"]) not in removed:
                merged[str(row["source_name"])] = row
        for index, raw in enumerate(overlay["added"], start=len(merged)):
            row = self._normalized_row(raw, index)
            if row and str(row["source_name"]) not in removed:
                merged[str(row["source_name"])] = row
        return list(merged.values())

    def add(self, entity: object, *, source_name: str = "") -> dict[str, Any]:
        entity_id = normalize_entity(entity)
        name = str(source_name or entity_id).strip().lstrip("@") or entity_id
        overlay = self._overlay()
        overlay["removed"] = [x for x in overlay["removed"] if x not in {name, entity_id}]
        added = [dict(x) for x in overlay["added"] if str(x.get("source_name") or "") != name]
        row = {
            "source_name": name,
            "entity_id": entity_id,
            "source_type": "telegram_channel",
            "is_enabled": True,
        }
        added.append(row)
        overlay["added"] = added
        self._write_overlay(overlay)
        return row

    def remove(self, source_name: object) -> None:
        name = str(source_name or "").strip()
        if not name:
            raise ValueError("source_name_required")
        overlay = self._overlay()
        overlay["added"] = [dict(x) for x in overlay["added"] if str(x.get("source_name") or "") != name]
        removed = list(dict.fromkeys([*overlay["removed"], name]))
        overlay["removed"] = removed
        self._write_overlay(overlay)


__all__ = ["AdminSourceRegistry", "default_overlay_path", "normalize_entity"]
