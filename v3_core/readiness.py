"""Read-only V3 readiness checks.

This module never initializes repositories, creates directories, changes SQLite
journal mode, renders media, contacts Telegram, or sends messages.  Storage
creation is available only through ``v3_core.storage.bootstrap``.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import importlib.util
import json
import os
from pathlib import Path
from typing import Mapping, Sequence

from v3_core.media.cover_styles import FINAL_COVER_STYLES, cover_template_path
from v3_core.storage.bootstrap import REQUIRED_V3_TABLES, existing_tables


@dataclass(frozen=True)
class ReadinessCheck:
    name: str
    ok: bool
    detail: str


@dataclass(frozen=True)
class ReadinessReport:
    component: str
    db_path: str
    checks: tuple[ReadinessCheck, ...]

    @property
    def ok(self) -> bool:
        return all(check.ok for check in self.checks)

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "component": self.component,
            "db_path": self.db_path,
            "checks": [asdict(check) for check in self.checks],
        }


COMPONENTS = frozenset({"all", "storage", "collector", "parser", "publisher"})
ENTRYPOINTS = {
    "collector": "run_v3_collector.py",
    "parser": "run_v3_canonical_worker.py",
    "publisher": "run_v3_publisher_bot.py",
}


def _truthy(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _check(name: str, ok: bool, detail: str) -> ReadinessCheck:
    return ReadinessCheck(name=name, ok=bool(ok), detail=str(detail))


def _resolve(repo_root: Path, value: str | Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (repo_root / path).resolve()


def _required_env(env: Mapping[str, str], names: Sequence[str]) -> list[ReadinessCheck]:
    return [
        _check(
            f"env:{name}",
            bool(str(env.get(name, "") or "").strip()),
            "set" if str(env.get(name, "") or "").strip() else "missing",
        )
        for name in names
    ]


def _source_config_check(path: Path) -> ReadinessCheck:
    if not path.is_file():
        return _check("collector:sources", False, f"missing:{path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return _check("collector:sources", False, f"invalid_json:{type(exc).__name__}")
    if not isinstance(value, list):
        return _check("collector:sources", False, "root_must_be_array")
    enabled = []
    for row in value:
        if not isinstance(row, dict) or row.get("is_enabled") is False:
            continue
        source_name = str(row.get("source_name") or "").strip()
        entity = str(row.get("entity_id") or row.get("entity") or "").strip()
        if source_name and entity:
            enabled.append(row)
    return _check(
        "collector:sources",
        bool(enabled),
        f"enabled_sources:{len(enabled)}" if enabled else "no_valid_enabled_source",
    )


def _output_dir_check(path: Path) -> ReadinessCheck:
    if path.exists():
        ok = path.is_dir() and os.access(path, os.W_OK)
        return _check("publisher:cover_output", ok, f"existing:{path}")
    parent = path.parent
    ok = parent.is_dir() and os.access(parent, os.W_OK)
    return _check(
        "publisher:cover_output",
        ok,
        f"creatable_under:{parent}" if ok else f"parent_not_writable:{parent}",
    )


def _dependency_checks(component: str) -> list[ReadinessCheck]:
    modules: set[str] = {"dotenv"}
    if component in {"all", "collector"}:
        modules.add("telethon")
    if component in {"all", "publisher"}:
        modules.update({"telegram", "PIL", "playwright"})
    return [
        _check(
            f"dependency:{module}",
            importlib.util.find_spec(module) is not None,
            "importable" if importlib.util.find_spec(module) is not None else "missing",
        )
        for module in sorted(modules)
    ]


def run_preflight(
    *,
    repo_root: str | Path,
    env: Mapping[str, str] | None = None,
    db_path: str | Path | None = None,
    component: str = "all",
) -> ReadinessReport:
    """Run readiness checks without mutating the repository, DB, or filesystem."""
    clean_component = str(component or "all").strip().lower()
    if clean_component not in COMPONENTS:
        raise ValueError(f"unsupported_component:{clean_component}")
    root = Path(repo_root).expanduser().resolve()
    values = dict(os.environ if env is None else env)
    resolved_db = _resolve(
        root,
        db_path or values.get("DB_PATH") or values.get("SQLITE_PATH") or "data/qiaolian_dual_bot.db",
    )
    checks: list[ReadinessCheck] = []

    db_exists = resolved_db.is_file()
    checks.append(_check("storage:database", db_exists, str(resolved_db)))
    tables = existing_tables(resolved_db) if db_exists else frozenset()
    missing_tables = sorted(REQUIRED_V3_TABLES - tables)
    checks.append(
        _check(
            "storage:v3_tables",
            not missing_tables,
            "complete" if not missing_tables else "missing:" + ",".join(missing_tables),
        )
    )

    active_components = (
        {"collector", "parser", "publisher"}
        if clean_component == "all"
        else ({clean_component} if clean_component != "storage" else set())
    )
    for name in sorted(active_components):
        entry = root / ENTRYPOINTS[name]
        checks.append(_check(f"entrypoint:{name}", entry.is_file(), str(entry)))

    checks.extend(_dependency_checks(clean_component))

    if "collector" in active_components:
        checks.extend(_required_env(values, ("TG_API_ID", "TG_API_HASH")))
        api_id = str(values.get("TG_API_ID", "") or "").strip()
        checks.append(
            _check(
                "collector:tg_api_id_numeric",
                api_id.isdigit() and int(api_id) > 0 if api_id else False,
                "valid" if api_id.isdigit() and int(api_id) > 0 else "invalid",
            )
        )
        sources_path = _resolve(
            root,
            values.get("COLLECTOR_SOURCES_JSON") or "sources.json",
        )
        checks.append(_source_config_check(sources_path))

    if "publisher" in active_components:
        checks.extend(_required_env(values, ("PUBLISHER_BOT_TOKEN", "ADMIN_IDS", "CHANNEL_ID")))
        bot_username = str(
            values.get("DEEPLINK_BOT_USERNAME")
            or values.get("USER_BOT_USERNAME")
            or ""
        ).strip().lstrip("@")
        checks.append(
            _check(
                "env:DEEPLINK_BOT_USERNAME/USER_BOT_USERNAME",
                bool(bot_username),
                "set" if bot_username else "missing",
            )
        )
        admin_raw = str(values.get("ADMIN_IDS", "") or "")
        admin_parts = [part.strip() for part in admin_raw.split(",") if part.strip()]
        admins_ok = bool(admin_parts) and all(part.lstrip("-").isdigit() for part in admin_parts)
        checks.append(_check("publisher:admin_ids", admins_ok, "valid" if admins_ok else "invalid"))

        styles = (*FINAL_COVER_STYLES, "video_vertical")
        for style in styles:
            template = cover_template_path(style, allow_video=True).resolve()
            checks.append(_check(f"template:{style}", template.is_file(), str(template)))

        cover_output = _resolve(
            root,
            values.get("V3_COVER_OUTPUT_DIR") or "media/covers_v3",
        )
        checks.append(_output_dir_check(cover_output))

        discussion_enabled = _truthy(values.get("CHANNEL_DISCUSSION_ENABLED")) or _truthy(
            values.get("PUBLISH_DISCUSSION_ENABLED")
        )
        checks.append(
            _check(
                "publisher:discussion_default_off",
                not discussion_enabled,
                "disabled" if not discussion_enabled else "enabled_not_allowed_for_v3_cutover",
            )
        )

    return ReadinessReport(
        component=clean_component,
        db_path=str(resolved_db),
        checks=tuple(checks),
    )


__all__ = [
    "COMPONENTS",
    "ENTRYPOINTS",
    "ReadinessCheck",
    "ReadinessReport",
    "run_preflight",
]
