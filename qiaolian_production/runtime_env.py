from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

PACKAGE_ROOT = Path(__file__).resolve().parent


def _absolute_from_app_root(value: str | os.PathLike[str], app_root: Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (app_root / path).resolve()


def configure_environment() -> Path:
    """Preserve production paths while code lives under qiaolian_production/.

    The extraction branch intentionally keeps runtime state outside the copied
    source tree. Existing .env, SQLite data, Telethon sessions and media remain
    rooted at the original application directory unless explicitly overridden.
    """
    explicit_root = str(os.getenv("QIAOLIAN_RUNTIME_ROOT", "")).strip()
    app_root = _absolute_from_app_root(explicit_root, PACKAGE_ROOT.parent) if explicit_root else PACKAGE_ROOT.parent.resolve()
    os.environ.setdefault("QIAOLIAN_RUNTIME_ROOT", str(app_root))

    # Load the same production .env before any copied module imports its own
    # path-relative fallback. Never override variables already supplied by
    # systemd or the process environment.
    load_dotenv(app_root / ".env", override=False)

    data_dir = _absolute_from_app_root(os.getenv("DATA_DIR", str(app_root / "data")), app_root)
    db_path = _absolute_from_app_root(
        os.getenv("DB_PATH", str(data_dir / "qiaolian_dual_bot.db")), app_root
    )

    os.environ.setdefault("DATA_DIR", str(data_dir))
    os.environ.setdefault("DB_PATH", str(db_path))
    os.environ.setdefault("SQLITE_PATH", str(db_path))
    os.environ.setdefault("COLLECTOR_SOURCES_JSON", str(app_root / "sources.json"))
    os.environ.setdefault("COLLECTOR_DOWNLOAD_DIR", str(app_root / "media" / "collector_downloads"))
    os.environ.setdefault("QIAOLIAN_RENDER_TMP", str(app_root / "media" / "renders" / "runtime"))
    os.environ.setdefault(
        "CORNER_LOGO_PATH",
        str(app_root / "assets" / "brand" / "qiaolian_corner_mark_120x40.png"),
    )

    # collector_bot historically resolves COLLECTOR_SESSION_NAME relative to
    # the repository root. Once copied into collector/, that implicit base
    # would move. Pin an equivalent absolute path unless the operator already
    # supplied TELETHON_SESSION_PATH.
    if not str(os.getenv("TELETHON_SESSION_PATH", "")).strip():
        session_name = str(os.getenv("COLLECTOR_SESSION_NAME", "")).strip() or "qiaolian_collector"
        os.environ["TELETHON_SESSION_PATH"] = str(app_root / "telethon_sessions" / session_name)

    return app_root


def patch_legacy_path_globals(app_root: Path) -> None:
    """Repair path globals in copied modules without changing business logic.

    These modules historically used __file__ because they lived at repository
    root. The copied versions now live one or two directories deeper, so only
    their filesystem roots are rebound to the original application root.
    """
    try:
        import cover_generator

        cover_generator.BASE_DIR = app_root
        cover_generator.COVER_DIR = app_root / "media" / "covers"
        cover_generator.DB_PATH_DEFAULT = os.environ["DB_PATH"]
    except ModuleNotFoundError:
        pass

    try:
        import publication_package

        publication_package.ROOT = app_root
        publication_package.PACKAGE_ROOT = app_root / "media" / "publication_packages"
    except ModuleNotFoundError:
        pass
