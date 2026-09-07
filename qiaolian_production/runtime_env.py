from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

PACKAGE_ROOT = Path(__file__).resolve().parent


def _absolute_from_app_root(value: str | os.PathLike[str], app_root: Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (app_root / path).resolve()


def configure_environment() -> Path:
    """Preserve production state paths while source lives under qiaolian_production/."""
    explicit_root = str(os.getenv("QIAOLIAN_RUNTIME_ROOT", "")).strip()
    app_root = (
        _absolute_from_app_root(explicit_root, PACKAGE_ROOT.parent)
        if explicit_root
        else PACKAGE_ROOT.parent.resolve()
    )
    os.environ["QIAOLIAN_RUNTIME_ROOT"] = str(app_root)

    # Production keeps one .env at the application root. Process/systemd
    # variables retain precedence; relative filesystem values are normalized
    # against that same root before copied modules import them.
    load_dotenv(app_root / ".env", override=False)

    data_dir = _absolute_from_app_root(
        os.getenv("DATA_DIR", str(app_root / "data")), app_root
    )
    db_path = _absolute_from_app_root(
        os.getenv("DB_PATH", str(data_dir / "qiaolian_dual_bot.db")), app_root
    )
    sqlite_path = _absolute_from_app_root(
        os.getenv("SQLITE_PATH", str(db_path)), app_root
    )
    sources_path = _absolute_from_app_root(
        os.getenv("COLLECTOR_SOURCES_JSON", str(app_root / "sources.json")), app_root
    )
    download_dir = _absolute_from_app_root(
        os.getenv("COLLECTOR_DOWNLOAD_DIR", str(app_root / "media" / "collector_downloads")),
        app_root,
    )
    media_root = _absolute_from_app_root(
        os.getenv("MEDIA_ROOT", str(app_root / "media")), app_root
    )
    render_tmp = _absolute_from_app_root(
        os.getenv("QIAOLIAN_RENDER_TMP", str(media_root / "renders" / "runtime")),
        app_root,
    )
    discussion_map = _absolute_from_app_root(
        os.getenv("DISCUSSION_MAP_FILE", str(data_dir / "discussion_map.json")), app_root
    )
    discussion_bridge = _absolute_from_app_root(
        os.getenv("DISCUSSION_BRIDGE_FILE", str(data_dir / "discussion_bridge.json")), app_root
    )
    playwright_browsers = _absolute_from_app_root(
        os.getenv("PLAYWRIGHT_BROWSERS_PATH", str(app_root / ".playwright-browsers")), app_root
    )
    corner_logo = _absolute_from_app_root(
        os.getenv(
            "CORNER_LOGO_PATH",
            str(app_root / "assets" / "brand" / "qiaolian_corner_mark_120x40.png"),
        ),
        app_root,
    )
    gallery_logo = _absolute_from_app_root(
        os.getenv("QIAOLIAN_GALLERY_LOGO", str(corner_logo)), app_root
    )

    os.environ["DATA_DIR"] = str(data_dir)
    os.environ["DB_PATH"] = str(db_path)
    os.environ["SQLITE_PATH"] = str(sqlite_path)
    os.environ["COLLECTOR_SOURCES_JSON"] = str(sources_path)
    os.environ["COLLECTOR_DOWNLOAD_DIR"] = str(download_dir)
    os.environ["MEDIA_ROOT"] = str(media_root)
    os.environ["QIAOLIAN_RENDER_TMP"] = str(render_tmp)
    os.environ["DISCUSSION_MAP_FILE"] = str(discussion_map)
    os.environ["DISCUSSION_BRIDGE_FILE"] = str(discussion_bridge)
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(playwright_browsers)
    os.environ["CORNER_LOGO_PATH"] = str(corner_logo)
    os.environ["QIAOLIAN_GALLERY_LOGO"] = str(gallery_logo)

    explicit_session = str(os.getenv("TELETHON_SESSION_PATH", "")).strip()
    if explicit_session:
        session_path = _absolute_from_app_root(explicit_session, app_root)
    else:
        session_name = str(os.getenv("COLLECTOR_SESSION_NAME", "")).strip() or "qiaolian_collector"
        session_path = app_root / "telethon_sessions" / session_name
    os.environ["TELETHON_SESSION_PATH"] = str(session_path)

    return app_root


def patch_legacy_path_globals(app_root: Path, *, publisher_runtime: bool = False) -> None:
    """Rebind only filesystem roots that historically depended on __file__."""
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

    if publisher_runtime:
        # autopilot_publish_bot is a compatibility helper used by the active
        # publisher patches. It is not a separate polling service, but its
        # source/scratch paths must remain rooted at the production app.
        import autopilot_publish_bot

        autopilot_publish_bot.BASE_DIR = app_root
        autopilot_publish_bot.DB_PATH = os.environ["DB_PATH"]
        autopilot_publish_bot._WEATHER_TEMPLATE_PATH = (
            PACKAGE_ROOT / "shared" / "assets" / "v2_2" / "weather_reminder_templates.json"
        )
