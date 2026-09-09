from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv


@dataclass(frozen=True)
class RuntimePaths:
    repo_root: Path
    runtime_root: Path
    data_dir: Path
    db_path: Path
    media_root: Path
    download_dir: Path
    session_path: Path


def _resolve(value: str | os.PathLike[str], base: Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (base / path).resolve()


def configure_candidate_environment(repo_root: str | os.PathLike[str] | None = None) -> RuntimePaths:
    """Resolve all mutable state below one candidate runtime root.

    Business modules keep using their existing DB_PATH/SQLITE_PATH and media env
    variables; only their filesystem roots are isolated from production.
    """
    repo = Path(repo_root or Path(__file__).resolve().parents[1]).resolve()
    runtime_root = _resolve(os.getenv("QIAOLIAN_RUNTIME_ROOT", str(repo)), repo)

    # Candidate-local .env is preferred. Process environment always wins.
    load_dotenv(runtime_root / ".env", override=False)
    if runtime_root != repo:
        load_dotenv(repo / ".env", override=False)

    data_dir = _resolve(os.getenv("DATA_DIR", "data"), runtime_root)
    db_path = _resolve(
        os.getenv("DB_PATH") or os.getenv("SQLITE_PATH") or str(data_dir / "qiaolian_dual_bot.db"),
        runtime_root,
    )
    media_root = _resolve(os.getenv("MEDIA_ROOT", "media"), runtime_root)
    download_dir = _resolve(
        os.getenv("COLLECTOR_DOWNLOAD_DIR", str(media_root / "collector_downloads")),
        runtime_root,
    )
    session_path = _resolve(
        os.getenv("TELETHON_SESSION_PATH", "telethon_sessions/qiaolian_collector"),
        runtime_root,
    )

    for directory in (data_dir, media_root, download_dir, session_path.parent):
        directory.mkdir(parents=True, exist_ok=True)

    os.environ["QIAOLIAN_RUNTIME_ROOT"] = str(runtime_root)
    os.environ["DATA_DIR"] = str(data_dir)
    os.environ["DB_PATH"] = str(db_path)
    os.environ["SQLITE_PATH"] = str(db_path)
    os.environ["MEDIA_ROOT"] = str(media_root)
    os.environ["COLLECTOR_DOWNLOAD_DIR"] = str(download_dir)
    os.environ["TELETHON_SESSION_PATH"] = str(session_path)

    return RuntimePaths(
        repo_root=repo,
        runtime_root=runtime_root,
        data_dir=data_dir,
        db_path=db_path,
        media_root=media_root,
        download_dir=download_dir,
        session_path=session_path,
    )


__all__ = ["RuntimePaths", "configure_candidate_environment"]
