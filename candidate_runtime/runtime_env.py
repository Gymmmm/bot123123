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
    backup_dir: Path


def _resolve(value: str | os.PathLike[str], base: Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (base / path).resolve()


def _assert_isolated(runtime_root: Path, repo_root: Path) -> None:
    allow_repo_root = str(os.getenv("CANDIDATE_ALLOW_REPO_RUNTIME", "0")).strip().lower() in {"1", "true", "yes", "on"}
    if runtime_root == repo_root and not allow_repo_root:
        raise RuntimeError(
            "candidate runtime root must be isolated from the repository; set QIAOLIAN_RUNTIME_ROOT "
            "or explicitly opt in with CANDIDATE_ALLOW_REPO_RUNTIME=1"
        )


def configure_candidate_environment(repo_root: str | os.PathLike[str] | None = None) -> RuntimePaths:
    repo = Path(repo_root or Path(__file__).resolve().parents[1]).resolve()
    runtime_root = _resolve(os.getenv("QIAOLIAN_RUNTIME_ROOT", str(repo)), repo)
    _assert_isolated(runtime_root, repo)

    # Candidate-local .env is authoritative for local testing; process variables win.
    load_dotenv(runtime_root / ".env", override=False)
    load_dotenv(repo / ".env", override=False)

    data_dir = _resolve(os.getenv("DATA_DIR", "data"), runtime_root)
    db_path = _resolve(os.getenv("DB_PATH") or os.getenv("SQLITE_PATH") or str(data_dir / "qiaolian_v3_candidate.db"), runtime_root)
    media_root = _resolve(os.getenv("MEDIA_ROOT", "media"), runtime_root)
    download_dir = _resolve(os.getenv("COLLECTOR_DOWNLOAD_DIR", str(media_root / "collector_downloads")), runtime_root)
    session_path = _resolve(os.getenv("TELETHON_SESSION_PATH", "telethon_sessions/qiaolian_v3_candidate"), runtime_root)
    backup_dir = _resolve(os.getenv("CANDIDATE_BACKUP_DIR", "backups"), runtime_root)

    for directory in (runtime_root, data_dir, media_root, download_dir, session_path.parent, backup_dir):
        directory.mkdir(parents=True, exist_ok=True)

    # Keep every mutable path under the candidate root. This catches accidental reuse
    # of /opt/qiaolian_dual_bots or any other production path through a copied .env.
    for name, path in {
        "DATA_DIR": data_dir,
        "DB_PATH": db_path,
        "MEDIA_ROOT": media_root,
        "COLLECTOR_DOWNLOAD_DIR": download_dir,
        "TELETHON_SESSION_PATH": session_path,
        "CANDIDATE_BACKUP_DIR": backup_dir,
    }.items():
        try:
            path.relative_to(runtime_root)
        except ValueError as exc:
            raise RuntimeError(f"{name} escapes candidate runtime root: {path}") from exc

    os.environ.update({
        "QIAOLIAN_RUNTIME_ROOT": str(runtime_root),
        "DATA_DIR": str(data_dir),
        "DB_PATH": str(db_path),
        "SQLITE_PATH": str(db_path),
        "MEDIA_ROOT": str(media_root),
        "COLLECTOR_DOWNLOAD_DIR": str(download_dir),
        "TELETHON_SESSION_PATH": str(session_path),
        "CANDIDATE_BACKUP_DIR": str(backup_dir),
    })

    return RuntimePaths(repo, runtime_root, data_dir, db_path, media_root, download_dir, session_path, backup_dir)


__all__ = ["RuntimePaths", "configure_candidate_environment"]
