"""V3 frozen publication package store.

The package freezes bytes and public presentation before approval.  It does not
read drafts or render at send time.  Approved/published packages are immutable;
rebuilds create a new version and may supersede only package_ready rows.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any
import uuid


DDL = """
CREATE TABLE IF NOT EXISTS publication_packages_v3 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    package_id TEXT NOT NULL UNIQUE,
    listing_id TEXT NOT NULL,
    offer_id TEXT NOT NULL,
    canonical_record_id TEXT NOT NULL,
    package_version INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'package_ready',
    cover_style TEXT NOT NULL,
    cover_path TEXT NOT NULL,
    gallery_json TEXT NOT NULL DEFAULT '[]',
    post_text TEXT NOT NULL,
    actions_json TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    frozen_file_hashes_json TEXT NOT NULL,
    source_identity_json TEXT NOT NULL DEFAULT '{}',
    public_token TEXT NOT NULL,
    canonical_facts_hash TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    approved_by TEXT NOT NULL DEFAULT '',
    approved_at TEXT,
    published_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(offer_id, package_version)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_packages_v3_one_frozen
ON publication_packages_v3(offer_id)
WHERE status IN ('approved','published');
CREATE INDEX IF NOT EXISTS idx_packages_v3_status
ON publication_packages_v3(status, created_at);
"""


@dataclass(frozen=True)
class FrozenPackage:
    package_id: str
    listing_id: str
    offer_id: str
    canonical_record_id: str
    package_version: int
    status: str
    cover_style: str
    cover_path: str
    gallery: tuple[str, ...]
    post_text: str
    actions: dict[str, str]
    snapshot: dict[str, Any]
    frozen_file_hashes: dict[str, str]
    source_identity: dict[str, Any]
    public_token: str
    canonical_facts_hash: str
    content_hash: str


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _file_hash(path: str) -> str:
    target = Path(str(path))
    if not target.is_file():
        raise ValueError(f"package_frozen_media_file_missing:{target}")
    return hashlib.sha256(target.read_bytes()).hexdigest()


def _content_payload(
    *,
    package_id: str,
    listing_id: str,
    offer_id: str,
    canonical_record_id: str,
    package_version: int,
    cover_style: str,
    cover_path: str,
    gallery: list[str],
    post_text: str,
    actions: dict[str, str],
    snapshot: dict[str, Any],
    frozen_file_hashes: dict[str, str],
    source_identity: dict[str, Any],
    public_token: str,
    canonical_facts_hash: str,
) -> dict[str, Any]:
    return {
        "package_id": package_id,
        "listing_id": listing_id,
        "offer_id": offer_id,
        "canonical_record_id": canonical_record_id,
        "package_version": int(package_version),
        "cover_style": cover_style,
        "cover_path": cover_path,
        "gallery": list(gallery),
        "post_text": post_text,
        "actions": dict(actions),
        "snapshot": dict(snapshot),
        "frozen_file_hashes": dict(frozen_file_hashes),
        "source_identity": dict(source_identity),
        "public_token": public_token,
        "canonical_facts_hash": canonical_facts_hash,
    }


def _content_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


class FrozenPackageStore:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        with self._connect() as conn:
            conn.executescript(DDL)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @staticmethod
    def _model(row: sqlite3.Row) -> FrozenPackage:
        return FrozenPackage(
            package_id=str(row["package_id"]),
            listing_id=str(row["listing_id"]),
            offer_id=str(row["offer_id"]),
            canonical_record_id=str(row["canonical_record_id"]),
            package_version=int(row["package_version"]),
            status=str(row["status"]),
            cover_style=str(row["cover_style"]),
            cover_path=str(row["cover_path"]),
            gallery=tuple(json.loads(str(row["gallery_json"] or "[]"))),
            post_text=str(row["post_text"]),
            actions=dict(json.loads(str(row["actions_json"] or "{}"))),
            snapshot=dict(json.loads(str(row["snapshot_json"] or "{}"))),
            frozen_file_hashes=dict(
                json.loads(str(row["frozen_file_hashes_json"] or "{}"))
            ),
            source_identity=dict(
                json.loads(str(row["source_identity_json"] or "{}"))
            ),
            public_token=str(row["public_token"]),
            canonical_facts_hash=str(row["canonical_facts_hash"]),
            content_hash=str(row["content_hash"]),
        )

    def get(self, package_id: str) -> FrozenPackage | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM publication_packages_v3 WHERE package_id=?",
                (str(package_id),),
            ).fetchone()
        return self._model(row) if row else None

    def create(
        self,
        *,
        listing_id: str,
        offer_id: str,
        canonical_record_id: str,
        canonical_facts_hash: str,
        cover_style: str,
        cover_path: str,
        gallery: list[str],
        post_text: str,
        actions: dict[str, str],
        snapshot: dict[str, Any],
        source_identity: dict[str, Any],
        public_token: str,
    ) -> FrozenPackage:
        required = {
            "listing_id": listing_id,
            "offer_id": offer_id,
            "canonical_record_id": canonical_record_id,
            "canonical_facts_hash": canonical_facts_hash,
            "cover_style": cover_style,
            "cover_path": cover_path,
            "post_text": post_text,
            "public_token": public_token,
        }
        missing = [name for name, value in required.items() if not str(value or "").strip()]
        if missing:
            raise ValueError("package_missing:" + ",".join(missing))
        if len(str(post_text)) > 1024:
            raise ValueError("channel_caption_too_long")
        action_keys = tuple(actions.keys())
        if action_keys != ("details", "photos", "book"):
            raise ValueError("package_actions_must_be_details_photos_book")

        paths = [str(cover_path), *[str(path) for path in gallery or []]]
        unique_paths = list(dict.fromkeys(paths))
        frozen_hashes = {path: _file_hash(path) for path in unique_paths}

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            frozen = conn.execute(
                """SELECT package_id FROM publication_packages_v3
                   WHERE offer_id=? AND status IN ('approved','published') LIMIT 1""",
                (str(offer_id),),
            ).fetchone()
            if frozen:
                raise ValueError("approved_package_frozen")
            version = int(
                conn.execute(
                    """SELECT COALESCE(MAX(package_version),0)+1
                       FROM publication_packages_v3 WHERE offer_id=?""",
                    (str(offer_id),),
                ).fetchone()[0]
            )
            package_id = "PKG3_" + uuid.uuid4().hex
            payload = _content_payload(
                package_id=package_id,
                listing_id=str(listing_id),
                offer_id=str(offer_id),
                canonical_record_id=str(canonical_record_id),
                package_version=version,
                cover_style=str(cover_style),
                cover_path=str(cover_path),
                gallery=[str(path) for path in gallery or []],
                post_text=str(post_text),
                actions=dict(actions),
                snapshot=dict(snapshot),
                frozen_file_hashes=frozen_hashes,
                source_identity=dict(source_identity),
                public_token=str(public_token),
                canonical_facts_hash=str(canonical_facts_hash),
            )
            digest = _content_hash(payload)
            conn.execute(
                """UPDATE publication_packages_v3 SET status='superseded',
                   updated_at=CURRENT_TIMESTAMP
                   WHERE offer_id=? AND status='package_ready'""",
                (str(offer_id),),
            )
            conn.execute(
                """INSERT INTO publication_packages_v3
                   (package_id,listing_id,offer_id,canonical_record_id,package_version,
                    status,cover_style,cover_path,gallery_json,post_text,actions_json,
                    snapshot_json,frozen_file_hashes_json,source_identity_json,
                    public_token,canonical_facts_hash,content_hash)
                   VALUES (?,?,?,?,?,'package_ready',?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    package_id,
                    str(listing_id),
                    str(offer_id),
                    str(canonical_record_id),
                    version,
                    str(cover_style),
                    str(cover_path),
                    _canonical_json(list(gallery or [])),
                    str(post_text),
                    _canonical_json(dict(actions)),
                    _canonical_json(dict(snapshot)),
                    _canonical_json(frozen_hashes),
                    _canonical_json(dict(source_identity)),
                    str(public_token),
                    str(canonical_facts_hash),
                    digest,
                ),
            )
            row = conn.execute(
                "SELECT * FROM publication_packages_v3 WHERE package_id=?",
                (package_id,),
            ).fetchone()
            conn.commit()
        assert row is not None
        return self._model(row)

    def verify_frozen(self, package_id: str) -> FrozenPackage:
        package = self.get(package_id)
        if package is None:
            raise KeyError(package_id)
        actual_hashes = {
            path: _file_hash(path) for path in package.frozen_file_hashes.keys()
        }
        if actual_hashes != package.frozen_file_hashes:
            raise ValueError("package_frozen_media_hash_mismatch")
        payload = _content_payload(
            package_id=package.package_id,
            listing_id=package.listing_id,
            offer_id=package.offer_id,
            canonical_record_id=package.canonical_record_id,
            package_version=package.package_version,
            cover_style=package.cover_style,
            cover_path=package.cover_path,
            gallery=list(package.gallery),
            post_text=package.post_text,
            actions=package.actions,
            snapshot=package.snapshot,
            frozen_file_hashes=package.frozen_file_hashes,
            source_identity=package.source_identity,
            public_token=package.public_token,
            canonical_facts_hash=package.canonical_facts_hash,
        )
        if _content_hash(payload) != package.content_hash:
            raise ValueError("package_content_hash_mismatch")
        return package

    def approve(self, package_id: str, *, approved_by: str) -> FrozenPackage:
        package = self.verify_frozen(package_id)
        if package.status != "package_ready":
            raise ValueError("package_not_ready")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            frozen = conn.execute(
                """SELECT package_id FROM publication_packages_v3
                   WHERE offer_id=? AND status IN ('approved','published')
                     AND package_id<>? LIMIT 1""",
                (package.offer_id, package.package_id),
            ).fetchone()
            if frozen:
                raise ValueError("approved_package_frozen")
            conn.execute(
                """UPDATE publication_packages_v3 SET status='approved',approved_by=?,
                   approved_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
                   WHERE package_id=? AND status='package_ready'""",
                (str(approved_by or ""), package.package_id),
            )
            row = conn.execute(
                "SELECT * FROM publication_packages_v3 WHERE package_id=?",
                (package.package_id,),
            ).fetchone()
            conn.commit()
        assert row is not None
        return self._model(row)

    def mark_published(self, package_id: str) -> FrozenPackage:
        package = self.verify_frozen(package_id)
        if package.status not in {"approved", "published"}:
            raise ValueError("package_not_approved")
        with self._connect() as conn:
            conn.execute(
                """UPDATE publication_packages_v3 SET status='published',
                   published_at=COALESCE(published_at,CURRENT_TIMESTAMP),
                   updated_at=CURRENT_TIMESTAMP WHERE package_id=?""",
                (package.package_id,),
            )
            row = conn.execute(
                "SELECT * FROM publication_packages_v3 WHERE package_id=?",
                (package.package_id,),
            ).fetchone()
            conn.commit()
        assert row is not None
        return self._model(row)


__all__ = ["FrozenPackage", "FrozenPackageStore"]
