#!/usr/bin/env python3
"""Media consistency checks and recovery helpers for the production publish path.

The publish gate intentionally checks the filesystem, not only database rows.
This module centralizes those checks so queueing, scheduled publish, and repair
tools all agree on what "media is usable" means.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sqlite3
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

DB_PATH = os.getenv("DB_PATH", str(BASE_DIR / "data" / "qiaolian_dual_bot.db"))
MEDIA_ROOT = Path(os.getenv("MEDIA_ROOT", str(BASE_DIR / "media"))).resolve()


@dataclass
class MediaIssue:
    code: str
    detail: str = ""


@dataclass
class DraftMediaStatus:
    draft_id: str
    source_post_id: int | None
    cover_path: str | None
    existing_real_media: list[str] = field(default_factory=list)
    missing_real_media: list[str] = field(default_factory=list)
    issues: list[MediaIssue] = field(default_factory=list)

    @property
    def is_ok_for_publish(self) -> bool:
        return not any(i.code in {"missing_cover", "missing_real_media"} for i in self.issues)

    @property
    def has_real_media(self) -> bool:
        return bool(self.existing_real_media)

    @property
    def has_cover(self) -> bool:
        return bool(self.cover_path and os.path.isfile(self.cover_path))

    @property
    def issue_codes(self) -> list[str]:
        return [i.code for i in self.issues]

    def note(self) -> str:
        if not self.issues:
            return "media:ok"
        bits = []
        for issue in self.issues:
            bits.append(issue.code if not issue.detail else f"{issue.code}:{issue.detail}")
        return "media:broken " + ",".join(bits)


def _conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _json_loads(raw: str | None, fallback: Any) -> Any:
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except Exception:
        return fallback


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _item_path(item: Any) -> str:
    if isinstance(item, str):
        return item.strip()
    if isinstance(item, dict):
        return (item.get("local_path") or item.get("path") or "").strip()
    return ""


def _message_id_from_item(item: Any) -> int | None:
    if isinstance(item, dict):
        mid = item.get("message_id")
        try:
            return int(mid) if mid is not None else None
        except (TypeError, ValueError):
            return None
    path = _item_path(item)
    m = re.search(r"_(\d+)(?:\s*\(\d+\))?\.[A-Za-z0-9]+$", os.path.basename(path))
    if m:
        return int(m.group(1))
    return None


def _target_path_for_item(source_name: str, item: Any, message_id: int, index: int) -> str:
    existing_path = _item_path(item)
    if existing_path.startswith("/"):
        return existing_path
    safe_source = re.sub(r"[^A-Za-z0-9_.-]+", "_", source_name or "source")
    return str(MEDIA_ROOT / "photos" / safe_source / f"{safe_source}_{message_id or index}.jpg")


def draft_row(draft_id: str, db_path: str = DB_PATH) -> sqlite3.Row | None:
    with _conn(db_path) as conn:
        return conn.execute("SELECT * FROM drafts WHERE draft_id=?", (draft_id,)).fetchone()


def source_post_row(source_post_id: int, db_path: str = DB_PATH) -> sqlite3.Row | None:
    with _conn(db_path) as conn:
        return conn.execute("SELECT * FROM source_posts WHERE id=?", (source_post_id,)).fetchone()


def cover_path_for_draft_row(row: sqlite3.Row, db_path: str = DB_PATH) -> str | None:
    cid = row["cover_asset_id"] if "cover_asset_id" in row.keys() else None
    if not cid:
        return None
    with _conn(db_path) as conn:
        asset = conn.execute("SELECT local_path FROM media_assets WHERE id=?", (cid,)).fetchone()
    return asset["local_path"] if asset and asset["local_path"] else None


def expected_real_media_paths(source_post_id: int | None, db_path: str = DB_PATH) -> list[str]:
    if not source_post_id:
        return []
    paths: list[str] = []
    with _conn(db_path) as conn:
        rows = conn.execute(
            """SELECT local_path FROM media_assets
               WHERE owner_type='source_post'
                 AND owner_ref_id=?
                 AND asset_type='photo'
                 AND local_path IS NOT NULL AND local_path != ''
               ORDER BY sort_order ASC, id ASC""",
            (source_post_id,),
        ).fetchall()
        paths.extend(r["local_path"] for r in rows if r["local_path"])
        sp = conn.execute(
            "SELECT raw_images_json FROM source_posts WHERE id=?",
            (source_post_id,),
        ).fetchone()
    for item in _json_loads(sp["raw_images_json"] if sp else None, []):
        p = _item_path(item)
        if p and p.startswith("/") and p not in paths:
            paths.append(p)
    return paths


def assess_draft_media(draft_id: str, db_path: str = DB_PATH) -> DraftMediaStatus:
    row = draft_row(draft_id, db_path)
    if not row:
        return DraftMediaStatus(
            draft_id=draft_id,
            source_post_id=None,
            cover_path=None,
            issues=[MediaIssue("draft_not_found")],
        )

    source_post_id = row["source_post_id"]
    cover_path = cover_path_for_draft_row(row, db_path)
    expected = expected_real_media_paths(source_post_id, db_path)
    existing = [p for p in expected if p and os.path.isfile(p)]
    missing = [p for p in expected if p and not os.path.isfile(p)]
    issues: list[MediaIssue] = []

    if not cover_path or not os.path.isfile(cover_path):
        detail = cover_path or "no_cover_asset"
        issues.append(MediaIssue("missing_cover", detail))
    if not existing:
        detail = f"expected={len(expected)} missing={len(missing)}"
        issues.append(MediaIssue("missing_real_media", detail))

    return DraftMediaStatus(
        draft_id=draft_id,
        source_post_id=source_post_id,
        cover_path=cover_path,
        existing_real_media=existing,
        missing_real_media=missing,
        issues=issues,
    )


def append_review_note(draft_id: str, note: str, db_path: str = DB_PATH) -> None:
    with _conn(db_path) as conn:
        row = conn.execute("SELECT review_note FROM drafts WHERE draft_id=?", (draft_id,)).fetchone()
        if not row:
            return
        current = (row["review_note"] or "").strip()
        parts = [p.strip() for p in current.split("|") if p.strip() and not p.strip().startswith("media:")]
        parts.append(note)
        merged = " | ".join(parts)[-500:]
        conn.execute(
            "UPDATE drafts SET review_note=?, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
            (merged, draft_id),
        )
        conn.commit()


def mark_draft_media_broken(draft_id: str, status: DraftMediaStatus, db_path: str = DB_PATH) -> None:
    append_review_note(draft_id, status.note(), db_path)


def media_blocks_ready(status: DraftMediaStatus) -> bool:
    """Ready queue requires restorable real media; cover can be rebuilt later."""
    return any(code in {"draft_not_found", "missing_real_media"} for code in status.issue_codes)


def media_blocks_publish(status: DraftMediaStatus) -> bool:
    """Publishing regenerates cover, but cannot proceed without real media."""
    return media_blocks_ready(status)


def media_issue_summary(status: DraftMediaStatus) -> str:
    return ",".join(status.issue_codes) or "media:ok"


def source_recoverability_hint(source_post_id: int | None, db_path: str = DB_PATH) -> str:
    if not source_post_id:
        return "source_unrecoverable:no_source_post_id"
    source = source_post_row(source_post_id, db_path)
    if not source:
        return "source_unrecoverable:source_post_not_found"
    items = _source_message_items(source)
    if not items:
        return "source_unrecoverable:no_message_ids"
    candidates = _source_entity_candidates(source)
    if not candidates:
        return "source_unrecoverable:no_entity_candidates"
    return "source_recoverable:entity_candidates=" + "|".join(str(c) for c in candidates[:5])


def find_broken_drafts(db_path: str = DB_PATH, statuses: tuple[str, ...] = ("pending", "ready"), limit: int = 50) -> list[DraftMediaStatus]:
    placeholders = ",".join("?" for _ in statuses)
    with _conn(db_path) as conn:
        rows = conn.execute(
            f"""SELECT draft_id FROM drafts
                WHERE review_status IN ({placeholders})
                ORDER BY CASE review_status WHEN 'ready' THEN 0 ELSE 1 END,
                         queue_score DESC, id ASC
                LIMIT ?""",
            (*statuses, limit),
        ).fetchall()
    out: list[DraftMediaStatus] = []
    for row in rows:
        status = assess_draft_media(row["draft_id"], db_path)
        if status.issues:
            out.append(status)
    return out


def _source_entity_candidates(source: sqlite3.Row) -> list[Any]:
    candidates: list[Any] = []
    meta = _json_loads(source["raw_meta_json"], {})
    chat_id = meta.get("chat_id") if isinstance(meta, dict) else None
    if chat_id:
        try:
            candidates.append(int(chat_id))
        except (TypeError, ValueError):
            candidates.append(str(chat_id))
    name = (source["source_name"] or "").strip()
    if name:
        candidates.append(f"@{name.lstrip('@')}")
        candidates.append(name.lstrip("@"))
    source_url = (source["source_url"] or "").strip() if "source_url" in source.keys() else ""
    m_public = re.search(r"t\.me/([^/]+)/\d+", source_url)
    if m_public and m_public.group(1) != "c":
        username = m_public.group(1)
        candidates.insert(0, f"@{username}")
        candidates.insert(1, username)
    m_private = re.search(r"t\.me/c/(\d+)/\d+", source_url)
    if m_private:
        candidates.insert(0, int("-100" + m_private.group(1)))
    return candidates


def _source_message_items(source: sqlite3.Row) -> list[dict[str, Any]]:
    raw_items = _json_loads(source["raw_images_json"], [])
    out: list[dict[str, Any]] = []
    for idx, item in enumerate(raw_items):
        mid = _message_id_from_item(item)
        if mid is None:
            try:
                mid = int(source["source_post_id"])
            except (TypeError, ValueError):
                mid = None
        if mid is None:
            continue
        out.append(
            {
                "index": idx,
                "message_id": mid,
                "target_path": _target_path_for_item(source["source_name"], item, mid, idx),
            }
        )
    return out


def _upsert_source_media_asset(
    conn: sqlite3.Connection,
    *,
    source_post_id: int,
    local_path: str,
    file_hash: str,
    message_id: int,
    sort_order: int,
) -> None:
    existing = conn.execute(
        """SELECT id FROM media_assets
           WHERE owner_type='source_post' AND owner_ref_id=? AND sort_order=?""",
        (source_post_id, sort_order),
    ).fetchone()
    if existing:
        conn.execute(
            """UPDATE media_assets
               SET local_path=?, file_hash=?, source_file_id=?, telegram_file_id=?,
                   media_type='photo', asset_type='photo', status='active',
                   updated_at=CURRENT_TIMESTAMP
               WHERE id=?""",
            (local_path, file_hash, str(message_id), str(message_id), existing["id"]),
        )
        return
    conn.execute(
        """INSERT INTO media_assets (
             asset_id, owner_type, owner_ref_id, owner_ref_key,
             asset_type, source_type, source_file_id, local_path, file_hash,
             telegram_file_id, media_type, is_watermarked, is_cover, sort_order,
             status, created_at, updated_at
           ) VALUES (?, 'source_post', ?, ?, 'photo', 'telegram', ?, ?, ?, ?, 'photo',
                     0, ?, ?, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""",
        (
            f"AST_{uuid.uuid4().hex[:16].upper()}",
            source_post_id,
            str(source_post_id),
            str(message_id),
            local_path,
            file_hash,
            str(message_id),
            1 if sort_order == 0 else 0,
            sort_order,
        ),
    )


async def redownload_source_post_media(source_post_id: int, db_path: str = DB_PATH, dry_run: bool = False) -> dict[str, Any]:
    from telethon import TelegramClient

    source = source_post_row(source_post_id, db_path)
    if not source:
        return {"ok": False, "error": "source_post_not_found", "downloaded": []}
    items = _source_message_items(source)
    if not items:
        return {"ok": False, "error": "no_message_ids", "downloaded": []}
    if dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "entity_candidates": [str(c) for c in _source_entity_candidates(source)],
            "downloaded": [
                {**item, "path": item["target_path"], "status": "dry_run"}
                for item in items
            ],
        }
    api_id = int(os.getenv("TG_API_ID", "0") or 0)
    api_hash = (os.getenv("TG_API_HASH", "") or "").strip()
    session = (os.getenv("TELETHON_SESSION_PATH") or str(BASE_DIR / "v2" / "qiaolian_crawler_session")).strip()
    if not api_id or not api_hash:
        return {"ok": False, "error": "missing_tg_api_credentials", "downloaded": []}

    entity = None
    entity_error = ""
    client = TelegramClient(session, api_id, api_hash)
    await client.start()
    try:
        for cand in _source_entity_candidates(source):
            try:
                entity = await client.get_entity(cand)
                break
            except Exception as exc:  # keep trying candidates
                entity_error = str(exc)
        if entity is None:
            return {"ok": False, "error": f"entity_not_found:{entity_error}", "downloaded": []}

        downloaded: list[dict[str, Any]] = []
        for item in items:
            target = Path(item["target_path"])
            if target.exists():
                downloaded.append({**item, "path": str(target), "status": "exists"})
                continue
            if dry_run:
                downloaded.append({**item, "path": str(target), "status": "dry_run"})
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            msg = await client.get_messages(entity, ids=item["message_id"])
            if not msg or not getattr(msg, "media", None):
                downloaded.append({**item, "path": str(target), "status": "missing_message_media"})
                continue
            got = await client.download_media(msg, file=str(target))
            final_path = str(Path(got or target).resolve())
            if not os.path.isfile(final_path):
                downloaded.append({**item, "path": final_path, "status": "download_failed"})
                continue
            downloaded.append({**item, "path": final_path, "status": "downloaded"})
    finally:
        await client.disconnect()

    successful = [d for d in downloaded if d["status"] in {"downloaded", "exists"} and os.path.isfile(d["path"])]
    if not dry_run and successful:
        raw_images = []
        with _conn(db_path) as conn:
            for idx, item in enumerate(successful):
                file_hash = _sha256(item["path"])
                _upsert_source_media_asset(
                    conn,
                    source_post_id=source_post_id,
                    local_path=item["path"],
                    file_hash=file_hash,
                    message_id=item["message_id"],
                    sort_order=idx,
                )
                raw_images.append(
                    {
                        "local_path": item["path"],
                        "file_hash": file_hash,
                        "message_id": item["message_id"],
                    }
                )
            conn.execute(
                "UPDATE source_posts SET raw_images_json=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (json.dumps(raw_images, ensure_ascii=False), source_post_id),
            )
            conn.commit()
    return {"ok": bool(successful), "downloaded": downloaded}


def rebuild_cover(draft_id: str, db_path: str = DB_PATH) -> dict[str, Any]:
    from cover_generator import CoverGenerator

    asset_id, path = CoverGenerator(db_path).generate_for_draft(draft_id)
    return {"ok": bool(asset_id and path and os.path.isfile(path)), "asset_id": asset_id, "path": path}


async def repair_draft_media(draft_id: str, db_path: str = DB_PATH, dry_run: bool = False) -> dict[str, Any]:
    row = draft_row(draft_id, db_path)
    if not row:
        return {"ok": False, "error": "draft_not_found"}
    source_post_id = row["source_post_id"]
    before = assess_draft_media(draft_id, db_path)
    redownload = await redownload_source_post_media(source_post_id, db_path, dry_run=dry_run)
    cover = {"ok": False, "skipped": dry_run}
    if not dry_run and redownload.get("ok"):
        cover = rebuild_cover(draft_id, db_path)
    after = assess_draft_media(draft_id, db_path) if not dry_run else before
    if dry_run:
        pass
    elif after.issues:
        mark_draft_media_broken(draft_id, after, db_path)
    else:
        append_review_note(draft_id, after.note(), db_path)
    return {
        "ok": after.is_ok_for_publish,
        "before": before.__dict__,
        "redownload": redownload,
        "cover": cover,
        "after": after.__dict__,
    }


def mark_broken_ready(db_path: str = DB_PATH, limit: int = 50, dry_run: bool = False) -> dict[str, Any]:
    with _conn(db_path) as conn:
        rows = conn.execute(
            """SELECT draft_id FROM drafts
               WHERE review_status='ready'
               ORDER BY queue_score DESC, id ASC
               LIMIT ?""",
            (limit,),
        ).fetchall()

    scanned = len(rows)
    hits: list[dict[str, Any]] = []
    reverted = 0
    for row in rows:
        draft_id = row["draft_id"]
        status = assess_draft_media(draft_id, db_path)
        if not media_blocks_ready(status):
            continue
        note = status.note()
        hits.append(
            {
                "draft_id": draft_id,
                "issues": status.issue_codes,
                "note": note,
            }
        )
        if dry_run:
            continue
        mark_draft_media_broken(draft_id, status, db_path)
        with _conn(db_path) as conn:
            conn.execute(
                "UPDATE drafts SET review_status='pending', updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
                (draft_id,),
            )
            conn.commit()
        reverted += 1
    return {"scanned": scanned, "hits": len(hits), "reverted": reverted, "items": hits}


def _print_status(status: DraftMediaStatus, db_path: str = DB_PATH) -> None:
    verdict = "OK" if not status.issues else "BROKEN"
    print(f"{status.draft_id}: {verdict}")
    print(f"  source_post_id: {status.source_post_id}")
    print(f"  cover: {status.cover_path} exists={bool(status.cover_path and os.path.isfile(status.cover_path))}")
    print(f"  real_media: existing={len(status.existing_real_media)} missing={len(status.missing_real_media)}")
    print(f"  blocks_ready: {'yes' if media_blocks_ready(status) else 'no'}")
    print(f"  blocks_publish: {'yes' if media_blocks_publish(status) else 'no'}")
    if status.issues:
        print("  issues: " + ", ".join(status.issue_codes))
    if "missing_real_media" in status.issue_codes:
        print("  recovery: " + source_recoverability_hint(status.source_post_id, db_path))
    for path in status.missing_real_media[:10]:
        print(f"  missing: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Check and recover media files referenced by drafts.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_report = sub.add_parser("report", help="Report missing media for drafts")
    p_report.add_argument("--draft-id")
    p_report.add_argument("--limit", type=int, default=50)
    p_repair = sub.add_parser("repair", help="Redownload real media and rebuild cover for one draft")
    p_repair.add_argument("--draft-id", required=True)
    p_repair.add_argument("--dry-run", action="store_true")
    p_mark = sub.add_parser("mark-broken-ready", help="Move ready drafts blocked by media gate back to pending")
    p_mark.add_argument("--dry-run", action="store_true")
    p_mark.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    if args.cmd == "report":
        if args.draft_id:
            _print_status(assess_draft_media(args.draft_id), DB_PATH)
        else:
            for status in find_broken_drafts(limit=args.limit):
                _print_status(status, DB_PATH)
        return
    if args.cmd == "repair":
        result = asyncio.run(repair_draft_media(args.draft_id, dry_run=args.dry_run))
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return
    if args.cmd == "mark-broken-ready":
        result = mark_broken_ready(DB_PATH, limit=args.limit, dry_run=args.dry_run)
        mode = "dry-run" if args.dry_run else "apply"
        print(
            f"mark-broken-ready ({mode}): scanned={result['scanned']} "
            f"hits={result['hits']} reverted={result['reverted']}"
        )
        for item in result["items"]:
            print(f"  {item['draft_id']}: {','.join(item['issues'])} -> pending")


if __name__ == "__main__":
    main()
