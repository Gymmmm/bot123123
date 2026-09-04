#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import unicodedata
from types import SimpleNamespace

from qiaolian_dual.canonical_facts import canonicalize_source
from qiaolian_dual.canonical_listing_materializer import materialize_draft_facts
from source_sanitizer import sanitize_source_text


def _row_value(row: sqlite3.Row, key: str, default=None):
    return row[key] if key in row.keys() else default


def reparse(db_path: str, draft_id: str, source_post_id: int) -> dict:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    draft = conn.execute("SELECT * FROM drafts WHERE draft_id=?", (draft_id,)).fetchone()
    source = conn.execute("SELECT * FROM source_posts WHERE id=?", (source_post_id,)).fetchone()
    if draft is None:
        raise RuntimeError("target_draft_not_found")
    if source is None:
        raise RuntimeError("target_source_post_not_found")
    if str(_row_value(draft, "source_post_id", "")) != str(source_post_id):
        raise RuntimeError("draft_source_mismatch")

    raw = str(_row_value(source, "raw_text", "") or "")
    noise = sorted({
        (ord(ch), unicodedata.category(ch), unicodedata.name(ch, "PRIVATE_USE_OR_UNNAMED"))
        for ch in raw
        if unicodedata.category(ch) in {"Cf", "Co"}
    })
    print("RAW_UNICODE_NOISE=", [(f"U+{cp:04X}", cat, name) for cp, cat, name in noise])

    try:
        old_facts = json.loads(_row_value(draft, "normalized_data", "{}") or "{}")
    except Exception:
        old_facts = {}
    media_summary = dict(old_facts.get("media_summary") or {})
    media_count = conn.execute(
        """SELECT COUNT(*) FROM media_assets
           WHERE owner_type='source_post'
             AND CAST(owner_ref_id AS TEXT)=CAST(? AS TEXT)
             AND COALESCE(status,'active')='active'""",
        (source_post_id,),
    ).fetchone()[0]
    media_summary["image_count"] = max(int(media_summary.get("image_count") or 0), int(media_count or 0))
    media_summary.setdefault("media_type", "image")

    sanitized = sanitize_source_text(raw).text
    source_identity = {
        "source_post_id": source_post_id,
        "source_type": str(_row_value(source, "source_type", "") or ""),
        "source_name": str(_row_value(source, "source_name", "") or ""),
    }
    facts = canonicalize_source(
        raw,
        sanitized_text=sanitized,
        source_identity=source_identity,
        media_summary=media_summary,
    )
    quality = dict(facts.get("quality") or {})
    print("REPARSE_PROJECT=", facts.get("project_name"))
    print("REPARSE_PROPERTY=", facts.get("property_type"), facts.get("property_type_status"))
    print("REPARSE_DEAL_TYPE=", facts.get("deal_type"))
    print("REPARSE_BLOCKERS=", quality.get("blocking_flags"))
    print("REPARSE_WARNINGS=", quality.get("warning_flags"))
    print("REPARSE_MEDIA_COUNT=", media_summary.get("image_count"))

    if facts.get("project_key") != "the_pinnacle":
        raise RuntimeError(f"unexpected_project:{facts.get('project_key')}")
    if facts.get("property_type") != "公寓":
        raise RuntimeError(f"unexpected_property_type:{facts.get('property_type')}")
    if facts.get("deal_type") != "rent":
        raise RuntimeError(f"unexpected_deal_type:{facts.get('deal_type')}")
    blockers = list(quality.get("blocking_flags") or [])
    if blockers:
        raise RuntimeError(f"blocking_flags_remain:{blockers}")

    materialize_draft_facts(conn, draft_id=draft_id, facts=facts)
    conn.commit()
    refreshed = conn.execute(
        "SELECT review_status, normalized_data FROM drafts WHERE draft_id=?", (draft_id,)
    ).fetchone()
    normalized = json.loads(refreshed["normalized_data"] or "{}")
    if str(refreshed["review_status"] or "") != "pending":
        raise RuntimeError(f"unexpected_review_status_after_reparse:{refreshed['review_status']}")
    if (normalized.get("quality") or {}).get("blocking_flags"):
        raise RuntimeError("materialized_blockers_remain")
    return {"noise": noise, "facts": facts}


class _Message:
    async def reply_text(self, text, **kwargs):
        print("ADMIN_REPLY=", str(text).replace("\n", " | ")[:1200])


class _Bot:
    async def send_message(self, **kwargs):
        print("ADMIN_BOT_MESSAGE=", str(kwargs.get("text", "")).replace("\n", " | ")[:1200])


async def approve_and_publish(draft_id: str) -> None:
    import autopilot_publish_bot as ap

    if not ap.ADMIN_IDS:
        raise RuntimeError("no_admin_ids_configured")
    admin_id = next(iter(ap.ADMIN_IDS))
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=admin_id),
        effective_message=_Message(),
        message=_Message(),
        effective_chat=SimpleNamespace(id=admin_id),
        callback_query=None,
    )
    context = SimpleNamespace(args=[draft_id], bot=_Bot())
    await ap.cmd_approve(update, context)
    await ap.cmd_send(update, context)


def verify_published(db_path: str, draft_id: str) -> None:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    draft = conn.execute(
        "SELECT draft_id,review_status,listing_id FROM drafts WHERE draft_id=?", (draft_id,)
    ).fetchone()
    package = conn.execute(
        """SELECT package_id,status,property_id FROM publication_packages
           WHERE draft_id=? ORDER BY package_version DESC LIMIT 1""",
        (draft_id,),
    ).fetchone()
    post = conn.execute(
        """SELECT post_id,channel_message_id,publish_status FROM posts
           WHERE draft_id=? ORDER BY id DESC LIMIT 1""",
        (draft_id,),
    ).fetchone()
    print("FINAL_DRAFT=", dict(draft) if draft else None)
    print("FINAL_PACKAGE=", dict(package) if package else None)
    print("FINAL_POST=", dict(post) if post else None)
    if not draft or draft["review_status"] != "published":
        raise RuntimeError("draft_not_published")
    if not package or package["status"] != "published":
        raise RuntimeError("package_not_published")
    if not post or post["publish_status"] != "published" or not str(post["channel_message_id"] or "").strip():
        raise RuntimeError("post_not_published")
    conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=os.getenv("DB_PATH", "data/qiaolian_dual_bot.db"))
    parser.add_argument("--draft-id", required=True)
    parser.add_argument("--source-post-id", required=True, type=int)
    args = parser.parse_args()
    reparse(args.db, args.draft_id, args.source_post_id)
    asyncio.run(approve_and_publish(args.draft_id))
    verify_published(args.db, args.draft_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
