#!/usr/bin/env python3
"""Batch orchestration of the existing V3 media/package/edit boundaries.

No new posts are sent. A batch examines at most five publication identities in
message-id order. Missing source/package identities and quality failures skip.
Any uncertain Telegram result stops execution and blocks automatic retry.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from telegram import Bot

from v3_core.ingest.source_reader import SourceReader
from v3_core.media.cover_service import CoverRenderService
from v3_core.media.cover_styles import recommended_cover_style
from v3_core.media.service import MediaPreparationService
from v3_core.publishing.admin_bot import load_settings
from v3_core.publishing.delivery_coordinator import TelegramSendCommand
from v3_core.publishing.delivery_state import PublicationDeliveryStateRepository
from v3_core.publishing.eligibility import evaluate_offer_eligibility
from v3_core.publishing.package_service import PackageBuildService
from v3_core.publishing.package_store import FrozenPackageStore
from v3_core.publishing.telegram_adapter import TelegramChannelAdapter
from v3_core.storage.inventory_reader import InventoryReader


def connect(db):
    c = sqlite3.connect(str(db), timeout=30)
    c.row_factory = sqlite3.Row
    return c


async def run(args):
    settings = load_settings()
    db = Path(settings.db_path)
    folder = Path(args.run_dir).resolve()
    folder.mkdir(parents=True, exist_ok=True)
    reader = InventoryReader(db)
    source_reader = SourceReader(db)
    media_service = MediaPreparationService(source_reader)
    covers = CoverRenderService(reader=reader, output_dir=folder / "covers")
    packages = FrozenPackageStore(db)
    builder = PackageBuildService(reader=reader, store=packages,
        user_bot_username=settings.user_bot_username, advisor_url=settings.advisor_url)
    attempts = PublicationDeliveryStateRepository(db)
    results = []
    with connect(db) as c:
        comparison = "<" if args.newest_first else ">"
        direction = "DESC" if args.newest_first else "ASC"
        rows = c.execute(f"""SELECT * FROM publication_instances
            WHERE platform='telegram' AND publish_status='published'
              AND CAST(channel_message_id AS INTEGER){comparison}?
            ORDER BY CAST(channel_message_id AS INTEGER) {direction},id LIMIT ?""",
            (args.after, min(5, max(1, args.limit)))).fetchall()
        if args.execute and not args.no_backup:
            with sqlite3.connect(folder / "before.sqlite3") as backup:
                c.backup(backup)
    bot = Bot(settings.token) if args.execute else None
    try:
        if bot:
            await bot.initialize()
            channel = await bot.get_chat(settings.channel_chat_id)
            aliases = {str(settings.channel_chat_id), str(channel.id)}
        else:
            aliases = None
        for row in rows:
            pub = dict(row)
            item = {"message_id": int(pub["channel_message_id"]),
                    "instance_id": pub["instance_id"], "status": "skipped"}
            held = []
            new = None
            crossed = False
            try:
                if aliases and str(pub["channel_chat_id"]) not in aliases:
                    raise ValueError("destination_channel_mismatch")
                if str(pub.get("media_group_id") or ""):
                    raise ValueError("old_album_requires_manual_review")
                old = packages.get(pub["package_id"])
                if old is None:
                    raise ValueError("missing_package_identity")
                listing = reader.listing(pub["listing_id"])
                offer = reader.offer(pub["offer_id"])
                canonical = reader.canonical(listing["canonical_record_id"])
                source_id = int(canonical["source_post_id"])
                source = source_reader.source_post(source_id)
                if str(source.get("source_name") or "").lower() != args.source.lower():
                    raise ValueError("source_not_selected:" + str(source.get("source_name") or ""))
                with connect(db) as c:
                    uncertain = c.execute("""SELECT 1 FROM publication_delivery_attempts_v3
                        WHERE listing_id=? AND state IN ('sending','unknown') LIMIT 1""",
                        (pub["listing_id"],)).fetchone()
                if uncertain:
                    raise ValueError("unresolved_delivery_attempt")
                style = recommended_cover_style(listing.get("property_type"), listing.get("property_subtype"))
                media = await asyncio.to_thread(media_service.prepare, source_post_id=source_id, cover_style=style)
                gate = evaluate_offer_eligibility(facts=canonical["facts"], offer=offer,
                    review_approved=reader.review_approved(offer_id=pub["offer_id"]),
                    media_count=len(media.gallery_paths), cover_exists=True)
                if not gate.ok:
                    raise ValueError("quality:" + ",".join(gate.blocking))
                cover = await asyncio.to_thread(covers.render, listing_id=pub["listing_id"], offer_id=pub["offer_id"],
                    media=media, style=style,
                    output_path=folder / "covers" / f"{item['message_id']}_{listing['public_listing_id']}.png")
                item.update(public_listing_id=listing["public_listing_id"], source_post_id=source_id,
                    inventory_status=listing["inventory_status"], cover_path=cover.output_path,
                    gallery_count=len(media.gallery_paths), status="ready")
                if not args.execute:
                    results.append(item)
                    print(json.dumps(item, ensure_ascii=False), flush=True)
                    continue
                with connect(db) as c:
                    held = [tuple(r) for r in c.execute("""SELECT package_id,status
                        FROM publication_packages_v3 WHERE offer_id=?
                        AND status IN ('approved','published')""", (pub["offer_id"],))]
                    c.execute("""UPDATE publication_packages_v3 SET status='rebuild_hold'
                        WHERE offer_id=? AND status IN ('approved','published')""", (pub["offer_id"],))
                new = builder.build(listing_id=pub["listing_id"], offer_id=pub["offer_id"],
                    cover_style=style, cover_path=cover.output_path, gallery=list(media.gallery_paths),
                    source_identity=media.source_identity)
                packages.verify_frozen(new.package_id)
                attempt = attempts.prepare(package_id=new.package_id, listing_id=new.listing_id,
                    offer_id=new.offer_id, channel_chat_id=pub["channel_chat_id"])
                command = TelegramSendCommand(attempt_id=attempt.attempt_id, package_id=new.package_id,
                    channel_chat_id=pub["channel_chat_id"], cover_path=new.cover_path,
                    caption=new.post_text, actions=new.actions, inventory_status=listing["inventory_status"])
                if not args.no_backup:
                    (folder / f"{item['message_id']}_before.json").write_text(json.dumps(pub, ensure_ascii=False, indent=2))
                attempts.mark_sending(attempt.attempt_id)
                crossed = True
                try:
                    receipt = await TelegramChannelAdapter(bot).edit(command, message_id=item["message_id"])
                except Exception as exc:
                    attempts.mark_unknown(attempt.attempt_id, type(exc).__name__)
                    item.update(status="unknown", reason=type(exc).__name__, package_id=new.package_id)
                    results.append(item)
                    print(json.dumps(item, ensure_ascii=False), flush=True)
                    break
                attempts.mark_sent(attempt.attempt_id, receipt)
                with connect(db) as c:
                    c.execute("BEGIN IMMEDIATE")
                    for package_id, _ in held:
                        c.execute("UPDATE publication_packages_v3 SET status='superseded',updated_at=CURRENT_TIMESTAMP WHERE package_id=?", (package_id,))
                    c.execute("UPDATE publication_packages_v3 SET status='published',published_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE package_id=?", (new.package_id,))
                    c.execute("""UPDATE publication_instances SET package_id=?,post_text=?,
                        updated_at=CURRENT_TIMESTAMP WHERE instance_id=? AND channel_message_id=?""",
                        (new.package_id, new.post_text, pub["instance_id"], pub["channel_message_id"]))
                attempts.mark_committed(attempt.attempt_id)
                item.update(status="updated", package_id=new.package_id, edit_mode=receipt["edit_mode"])
                await asyncio.sleep(2)
            except Exception as exc:
                item["reason"] = str(exc)[:240]
                if held and not crossed:
                    with connect(db) as c:
                        for package_id, status in held:
                            c.execute("UPDATE publication_packages_v3 SET status=? WHERE package_id=? AND status='rebuild_hold'", (status, package_id))
                        if new:
                            c.execute("UPDATE publication_packages_v3 SET status='superseded' WHERE package_id=?", (new.package_id,))
                if crossed:
                    item["status"] = "commit_attention"
            results.append(item)
            print(json.dumps(item, ensure_ascii=False), flush=True)
            if crossed and item["status"] != "updated":
                break
    finally:
        if bot:
            await bot.shutdown()
        report = {"mode": "execute" if args.execute else "dry_run", "results": results,
            "next_cursor": (min if args.newest_first else max)([args.after] + [r["message_id"] for r in results]),
            "stopped": any(r["status"] in {"unknown", "commit_attention"} for r in results)}
        (folder / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print("BATCH_RESULT " + json.dumps(report, ensure_ascii=False), flush=True)
    return report


async def run_batches(args):
    """Continue automatically; never retry an uncertain edit or restart at zero."""
    root = Path(args.run_dir).resolve()
    while True:
        cursor = args.after
        args.run_dir = str(root / f"batch-{cursor}") if args.continuous else str(root)
        report = await run(args)
        if report["stopped"] or not args.continuous or not report["results"]:
            return report
        if (report["next_cursor"] >= cursor if args.newest_first else report["next_cursor"] <= cursor):
            raise RuntimeError("reedit_cursor_did_not_advance")
        args.after = report["next_cursor"]
        await asyncio.sleep(max(30, args.batch_interval))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--execute", action="store_true")
    p.add_argument("--no-backup", action="store_true")
    p.add_argument("--source", default="zufang555")
    p.add_argument("--after", type=int, default=0)
    p.add_argument("--limit", type=int, default=5)
    p.add_argument("--run-dir", required=True)
    p.add_argument("--continuous", action="store_true")
    p.add_argument("--batch-interval", type=int, default=30)
    p.add_argument("--newest-first", action="store_true")
    asyncio.run(run_batches(p.parse_args()))
