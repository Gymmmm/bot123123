#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv
from telegram import Bot
from telegram.error import NetworkError, RetryAfter, TimedOut

ROOT = Path('/opt/qiaolian_v3/current').resolve()
load_dotenv('/opt/qiaolian_v3/runtime.env', override=True)
os.chdir(ROOT)

from v3_core.publishing.admin_bot import load_settings
from v3_core.publishing.delivery_coordinator import TelegramSendCommand
from v3_core.publishing.package_service import PackageBuildService
from v3_core.publishing.package_store import FrozenPackageStore
from v3_core.publishing.publication_instances import PublicationInstanceRepository
from v3_core.publishing.telegram_adapter import TelegramChannelAdapter, build_channel_keyboard
from v3_core.storage.inventory_reader import InventoryReader

SOURCE = 'zufang555'
TARGET = 50
INTERVAL = 8.0
EXPECTED_SHA = 'cfe49d2624ce83a5a7eed91c17cd599710d37504'


def source_anchor(row: sqlite3.Row) -> int:
    try:
        meta = json.loads(str(row['raw_meta_json'] or '{}'))
    except Exception:
        meta = {}
    try:
        value = int(meta.get('anchor_message_id') or 0)
    except Exception:
        value = 0
    if value > 0:
        return value
    try:
        tail = urlparse(str(row['source_url'] or '')).path.rstrip('/').rsplit('/', 1)[-1]
        return int(tail) if tail.isdigit() else 0
    except Exception:
        return 0


def source_identity(raw: str) -> tuple[str, str]:
    try:
        obj = json.loads(raw or '{}')
    except Exception:
        return '', ''
    return (
        str(obj.get('source_name') or obj.get('name') or ''),
        str(obj.get('source_post_id') or obj.get('post_id') or obj.get('message_id') or ''),
    )


def select_targets(db: Path) -> tuple[list[dict], set[str]]:
    with sqlite3.connect(str(db), timeout=30) as c:
        c.row_factory = sqlite3.Row
        rows = c.execute(
            'SELECT id,source_post_id,source_url,raw_meta_json FROM source_posts WHERE source_name=?',
            (SOURCE,),
        ).fetchall()
        selected = sorted(rows, key=lambda r: (source_anchor(r), int(r['id'])), reverse=True)[:TARGET]
        allowed = {str(r['source_post_id']) for r in selected}
        pubs = c.execute(
            '''SELECT i.* , p.source_identity_json
               FROM publication_instances i
               JOIN publication_packages_v3 p ON p.package_id=i.package_id
               WHERE i.platform='telegram' AND i.publish_status='published'
                 AND CAST(COALESCE(i.channel_message_id,'0') AS INTEGER)>0'''
        ).fetchall()

    candidates = []
    for row in pubs:
        name, post = source_identity(str(row['source_identity_json'] or ''))
        if name == SOURCE and post in allowed:
            item = dict(row)
            item['_source_post_id'] = post
            candidates.append(item)

    latest_by_offer: dict[str, dict] = {}
    for item in candidates:
        offer = str(item['offer_id'])
        old = latest_by_offer.get(offer)
        if old is None or int(item['channel_message_id']) > int(old['channel_message_id']):
            latest_by_offer[offer] = item
    targets = sorted(latest_by_offer.values(), key=lambda x: int(x['channel_message_id']))
    return targets, allowed


def hold_packages(db: Path, offer_id: str) -> list[tuple[str, str]]:
    held = []
    with sqlite3.connect(str(db), timeout=30) as c:
        c.row_factory = sqlite3.Row
        c.execute('BEGIN IMMEDIATE')
        rows = c.execute(
            "SELECT package_id,status FROM publication_packages_v3 WHERE offer_id=? AND status IN ('approved','published')",
            (offer_id,),
        ).fetchall()
        for row in rows:
            cur = c.execute(
                "UPDATE publication_packages_v3 SET status='rebuild_hold',updated_at=CURRENT_TIMESTAMP WHERE package_id=? AND status=?",
                (row['package_id'], row['status']),
            )
            if cur.rowcount == 1:
                held.append((str(row['package_id']), str(row['status'])))
        c.commit()
    return held


def restore_held(db: Path, held: list[tuple[str, str]]) -> None:
    with sqlite3.connect(str(db), timeout=30) as c:
        c.execute('BEGIN IMMEDIATE')
        for package_id, status in held:
            c.execute(
                "UPDATE publication_packages_v3 SET status=?,updated_at=CURRENT_TIMESTAMP WHERE package_id=? AND status='rebuild_hold'",
                (status, package_id),
            )
        c.commit()


def finalize(db: Path, pub: dict, old, new, caption: str, held: list[tuple[str, str]]) -> None:
    with sqlite3.connect(str(db), timeout=30) as c:
        c.execute('BEGIN IMMEDIATE')
        for package_id, _ in held:
            c.execute(
                "UPDATE publication_packages_v3 SET status='superseded',updated_at=CURRENT_TIMESTAMP WHERE package_id=? AND status='rebuild_hold'",
                (package_id,),
            )
        cur = c.execute(
            "UPDATE publication_packages_v3 SET status='published',published_at=COALESCE(published_at,CURRENT_TIMESTAMP),updated_at=CURRENT_TIMESTAMP WHERE package_id=? AND status='package_ready'",
            (new.package_id,),
        )
        if cur.rowcount != 1:
            raise RuntimeError('new_package_publish_transition_failed')
        cur = c.execute(
            '''UPDATE publication_instances SET package_id=?,listing_id=?,offer_id=?,post_text=?,publish_status='published',updated_at=CURRENT_TIMESTAMP
               WHERE instance_id=? AND channel_message_id=?''',
            (new.package_id, new.listing_id, new.offer_id, caption, pub['instance_id'], pub['channel_message_id']),
        )
        if cur.rowcount != 1:
            raise RuntimeError('publication_rebind_failed')
        c.commit()


async def edit_retry(adapter, command, message_id: str):
    last = None
    for attempt in range(1, 5):
        try:
            return await adapter.edit(command, message_id=message_id)
        except RetryAfter as exc:
            last = exc
            await asyncio.sleep(int(getattr(exc, 'retry_after', 5) or 5) + 2)
        except (TimedOut, NetworkError) as exc:
            last = exc
            await asyncio.sleep(3 * attempt)
    raise last


async def main() -> None:
    deployed = Path('/opt/qiaolian_v3/.deployed_sha').read_text().strip()
    if deployed != EXPECTED_SHA or ROOT.name != EXPECTED_SHA:
        raise SystemExit(f'REEDIT_BLOCKED production={deployed} current={ROOT.name}')

    settings = load_settings()
    db = Path(settings.db_path)
    reader = InventoryReader(db)
    packages = FrozenPackageStore(db)
    builder = PackageBuildService(reader=reader, store=packages, user_bot_username=settings.user_bot_username, advisor_url=settings.advisor_url)
    targets, allowed = select_targets(db)
    print('REEDIT_SCOPE latest50=', len(allowed), 'existing_unique_targets=', len(targets), flush=True)
    if not targets:
        raise SystemExit('REEDIT_NO_TARGETS')

    bot = Bot(token=settings.token)
    adapter = TelegramChannelAdapter(bot)
    ok = failed = 0
    before_ids = [str(x['channel_message_id']) for x in targets]
    try:
        for index, pub in enumerate(targets, 1):
            held = []
            try:
                old = packages.get(pub['package_id'])
                if old is None:
                    raise KeyError(pub['package_id'])
                if str(pub['_source_post_id']) not in allowed:
                    raise RuntimeError('scope_escape')
                held = hold_packages(db, old.offer_id)
                new = builder.build(
                    listing_id=old.listing_id,
                    offer_id=old.offer_id,
                    cover_style=old.cover_style,
                    cover_path=old.cover_path,
                    gallery=list(old.gallery),
                    source_identity=old.source_identity,
                )
                listing = reader.listing(old.listing_id)
                status = str(listing.get('inventory_status') or 'pending').lower()
                command = TelegramSendCommand(
                    attempt_id='latest50-reedit:' + str(pub['instance_id']),
                    package_id=new.package_id,
                    channel_chat_id=str(pub['channel_chat_id']),
                    cover_path=new.cover_path,
                    caption=new.post_text,
                    actions=dict(new.actions),
                    inventory_status=status,
                )
                kb = build_channel_keyboard(command.actions, inventory_status=status)
                labels = [[b.text for b in row] for row in kb.inline_keyboard]
                result = await edit_retry(adapter, command, str(pub['channel_message_id']))
                finalize(db, pub, old, new, str(result.get('caption') or command.caption), held)
                ok += 1
                print('REEDIT_OK', index, pub['channel_message_id'], pub['_source_post_id'], status, labels, flush=True)
            except Exception as exc:
                if held:
                    try:
                        restore_held(db, held)
                    except Exception as restore_exc:
                        print('RESTORE_FAIL', pub['channel_message_id'], type(restore_exc).__name__, str(restore_exc)[:160], flush=True)
                failed += 1
                print('REEDIT_FAIL', index, pub['channel_message_id'], type(exc).__name__, str(exc)[:220], flush=True)
            if index < len(targets):
                await asyncio.sleep(INTERVAL)
    finally:
        await bot.shutdown()

    after_targets, _ = select_targets(db)
    after_ids = [str(x['channel_message_id']) for x in after_targets]
    print('REEDIT_RESULT selected=', len(targets), 'ok=', ok, 'failed=', failed, 'message_ids_preserved=', before_ids == after_ids, flush=True)
    if failed or ok != len(targets):
        raise SystemExit(2)


if __name__ == '__main__':
    asyncio.run(main())
