#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from telethon import TelegramClient

ROOT = Path('/opt/qiaolian_v3/current').resolve()
load_dotenv('/opt/qiaolian_v3/runtime.env', override=True)
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

from v3_core.ingest.telegram_collector import from_environment, load_sources, sender_label  # noqa: E402

SOURCE_NAME = os.getenv('BACKFILL_SOURCE_NAME', 'zufang555').strip()
TARGET_GROUPS = max(1, int(os.getenv('BACKFILL_TARGET_GROUPS', '50') or 50))
FETCH_LIMIT = max(TARGET_GROUPS, int(os.getenv('BACKFILL_FETCH_LIMIT', '1200') or 1200))


async def main() -> None:
    app = from_environment(ROOT)
    sources = load_sources(app.sources_path)
    cfg = next((dict(x) for x in sources if str(x.get('source_name', '')).strip() == SOURCE_NAME), None)
    if cfg is None:
        raise SystemExit(f'BACKFILL_FATAL source_not_found={SOURCE_NAME}')

    client = TelegramClient(app.session_path, app.api_id, app.api_hash)
    await client.start()
    try:
        entity = await client.get_entity(cfg['entity_id'])
        messages: list[Any] = []
        async for m in client.iter_messages(entity, limit=FETCH_LIMIT):
            messages.append(m)

        albums: dict[int, list[Any]] = defaultdict(list)
        singles: list[Any] = []
        for m in messages:
            gid = getattr(m, 'grouped_id', None)
            if gid is None:
                singles.append(m)
            else:
                albums[int(gid)].append(m)

        units: list[tuple[str, list[Any]]] = []
        units.extend(('album', sorted(parts, key=lambda m: int(m.id))) for parts in albums.values())
        units.extend(('single', [m]) for m in singles)
        units.sort(key=lambda u: max(int(m.id) for m in u[1]), reverse=True)
        selected = units[:TARGET_GROUPS]

        inserted = duplicates = ignored = failed = images = 0
        first_id = last_id = 0
        for kind, parts in reversed(selected):
            newest_id = max(int(m.id) for m in parts)
            if not first_id:
                first_id = newest_id
            last_id = newest_id
            try:
                if kind == 'album':
                    anchor = parts[0]
                    raw_images: list[dict[str, Any]] = []
                    raw_text = ''
                    has_video = False
                    for m in parts:
                        if getattr(m, 'video', None):
                            has_video = True
                        if getattr(m, 'media', None):
                            await app.append_supported_media(client, m, raw_images)
                        text = str(getattr(m, 'message', '') or '').strip()
                        if text and not raw_text:
                            raw_text = text
                    if not raw_images and not raw_text and not has_video:
                        ignored += 1
                        continue
                    gid = getattr(anchor, 'grouped_id', None)
                    result = app.persist(
                        source_cfg=cfg,
                        chat_id=int(anchor.chat_id),
                        source_post_id=f'album_{gid}' if gid is not None else f'album_{anchor.id}',
                        anchor_message_id=int(anchor.id),
                        raw_text=raw_text,
                        raw_images=raw_images,
                        grouped_id=int(gid) if gid is not None else None,
                        source_author=sender_label(anchor),
                        ingest_kind='history_album',
                        message_count=len(parts),
                    )
                    images += len(raw_images)
                else:
                    m = parts[0]
                    raw_text = str(getattr(m, 'message', '') or '')
                    raw_images: list[dict[str, Any]] = []
                    mode = 'text'
                    if getattr(m, 'media', None):
                        mode = await app.append_supported_media(client, m, raw_images)
                    if mode == 'unsupported' and not raw_text:
                        ignored += 1
                        continue
                    result = app.persist(
                        source_cfg=cfg,
                        chat_id=int(m.chat_id),
                        source_post_id=str(m.id),
                        anchor_message_id=int(m.id),
                        raw_text=raw_text,
                        raw_images=raw_images,
                        grouped_id=None,
                        source_author=sender_label(m),
                        ingest_kind=f'history_{mode}',
                        message_count=1,
                    )
                    images += len(raw_images)

                if result.status == 'duplicate':
                    duplicates += 1
                else:
                    inserted += 1
            except Exception as exc:
                failed += 1
                print(f'BACKFILL_ERROR id={newest_id} type={type(exc).__name__} error={exc}')

        print(
            'BACKFILL_RESULT '
            f'source={SOURCE_NAME} scanned_messages={len(messages)} logical_groups={len(units)} '
            f'selected={len(selected)} inserted={inserted} duplicates={duplicates} '
            f'ignored={ignored} failed={failed} images={images} oldest_selected_id={first_id} newest_selected_id={last_id}'
        )
    finally:
        await client.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
