"""Telegram collector with hot-reloadable operator source identities.

Only the source identity list is dynamic.  Every collected post still goes
through ``TelegramCollectorApp.handle_single/handle_album`` and therefore keeps
the existing collector media policy, IntakeService gate, parser, dedupe and
publication rules unchanged.
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from telethon import TelegramClient, events

from .source_registry import AdminSourceRegistry
from .telegram_collector import TelegramCollectorApp


log = logging.getLogger("v3_collector")


class DynamicTelegramCollectorApp(TelegramCollectorApp):
    async def _register_source(self, client: TelegramClient, cfg: dict[str, Any]) -> bool:
        source_cfg = dict(cfg)
        source_name = str(source_cfg["source_name"])
        self.runtime.ensure_source(source_name, enabled=True)
        try:
            entity = await client.get_entity(source_cfg["entity_id"])
        except Exception as exc:
            self.runtime.touch_source(source_name, error=f"{type(exc).__name__}: {exc}")
            log.exception("collector source resolve failed source=%s", source_name)
            return False

        async def _single(event: Any, config=source_cfg):
            name = str(config["source_name"])
            if not self.runtime.source_enabled(name):
                return
            try:
                result = await self.handle_single(event, config)
            except Exception as exc:
                self.runtime.touch_source(name, error=f"{type(exc).__name__}: {exc}")
                self.runtime.heartbeat("collector", state="running", event=True, error=f"{type(exc).__name__}: {exc}")
                log.exception("collector intake failed source=%s", name)
                return
            self._record_result(name, result)

        async def _album(event: Any, config=source_cfg):
            name = str(config["source_name"])
            if not self.runtime.source_enabled(name):
                return
            try:
                result = await self.handle_album(event, config)
            except Exception as exc:
                self.runtime.touch_source(name, error=f"{type(exc).__name__}: {exc}")
                self.runtime.heartbeat("collector", state="running", event=True, error=f"{type(exc).__name__}: {exc}")
                log.exception("collector album failed source=%s", name)
                return
            self._record_result(name, result)

        client.add_event_handler(
            _single,
            events.NewMessage(chats=entity, func=lambda event: event.grouped_id is None),
        )
        client.add_event_handler(_album, events.Album(chats=entity))
        return True

    async def run(self) -> None:
        registry = AdminSourceRegistry(base_path=self.sources_path, db_path=self.db_path)
        initial = registry.rows()
        if not initial:
            raise RuntimeError("sources.json 中没有可用源")

        client = TelegramClient(self.session_path, self.api_id, self.api_hash)
        await client.start()
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        registered: set[str] = set()
        try:
            while True:
                current = {str(row["source_name"]): row for row in registry.rows()}

                # Removed sources are stopped immediately through the existing
                # runtime enabled flag. Their registered handler remains safe
                # because it checks this flag before every intake.
                for name in registered - set(current):
                    self.runtime.set_source_enabled(name, False)

                for name, cfg in current.items():
                    if name in registered:
                        continue
                    # ensure_source inside _register_source intentionally does
                    # not overwrite an operator's existing pause state.
                    if await self._register_source(client, cfg):
                        registered.add(name)

                if not registered:
                    raise RuntimeError("所有采集源均无法连接")
                self.runtime.heartbeat(
                    "collector",
                    state="running",
                    event=True,
                    meta={"configured_sources": len(current), "registered_sources": len(registered)},
                )

                # Shield Telethon's disconnect future: wait_for must not cancel
                # it on each registry polling timeout.
                try:
                    await asyncio.wait_for(asyncio.shield(client.disconnected), timeout=10)
                    break
                except asyncio.TimeoutError:
                    continue
        finally:
            heartbeat_task.cancel()
            self.runtime.heartbeat("collector", state="stopped")
            await client.disconnect()


def from_environment(base_factory: Any, repo_root: str | Path | None = None) -> DynamicTelegramCollectorApp:
    base = base_factory(repo_root)
    return DynamicTelegramCollectorApp(
        db_path=base.db_path,
        sources_path=base.sources_path,
        download_dir=str(base.download_dir),
        session_path=base.session_path,
        api_id=base.api_id,
        api_hash=base.api_hash,
        min_listing_images=base.intake.min_listing_images,
    )


__all__ = ["DynamicTelegramCollectorApp", "from_environment"]
