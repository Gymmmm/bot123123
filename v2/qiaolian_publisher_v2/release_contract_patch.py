from __future__ import annotations

import json
import sqlite3
from typing import Any

_INSTALLED = False


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _effective_package_media_count(row: Any) -> int:
    """Return the number of real frozen/source photos represented by a package.

    Current one-cover packages intentionally keep only the generated cover in
    ``main_images_json`` and freeze the real photos in ``discussion_images_json``.
    Approval therefore must never use ``len(main_images_json)`` as the listing's
    media count.

    Priority for current packages:
      1. usable frozen discussion/gallery images;
      2. frozen ``source_identity.media_count`` when it proves more source media
         than the gallery list (including empty/partial gallery metadata).

    Old packages created before source identity existed keep their historical
    multi-image behaviour by falling back to the combined frozen image lists.
    """
    try:
        discussion_raw = row["discussion_images_json"]
    except (KeyError, IndexError, TypeError):
        discussion_raw = None
    discussion = [item for item in _json_list(discussion_raw) if str(item or "").strip()]
    discussion_count = len(discussion)

    try:
        identity_raw = row["source_identity_json"]
    except (KeyError, IndexError, TypeError):
        identity_raw = None
    identity: dict[str, Any] = {}
    if isinstance(identity_raw, dict):
        identity = identity_raw
    else:
        try:
            parsed = json.loads(identity_raw or "{}")
            if isinstance(parsed, dict):
                identity = parsed
        except (TypeError, ValueError, json.JSONDecodeError):
            identity = {}
    try:
        identity_count = max(0, int(identity.get("media_count") or 0))
    except (TypeError, ValueError):
        identity_count = 0

    # Once source identity exists, it is the immutable source-level fallback.
    # Do not add the generated cover from main_images_json to the real-photo count.
    if identity:
        return max(discussion_count, identity_count)

    # Legacy packages may have frozen real photos in main_images_json and have
    # no source identity at all. Preserve that historical approval behaviour.
    try:
        main_raw = row["main_images_json"]
    except (KeyError, IndexError, TypeError):
        main_raw = None
    combined: list[str] = []
    seen: set[str] = set()
    for item in [*_json_list(main_raw), *discussion]:
        value = str(item or "").strip()
        if value and value not in seen:
            seen.add(value)
            combined.append(value)
    return len(combined)


class _BotProxy:
    def __init__(self, bot: Any) -> None:
        self._bot = bot

    def __getattr__(self, name: str) -> Any:
        return getattr(self._bot, name)

    async def send_message(self, *args, **kwargs):
        text = kwargs.get("text")
        if text is None and len(args) >= 2:
            text = args[1]
        if isinstance(text, str):
            text = text.replace(
                "📌 评论区：更多实拍与补充说明（独立入口）",
                "📸 更多实拍：点击主帖「更多实拍」查看",
            )
            text = text.replace(
                "点击一套即发送其已冻结的封面、图片、文案和评论区详情；不会临时重写。",
                "点击一套即发送当前已审核的封面、正文和三个行动按钮；更多实拍由用户 Bot 承接。",
            )
            if "QJ" in text:
                import re
                text = re.sub(r"\bQJ[-_]?(\d{1,8})\b", lambda m: f"QC{int(m.group(1)):04d}", text)
            if "text" in kwargs:
                kwargs["text"] = text
            elif len(args) >= 2:
                args = (args[0], text, *args[2:])
        return await self._bot.send_message(*args, **kwargs)


class _ContextProxy:
    def __init__(self, context: Any) -> None:
        self._context = context
        self.bot = _BotProxy(context.bot)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._context, name)


def install_release_contract_patch() -> None:
    """Lock the production flow to current QC/button-layout contracts."""
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True

    import autopilot_publish_bot as ap
    import meihua_publisher as publisher
    import publication_package as package

    original_build_caption = publisher.build_caption

    def patched_build_caption(*args, **kwargs):
        import re
        text = original_build_caption(*args, **kwargs)
        if isinstance(text, str):
            text = re.sub(r"\bQJ[-_]?(\d{1,8})\b", lambda m: f"QC{int(m.group(1)):04d}", text)
        return text

    publisher.build_caption = patched_build_caption

    original_visual_preview = ap._send_visual_preview

    async def patched_visual_preview(*, update, context, row, caption_variant="a"):
        return await original_visual_preview(
            update=update,
            context=_ContextProxy(context),
            row=row,
            caption_variant=caption_variant,
        )

    ap._send_visual_preview = patched_visual_preview

    original_approve = package.approve_package
    original_evaluate_publishability = package.evaluate_publishability

    def patched_approve_package(db_path: str, draft_id: str, approved_by: str = "") -> dict:
        # Never reuse a stale package_ready body. Approved/published packages stay frozen.
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            frozen = conn.execute(
                """SELECT status FROM publication_packages
                   WHERE draft_id=? AND status IN ('approved','published')
                   ORDER BY package_version DESC LIMIT 1""",
                (draft_id,),
            ).fetchone()
        if not frozen:
            package.build_package(db_path, draft_id)

        # Approval has a second publishability gate. The package builder already
        # validated the source media count, but the legacy approval implementation
        # re-counted only main_images_json. With the current one-cover contract that
        # value is always 1, so valid listings were incorrectly blocked.
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            ready = conn.execute(
                """SELECT main_images_json, discussion_images_json, source_identity_json
                   FROM publication_packages
                   WHERE draft_id=? AND status='package_ready'
                   ORDER BY package_version DESC LIMIT 1""",
                (draft_id,),
            ).fetchone()
        effective_media_count = _effective_package_media_count(ready) if ready else 0

        def evaluate_with_frozen_media(facts, *, media_count, cover_exists):
            _ = media_count
            return original_evaluate_publishability(
                facts,
                media_count=effective_media_count,
                cover_exists=cover_exists,
            )

        package.evaluate_publishability = evaluate_with_frozen_media
        try:
            return original_approve(db_path, draft_id, approved_by)
        except ValueError as exc:
            # Reuse the publisher's existing human-readable error mapping instead
            # of surfacing this expected gate result as an opaque/unknown failure.
            if str(exc) == "package_publishability_blocked:insufficient_media":
                raise ValueError("canonical_package_gate_blocked:insufficient_media") from exc
            raise
        finally:
            package.evaluate_publishability = original_evaluate_publishability

    package.approve_package = patched_approve_package
