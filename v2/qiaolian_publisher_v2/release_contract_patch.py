from __future__ import annotations

import sqlite3
from typing import Any

_INSTALLED = False


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
        try:
            return original_approve(db_path, draft_id, approved_by)
        except ValueError as exc:
            # Reuse the publisher's existing human-readable error mapping instead
            # of surfacing this expected gate result as an opaque/unknown failure.
            if str(exc) == "package_publishability_blocked:insufficient_media":
                raise ValueError("canonical_package_gate_blocked:insufficient_media") from exc
            raise

    package.approve_package = patched_approve_package
