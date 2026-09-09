from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.assurance_views import assurance_asset_bundle, build_assurance_home_view
from v3_core.user_bot.telegram_assurance_handler import handle_v3_assurance_callback


class FakeMessage:
    def __init__(self, calls, *, fail_delete=False):
        self.calls = calls
        self.fail_delete = fail_delete

    async def delete(self):
        self.calls.append(("delete",))
        if self.fail_delete:
            raise RuntimeError("cannot_delete")


class FakeQuery:
    def __init__(self, data, calls, *, fail_delete=False):
        self.data = data
        self.calls = calls
        self.message = FakeMessage(calls, fail_delete=fail_delete)

    async def answer(self):
        self.calls.append(("answer",))


class FakeBot:
    def __init__(self, calls):
        self.calls = calls

    async def send_photo(self, **kwargs):
        self.calls.append(("photo", kwargs["chat_id"]))

    async def send_document(self, **kwargs):
        self.calls.append(("document", kwargs["chat_id"]))

    async def send_message(self, **kwargs):
        self.calls.append(("message", kwargs["chat_id"], kwargs["text"], kwargs["reply_markup"]))


def _update(query):
    return SimpleNamespace(
        callback_query=query,
        effective_chat=SimpleNamespace(id=888),
    )


def _context(bot):
    return SimpleNamespace(bot=bot, user_data={})


def _write_bundle(root, kind):
    generated = root / "assets" / "v2_2" / "generated"
    generated.mkdir(parents=True)
    (generated / f"{kind}.png").write_bytes(b"png")
    (generated / f"{kind}.pdf").write_bytes(b"pdf")


def test_assurance_home_uses_only_v3_callbacks():
    view = build_assurance_home_view()
    callbacks = [choice.callback_data for row in view.rows for choice in row if choice.callback_data]
    assert callbacks == [
        "v3u:assure:handover",
        "v3u:assure:deposit",
        "v3u:assure:moving",
        "v3u:home:contact",
    ]
    assert not any(value.startswith("hub:") for value in callbacks)


def test_assurance_asset_bundle_uses_existing_locked_asset_paths(tmp_path):
    bundle = assurance_asset_bundle(tmp_path, "handover")
    assert bundle.image_path == tmp_path / "assets" / "v2_2" / "generated" / "handover.png"
    assert bundle.pdf_path == tmp_path / "assets" / "v2_2" / "generated" / "handover.pdf"
    assert bundle.title == "入住交接"


@pytest.mark.asyncio
async def test_asset_callback_replaces_panel_then_sends_png_pdf_and_confirmation(tmp_path):
    _write_bundle(tmp_path, "handover")
    calls = []
    query = FakeQuery("v3u:assure:handover", calls)
    outcome = await handle_v3_assurance_callback(
        _update(query),
        _context(FakeBot(calls)),
        repo_root=tmp_path,
    )

    assert outcome.handled and outcome.assets_sent
    assert [call[0] for call in calls] == ["answer", "delete", "photo", "document", "message"]
    markup = calls[-1][3]
    callbacks = [button.callback_data for row in markup.inline_keyboard for button in row]
    assert callbacks == ["v3u:home:contact", "v3u:home:rental"]


@pytest.mark.asyncio
async def test_panel_delete_failure_is_non_fatal_like_fixed_sha(tmp_path):
    _write_bundle(tmp_path, "deposit")
    calls = []
    query = FakeQuery("v3u:assure:deposit", calls, fail_delete=True)

    outcome = await handle_v3_assurance_callback(
        _update(query),
        _context(FakeBot(calls)),
        repo_root=tmp_path,
    )

    assert outcome.assets_sent
    assert [call[0] for call in calls] == ["answer", "delete", "photo", "document", "message"]


@pytest.mark.asyncio
async def test_missing_assurance_files_fail_before_bot_sends(tmp_path):
    calls = []
    query = FakeQuery("v3u:assure:handover", calls)
    with pytest.raises(FileNotFoundError):
        await handle_v3_assurance_callback(
            _update(query),
            _context(FakeBot(calls)),
            repo_root=tmp_path,
        )
    assert [call[0] for call in calls] == ["answer", "delete"]
