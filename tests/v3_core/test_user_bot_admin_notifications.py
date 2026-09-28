from __future__ import annotations

import pytest

from v3_core.user_bot.admin_notifications import AdminNotification, TelegramAdminNotifier


class FakeBot:
    def __init__(self, fail_ids=()):
        self.fail_ids = set(fail_ids)
        self.calls = []

    async def send_message(self, **kwargs):
        self.calls.append(dict(kwargs))
        if kwargs["chat_id"] in self.fail_ids:
            raise RuntimeError("send_failed")


def test_notification_text_matches_fixed_sha_bell_and_body_shape():
    note = AdminNotification(
        title="用户联系我们",
        lines=("用户：Alice", "", "入口：hub"),
    )

    assert note.text == "🔔 <b>用户联系我们</b>\n\n用户：Alice\n入口：hub"


@pytest.mark.asyncio
async def test_admin_delivery_is_sorted_unique_and_failure_does_not_stop_others():
    bot = FakeBot(fail_ids={20})
    notifier = TelegramAdminNotifier([30, 20, 10, 20, 0])
    note = AdminNotification(
        title="新预约 #51",
        lines=("客户：Alice",),
        show_bell=False,
    )

    result = await notifier.send(bot, note)

    assert result.attempted_admin_ids == (10, 20, 30)
    assert result.sent_admin_ids == (10, 30)
    assert result.failed_admin_ids == (20,)
    assert [call["chat_id"] for call in bot.calls] == [10, 20, 30]
    assert all(call["disable_web_page_preview"] is True for call in bot.calls)
