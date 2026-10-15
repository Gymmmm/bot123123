import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from v3_core.publishing.simple_admin import SimplePublisherAdminController


def test_source_toggle_requires_confirmation_before_mutation():
    controller = object.__new__(SimplePublisherAdminController)
    controller.runtime = SimpleNamespace(set_source_enabled=Mock())
    controller.show_sources = AsyncMock()
    message = SimpleNamespace(reply_text=AsyncMock())
    context = SimpleNamespace(user_data={})

    async def invoke(data):
        update = SimpleNamespace(callback_query=SimpleNamespace(data=data, message=message))
        return await controller.handle_callback(update, context)

    assert asyncio.run(invoke('v3smp|source_toggle|0|test_source')) is True
    controller.runtime.set_source_enabled.assert_not_called()
    markup = message.reply_text.await_args.kwargs['reply_markup']
    callbacks = [b.callback_data for row in markup.inline_keyboard for b in row]
    assert 'v3smp|source_apply|0|test_source' in callbacks
    assert 'v3smp|sources' in callbacks
    assert asyncio.run(invoke('v3smp|source_apply|0|test_source')) is True
    controller.runtime.set_source_enabled.assert_called_once_with('test_source', False)
