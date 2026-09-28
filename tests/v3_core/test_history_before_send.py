from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.error import NetworkError

from scripts.v3 import reedit_channel_posts as editor
from v3_core.storage.bootstrap import initialize_v3_storage


@pytest.mark.asyncio
async def test_initial_connection_failure_is_retryable_without_delivery_attempt(tmp_path, monkeypatch):
    db = tmp_path / 'history.sqlite3'
    initialize_v3_storage(db)
    settings = SimpleNamespace(db_path=str(db), user_bot_username='test_bot', advisor_url='https://t.me/test_advisor', token='unused')
    monkeypatch.setattr(editor, 'load_settings', lambda: settings)
    bot = SimpleNamespace(initialize=AsyncMock(side_effect=NetworkError('offline')), shutdown=AsyncMock())
    monkeypatch.setattr(editor, 'Bot', lambda token: bot)
    args = SimpleNamespace(run_dir=str(tmp_path / 'run'), newest_first=True, after=99999,
                           limit=1, execute=True, no_backup=True)
    with pytest.raises(editor.BeforeSendNetworkError):
        await editor.run(args)
    with editor.connect(db) as conn:
        assert conn.execute('SELECT COUNT(*) FROM publication_delivery_attempts_v3').fetchone()[0] == 0
    assert bot.shutdown.await_count == 1
