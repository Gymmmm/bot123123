import sqlite3
from types import SimpleNamespace

from scripts.v3 import backfill_source_history as history


def test_history_client_uses_memory_and_does_not_mutate_live_session(tmp_path, monkeypatch):
    path = tmp_path / 'collector.session'
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE sessions(dc_id INTEGER,server_address TEXT,port INTEGER,auth_key BLOB)')
        conn.execute('INSERT INTO sessions VALUES(2,?,?,?)', ('149.154.167.51',443,b'x'*256))
    before = path.read_bytes()
    monkeypatch.setenv('BACKFILL_READ_ONLY_SESSION','1')
    calls = []
    monkeypatch.setattr(history, 'TelegramClient', lambda *args, **kwargs: calls.append((args,kwargs)))
    history.history_client(SimpleNamespace(session_path=str(path)[:-8], api_id=1,api_hash='test-only'))
    assert calls[0][0][0].__class__.__name__ == 'MemorySession'
    assert calls[0][1]['receive_updates'] is False
    assert path.read_bytes() == before
