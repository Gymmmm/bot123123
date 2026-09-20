from __future__ import annotations

from pathlib import Path


def _app_source() -> str:
    return Path("v3_core/user_bot/app.py").read_text(encoding="utf-8")


def test_start_preserves_existing_telegram_deeplink_args():
    source = _app_source()
    assert 'async def _run_start_payload(update, context, payload: str | None = None):' in source
    assert 'if payload is not None:' in source
    assert 'context.args = [payload] if payload else []' in source
    start_block = source.split('async def start(update, context):', 1)[1].split('async def find(update, context):', 1)[0]
    assert 'await _run_start_payload(update, context)' in start_block
    assert 'await _run_start_payload(update, context, "")' not in start_block


def test_plain_start_still_uses_empty_args_when_telegram_supplies_none():
    source = _app_source()
    helper = source.split('async def _run_start_payload', 1)[1].split('async def start(update, context):', 1)[0]
    assert 'previous_args = tuple(getattr(context, "args", None) or ())' in helper
    assert 'if payload is not None:' in helper
    assert 'finally:' in helper
    assert 'context.args = list(previous_args)' in helper
