import ast
import inspect

import pytest

from qiaolian_dual import start_routes
from qiaolian_dual.runtime_guard import (
    acquire_user_bot_polling_lock,
    release_user_bot_polling_lock,
)


def test_start_route_imports_no_match_keyboard_before_use():
    """Regression: channel /start deep links must not NameError on no-match path."""
    source = inspect.getsource(start_routes.route_start_arg)
    tree = ast.parse(source)

    imported = {
        alias.asname or alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.level == 1
        and node.module == "keyboards_common"
        for alias in node.names
    }
    loaded = {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
    }

    assert "no_match_followup_keyboard" in loaded
    assert "no_match_followup_keyboard" in imported


def test_user_bot_polling_lock_rejects_second_instance(tmp_path):
    lock_path = str(tmp_path / "qiaolian-user-bot.lock")
    first = acquire_user_bot_polling_lock(lock_path)
    try:
        with pytest.raises(RuntimeError, match="已有 polling 实例"):
            acquire_user_bot_polling_lock(lock_path)
    finally:
        release_user_bot_polling_lock(first)

    # A stopped process must release the kernel lock even if the lock file stays.
    second = acquire_user_bot_polling_lock(lock_path)
    release_user_bot_polling_lock(second)
