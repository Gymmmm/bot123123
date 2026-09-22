from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_production_user_entry_starts_3858_with_polling_guard():
    source = _read("run_v3_user_bot.py")

    assert "from qiaolian_dual.user_bot import main" in source
    assert "acquire_user_bot_polling_lock" in source
    assert "release_user_bot_polling_lock" in source
    assert "lock_handle = acquire_user_bot_polling_lock()" in source
    assert "main()" in source
    assert "release_user_bot_polling_lock(lock_handle)" in source

    assert "v3_core.user_bot.app" not in source
    assert "run_v3_user_bot" not in source
    for handler_name in (
        "handle_v3_start",
        "handle_v3_home_callback",
        "handle_v3_listing_callback",
        "handle_v3_transition_action",
        "handle_v3_transition_text",
        "handle_v3_service_callback",
        "handle_v3_keyword_search_text",
        "build_home_view",
        "build_home_keyboard",
    ):
        assert handler_name not in source


def test_run_component_keeps_single_user_entry_and_other_commands_unchanged():
    source = _read("deploy/v3/run_component.sh")

    assert source.count('exec "$PYTHON" "$CODE_ROOT/run_v3_user_bot.py"') == 1
    assert 'exec "$PYTHON" "$CODE_ROOT/run_v3_collector.py"' in source
    assert 'exec "$PYTHON" "$CODE_ROOT/run_v3_canonical_worker.py" \\' in source
    assert 'exec "$PYTHON" "$CODE_ROOT/run_v3_publisher_bot.py"' in source


def test_3858_application_keeps_admin_user_routes_and_single_lease_reminder():
    source = _read("qiaolian_dual/app.py")

    assert "CommandHandler('contracts', cmd_contracts)" in source
    assert "CommandHandler('admin', cmd_admin_home)" in source
    assert "CallbackQueryHandler(handle_admin_query, pattern=r'^adminq:')" in source
    assert "CommandHandler('start', start_with_attribution)" in source
    assert "CommandHandler('find', cmd_find)" in source
    assert "CommandHandler('appointments', cmd_appointments)" in source
    assert "CommandHandler('service', cmd_service)" in source
    assert "CommandHandler('contact', cmd_contact)" in source
    assert "CallbackQueryHandler(handle_ui_callback, pattern=_MAIN_CB_PATTERN)" in source
    assert "MessageHandler(filters.TEXT & ~filters.COMMAND, handle_main_message)" in source
    assert source.count("run_daily(lease_reminder_job") == 1


def test_3858_admin_callback_keeps_lead_and_repair_closure():
    source = _read("qiaolian_dual/callback_admin.py")

    assert "data.startswith('adminrepair:')" in source
    assert "data.startswith('adminlead:')" in source
    assert "if action == 'done':" in source
    assert "from .admin_consult import handle_admin_done" in source
    assert "await handle_admin_done(" in source
    assert "'claim': ('claimed', 'assigned'" in source
    assert "'contacted': ('contacted', 'contacted'" in source
    assert "'invalid': ('invalid', 'cancelled'" in source
