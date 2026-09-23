#!/usr/bin/env python3
"""Production User Bot entrypoint for the V3 takeover runtime."""

from qiaolian_dual.runtime_guard import (
    acquire_user_bot_polling_lock,
    release_user_bot_polling_lock,
)
from qiaolian_dual.admin_consult import cmd_admin_home, handle_admin_query
from qiaolian_dual.admin_contract import _all_user_admin_ids, _is_admin_user
from qiaolian_dual.admin_contract_ui import (
    cmd_contracts,
    handle_callback as handle_admin_contract_callback,
    handle_message as handle_admin_contract_message,
)
from qiaolian_dual.admin_commands import (
    cmd_deal_done,
    cmd_lead_response,
    cmd_push_all,
    cmd_push_local,
    cmd_repair_update,
)
from qiaolian_dual.jobs import lease_reminder_job
from qiaolian_dual.message_handlers import (
    cmd_admin_add,
    cmd_admin_list,
    cmd_admin_remove,
)
from qiaolian_dual.results_admin import admin_repair_keyboard
from qiaolian_dual.v3_admin_workflow_bridge import handle_v3_admin_workflow
from qiaolian_dual.user_bot import handle_ui_callback as handle_legacy_callback
from qiaolian_dual.user_bot import route_start_arg as handle_legacy_start
from v3_core.user_bot.app import (
    V3UserBotConfig,
    build_takeover_user_bot_dependencies,
    run_v3_user_bot,
)
from v3_core.user_bot.takeover_storage import ProductionAdminAppointmentReader


def main() -> None:
    config = V3UserBotConfig.from_env()
    dependencies = build_takeover_user_bot_dependencies(
        config,
        repair_reply_markup_factory=admin_repair_keyboard,
    )
    run_v3_user_bot(
        config=config,
        dependencies=dependencies,
        admin_appointment_reader=ProductionAdminAppointmentReader(config.db_path),
        admin_command_ids=tuple(sorted(_all_user_admin_ids())),
        admin_authorizer=_is_admin_user,
        admin_home_handler=cmd_admin_home,
        admin_query_handler=handle_admin_query,
        admin_contract_command_handler=cmd_contracts,
        admin_contract_callback_handler=handle_admin_contract_callback,
        admin_contract_text_handler=handle_admin_contract_message,
        admin_workflow_callback_handler=handle_v3_admin_workflow,
        lease_reminder_handler=lease_reminder_job,
        compat_start_handler=handle_legacy_start,
        compat_callback_handler=handle_legacy_callback,
        compat_command_handlers={
            "admin_list": cmd_admin_list,
            "admin_add": cmd_admin_add,
            "admin_remove": cmd_admin_remove,
            "deal_done": cmd_deal_done,
            "lead_response": cmd_lead_response,
            "repair_update": cmd_repair_update,
            "push_local": cmd_push_local,
            "push_all": cmd_push_all,
        },
    )


if __name__ == "__main__":
    lock_handle = acquire_user_bot_polling_lock()
    try:
        main()
    finally:
        release_user_bot_polling_lock(lock_handle)
