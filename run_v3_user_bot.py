#!/usr/bin/env python3
"""Production V3 User Bot entrypoint with complete admin adapters."""
from qiaolian_dual.admin_consult import handle_admin_query
from qiaolian_dual.admin_contract import _all_user_admin_ids, _is_admin_user
from qiaolian_dual.admin_contract_ui import (
    cmd_contracts,
    handle_callback as handle_admin_contract_callback,
    handle_message as handle_admin_contract_text,
)
from qiaolian_dual.v3_admin_workflow_bridge import handle_v3_admin_workflow
from v3_core.user_bot.admin_console_bridge import show_unified_admin_home
from v3_core.user_bot.app import run_v3_user_bot


if __name__ == "__main__":
    run_v3_user_bot(
        admin_command_ids=tuple(sorted(_all_user_admin_ids())),
        admin_authorizer=_is_admin_user,
        admin_home_handler=show_unified_admin_home,
        admin_query_handler=handle_admin_query,
        admin_contract_command_handler=cmd_contracts,
        admin_contract_callback_handler=handle_admin_contract_callback,
        admin_contract_text_handler=handle_admin_contract_text,
        admin_workflow_callback_handler=handle_v3_admin_workflow,
    )
