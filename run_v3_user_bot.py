#!/usr/bin/env python3
"""Production V3 User Bot entrypoint with the existing admin console adapter."""
from qiaolian_dual.admin_consult import cmd_admin_home, handle_admin_query
from qiaolian_dual.admin_contract import _all_user_admin_ids, _is_admin_user
from v3_core.user_bot.app import run_v3_user_bot


if __name__ == "__main__":
    run_v3_user_bot(
        admin_command_ids=tuple(sorted(_all_user_admin_ids())),
        admin_authorizer=_is_admin_user,
        admin_home_handler=cmd_admin_home,
        admin_query_handler=handle_admin_query,
    )
