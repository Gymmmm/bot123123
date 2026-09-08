#!/usr/bin/env python3
"""Side-by-side V3 User Bot entrypoint. Does not alter production systemd."""
from v3_core.user_bot.app import run_v3_user_bot


if __name__ == "__main__":
    run_v3_user_bot()
