#!/usr/bin/env python3
"""Side-by-side V3 Publisher admin entrypoint; not wired to systemd yet."""
from v3_core.publishing.admin_bot import run


if __name__ == "__main__":
    run()
