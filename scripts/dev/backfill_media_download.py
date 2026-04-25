#!/usr/bin/env python3
"""Compatibility wrapper for media recovery.

The old backfill script used pre-v2 media_assets column names. Keep this entry
point so existing operator muscle memory still works, but route all work through
the current owner_type/owner_ref_id based recovery implementation.
"""
from __future__ import annotations

import sys

from media_consistency import main as media_consistency_main


def main() -> None:
    if len(sys.argv) == 1:
        sys.argv.extend(["report", "--limit", "50"])
    media_consistency_main()


if __name__ == "__main__":
    main()
