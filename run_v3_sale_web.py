#!/usr/bin/env python3
"""Optional read-only V3 sale catalog web entrypoint.

Sale offers are produced by the existing V3 collector/canonical/inventory flow.
Starting this web service never reparses sources, mutates inventory, or changes
publication policy.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

from v3_core.sale.web import serve


def run() -> None:
    load_dotenv()
    root = Path(__file__).resolve().parent
    db_path = Path(
        os.getenv("V3_DB_PATH")
        or os.getenv("DB_PATH")
        or os.getenv("SQLITE_PATH")
        or "data/qiaolian_dual_bot.db"
    ).expanduser()
    index_path = Path(os.getenv("V3_SALE_WEB_INDEX") or root / "web" / "sale" / "index.html")
    host = os.getenv("V3_SALE_WEB_HOST", "127.0.0.1")
    port = int(os.getenv("V3_SALE_WEB_PORT", "8091"))
    serve(db_path=db_path, index_path=index_path, host=host, port=port)


if __name__ == "__main__":
    run()
