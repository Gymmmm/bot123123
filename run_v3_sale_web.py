#!/usr/bin/env python3
"""Optional V3 sale catalog web entrypoint.

On start it applies today's sale inventory scan against the shared V3 database,
then serves the sale-only page. Sale offers stay store_only.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from dotenv import load_dotenv

from v3_core.sale.align import SaleInventoryAligner
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
    force = str(os.getenv("V3_SALE_ALIGN_FORCE") or "").strip().lower() in {"1", "true", "yes"}
    stats = SaleInventoryAligner(db_path).align_if_due(force=force)
    print(json.dumps({"sale_align": stats}, ensure_ascii=False, sort_keys=True), flush=True)
    serve(db_path=db_path, index_path=index_path, host=host, port=port)


if __name__ == "__main__":
    run()
