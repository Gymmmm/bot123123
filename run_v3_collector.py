#!/usr/bin/env python3
"""Production V3 collector executable."""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from v3_core.ingest.dynamic_collector import from_environment as dynamic_from_environment
from v3_core.ingest.telegram_collector import from_environment as base_from_environment


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] v3-collector: %(message)s",
    )
    root = Path(__file__).resolve().parent
    app = dynamic_from_environment(base_from_environment, root)
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        logging.getLogger("v3_collector").info("collector stopped")


if __name__ == "__main__":
    main()
