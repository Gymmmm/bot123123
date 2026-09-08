#!/usr/bin/env python3
"""Side-by-side V3 collector executable. Not wired to production systemd."""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from v3_core.ingest.telegram_collector import from_environment


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] v3-collector: %(message)s",
    )
    app = from_environment(Path(__file__).resolve().parent)
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        logging.getLogger("v3_collector").info("collector stopped")


if __name__ == "__main__":
    main()
