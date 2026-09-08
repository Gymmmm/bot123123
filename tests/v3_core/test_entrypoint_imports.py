"""V3 executable entrypoints must remain import-safe before systemd cut-over."""
from __future__ import annotations

import importlib


ENTRYPOINT_MODULES = (
    "run_v3_collector",
    "run_v3_canonical_worker",
    "run_v3_publisher_bot",
    "run_v3_user_bot",
    "run_v3_preflight",
)


def test_v3_entrypoints_import_without_starting_services():
    imported = [importlib.import_module(name) for name in ENTRYPOINT_MODULES]
    assert [module.__name__ for module in imported] == list(ENTRYPOINT_MODULES)


def test_v3_entrypoints_expose_explicit_callable_boundary():
    collector = importlib.import_module("run_v3_collector")
    worker = importlib.import_module("run_v3_canonical_worker")
    publisher = importlib.import_module("run_v3_publisher_bot")
    user_bot = importlib.import_module("run_v3_user_bot")
    preflight = importlib.import_module("run_v3_preflight")

    assert callable(collector.main)
    assert callable(worker.main)
    assert callable(publisher.run)
    assert callable(user_bot.run_v3_user_bot)
    assert callable(preflight.main)
