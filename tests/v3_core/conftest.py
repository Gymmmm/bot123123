"""Tests never reach a real vision API, whatever the developer's shell exports."""
from __future__ import annotations

import os

import pytest

_VISION_ENV = ("GEMINI_API_KEY",)


@pytest.fixture(autouse=True)
def _no_real_vision_provider(monkeypatch):
    for name in list(os.environ):
        if name.startswith("V3_VISION_") or name in _VISION_ENV:
            monkeypatch.delenv(name, raising=False)

    def _blocked(*args, **kwargs):
        raise AssertionError("tests must not call a real vision API")

    monkeypatch.setattr("v3_core.media.vision._http_post_json", _blocked)
