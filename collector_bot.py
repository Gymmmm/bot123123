"""Backward-compatible wrapper for collectors.telegram."""

from collectors import telegram as _telegram

for _name in dir(_telegram):
    if not (_name.startswith("__") and _name.endswith("__")):
        globals()[_name] = getattr(_telegram, _name)


if __name__ == "__main__":
    import runpy

    runpy.run_module("collectors.telegram", run_name="__main__")
