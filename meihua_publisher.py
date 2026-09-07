"""Backward-compatible wrapper for publishers.meihua."""

from publishers import meihua as _meihua

for _name in dir(_meihua):
    if not (_name.startswith("__") and _name.endswith("__")):
        globals()[_name] = getattr(_meihua, _name)


if __name__ == "__main__":
    import runpy

    runpy.run_module("publishers.meihua", run_name="__main__")
