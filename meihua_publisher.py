"""Backward-compatible wrapper for publishers.meihua."""

if __name__ == "__main__":
    import runpy

    runpy.run_module("publishers.meihua", run_name="__main__")
else:
    import sys

    from publishers import meihua as _meihua

    sys.modules[__name__] = _meihua
