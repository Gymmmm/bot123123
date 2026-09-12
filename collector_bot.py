"""Backward-compatible wrapper for collectors.telegram."""

if __name__ == "__main__":
    import runpy

    runpy.run_module("collectors.telegram", run_name="__main__")
else:
    import sys

    from collectors import telegram as _telegram

    sys.modules[__name__] = _telegram
