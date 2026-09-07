"""Backward-compatible wrapper for renderers.cover."""

if __name__ == "__main__":
    import runpy

    runpy.run_module("renderers.cover", run_name="__main__")
else:
    import sys

    from renderers import cover as _cover

    sys.modules[__name__] = _cover
