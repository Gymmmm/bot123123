"""Backward-compatible wrapper for renderers.cover."""

from renderers import cover as _cover

for _name in dir(_cover):
    if not (_name.startswith("__") and _name.endswith("__")):
        globals()[_name] = getattr(_cover, _name)


if __name__ == "__main__":
    import runpy

    runpy.run_module("renderers.cover", run_name="__main__")
