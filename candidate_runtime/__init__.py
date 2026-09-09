"""Isolated runtime harness for V3 candidate testing.

This package never changes production systemd units. It only resolves candidate
state paths, performs explicit V3-native bootstrap/preflight, and dispatches the
existing V3 entrypoints.
"""

from .runtime_env import RuntimePaths, configure_candidate_environment

__all__ = ["RuntimePaths", "configure_candidate_environment"]
