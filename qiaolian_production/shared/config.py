"""Compatibility config for extracted top-level production modules.

The original root-level helpers import ``config`` when they are executed as
standalone modules. In the extracted runtime those helpers live under
``qiaolian_production/shared``; re-export the canonical User Bot config so they
never fall back to the repository-root copy.
"""
from qiaolian_dual.config import *  # noqa: F401,F403
from qiaolian_dual.config import DB_PATH, logger  # explicit public dependencies
