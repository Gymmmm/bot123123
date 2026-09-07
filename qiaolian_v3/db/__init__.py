"""V3 database foundation.

Phase 1 owns schema, migrations, repositories and transaction boundaries only.
It is intentionally not wired into production Parser/Collector/Publisher/User Bot.
"""

from .connection import connect
from .migration_runner import MigrationRunner
from .unit_of_work import UnitOfWork

__all__ = ["connect", "MigrationRunner", "UnitOfWork"]
