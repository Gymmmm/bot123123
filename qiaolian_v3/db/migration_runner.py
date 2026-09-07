from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Iterable


LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL,
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""


@dataclass(frozen=True)
class Migration:
    version: str
    name: str
    sql: str
    checksum: str


def _statements(sql: str) -> Iterable[str]:
    """Yield complete SQLite statements without forcing implicit commits."""
    buf: list[str] = []
    for line in sql.splitlines(keepends=True):
        buf.append(line)
        candidate = ''.join(buf).strip()
        if candidate and sqlite3.complete_statement(candidate):
            yield candidate
            buf = []
    tail = ''.join(buf).strip()
    if tail:
        raise ValueError('incomplete migration SQL')


class MigrationRunner:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.conn.execute('PRAGMA foreign_keys=ON')

    def discover(self) -> list[Migration]:
        root = resources.files('qiaolian_v3.db.migrations')
        found: list[Migration] = []
        for entry in sorted(root.iterdir(), key=lambda p: p.name):
            if not entry.name.endswith('.sql'):
                continue
            version = entry.name.split('_', 1)[0]
            sql = entry.read_text(encoding='utf-8')
            found.append(Migration(
                version=version,
                name=entry.name,
                sql=sql,
                checksum=hashlib.sha256(sql.encode('utf-8')).hexdigest(),
            ))
        return found

    def _ensure_ledger(self) -> None:
        self.conn.execute(LEDGER_DDL)

    def applied(self) -> dict[str, str]:
        self._ensure_ledger()
        rows = self.conn.execute('SELECT version, checksum FROM schema_migrations').fetchall()
        return {str(row[0]): str(row[1]) for row in rows}

    def pending(self) -> list[Migration]:
        applied = self.applied()
        pending: list[Migration] = []
        for migration in self.discover():
            old = applied.get(migration.version)
            if old is None:
                pending.append(migration)
            elif old != migration.checksum:
                raise RuntimeError(f'migration_checksum_mismatch:{migration.version}')
        return pending

    def _execute(self, migration: Migration) -> None:
        for statement in _statements(migration.sql):
            self.conn.execute(statement)

    def migrate(self) -> list[str]:
        self._ensure_ledger()
        applied_versions: list[str] = []
        with self.conn:
            for migration in self.pending():
                self._execute(migration)
                self.conn.execute(
                    'INSERT INTO schema_migrations(version,name,checksum) VALUES (?,?,?)',
                    (migration.version, migration.name, migration.checksum),
                )
                applied_versions.append(migration.version)
            violations = self.conn.execute('PRAGMA foreign_key_check').fetchall()
            if violations:
                raise sqlite3.IntegrityError(f'foreign_key_check_failed:{violations!r}')
        return applied_versions

    def simulate_pending(self) -> list[str]:
        """Execute pending migrations inside a savepoint and always roll back."""
        self._ensure_ledger()
        migrations = self.pending()
        self.conn.execute('SAVEPOINT v3_migration_simulation')
        try:
            for migration in migrations:
                self._execute(migration)
            violations = self.conn.execute('PRAGMA foreign_key_check').fetchall()
            if violations:
                raise sqlite3.IntegrityError(f'foreign_key_check_failed:{violations!r}')
            return [m.version for m in migrations]
        finally:
            self.conn.execute('ROLLBACK TO v3_migration_simulation')
            self.conn.execute('RELEASE v3_migration_simulation')
