"""Ready handoff for the existing history intake and canonical worker."""
import sqlite3


def ensure(conn):
    conn.execute('CREATE TABLE IF NOT EXISTS history_streams_v3(task_id TEXT PRIMARY KEY,status TEXT NOT NULL)')
    conn.execute('CREATE TABLE IF NOT EXISTS history_stream_ready_v3(task_id TEXT,source_id INTEGER,PRIMARY KEY(task_id,source_id))')
    conn.commit()


def set_status(db, task, status):
    with sqlite3.connect(db,timeout=30) as c:
        ensure(c)
        c.execute('INSERT INTO history_streams_v3 VALUES (?,?) ON CONFLICT(task_id) DO UPDATE SET status=excluded.status',(task,status))


def publish_ready(db, task, source_id):
    from v3_core.parser.worker import CanonicalWorker
    worker = CanonicalWorker(db)
    result = worker.process_one(int(source_id))
    with sqlite3.connect(db,timeout=30) as c:
        ensure(c)
        c.execute('INSERT OR IGNORE INTO history_stream_ready_v3 VALUES (?,?)',(task,int(source_id)))
    return result.status
