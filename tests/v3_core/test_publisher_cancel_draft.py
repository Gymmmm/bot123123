import sqlite3

from v3_core.publishing.autopilot import AutoPublishRepository


def test_cancel_manual_draft_removes_only_unsent_manual_task(tmp_path):
    db = tmp_path / "publisher.db"
    with sqlite3.connect(db) as conn:
        conn.execute("""CREATE TABLE publisher_auto_items_v3 (
            offer_id TEXT PRIMARY KEY, origin TEXT, state TEXT, ignored INTEGER,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        conn.executemany(
            "INSERT INTO publisher_auto_items_v3(offer_id,origin,state,ignored) VALUES (?,?,?,0)",
            [("manual_draft", "manual", "preview_ready"),
             ("manual_confirm", "manual", "held"),
             ("manual_sent", "manual", "published"),
             ("collector_queued", "collector", "queued")],
        )
    repo = AutoPublishRepository(db)
    for offer_id in ("manual_draft", "manual_confirm", "manual_sent", "collector_queued"):
        repo.cancel_manual_draft(offer_id)
    with sqlite3.connect(db) as conn:
        rows = dict((offer_id, (state, ignored)) for offer_id, state, ignored in conn.execute(
            "SELECT offer_id,state,ignored FROM publisher_auto_items_v3"
        ))
    assert rows == {
        "manual_draft": ("ignored", 1),
        "manual_confirm": ("ignored", 1),
        "manual_sent": ("published", 0),
        "collector_queued": ("queued", 0),
    }
