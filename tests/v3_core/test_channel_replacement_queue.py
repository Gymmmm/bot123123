import json
import sqlite3

import pytest

from scripts.v3.replace_channel_history import snapshot


def database():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE publication_instances(id INTEGER,instance_id TEXT,channel_message_id TEXT,
            platform TEXT,publish_status TEXT,channel_chat_id TEXT,media_group_id TEXT);
        CREATE TABLE source_posts(id INTEGER,raw_meta_json TEXT,source_name TEXT,parse_status TEXT DEFAULT 'parsed');
        CREATE TABLE canonical_records(source_post_id TEXT,canonical_record_id TEXT);
        CREATE TABLE listings_v3(listing_id TEXT,canonical_record_id TEXT);
        CREATE TABLE listing_offers(offer_id TEXT,listing_id TEXT,offer_type TEXT,
            offer_status TEXT,publication_policy TEXT);
    """)
    for i in (2, 9, 4):
        c.execute("INSERT INTO publication_instances VALUES (?,?,?,'telegram','published','-1001','')", (i, f"p{i}", str(i)))
    for i, anchor in ((1, 100), (2, 500), (3, 200), (4, 0)):
        c.execute("INSERT INTO source_posts(id,raw_meta_json,source_name) VALUES (?,?,?)", (i, json.dumps({"anchor_message_id": anchor}), "zufang555"))
        c.execute("INSERT INTO canonical_records VALUES (?,?)", (str(i), f"c{i}"))
        c.execute("INSERT INTO listings_v3 VALUES (?,?)", (f"l{i}", f"c{i}"))
        c.execute("INSERT INTO listing_offers VALUES (?,?,'rent','active','telegram_rent')", (f"o{i}", f"l{i}"))
    return c


def test_both_queues_descend_by_real_message_id_not_database_insertion_id():
    state = snapshot(database(), "zufang555", ("-1001", "@channel"))
    assert [r["channel_message_id"] for r in state["targets"]] == ["9", "4", "2"]
    assert [r["anchor"] for r in state["sources"]] == [500, 200, 100]
    assert [r["source_id"] for r in state["sources"]] == [2, 3, 1]
    assert state["group_successful"] == 0


def test_unrelated_channel_is_not_a_replacement_target():
    with pytest.raises(ValueError, match="targets_or_sources_empty"):
        snapshot(database(), "zufang555", ("-1002", "@other"))


def test_duplicate_offers_do_not_repeat_the_same_listing():
    c = database()
    c.execute("INSERT INTO listing_offers VALUES ('extra','l2','rent','active','telegram_rent')")
    assert len(snapshot(c, "zufang555", ("-1001", "@channel"))["sources"]) == 3
