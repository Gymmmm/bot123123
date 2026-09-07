from tests.v3._helpers import make_conn


def test_source_posts_schema_supports_revision_and_content_hash():
    conn = make_conn()
    cols = {row[1] for row in conn.execute('PRAGMA table_info(source_posts)')}
    assert {'revision', 'content_hash', 'ingest_origin'} <= cols


def test_revision_can_advance_without_changing_source_identity():
    conn = make_conn()
    conn.execute("INSERT INTO source_posts (id,source_type,source_name,source_post_id,raw_text,content_hash) VALUES (1,'telegram_channel','x','99','a','h1')")
    before = conn.execute("SELECT source_type,source_name,source_post_id,revision FROM source_posts WHERE id=1").fetchone()
    conn.execute("UPDATE source_posts SET raw_text='b', content_hash='h2', revision=revision+1 WHERE id=1")
    after = conn.execute("SELECT source_type,source_name,source_post_id,revision FROM source_posts WHERE id=1").fetchone()
    assert before[:3] == after[:3]
    assert after[3] == before[3] + 1
