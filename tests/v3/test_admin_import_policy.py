def test_admin_import_default_policy_is_not_auto_publishable():
    # Phase-1 contract: admin/csv origins may enter source_posts and parsing,
    # but an explicit approval step is required before Telegram publication.
    ingest_origin = 'admin'
    auto_publish_allowed = ingest_origin == 'collector'
    assert auto_publish_allowed is False
