import pytest

from v3_core.publishing.delivery_state import (
    DeliveryBlocked,
    PublicationDeliveryStateRepository,
)
from v3_core.publishing.publication_instances import PublicationInstanceRepository
from v3_core.storage.bootstrap import initialize_v3_storage


def _receipt(message_id: int, *, caption: str = "房源") -> dict:
    return {
        "media_message_ids": [message_id],
        "caption_message_id": message_id,
        "button_message_id": "",
        "caption": caption,
    }


def test_delivery_state_machine_is_idempotent_and_blocks_unsafe_retry(tmp_path):
    db = tmp_path / "v3.sqlite3"
    initialize_v3_storage(db)
    repo = PublicationDeliveryStateRepository(str(db))

    attempt = repo.prepare(
        package_id="PKG_1",
        listing_id="l_1",
        offer_id="OFF_1",
        channel_chat_id="-100123",
    )
    assert attempt.state == "prepared"

    same = repo.prepare(
        package_id="PKG_1",
        listing_id="l_1",
        offer_id="OFF_1",
        channel_chat_id="-100123",
    )
    assert same.attempt_id == attempt.attempt_id

    repo.mark_sending(attempt.attempt_id)
    assert repo.get(attempt.attempt_id).state == "sending"

    with pytest.raises(DeliveryBlocked, match="reconcile"):
        repo.prepare(
            package_id="PKG_1",
            listing_id="l_1",
            offer_id="OFF_1",
            channel_chat_id="-100123",
        )

    repo.mark_sent(attempt.attempt_id, _receipt(321))
    sent = repo.get(attempt.attempt_id)
    assert sent.state == "sent"
    assert sent.telegram_result["media_message_ids"] == [321]

    committed = repo.mark_committed(attempt.attempt_id)
    assert committed.state == "committed"
    assert repo.mark_committed(attempt.attempt_id).state == "committed"


def test_unknown_delivery_requires_reconciliation_before_retry(tmp_path):
    db = tmp_path / "v3.sqlite3"
    initialize_v3_storage(db)
    repo = PublicationDeliveryStateRepository(str(db))
    attempt = repo.prepare(
        package_id="PKG_2",
        listing_id="l_2",
        channel_chat_id="-100123",
    )
    repo.mark_sending(attempt.attempt_id)
    repo.mark_unknown(attempt.attempt_id, "timeout")

    assert repo.get(attempt.attempt_id).state == "unknown"
    with pytest.raises(DeliveryBlocked, match="reconcile"):
        repo.prepare(
            package_id="PKG_2",
            listing_id="l_2",
            channel_chat_id="-100123",
        )


def test_publication_instance_keeps_exact_telegram_message_identity(tmp_path):
    db = tmp_path / "v3.sqlite3"
    initialize_v3_storage(db)
    repo = PublicationInstanceRepository(str(db))

    instance = repo.record_telegram_publication(
        package_id="PKG_1",
        listing_id="l_1",
        offer_id="OFF_1",
        channel_chat_id="-100123",
        telegram_result=_receipt(777, caption="A"),
    )
    assert instance.channel_message_id == "777"
    assert instance.caption_message_id == "777"

    same = repo.record_telegram_publication(
        package_id="PKG_1",
        listing_id="l_1",
        offer_id="OFF_1",
        channel_chat_id="-100123",
        telegram_result=_receipt(777, caption="A"),
    )
    assert same.instance_id == instance.instance_id

    with pytest.raises(DeliveryBlocked, match="conflicts"):
        repo.record_telegram_publication(
            package_id="PKG_1",
            listing_id="l_1",
            offer_id="OFF_1",
            channel_chat_id="-100123",
            telegram_result=_receipt(778, caption="B"),
        )
