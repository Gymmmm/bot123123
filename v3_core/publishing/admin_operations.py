"""Explicit admin operations for the V3 publisher.

This module owns review-state changes and canonical fact corrections.  Telegram
handlers must not update V3 tables directly.  Corrections are recorded in
``canonical_overrides``, refresh the canonical hash, rematerialize listing/offer
projections, invalidate any mutable package_ready artifact, and return the
review to pending.  Approved/published packages remain immutable.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
import sqlite3
from typing import Any

from v3_core.storage.inventory_reader import InventoryReader
from v3_core.storage.inventory_repository import InventoryRepository


EDITABLE_FACT_FIELDS: dict[str, tuple[str, str]] = {
    "project_name": ("项目名称", "text"),
    "project_alias": ("项目别名", "text"),
    "property_type": ("物业类型", "text"),
    "public_location_display": ("公开区域", "text"),
    "layout": ("户型", "text"),
    "size_sqm": ("面积㎡", "float"),
    "floor": ("楼层", "text"),
    "monthly_rent_usd": ("月租 USD", "int"),
    "deposit_payment_terms": ("押付方式", "text"),
    "contract_term_display": ("租期", "text"),
    "available_date": ("可租日期", "text"),
    "highlights": ("房源亮点", "list"),
}

REVIEW_ADMIN_STATUSES = frozenset({"pending", "approved", "hold", "rejected"})


@dataclass(frozen=True)
class QueueCounts:
    pending: int = 0
    approved: int = 0
    hold: int = 0
    rejected: int = 0
    package_ready: int = 0
    package_approved: int = 0
    published: int = 0
    failed_before_send: int = 0
    unknown: int = 0


class PublisherAdminOperations:
    def __init__(
        self,
        *,
        db_path: str,
        reader: InventoryReader,
        inventory: InventoryRepository,
    ) -> None:
        self.db_path = str(db_path)
        self.reader = reader
        self.inventory = inventory

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @classmethod
    def _stable_hash(cls, facts: dict[str, Any]) -> str:
        payload = {key: value for key, value in facts.items() if key != "canonical_facts_hash"}
        return hashlib.sha256(cls._json(payload).encode("utf-8")).hexdigest()

    @staticmethod
    def _coerce(field_name: str, raw_value: Any) -> Any:
        if field_name not in EDITABLE_FACT_FIELDS:
            raise ValueError(f"publisher_field_not_editable:{field_name}")
        _label, kind = EDITABLE_FACT_FIELDS[field_name]
        text = str(raw_value or "").strip()
        if kind == "int":
            cleaned = re.sub(r"[^0-9]", "", text)
            if not cleaned or int(cleaned) <= 0:
                raise ValueError("publisher_edit_requires_positive_integer")
            return int(cleaned)
        if kind == "float":
            cleaned = re.sub(r"[^0-9.]", "", text)
            try:
                value = float(cleaned)
            except (TypeError, ValueError):
                raise ValueError("publisher_edit_requires_positive_number") from None
            if value <= 0:
                raise ValueError("publisher_edit_requires_positive_number")
            return int(value) if value.is_integer() else value
        if kind == "list":
            parts = [
                item.strip()
                for item in re.split(r"[\n,，;；]+", text)
                if item.strip()
            ]
            return list(dict.fromkeys(parts))[:12]
        return text

    @staticmethod
    def _display_title(facts: dict[str, Any]) -> str:
        project = str(facts.get("project_name") or "").strip()
        area = str(
            facts.get("canonical_area_display")
            or (facts.get("public_location_display") if not project else "")
            or ""
        ).strip()
        layout = str(facts.get("layout") or "").strip()
        property_type = str(facts.get("property_type_display") or facts.get("property_type") or "未知").strip()
        parts: list[str] = []
        for value in (project, area, layout, "" if property_type == "未知" else property_type):
            if value and value not in parts:
                parts.append(value)
        return "｜".join(parts) if parts else "待确认房源"

    @staticmethod
    def _refresh_layout_counts(facts: dict[str, Any]) -> None:
        layout = str(facts.get("layout") or "")
        bedroom = re.search(r"(\d{1,2})\s*房", layout)
        bathroom = re.search(r"(\d{1,2})\s*卫", layout)
        if bedroom:
            facts["bedrooms"] = int(bedroom.group(1))
        if bathroom:
            facts["bathrooms"] = int(bathroom.group(1))

    def _assert_not_frozen(self, offer_id: str) -> None:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT package_id,status FROM publication_packages_v3
                   WHERE offer_id=? AND status IN ('approved','published')
                   ORDER BY package_version DESC LIMIT 1""",
                (str(offer_id),),
            ).fetchone()
        if row is not None:
            raise ValueError(f"publisher_offer_has_frozen_package:{row['status']}:{row['package_id']}")

    def set_review_status(
        self,
        *,
        review_id: str,
        status: str,
        operator_user_id: str,
        note: str = "",
    ) -> dict[str, Any]:
        target = str(status or "").strip().lower()
        if target not in {"pending", "hold", "rejected"}:
            raise ValueError(f"publisher_review_status_not_allowed:{target}")
        review = self.reader.review(review_id)
        offer_id = str(review.get("offer_id") or "")
        if offer_id:
            self._assert_not_frozen(offer_id)
        reason = {
            "pending": "review_required",
            "hold": "review_hold",
            "rejected": "review_rejected",
        }[target]
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                """UPDATE review_items
                   SET review_status=?,operator_user_id=?,review_note=?,approved_at=NULL,
                       updated_at=CURRENT_TIMESTAMP
                   WHERE review_id=? AND review_status IN ('pending','approved','hold','rejected')""",
                (target, str(operator_user_id or ""), str(note or "")[:1000], str(review_id)),
            )
            if cur.rowcount != 1:
                raise KeyError(review_id)
            if offer_id:
                conn.execute(
                    """UPDATE listing_offers SET publishable=0,publish_block_reason=?,
                       updated_at=CURRENT_TIMESTAMP WHERE offer_id=?""",
                    (reason, offer_id),
                )
                conn.execute(
                    """UPDATE publication_packages_v3 SET status='superseded',
                       updated_at=CURRENT_TIMESTAMP
                       WHERE offer_id=? AND status='package_ready'""",
                    (offer_id,),
                )
            row = conn.execute("SELECT * FROM review_items WHERE review_id=?", (str(review_id),)).fetchone()
            conn.commit()
        assert row is not None
        return dict(row)

    def edit_review_field(
        self,
        *,
        review_id: str,
        field_name: str,
        raw_value: Any,
        operator_user_id: str,
        reason: str = "publisher_admin_edit",
    ) -> dict[str, Any]:
        review = self.reader.review(review_id)
        listing_id = str(review.get("listing_id") or "")
        offer_id = str(review.get("offer_id") or "")
        canonical_id = str(review.get("canonical_record_id") or "")
        if not listing_id or not offer_id or not canonical_id:
            raise ValueError("review_has_no_materialized_offer")
        self._assert_not_frozen(offer_id)

        canonical = self.reader.canonical(canonical_id)
        facts = dict(canonical.get("facts") or {})
        old_value = facts.get(field_name)
        new_value = self._coerce(field_name, raw_value)
        facts[field_name] = new_value
        if field_name == "layout":
            self._refresh_layout_counts(facts)
        if field_name in {
            "project_name",
            "public_location_display",
            "layout",
            "property_type",
        }:
            facts["display_title"] = self._display_title(facts)
        facts["canonical_facts_hash"] = self._stable_hash(facts)

        self.inventory.add_override(
            canonical_record_id=canonical_id,
            field_name=field_name,
            old_value=old_value,
            new_value=new_value,
            reason=str(reason or "publisher_admin_edit"),
            operator_id=str(operator_user_id or ""),
        )

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """UPDATE canonical_records
                   SET facts_json=?,facts_hash=?,review_status='needs_review'
                   WHERE canonical_record_id=?""",
                (self._json(facts), str(facts["canonical_facts_hash"]), canonical_id),
            )
            conn.execute(
                """UPDATE review_items
                   SET review_status='pending',operator_user_id=?,approved_at=NULL,
                       review_note=?,updated_at=CURRENT_TIMESTAMP WHERE review_id=?""",
                (
                    str(operator_user_id or ""),
                    f"edited:{field_name}"[:1000],
                    str(review_id),
                ),
            )
            conn.execute(
                """UPDATE publication_packages_v3 SET status='superseded',
                   updated_at=CURRENT_TIMESTAMP
                   WHERE offer_id=? AND status='package_ready'""",
                (offer_id,),
            )
            conn.commit()

        listing = self.reader.listing(listing_id)
        self.inventory.upsert_listing(
            listing_id=listing_id,
            canonical_record_id=canonical_id,
            public_listing_id=str(listing.get("public_listing_id") or ""),
            facts=facts,
        )
        offers = self.inventory.sync_offers(listing_id=listing_id, facts=facts)
        rent_offer = next((row for row in offers if str(row["offer_type"]) == "rent"), None)
        if rent_offer is None:
            raise ValueError("publisher_edit_removed_rent_offer")
        new_offer_id = str(rent_offer["offer_id"])
        with self._connect() as conn:
            conn.execute(
                """UPDATE review_items SET offer_id=?,updated_at=CURRENT_TIMESTAMP
                   WHERE review_id=?""",
                (new_offer_id, str(review_id)),
            )
            conn.commit()
        return self.reader.review(review_id)

    def queue_counts(self) -> QueueCounts:
        with self._connect() as conn:
            review_rows = conn.execute(
                "SELECT review_status,COUNT(*) AS n FROM review_items GROUP BY review_status"
            ).fetchall()
            package_rows = conn.execute(
                "SELECT status,COUNT(*) AS n FROM publication_packages_v3 GROUP BY status"
            ).fetchall()
            delivery_rows = conn.execute(
                "SELECT state,COUNT(*) AS n FROM publication_delivery_attempts_v3 GROUP BY state"
            ).fetchall()
        reviews = {str(row["review_status"]): int(row["n"]) for row in review_rows}
        packages = {str(row["status"]): int(row["n"]) for row in package_rows}
        deliveries = {str(row["state"]): int(row["n"]) for row in delivery_rows}
        return QueueCounts(
            pending=reviews.get("pending", 0),
            approved=reviews.get("approved", 0),
            hold=reviews.get("hold", 0),
            rejected=reviews.get("rejected", 0),
            package_ready=packages.get("package_ready", 0),
            package_approved=packages.get("approved", 0),
            published=packages.get("published", 0),
            failed_before_send=deliveries.get("failed_before_send", 0),
            unknown=deliveries.get("unknown", 0),
        )

    def package_rows(self, status: str, *, limit: int = 10, offset: int = 0) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT p.*,l.public_listing_id,l.display_title,o.monthly_rent_usd
                   FROM publication_packages_v3 p
                   LEFT JOIN listings_v3 l ON l.listing_id=p.listing_id
                   LEFT JOIN listing_offers o ON o.offer_id=p.offer_id
                   WHERE p.status=? ORDER BY p.created_at DESC,p.id DESC LIMIT ? OFFSET ?""",
                (str(status), max(1, int(limit)), max(0, int(offset))),
            ).fetchall()
        return [dict(row) for row in rows]

    def delivery_rows(self, state: str, *, limit: int = 10, offset: int = 0) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT d.*,l.public_listing_id,l.display_title
                   FROM publication_delivery_attempts_v3 d
                   LEFT JOIN listings_v3 l ON l.listing_id=d.listing_id
                   WHERE d.state=? ORDER BY d.updated_at DESC,d.id DESC LIMIT ? OFFSET ?""",
                (str(state), max(1, int(limit)), max(0, int(offset))),
            ).fetchall()
        return [dict(row) for row in rows]


__all__ = [
    "EDITABLE_FACT_FIELDS",
    "PublisherAdminOperations",
    "QueueCounts",
    "REVIEW_ADMIN_STATUSES",
]
