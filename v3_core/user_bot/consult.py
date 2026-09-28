"""Published-only consultation intent for the side-by-side V3 User Bot.

Consultation is intentionally separated from lead persistence and admin
notification. Fixed-SHA allows users to contact us about a previously
published rent listing even when that listing is pending, rented, or offline.
The only visibility prerequisite here is the durable public publication
boundary enforced by ``PublicInventoryReader``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from v3_core.publishing.public_ids import normalize_public_id

from .public_inventory import PublicInventoryReader


ConsultStatus = Literal["ok", "invalid_link", "not_found"]


@dataclass(frozen=True)
class ConsultIntent:
    listing_id: str
    public_listing_id: str
    source: str
    inventory_status: str
    offer_status: str
    publication_instance_id: str
    touchpoint: str = ""


@dataclass(frozen=True)
class ConsultResult:
    status: ConsultStatus
    public_listing_id: str = ""
    reason: str = ""
    intent: ConsultIntent | None = None

    @property
    def ok(self) -> bool:
        return self.status == "ok" and self.intent is not None


class ConsultService:
    """Resolve one consultation intent without writing lead/admin state."""

    def __init__(self, inventory: PublicInventoryReader):
        self.inventory = inventory

    def resolve(
        self,
        public_listing_id: object,
        *,
        source: str = "listing_callback",
        touchpoint: str = "",
    ) -> ConsultResult:
        public_id = normalize_public_id(public_listing_id)
        if public_id is None:
            return ConsultResult(
                status="invalid_link",
                reason="invalid_public_listing_id",
            )

        view = self.inventory.resolve(public_id)
        if view is None:
            return ConsultResult(
                status="not_found",
                public_listing_id=public_id,
                reason="listing_not_publicly_published",
            )
        if not view.action_allowed("consult"):
            raise AssertionError("published_consult_must_be_allowed")

        return ConsultResult(
            status="ok",
            public_listing_id=public_id,
            intent=ConsultIntent(
                listing_id=view.listing_id,
                public_listing_id=public_id,
                source=str(source or "").strip(),
                inventory_status=str(
                    view.listing.get("inventory_status") or ""
                ).strip().lower(),
                offer_status=str(
                    view.offer.get("offer_status") or ""
                ).strip().lower(),
                publication_instance_id=str(
                    view.publication.get("instance_id") or ""
                ).strip(),
                touchpoint=str(touchpoint or "").strip(),
            ),
        )


ConsultationEnvelopeKind = Literal["ticket", "lease"]


@dataclass(frozen=True)
class ConsultationEnvelope:
    kind: ConsultationEnvelopeKind
    reference_id: str
    property_name: str = ""

    @property
    def reference_label(self) -> str:
        return "TicketID" if self.kind == "ticket" else "LeaseID"

    @property
    def subject_label(self) -> str:
        return "报修工单" if self.kind == "ticket" else "租约"

    def admin_details(self) -> str:
        lines = [
            f"{self.subject_label}咨询",
            f"{self.reference_label}：#{self.reference_id}",
        ]
        if self.property_name:
            lines.append(f"房源：{self.property_name}")
        return "\n".join(lines)

    def handoff_text(self) -> str:
        text = f"你好，我想咨询{self.subject_label} #{self.reference_id}。"
        if self.property_name:
            text += f"\n房源：{self.property_name}"
        return text


def build_consultation_envelope(
    kind: ConsultationEnvelopeKind,
    reference_id: object,
    *,
    property_name: object = "",
) -> ConsultationEnvelope:
    clean_kind = str(kind or "").strip().lower()
    if clean_kind not in {"ticket", "lease"}:
        raise ValueError("unsupported_consultation_envelope")
    clean_id = str(reference_id or "").strip().lstrip("#")
    if not clean_id:
        raise ValueError("consultation_reference_id_required")
    return ConsultationEnvelope(
        kind=clean_kind,  # type: ignore[arg-type]
        reference_id=clean_id,
        property_name=str(property_name or "").strip(),
    )


def consultation_handoff_url(
    advisor_url: object,
    envelope: ConsultationEnvelope,
) -> str:
    raw = str(advisor_url or "").strip()
    if not raw:
        return ""
    parts = urlsplit(raw)
    if parts.scheme in {"http", "https"} and parts.netloc.lower() in {
        "t.me", "www.t.me", "telegram.me", "www.telegram.me",
        "telegram.dog", "www.telegram.dog",
    }:
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        query["text"] = envelope.handoff_text()
        return urlunsplit(
            (parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment)
        )
    return raw


__all__ = ["ConsultIntent", "ConsultResult", "ConsultService", "ConsultationEnvelope", "ConsultationEnvelopeKind", "build_consultation_envelope", "consultation_handoff_url"]
