"""Small final product-policy overrides for the V3 Publisher.

Keep these rules close to application composition so the public publishing
contract can be tightened without reopening the legacy publisher paths.
"""
from __future__ import annotations

from typing import Any


def apply_publisher_product_policy() -> None:
    """Lock final Publisher adviser and manual-gate behavior.

    1. Publisher-generated adviser copy is always a single point.
    2. A stale canonical ``missing_layout`` flag cannot block manual publish
       after the operator flow has explicitly repaired/populated ``listing.layout``.
    """
    from . import package_service, publisher_adviser_ui
    from .publisher_adviser_ui import PublisherAdviserAdminController

    if getattr(PublisherAdviserAdminController, "_final_product_policy_applied", False):
        return

    original_generate = package_service.generate_adviser_text

    def generate_one_point(*args: Any, **kwargs: Any) -> str:
        kwargs["max_points"] = 1
        return original_generate(*args, **kwargs)

    # Both modules imported the generator directly, so patch both references.
    package_service.generate_adviser_text = generate_one_point
    publisher_adviser_ui.generate_adviser_text = generate_one_point

    original_manual_blockers = PublisherAdviserAdminController._manual_blockers

    async def manual_blockers(self: Any, detail: Any):
        blockers, media = await original_manual_blockers(self, detail)
        if "canonical_error" not in blockers:
            return blockers, media

        facts = dict(detail.canonical.get("facts") or {})
        quality = facts.get("quality") if isinstance(facts.get("quality"), dict) else {}
        blocking_flags = {
            str(flag).strip()
            for flag in (quality.get("blocking_flags") or [])
            if str(flag).strip()
        }
        layout = str(detail.listing.get("layout") or "").strip()

        # Manual intake may repair a labelled layout such as 房型：2+1 after the
        # canonical quality snapshot was created. Once the explicit listing
        # field is present, that old flag is resolved for this manual publish.
        if layout and blocking_flags and blocking_flags <= {"missing_layout"}:
            blockers = [code for code in blockers if code != "canonical_error"]
        return blockers, media

    PublisherAdviserAdminController._manual_blockers = manual_blockers
    PublisherAdviserAdminController._final_product_policy_applied = True


__all__ = ["apply_publisher_product_policy"]
