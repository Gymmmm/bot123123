"""运行时接线：把归因挂到现有 lead / 管理员通知，不改前台页面。"""
from __future__ import annotations

_INSTALLED = False


def install_attribution_runtime() -> None:
    global _INSTALLED
    if _INSTALLED:
        return

    from . import search
    from . import results_admin
    from . import callback_admin
    from .attribution import attach_to_lead_payload, remember_touch
    from .attribution_store import apply_lead_attribution_columns, ensure_attribution_schema
    from .admin_consult import consult_action_keyboard, enrich_admin_notification, handle_admin_done

    ensure_attribution_schema()

    original_create_lead = search.create_lead

    def create_lead_with_attribution(
        user,
        *,
        action: str,
        source: str,
        listing_id: str = "",
        area: str = "",
        property_type: str = "",
        budget_min=None,
        budget_max=None,
        payload=None,
    ):
        touch = remember_touch(
            user,
            action=action,
            source=source,
            listing_id=listing_id,
            payload=payload,
        )
        packed = attach_to_lead_payload(payload, touch)
        lead_id = original_create_lead(
            user,
            action=action,
            source=source,
            listing_id=listing_id,
            area=area,
            property_type=property_type,
            budget_min=budget_min,
            budget_max=budget_max,
            payload=packed,
        )
        apply_lead_attribution_columns(lead_id, touch)
        return lead_id

    search.create_lead = create_lead_with_attribution

    original_notify = results_admin._notify_admins

    async def notify_admins_with_attribution(context, *, title, lines, reply_markup=None, show_bell=True):
        next_title, next_lines = enrich_admin_notification(str(title or ""), list(lines or []))
        return await original_notify(
            context,
            title=next_title,
            lines=next_lines,
            reply_markup=reply_markup,
            show_bell=show_bell,
        )

    results_admin._notify_admins = notify_admins_with_attribution

    def admin_lead_keyboard_with_done(*, lead_id, appointment_id, user_id):
        lead = search.db.get_lead(int(lead_id or 0)) or {}
        return consult_action_keyboard(
            lead_id=lead_id,
            appointment_id=appointment_id,
            user_id=user_id,
            listing_id=str(lead.get("listing_id") or ""),
        )

    results_admin.admin_lead_keyboard = admin_lead_keyboard_with_done

    original_handle_admin = callback_admin.handle_admin_callback

    async def handle_admin_callback_with_done(update, context, query, data, user):
        if str(data or "").startswith("adminlead:done:"):
            parts = str(data).split(":")
            if len(parts) == 5 and all(part.isdigit() for part in parts[2:]):
                lead_id, appointment_id, customer_id = map(int, parts[2:])
                handled = await handle_admin_done(
                    update,
                    context,
                    query,
                    lead_id=lead_id,
                    appointment_id=appointment_id,
                    customer_id=customer_id,
                )
                if handled:
                    return callback_admin.MAIN
        return await original_handle_admin(update, context, query, data, user)

    callback_admin.handle_admin_callback = handle_admin_callback_with_done
    _INSTALLED = True
