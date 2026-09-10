# V3 Product Polish Round 2 — Reuse Map

Baseline: `5b834f65140ad890b83dacee08e8e8e71836f2bc`

This round is intentionally incremental. It reuses the live V3 domain/services first and does not merge or cherry-pick historical branches wholesale.

| Area | Current V3 source | Historical/reference source | Decision |
|---|---|---|---|
| Publisher manual status → existing channel post | `v3_core/publishing/simple_admin.py`, `v3_core/user_bot/channel_status_sync.py`, shared `v3_core/status_labels.py` | `83bb809...` product-polish implementation | Reuse exact saved `publication_instances` message identity and existing caption/keyboard helpers, but sync the explicitly selected admin status. Do not call the appointment-derived status recomputation path. |
| Admin appointment public ID fallback | `v3_core/user_bot/admin_appointments.py` | `83bb809...` | Port only the read-only published-package snapshot fallback, validating with the existing public-ID normalizer. No ID generation or DB backfill. |
| Appointment admin actions | `admin_notification_plans.py` already accepts `reply_markup`; `SQLiteAppointmentRepository` already owns status updates; `appointment_runtime_effects.py` already owns channel effects | Older appointment/admin flows | Add operational buttons around existing V3 appointment storage/effects; no new appointment table or parallel state machine. |
| User appointment edit/cancel | `v3_core/user_bot/appointment_management.py` | Legacy flow only as UX reference | Keep current V3 implementation; do not reinvent it. |
| Appointment success copy/navigation | `v3_core/user_bot/appointment_success_view.py` | `qiaolian_dual/flows.py` on `codex/publisher-user-polish-20260905` | Polish current V3 page in place: explicit date/time, pending-confirmation state, useful exits. |
| Concrete listing contact | `listing_contact.py`, `telegram_listing_callback.py`, `lead_effects.py`, `source_display.py` | legacy `contact_management()` | Keep current attribution/notification effects. Improve customer copy, listing context and navigation without reintroducing legacy runtime. |
| Listing cards/details/photos | `search_cards.py`, `listing_responses.py`, `telegram_*` adapters | historical customer flow screenshots/old branch | Modify active V3 renderers only; preserve current publication/listing truth and bookability rules. |
| Assurance handover/deposit material | `assurance_views.py`, `telegram_assurance_handler.py` | `qiaolian_dual/callback_rental.py` | Keep current real PNG/PDF assets. One customer click may send the material bundle; simplify copy/navigation and remove duplicate decision steps. |
| Existing/old tenant service | `tenant_bindings_v3` and `SQLiteTenantServiceRepository.get_active_binding()` | old tenant-service UI; PR #49 aftercare branch is only a reference | Restore a useful current-binding entry using the existing table only. Do not import PR #49 aftercare schema. If no active binding exists, present a real contact path instead of a fake workflow. |
| Service copy/navigation | `service_views.py`, `telegram_service_handler.py` | legacy `callback_service.py` | Reuse active V3 callbacks and normalize `联系中文顾问`/return paths. |
| Reply keyboard / home navigation | `telegram_home_ui.py`, `telegram_home_handler.py`, `app.py` | legacy keyboard only as reference | Audit only active V3 buttons; every visible callback/URL must have a real route. |

## Explicit non-reuse

- Do not merge/cherry-pick `83bb809` or any historical product-polish branch wholesale.
- Do not replace `v3_core/status_labels.py` with the older user-bot-local status helpers.
- Do not import PR #49's `rental_cases_v3` / operations tables into this product-polish round.
- Do not modify Collector, Canonical Parser, dedupe, listing identity, Auto Publish Gate, Frozen Publication Package design, durable delivery state, or the DB core schema.
