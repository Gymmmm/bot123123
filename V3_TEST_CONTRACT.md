# V3 Test Contract

V3 is not eligible for production cut-over unless every requirement below is covered by executable tests or an explicit operator preflight check. The locked production behavior baseline for extraction parity remains commit `8e4605cf5cc21dfec3ce30729654b09e39de9abf`.

## Inventory and canonical truth

- Rent and sale classification must remain deterministic.
- Rent and sale may share listing identity, but transaction facts live in `listing_offers`.
- Sale inventory must persist through canonical/inventory storage and must never enter the Telegram rent publisher.
- Sale publication policy is `store_only`; if `publishable` is retained it must be `0` with reason `sale_not_enabled_for_telegram`.
- Same-source updates must update/reuse the canonical listing identity rather than create a duplicate listing.

## Publication eligibility and package freeze

- Telegram rent selection requires `offer_type='rent'`, `publication_policy='telegram_rent'`, `offer_status='active'` and the V3 eligibility gates.
- Automatic publish is allowed only after all qualifying gates pass.
- Admin import/review must not publish without the required explicit approval/confirmation boundary.
- An approved/published publication package is immutable in place.
- Duplicate package/delivery attempts must not create duplicate external Telegram publications.
- Dry-run/preview paths must not cross the Telegram send boundary.

## Telegram publication identity

- One public post is one final cover + caption + inline keyboard.
- Public channel actions are exactly: `details`, `photos`, `book`.
- Public deep links use only the stable public `QL-*` id; internal `l_*` ids must never be exposed.
- A publication instance is the durable external Telegram identity and is distinct from a delivery attempt.
- Any edit of an existing post must use the stored exact `channel_chat_id` + `channel_message_id`; never search or guess a post id.
- Bulk rebuild/edit operations require preview before apply.
- Discussion is disabled by default and is not the `photos` path.

## User Bot

- Pure `/start` renders the complete V3 home surface.
- Channel deep links `details`, `photos`, and `book` resolve only published rent inventory.
- `photos` reads the frozen/package gallery, not discussion.
- Listing consultation records a V3 lead and sends a best-effort admin notification.
- Search results expose only published V3 inventory.
- Appointment persistence uses `appointments_v3` only.
- Appointment submission order is durable appointment -> lead -> availability/channel/admin effects -> user success page.
- Active appointment status rule matches the locked production contract: 0 -> `active`, 1-4 -> `reserved`, 5+ -> `pending`.
- `pending`, `rented`, `inactive`, and `offline` are never automatically relaxed by appointment logic.
- Appointment channel sync edits only the stored exact publication message id.
- Channel/admin notification failures do not delete or duplicate a successfully persisted appointment.
- Tenant/service tickets use V3 tables only and request tokens prevent duplicate repair tickets on retry.
- Assurance PNG/PDF assets are sent from the canonical `assets/v2_2/generated` bundle.

## Runtime and storage safety

- V3 runtime modules must not import legacy `qiaolian_dual`, `v2`, publisher patch modules, old CSV publisher paths, or old media shims.
- Runtime construction must not initialize or migrate SQLite schema implicitly.
- `run_v3_preflight.py --initialize` is the explicit additive V3 schema initialization boundary.
- V3 initialization must not rename, delete, overwrite, or mutate legacy production tables.
- `run_v3_collector.py`, `run_v3_canonical_worker.py`, `run_v3_publisher_bot.py`, and `run_v3_user_bot.py` must import without starting network services.
- Production systemd services must remain unchanged until an explicit cut-over instruction is given.

## Cut-over gate

Before switching production services:

1. `v3-core` CI must be green on the exact candidate SHA.
2. `python run_v3_preflight.py --component all` must pass on the target server/database/environment.
3. The existing production SHA remains the rollback target until post-cut-over verification completes.
4. No systemd unit is modified automatically by extraction/refactor commits.
