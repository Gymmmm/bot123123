# Window B handoff — Issue #25 listing/home adviser copy

Branch: `agent/c03c04c05-listing-home-B`
Base: `90180b834c5b13cb5e96aa51b24a081956bb18d2`

## Completed in-window

- Home/contact CTA locked to `💬 联系中文顾问`.
- Search no-match CTA locked to `💬 联系中文顾问`.
- Listing details title locked to `🏠 租赁详情`.
- Listing details actions preserve current bookability behavior and expose `📅 预约看房` / `📸 更多实拍` / `💬 联系中文顾问` for bookable listings.
- Listing photos end panel is emitted once by the existing callback renderer; response text no longer repeats listing title/price/area/id summary.
- Photos end actions use `🏠 租赁详情` / `📅 预约看房` / `💬 联系中文顾问` when bookable; rented/unbookable listings keep book removed.
- Public listing presenter normalizes placeholder-only string values (`暂无`, `未知`, `--`, `-`, `none`, `null`, `n/a`, `na`, `unknown`) to empty strings so detail rows are omitted instead of showing fake/unknown values.
- Related in-window tests were updated to lock these contracts.
- `listing_photos_panel.py` was not created: the existing out-of-window `telegram_callback_handler._render_photos` already sends all media groups and then exactly one final `send_message` carrying `response.text` + `response.keyboard`, so a second panel abstraction would duplicate the existing seam.

## Boundary evidence for Window E

1. `v3_core/user_bot/telegram_listing_callback.py` is outside Window B and still builds consultation buttons labelled `💬 联系我们` in `_render_contact`. Window E should change those user-visible labels to `💬 联系中文顾问` without changing the consultation side-effect flow.
2. `tests/v3_core/test_autopublish_simplified_contract.py` is outside Window B and still asserts invalid-link copy/button labels containing `联系我们`. Window E should reconcile that expectation together with its owning production invalid-link renderer; Window B intentionally did not edit either file.
3. `v3_core/user_bot/telegram_callback_handler.py` is outside Window B. Inspection confirms `_render_photos` sends each media group first and exactly one final operation message. No wiring change is required for the Window B photos response contract.

## PR #51 selective-reference note

Reviewed `cb36e8ae7a174ca8f942149fed68e294e83305c2`, `bac8cdc44c0a8488b2e34bd6c86bb0a4def8085d`, and `e13d2b7dfb35e2048e94ecd51f1ffc6cb3857ebc`. No whole-PR cherry-pick/merge was used. The accidental `🋩 看相近房源` hunk from `bac8cdc...` was not copied; production remains `🏘 看相近房源`.

## Test status

Required commands for Window B:

```bash
python -m compileall v3_core/user_bot/home_views.py v3_core/user_bot/search_no_match_view.py v3_core/user_bot/listing_responses.py v3_core/user_bot/listing_presenter.py
pytest -q tests/v3_core/test_user_bot_home_views.py tests/v3_core/test_user_bot_listing_responses.py tests/v3_core/test_user_bot_listing_presenter.py
```

Execution status in this connector-only session: **UNVERIFIED**. The GitHub connection can read/write repository content but does not expose a shell/worktree runner or workflow-dispatch action. The repository's `v3-core-extraction-check.yml` is manual or push-limited to `codex/v3-core-extraction-20260908`, so its green status cannot be honestly substituted for the exact Window B commands on this branch. Do not treat tests as PASS until the commands above run against this branch/commit.

## Safety / scope

- No production deployment.
- No systemd/service restart.
- No production Telegram channel/discussion/Bot message sent.
- No production database mutation.
- No integration-branch merge.
- Diff from base is limited to Window B allowlisted files plus this `HANDOFF.md`.
