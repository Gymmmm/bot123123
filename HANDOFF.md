# Window C handoff

Branch: `agent/c06-deeplink-unbookable-C`
Base: `90180b834c5b13cb5e96aa51b24a081956bb18d2`

## Completed

- `telegram_start_handler.py`: failure-path reason lookup is guarded with `getattr`, so unknown/broadcast fall-through results without `.reason` do not raise `AttributeError`.
- `public_flow.py`: blocked `property_*_book` results with `listing_not_bookable` retain the published listing details view.
- Unbookable book deep links render: `这套房暂时不能预约，可以看相近房源或联系中文顾问。` followed by the current listing details.
- The retained details response is built from the blocked listing itself. Its unbookable action rows contain no `📅 预约看房` button and retain `🏘 看相近房源` with the same `public_listing_id` target.
- Deep-link parser contract was reviewed and left unchanged: official `property_<PUBLIC_ID>_<details|photos|book>` parsing already preserves `public_listing_id` and action.
- Tests were strengthened for blocked-book details/property context and for the missing-`.reason` regression path.

## Files changed

- `v3_core/user_bot/telegram_start_handler.py`
- `v3_core/user_bot/public_flow.py`
- `tests/v3_core/test_user_bot_public_flow.py`
- `tests/v3_core/test_user_bot_broadcast_shortcuts.py`
- `HANDOFF.md`

`v3_core/user_bot/deeplink.py` and `tests/v3_core/test_user_bot_deeplink_contract.py` were reviewed but did not require code changes.

## Verification status

Required commands for Window C:

```bash
python -m compileall v3_core/user_bot/telegram_start_handler.py v3_core/user_bot/public_flow.py v3_core/user_bot/deeplink.py
pytest -q tests/v3_core/test_user_bot_public_flow.py tests/v3_core/test_user_bot_deeplink_contract.py tests/v3_core/test_user_bot_broadcast_shortcuts.py
```

The current execution container cannot resolve `github.com`, so the repository cannot be cloned/materialized here for local command execution. The branch push also did not automatically start a GitHub Actions run for the current head. Therefore these commands are **not reported as PASS** in this handoff; they must be run by Window E/integration CI before production acceptance.

Static/remote diff verification completed: branch is based exactly on `90180b83`, contains only Window C allowed production/test files plus this handoff, and does not modify deployment, database, channel adapter/sync, appointment transition/submit, workflow, schema, migration, collector, ingest, canonical, cover, or publisher files.

## Production safety

- 未部署。
- 未重启任何生产服务。
- 未向正式频道、正式讨论群或正式 Bot 用户发送 Telegram 消息。
- 未修改生产数据库。
- 未合并到 `chief/issue25-cta-appt-10commits`。
