# Runtime Cleanup Candidates

## Cross-Project Bot Triage

These are the non-Qiaolian bot folders currently visible under `/Users/a1/projects/`:

- `meichen_bot_remote`
  Keep as the reference copy of the Meichen bot. It is small, code-focused, and contains reusable ideas such as shared `config.json`, admin-editable bot copy, and agent round-robin assignment.
- `meichen_bot_review`
  Archived on 2026-04-20 to `/Users/a1/projects/_archived_projects/meichen_bot_review_20260420_2308`.
  This folder was mostly runtime residue: `.env`, `venv/`, `bot.log`, `__pycache__/`, and JSON working data. The compatibility symlink `/Users/a1/meichen_bot_review` now points to the archived snapshot.
- `qiaolian_dual_autopilot`
  Do not delete. A live local process is still running through the legacy symlink `/Users/a1/qiaolian_dual_autopilot -> /Users/a1/projects/qiaolian_dual_autopilot`.
- `qiaolian_bot`
  Archived on 2026-04-20 to `/Users/a1/projects/_archived_projects/qiaolian_bot_20260420_2310`.
  No active process was found, and it appears to be an older single-bot layout superseded by `run_user_bot.py` plus `v2/run_publisher_bot_v2.py`. The compatibility symlink `/Users/a1/qiaolian_bot` now points to the archived snapshot.
- `rent_channel_bot`
  Hold for manual review. Small and currently inactive, but not yet classified as safe to remove.

## Compatibility Symlink Note

The home-directory paths below are still active compatibility symlinks:

- `/Users/a1/qiaolian_dual_autopilot -> /Users/a1/projects/qiaolian_dual_autopilot`
- `/Users/a1/meichen_bot_remote -> /Users/a1/projects/meichen_bot_remote`
- `/Users/a1/meichen_bot_review -> /Users/a1/projects/meichen_bot_review`

If any project folder is moved or archived, either keep the symlink target valid or update the symlink in the same step.

## Production Keep List

These are the current production entrypoints confirmed from local docs and the server runtime:

- `run_user_bot.py`
- `qiaolian_dual/user_bot.py`
- `collector_bot.py`
- `run_pipeline_autopilot.py`
- `v2/run_publisher_bot_v2.py`
- `v2/qiaolian_publisher_v2/`
- `meihua_publisher.py`
- `v2_admin/admin_server.py`
- `scripts/bootstrap_db.py`
- `scripts/check_workflow.py`
- `scripts/server_deploy.sh`
- `scripts/smoke_server.sh`

## Local Cleanup Candidates

These files are not part of the active runtime map and are either manual helpers, one-off debug tools, or moved test wrappers:

- `test_one_new.py`
- `test_publish_one.py`
- `.pytest_cache/`
- `tmp_preview/`
- `reports/`
- `*.bak_*`

## Server Cleanup Candidates

These were observed on `/opt/qiaolian_dual_bots` and are good cleanup targets after one more production verification pass:

- `tools/publish_houses_csv.py.bak_20260420_134911`
- local-only test files: `test_*.py`
- local-only manual helpers: `manual_test_*.py`
- `.pytest_cache/`

## Do Not Delete Blindly

Keep these until there is an explicit replacement or migration:

- `autopilot_publish_bot.py`
  It is no longer a standalone service, but docs still treat it as a compatibility helper.
- `check_workflow.py`
  Root file stays as the canonical implementation; `scripts/check_workflow.py` is only a wrapper.
- `v2/qiaolian_crawler_session.session`
  Active Telethon collector session.
- `data/discussion_map.json`
- `data/discussion_bridge.json`
  Runtime state files; never overwrite during deploy.

## Suggested Cleanup Order

1. Keep `qiaolian_dual_autopilot` untouched until its local launchd/process usage is fully retired.
2. Archive duplicate or legacy worktrees first:
   `meichen_bot_review` and `qiaolian_bot` are already archived; keep using their home-directory symlinks only if you still need old path compatibility.
3. Delete backup and preview artifacts next.
4. Delete moved manual test wrappers after that.
5. Delete server-side test files only after the current production flow passes one full smoke run.
