# Changelog

## 2026-09-15 — adviser V1 rental details

- Replaced legacy adviser prose with the evidence-only V1 engine: no fee/network signals or fallback, at most two different categories.
- Rental details alone render the adviser heading and bullets; old frozen packages use their frozen facts, not obsolete prose.
- Unified the stable public listing ID seed and removed numeric-floor inference. Added 16 V1 regression cases; complete V3 suite: 584 passed.

## Unreleased — delivery hardening

- Consolidated the mobile channel-listing work into the single delivery branch.
- Removed the tracked hardcoded Telegram Bot token from current source.
- Changed local smoke tests to run the real pytest suite.
- Unified SQLite bootstrap for both the publishing pipeline and user Bot.
- Added regression coverage for a fresh unified delivery database.
- Corrected obsolete local paths in run and release documentation.
- Added explicit project status and friend-facing handoff documentation.
- Switched delivery acceptance to Gym's new test Bot and test channel instead of blocking on legacy credentials.
