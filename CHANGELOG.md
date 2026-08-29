# Changelog

## Unreleased — delivery hardening

- Consolidated the mobile channel-listing work into the single delivery branch.
- Removed the tracked hardcoded Telegram Bot token from current source.
- Changed local smoke tests to run the real pytest suite.
- Unified SQLite bootstrap for both the publishing pipeline and user Bot.
- Added regression coverage for a fresh unified delivery database.
- Corrected obsolete local paths in run and release documentation.
- Added explicit project status and friend-facing handoff documentation.
- Switched delivery acceptance to Gym's new test Bot and test channel instead of blocking on legacy credentials.
