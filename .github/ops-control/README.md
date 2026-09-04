# Production operations control

Writing `.github/ops-control/request.txt` on branch `ops/gpt-production-control`
triggers the safe production workflow. The file must contain exactly two lines:

1. one operation: `inspect`, `health`, `latest-draft`, `logs`, or `restart`;
2. a unique request identifier containing only letters, digits, `.`, `_`, `:`, or `-`.

The workflow does not accept shell commands. It does not read `.env`, tokens, or
database files directly. Database inspection uses SQLite read-only/query-only mode,
and journal message bodies are omitted from output.
