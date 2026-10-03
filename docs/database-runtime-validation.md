# Spending Analyzer: synthetic database validation, 3 October 2026

42 unit checks passed and 25 DB checks were skipped without a DSN; the full synthetic PostgreSQL suite passed 67/67 with no skips. Separate restricted-runtime tests passed startup, authenticated API reads, amount reversal, immutable-original-amount invoker trigger, CRUD/sequences and upload job completion/staleness. Historical filename/description/tag fixtures remained unchanged across startup and existing-schema adoption.

The final clean-room run used a newly initialized PostgreSQL 16 cluster under `/tmp/task4-db-*`, bound only to its mode-0700 UNIX socket. It was stopped after completion. No production database, backup or secret was used; test roles were NOLOGIN and no passwords were created. Application connections assumed the intended restricted role; independent negative tests used session authorization. Fault-injection observer access was kept separate from application pools.

Passed for this app:

- Empty-schema initialization, idempotent repeat, explicit adoption of existing schema, rejection of unadopted/missing/drifted schemas, no runtime fallback for the migration connection, and runtime rejection of an accidentally injected migration connection.
- Failure injected into an actual schema transaction left no tables or compatibility version committed.
- Runtime denied DDL, ledger writes, elevated-role membership, database creation, credential-catalog reads and other apps' table access. Future table, sequence and function access stayed closed.
- Application table contents compared equal before/after adoption.
- Custom-format synthetic backup restored with `--exit-on-error --no-owner --no-privileges --role`; table contents/ownership matched, and runtime access was absent until grants were reapplied.
- The exact [proposed grant SQL](database-access-proposal.sql) was rehearsed on a restored copy, substituting only the guarded target database name. Owner identity, migration SET ROLE membership, default privileges, runtime startup and denied temporary-table creation passed.
- `git diff --check` passed.

Evidence and reproducible test scripts are retained in the coordinator workspace `/Users/jgreco/Documents/Codex/2026-10-03/task-4/evidence/` and `tests/`. `final-validation.log` ends in `FINAL VALIDATION PASSED`; JSON summaries and `test-script-sha256.json` identify the checks and harness.

This is app-code/SQL preparation. Live catalog comparison, fresh app-specific private backup/isolated restore, secure credential handoff, final grant/ACL approval and coordinated cutover remain outstanding. No push or deployment occurred. Full browser, Docker and native release gates were not run in this local preparation and remain required where the repository's release workflow requires them.
