# Spending Analyzer — coding handoff

Snapshot: **2026-11-02 UTC**. This is a takeover map, not release authorization. Recheck Git status, the requested scope and current runtime before acting. Existing AGENTS.md and linked contract/runbooks remain applicable. Older dated status notes are evidence from their stated date, not current permissions.

## Purpose and architecture

Spending Analyzer imports statements and supports transaction review, tagging, filtering and read/edit sharing.

FastAPI/Python with PostgreSQL; directly served single-page HTML/JavaScript, no frontend bundler. Statement extraction and categorization are separate OpenAI paths; imports and classifications retain provenance.

## Locations and current work

- Usual local workspace: `/Users/jgreco/code/spending`.
- Source inspected for this handoff: `/Users/jgreco/Documents/Codex/2026-10-25/task/integration-2026-10-30/spending`.
- Existing repository: [jimgreco/spending_analyzer](https://github.com/jimgreco/spending_analyzer); default branch `main`, verified source head `6c6f74a997b8f9328d2c06be5fd71820d7b73cc5`.
- Documentation branch: `docs/agent-handoff-2026-11-02`, based on that head; application code was not changed.
- Primary checkout is at `11d6fc4fee6926c767e83101e1a27af2db1d1b70` on `main` and differs from remote main. Do not reset it or assume it contains released features; use the inspected source or a new isolated checkout.

## Code and guide map

- [webapp/app.py](../webapp/app.py)
- [webapp/index.html](../webapp/index.html)
- [webapp/requirements.txt](../webapp/requirements.txt)
- [webapp/migrate.py](../webapp/migrate.py)
- [webapp/categorization.py](../webapp/categorization.py)

Read the relevant existing guide before changing its contract:

- [webapp/README.md](../webapp/README.md)
- [docs/database-runtime-access.md](database-runtime-access.md)
- [docs/release-runtime-cutover.md](release-runtime-cutover.md)

## Local setup and dependencies

Commands below are source-verified entry points, **not commands executed during this documentation pass**. Use disposable local data. Read the command comments and linked setup guide before running migrations, creating fixtures or starting listeners. Reuse prepared environments where suitable.

- `python3 -m venv .venv`
- `source .venv/bin/activate`
- `python -m pip install -r webapp/requirements.txt`
- `Configure LOCAL_DEV and an isolated DATABASE_URL; supply local MIGRATION_DATABASE_URL separately.`
- `python webapp/migrate.py`
- `cd webapp && python -m uvicorn app:app --reload --host 127.0.0.1 --port 8000`

Configuration names/locations only; never copy secret values into source, terminal output or handoff documents:

- `LOCAL_DEV`
- `DATABASE_URL`
- `MIGRATION_DATABASE_URL`
- `SECRET_KEY`
- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `OWNER_EMAIL`
- `OPENAI_API_KEY`
- `OPENAI_MODEL`
- `OPENAI_TAG_MODEL`

Use existing lockfiles/manifests; do not upgrade dependencies to perform a handoff. For native work, select an actually installed Xcode SDK/simulator from the checked-in project configuration.

## Tests and verification

- `python3 -m unittest discover -s webapp -p 'test_tag_model.py' -v`
- `python3 -m unittest discover -s webapp -p 'test_*.py' -v` with `SPENDING_TEST_DATABASE_URL` set to a disposable loopback database whose name ends in `_test`; this suite clears its test tables.
- `Database fixtures must use an isolated loopback *_test database. Do not load real statements for a code-only check.`
- `Python syntax/import checks and affected browser/API journeys; CI validates synthetic PostgreSQL behavior and an ARM64 image build.`

Documentation-only changes require path/link, accuracy, diff and secret-exclusion review; do not run heavyweight builds or production smoke mutations for prose changes. Application tests were not rerun in this pass. Past test/release evidence below retains its original date. UI changes still require actual rendered inspection, and synthetic tests do not prove real provider/device acceptance.

## Decisions and constraints

- Device/account saved views, page size, selected-card totals and read-only transaction details already ship. Amount signs, missing values and excluded-from-total rows must remain distinct.
- Filtered CSV uses the same predicates/order as the list, snapshot semantics, size caps, text-only formula neutralization and account fences.
- Current startup validates schema; old AGENTS paragraphs describing startup migrations/Elastic Beanstalk are historical. Current migration and release runbooks govern.

## Release state and workflow effects

- Web/API: deployed and verified **2026-11-02**, source `6c6f74a997b8f9328d2c06be5fd71820d7b73cc5`. Exact revision/readiness and scoped activation were recorded; this documentation pass did not redeploy.
- Main CI validates synthetic PostgreSQL behavior and ARM64 build without deployment. Old deploy.yml refuses activation. Production uses the separately approved image-pinned runtime-cutover process.

Recent exact-source GitHub results observed November 2 (workflow purpose matters):

- [Validate Spending release run 41075859910](https://github.com/jimgreco/spending_analyzer/actions/runs/41075859910): `completed` / `success`.

The handoff is published on a documentation feature branch, not main. Current checked-in GitHub push triggers are main-only (or absent). Blog hosting branch settings were separately checked. No native release, image publication or server deployment is intended from this documentation push.

## Known gaps and next safe work

- October 23 signed-amount-range extra d8ccfd3 remains paused. Avoid silently reclassifying old transactions or using imports as tests.
- No native app/TestFlight path. Production smoke evidence and current deployment receipt are separate from local test results.

Before the next task, verify source HEAD, dirty/staged work and the selected subsystem; read only the relevant guide. Check whether a listed idea is already implemented in another branch before rebuilding it. Preserve unrelated work and paused experiments. Release/permission/data changes require the applicable current authorization.
