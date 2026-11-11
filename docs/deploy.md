# Production deployment

Every commit that lands on `main` releases to production once validation passes.
Releases use the shared server engine in
[`jimgreco/release-tools`](https://github.com/jimgreco/release-tools)
(`server/README.md` documents the manifest and every step), pinned by commit.

## Workflows

- `.github/workflows/ci.yml` — validation only. Runs on pull requests, manual
  dispatch, and when `deploy.yml` calls it. No secrets, no environment, no
  remote commands. Its `Required Checks` job is the branch-protection gate.
- `.github/workflows/deploy.yml` — runs on push to `main` (and manual dispatch on
  `main`). Job `validate` calls `ci.yml` without passing secrets; job `release`
  `needs: validate` and calls
  `jimgreco/release-tools/.github/workflows/server-release.yml@<commit>` with the
  same commit as `tools_ref`. Concurrency group `spending-production-deploy`
  queues releases and never cancels one in progress.

`tests/test_release_controls.py` enforces this: a secret-free PR path, releases
only from main after validation, the engine pinned by full commit, no app-local
ssh/rsync/Compose, and a manifest that never drops the DB runtime cutover overlay.

## What a release does

The manifest is `.github/release/server.json`.

1. **Publish.** The prebuild writes `webapp/version.json`
   (`{sha: <7-char commit>, timestamp: <commit time>, version: <webapp/VERSION>}`),
   which `/api/version` serves. The `webapp/` image is built for linux/arm64 and
   pushed as `ghcr.io/jimgreco/spending_analyzer:<commit>`.
2. **Release on the host** (`release.py`, environment `production`, pinned SSH):
   takes the shared release locks, rebuilds the Compose invocation recorded on the
   running `spending` container, and refuses on any config drift. It pulls the
   image and writes `~/deploy/releases/spending-image-<sha>.json`, which sets only
   the image. The recorded overlays stay in order, including any
   `db-runtime-spending-<hash>.json` runtime-role cutover overlay, so
   `DATABASE_URL` and every other setting carry over unchanged. Image-only
   overrides written by the retired helper (`~/deploy/spending-image-<sha>.json`)
   drop out of the chain, and only if nothing but the image changes.
3. **Preflight** in the new image refuses if the runtime environment contains
   `MIGRATION_DATABASE_URL` or lacks a PostgreSQL `DATABASE_URL`.
4. **Backup.** It takes a verified `pg_dump` of database `spending`, which must contain
   `transactions`, into `~/deploy/backups/spending/`.
5. **Replace.** It recreates only `spending` (`--no-deps --no-build`). The health check runs
   inside the container: `/app/version.json` and `http://127.0.0.1:8000/api/version`
   must both report the 7-character SHA. If it fails, the release rolls back to the previous image.
6. **Public check.** `https://spending-analyzer.jim-greco.com/api/version` must report the SHA.

Migrations: the container never migrates on start (startup only asserts schema
compatibility) and releases run none. Schema changes stay a separately reviewed
`migrate.py` run with `MIGRATION_DATABASE_URL`, before the release that needs them.

Records: `~/deploy/release-records/spending-<sha>.json`. Rollback images are tagged
`spending-rollback:<time>-<container>`.

## DB runtime cutover

`consolidated-deploy` `scripts/db-runtime/cutover.py` adds a durable overlay
`db-runtime-spending-<hash>.json` that pins a frozen image and points
`DATABASE_URL` at `${SPENDING_RUNTIME_DATABASE_URL}`. Releases keep that overlay
and supersede only its image. The retired helper also refused to release within
24 hours of a cutover. The engine has no such window. If you run another cutover,
disable `deploy.yml` until its observation window closes.

## Rollback

Revert the commit on `main` and let it release. To roll back by hand, use the
release record's `previousFiles` chain with `up -d --no-deps --no-build spending`.
The previous image is still present under its rollback tag.

## Secrets

The `production` environment, limited to `main`, holds `EC2_HOST`, `EC2_USER`,
`EC2_SSH_KEY` and `EC2_SSH_KNOWN_HOSTS`. The host's existing GHCR login pulls the image.
