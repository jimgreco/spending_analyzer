# Production deployment

Every commit that lands on `main` deploys to production once validation passes.
This matches Ritual Cue (`daily-checklist` `deploy-server` job) and replaces the
October 2026 validation-only policy.

## Workflows

- `.github/workflows/ci.yml` — validation only. Runs on pull requests, manual
  dispatch, and when `deploy.yml` calls it. No secrets, no environment, no
  remote commands. Its `Required Checks` job is the branch-protection gate.
- `.github/workflows/deploy.yml` — runs on push to `main` (and manual dispatch on
  `main`). Job `validate` calls `ci.yml` without passing secrets; job `deploy`
  `needs: validate`, so it runs only after every CI job passes. Only this job uses
  `environment: production` and the EC2 secrets. Concurrency group
  `spending-production-deploy` queues deploys and never cancels one in progress
  (GitHub keeps only the newest pending run, so the latest main commit wins).

`tests/test_release_controls.py` enforces this: a secret-free PR path, deploys only
from main after validation, pinned host keys, helper-only container changes, and
unconditional SSH key cleanup.

## Deploy job

1. Fail with a clear error if `EC2_HOST`, `EC2_USER`, `EC2_SSH_KEY` or
   `EC2_SSH_KNOWN_HOSTS` is missing.
2. Write the key and pinned `known_hosts` to `$RUNNER_TEMP/spending-ssh` and use an
   ssh config with `StrictHostKeyChecking yes`. An `if: always()` step deletes them.
3. Write `webapp/version.json` as `{sha: <7-char commit>, timestamp: <commit time>,
   version: <webapp/VERSION>}`. That is what `/api/version` serves (same format as the
   old `deploy.sh`). `version.json` is no longer in `.dockerignore`, so it reaches the image.
4. Copy `scripts/deploy-service.py` to `~/spending/scripts/` and run its read-only
   preflight. Any drift stops the job before the source changes.
5. Copy `webapp/` to `~/spending/webapp/` with rsync `--delete` (keeping `.env*`,
   virtualenvs, `__pycache__` and `.elasticbeanstalk`).
6. Run `deploy-service.py --release <sha>` on the host.
7. Smoke check: `https://spending-analyzer.jim-greco.com/api/version` must report the
   commit's 7-character SHA within about 2.5 minutes.

## Release helper (`scripts/deploy-service.py`)

The helper runs on the host with the system `python3`, so it uses Python 3.9
syntax. It never prints subprocess output, because Compose config and
`docker inspect` contain credentials. Errors name only the keys involved, never values.

How it picks what to rebuild:

- It finds exactly one running container labelled
  `com.docker.compose.project=deploy`, `com.docker.compose.service=spending`.
- The project, working directory (`~/deploy`) and ordered Compose files come from
  that container's labels. Every file must still exist inside `~/deploy`. The
  installed `docker-compose` version must equal the container's recorded version.
- The effective `spending` config hash must equal the live container's hash. The
  only exception is Compose 2.26.1's `--no-deps` normalization (the same rule Ritual
  Cue uses), proven by a lossless stdin round trip.
- The effective environment (image defaults plus Compose `environment`) must equal
  the running container's environment exactly. Whatever `DATABASE_URL` is live
  (admin, or the least-privilege `spending_runtime` URL after the DB cutover) is
  therefore kept exactly. `MIGRATION_DATABASE_URL` must not be present.
- The service must have a Compose `build:` whose context is `~/spending/webapp`. If
  Spending ever runs only a prebuilt `image:` with no `build:`, the helper refuses:
  there is no source to release from, and it will not switch artifacts.

Release, holding `/tmp/codex-shared-host-release.lock` and then
`~/deploy/.app-release.lock` (the same order as the DB cutover; waits up to 15 min):

1. Refuse if a `db-runtime-spending-*.json` cutover overlay recorded on the container
   is less than 24 hours old (the cutover's observation and recovery window).
2. Check that the transferred `version.json` matches the commit and `VERSION`.
   Require at least 1 GiB free in Docker's root directory. Nothing is ever pruned.
3. Tag the running image `spending-rollback:<sha>` and write
   `~/deploy/release-records/spending-<sha>.json` (no secrets). A record that
   already exists stops the release.
4. Add `~/deploy/spending-image-<sha>.json`, which sets only `services.spending.image`,
   after all recorded files (earlier helper image files are superseded). Prove the
   resulting model differs from the live one only in that image.
5. `docker-compose build spending`, pin the override to the built image ID, check the
   model again, check that the container did not change, then
   `up -d --no-deps --no-build spending`. No other service is touched.
6. Wait up to 2 minutes until the new container passes preflight, runs the built
   image, and serves `/api/version` with the new SHA from inside the container.

## Coexistence with the DB runtime cutover

`consolidated-deploy` `scripts/db-runtime/cutover.py` adds a durable overlay
`db-runtime-spending-<hash>.json` that pins a frozen image and points
`DATABASE_URL` at `${SPENDING_RUNTIME_DATABASE_URL}`. The helper keeps that overlay
in the file list, so the runtime DB URL and every other setting carry over
unchanged. It adds a later override that replaces only the image with one built
from the merged commit. That pin is superseded on purpose, because the owner
decided merges deploy. The helper prints a notice when it replaces an
overlay-pinned image.

For 24 hours after a cutover the helper refuses to deploy, so the cutover's
`verify`/`recover` still see their frozen image. After the first auto-deploy,
cutover `verify`/`recover` for Spending no longer apply, and recovery uses the
rollback image below. Before the cutover, the base `docker-compose.yml` admin URL
is preserved exactly in the same way.

Images built on the host resolve `requirements.txt` ranges at build time, so they
are not the frozen, inventory-checked cutover artifact. That is the accepted
trade-off of deploy-on-merge.

## Rollback

The release record names `rollbackTag`, `previousImage` and `candidateCompose`. To
roll back, edit `~/deploy/spending-image-<sha>.json` so `image` is the
`previousImage` ID, then run the record's `candidateCompose` command with
`up -d --no-deps --no-build spending`. Or revert the commit on `main` and let it deploy.

## One-time owner setup

1. Create the GitHub environment `production` in `jimgreco/spending_analyzer`, with a
   deployment branch rule allowing only `main`.
2. Add environment secret `EC2_SSH_KNOWN_HOSTS`: the host's public key line(s),
   e.g. from `ssh-keyscan -t ed25519 <host>`, checked against the fingerprint on
   the instance (`ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub`). The value
   Ritual Cue's `production` environment uses works if it pins the same host.
3. Move `EC2_HOST`, `EC2_USER` and `EC2_SSH_KEY` from repository secrets into the
   `production` environment and delete the repository-level copies. Environment
   secrets with a main-only branch rule cannot be read by a workflow edited in a
   pull request. Repository secrets can.
4. Re-enable the `deploy.yml` workflow if it was disabled when automatic
   deployment was retired. It keeps the same file path.
