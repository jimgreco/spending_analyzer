# Release policy and the DB runtime cutover

**Superseded (November 2026).** The October 2026 validation-only policy, which
retired `deploy.yml` and made the frozen cutover image the only route to
production, has been replaced. The owner decided that every app deploys on merge to
`main`. See [deploy.md](deploy.md) for the current model.

What still applies from the cutover:

- The least-privilege DB role cutover runs with `jimgreco/consolidated-deploy`
  `scripts/db-runtime/cutover.py` and is coordinated separately. Only
  `spending_runtime` needs LOGIN; `spending_owner` and `spending_migrator` stay
  NOLOGIN. Do not rerun historical Spending corrections.
- Automatic releases never change `DATABASE_URL` or any other runtime
  setting. It keeps the recorded Compose overlays, including
  `db-runtime-spending-*.json`, so whichever cutover state is live carries over. It
  replaces the cutover-pinned image with the merged commit's image. (The old
  helper's 24-hour post-cutover window is gone; see deploy.md.)
- The production host is aarch64. CI still builds a native ARM64 image on
  `ubuntu-24.04-arm` to validate it. Production images are built in CI
  (`ubuntu-24.04-arm`) and pulled from GHCR.

Local checks: `python3 -B -m unittest discover -s tests -v` (requires PyYAML),
`actionlint .github/workflows/*.yml`, and the app suite with
`SPENDING_TEST_DATABASE_URL` pointing only to a disposable local `_test` database.
