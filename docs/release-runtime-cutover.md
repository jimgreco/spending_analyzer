# Validation-only publication and runtime cutover

The previous `deploy.yml` automatically transferred code, rebuilt/restarted
Spending and pruned Docker on every main push. This change retires that workflow:
it only responds to an explicit manual dispatch with a refusal. Re-enabling the
old workflow ID cannot reactivate production deployment from this new source.

The new independent `ci.yml` runs on pull requests, main pushes and manual
validation. It uses only synthetic PostgreSQL credentials, runs publication-policy
tests and the full app suite, builds a native ARM64 image without publishing it, and exposes
`Required Checks`. It has read-only repository permission, no deployment secrets,
no production environment and no SSH/transfer/restart/prune commands.

Before publishing these changes, the coordinator must disable the old remote
workflow and check for queued/running old deployments. Nothing in this local
commit performs those remote actions. Do not push main while the trigger risk
remains. The new CI file can be reviewed/tested independently of the disabled old
workflow ID. Keep the retired workflow disabled after integration; no re-enable
step is required for validation.

Production activation uses the separately approved, pinned-image one-time utility
in `jimgreco/consolidated-deploy` at `scripts/db-runtime/cutover.py`. It preserves
all current Compose settings except the explicitly reviewed runtime DB URL and
image, checks four worker DB sessions, and supports one bounded fallback to admin
with the same separation-aware image. It does not run migrations or legacy repairs.

Only `spending_runtime` needs LOGIN and a privately owner-provisioned password for
this release. `spending_owner` and `spending_migrator` remain NOLOGIN. Adoption and
permissions were already prepared; do not rerun historical Spending corrections.

The frozen runtime source is `400cb5f88ad37ccd002f68051e2b1f2a6a5bb743`; this release
control change leaves its complete `webapp` build-context tree unchanged. Keep the
saved image's original source label and dependency inventory. After integration,
verify that tree identity plus required hosted CI; if runtime inputs changed,
review/rebuild/retest a new artifact. Do not silently use a floating-dependency
image rebuilt by CI as the approved deployment artifact.

Local checks: `python3 -B -m unittest discover -s tests -v` (requires PyYAML),
`actionlint .github/workflows/ci.yml .github/workflows/deploy.yml`, and the app suite
with `SPENDING_TEST_DATABASE_URL` pointing only to a disposable local `_test` DB.
The cutover rehearsal and stored image evidence are maintained in the coordinator's
DB preparation workspace. Hosted CI, private owner input, real target storage and
the serialized production approval remain separate gates.

The production host is aarch64. The image job uses `ubuntu-24.04-arm`, requests `linux/arm64` and verifies its output architecture. Old AMD64 artifacts are local-only evidence. Native Spending storage remains blocked pending separately approved capacity resolution; validation success does not authorize a load or cleanup.
