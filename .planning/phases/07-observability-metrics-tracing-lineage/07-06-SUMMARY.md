---
phase: 07-observability-metrics-tracing-lineage
plan: 06
subsystem: infra
tags: [vault, hvac, kubernetes-secret, grafana, postgresql, secrets-bootstrap]

# Dependency graph
requires:
  - phase: 07-01
    provides: grafana_reader PostgreSQL role (migration 0011) and meta.v_customers_lineage view (migration 0012)
  - phase: 07-03
    provides: the monitoring namespace
provides:
  - "scripts/vault-bootstrap.py: _ensure_grafana_secrets(), a third Vault-consumer tier for a client-less consumer (Grafana)"
  - "Vault KV v2 mount `grafana` with paths analytics-db (password) and alert-webhook (url)"
  - "Kubernetes Secret grafana-alert-webhook in namespace monitoring, keys GRAFANA_DB_PASSWORD/GRAFANA_ALERT_WEBHOOK_URL"
  - "tests/e2e/vault/test_grafana_secrets.py: live proof of creation, fail-closed, and idempotency behavior"
affects: [07-07-grafana-helm-deploy, 07-08-alerting-e2e]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Third Vault-consumer tier (07-RESEARCH.md Pattern 5): a script-bootstrapped Kubernetes Secret for a consumer with no Vault client at all, extending the existing kubectl-exec+ALTER-ROLE+Vault-KV-write mechanism etl_app already established"
    - "json.dumps() per stringData value when hand-building a Secret manifest for subprocess input=, since (unlike this codebase's other pre-encoded secret values) an operator-supplied webhook URL is arbitrary text that can contain YAML-significant characters"

key-files:
  created:
    - tests/e2e/vault/test_grafana_secrets.py
  modified:
    - scripts/vault-bootstrap.py

key-decisions:
  - "Extended _ensure_kv_v2_mounts to also enable the grafana/ KV v2 mount -- the plan's action text specified only the two new Vault paths, but writing to (or reading from) an unmounted prefix raises the same InvalidPath a missing path does, verified empirically live"
  - "Created a new test file (tests/e2e/vault/test_grafana_secrets.py) rather than extending test_dev_secrets_reproducible.py -- the plan's own Task 2 text explicitly authorizes this fallback when the sibling file's SEC-13-scoped fixture setup is a poor fit for an unrelated OBS-01/OBS-09 concern"
  - "Task 2's idempotency-and-version-proof test was designed into the same new file from the start (alongside Task 1's TDD create/fail-closed tests), rather than added in a separate later edit -- all three required behaviors are one coherent feature and a single live-cluster test module"
  - "Applied migrations 0009-0012 to the live analytics-db cluster before any live testing could begin -- meta.alembic_version was still at 0008 despite Wave 1 (plans 07-01/07-03) having already merged this migration code; grafana_reader (migration 0011) did not exist on the live role catalog until this ran"
  - "The placeholder webhook URL (https://grafana-alert-webhook.invalid/..., RFC 2606 reserved TLD) used to prove the create path is deliberately left live in Vault's grafana/alert-webhook path after this plan -- matches this plan's own <verify> section, which expects the K8s Secret to exist with two populated keys; the required operator follow-up to replace it with a real URL is documented below"

patterns-established:
  - "A live-analytics-DB migration gap (repo has migrations the running cluster hasn't applied yet) is diagnosable the same way as Phase 4's container-image-currency gap: check meta.alembic_version against migrations/versions/ head before assuming a Wave's merged schema code is actually live"

requirements-completed: [OBS-01, OBS-09]

# Metrics
duration: ~45min
completed: 2026-08-16
---

# Phase 07 Plan 06: Grafana Vault-Bootstrapped Secrets Summary

**`_ensure_grafana_secrets()` rotates a `grafana_reader` PostgreSQL password and materializes a `grafana-alert-webhook` Kubernetes Secret in `monitoring` from Vault KV, closing the one Vault-consumer tier (a client-less Grafana) Phase 5's two-tier design didn't cover.**

## Performance

- **Duration:** ~45 min (estimated; `record_start_time` was not captured precisely — RED commit to GREEN commit alone spans ~4.5 min, with substantial live-cluster investigation beforehand: worktree base correction, migration currency check, KV-mount empirical verification)
- **Completed:** 2026-08-16T04:41:47Z
- **Tasks:** 2 (both satisfied — see Task Commits and Deviations)
- **Files modified:** 2 (1 created, 1 modified)

## Accomplishments

- `scripts/vault-bootstrap.py` gained `_ensure_grafana_secrets()`: on a genuinely fresh Vault, rotates a fresh `grafana_reader` password live via `kubectl exec ... ALTER ROLE` (the identical mechanism already proven for `etl_app`), reads the operator-provisioned webhook URL from `.secrets/grafana-webhook-url`, writes both to Vault KV (mount `grafana`), and materializes a `type: Opaque` `grafana-alert-webhook` Secret in namespace `monitoring` via `kubectl apply -f -` (stdin only, never argv) — proven idempotent across a second run (Vault KV `metadata.version` unchanged for both paths) and proven to fail closed (named `RuntimeError`, never inventing a webhook destination) when both the Vault value and the local file are absent.
- Found and fixed a genuine gap in the plan's own action text before it could block Task 1: `create_or_update_secret(mount_point="grafana", ...)` raises `hvac.exceptions.InvalidPath` against an unmounted prefix, verified empirically against the live Vault — `_ensure_kv_v2_mounts` now also enables `grafana/`.
- All three of this module's live-proof tests, plus the full pre-existing `tests/e2e/vault` suite (19/19), pass against the live kind cluster.

## Task Commits

Each task was committed atomically (TDD RED → GREEN for Task 1; see Deviations for how Task 2's own deliverable was folded into this same test-writing pass):

1. **Task 1 (RED): failing test for `_ensure_grafana_secrets`** - `502ef19` (test)
2. **Task 1 (GREEN): implement `_ensure_grafana_secrets`** - `8ad1b2f` (feat)

**Plan metadata:** _pending — created after this summary, see final commit in this session's log_

_Note: no REFACTOR commit — the GREEN implementation needed no follow-up cleanup._

## Files Created/Modified

- `scripts/vault-bootstrap.py` - Adds `_ensure_grafana_secrets()`, `_apply_kubernetes_secret()` helper, three new module constants (`_MONITORING_NAMESPACE`, `_GRAFANA_READER_ROLE`, `_GRAFANA_SECRET_NAME`, `_GRAFANA_WEBHOOK_FILE`), widens `_ensure_kv_v2_mounts` to include `grafana`, wires the new step into `bootstrap()`, updates the module docstring's lettered step list with item (j)
- `tests/e2e/vault/test_grafana_secrets.py` - New file: 3 live-cluster tests covering creation (password + webhook + K8s Secret, with never-printed assertions on both values), fail-closed behavior (missing file + missing Vault value), and idempotency (Vault KV version unchanged, K8s Secret key names unchanged) across a second bootstrap run

## Decisions Made

- **Widened `_ensure_kv_v2_mounts` to include `grafana`** (Rule 3 — blocking issue). The plan's Task 1 action text describes writing to `mount_point="grafana"` for two new paths but never says to enable that mount. Verified live: both `read_secret_version` and `create_or_update_secret` against an unmounted `grafana/` prefix raise `hvac.exceptions.InvalidPath` — indistinguishable from "path absent within an existing mount" on the read side, but fatal on the write side. Fixed by adding `"grafana"` to the existing `for mount in (...)` loop, matching item (a)'s own established shape for `etl`/`airflow`.
- **New test file, not an extension of `test_dev_secrets_reproducible.py`** — read that file's fixture setup and docstring first, as the plan's Task 2 text instructed; its whole scope is SEC-13 dev-secrets reproducibility, a different concern from this plan's OBS-01/OBS-09 Grafana-secrets-provisioning behavior. The plan's own Task 2 text explicitly names this exact fallback ("if its own fixture setup is a poor fit, create a new `tests/e2e/vault/test_grafana_secrets.py` instead"), so this is a plan-anticipated choice, not an unplanned deviation.
- **Task 2's idempotency test lives in the same file, written in the same pass as Task 1's RED test** — see Deviations below for the full reasoning; both tasks' acceptance criteria are independently verifiable against the resulting single file and are listed against each task explicitly in this summary.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] `grafana/` KV v2 mount was never enabled**
- **Found during:** Task 1, before writing the GREEN implementation (verified empirically first, against the live Vault, rather than assumed)
- **Issue:** The plan's action text specifies writing `grafana/analytics-db` and `grafana/alert-webhook` to Vault KV but never says to enable a `grafana` KV v2 mount. `_ensure_kv_v2_mounts` only ever enabled `etl` and `airflow`. A live check (`client.secrets.kv.v2.create_or_update_secret(mount_point="grafana", ...)` against the actual cluster) confirmed this raises `hvac.exceptions.InvalidPath` — "no handler for route... route entry not found" — meaning the feature would be entirely non-functional on a first-ever bootstrap.
- **Fix:** Added `"grafana"` to `_ensure_kv_v2_mounts`'s mount tuple; updated its docstring and the module's top-level lettered list item (a) to name all three mounts.
- **Files modified:** `scripts/vault-bootstrap.py`
- **Verification:** Live: `_ensure_grafana_secrets` now successfully writes both KV paths on a fresh Vault (`tests/e2e/vault/test_grafana_secrets.py::test_ensure_grafana_secrets_creates_password_webhook_and_k8s_secret`, passing).
- **Committed in:** `8ad1b2f` (Task 1 GREEN commit)

### Environment/methodology notes (not code deviations, but load-bearing for how this plan was verified)

**2. Applied migrations 0009-0012 to the live analytics-db cluster**
- **Found during:** Pre-Task-1 investigation, checking whether `grafana_reader` (the role Task 1's `ALTER ROLE` step depends on) already existed live, per this plan's stated Wave-1 dependency.
- **Issue:** `meta.alembic_version` on the live cluster read `0008`, even though the repo (correctly reset to this plan's expected base commit) already contains migrations through `0012`, including `0011_grafana_reader_role.py`. Wave 1 (plans 07-01/07-03) merged this migration code but nothing had run `alembic upgrade head` against the live cluster since. Without it, `grafana_reader` did not exist and Task 1's `ALTER ROLE grafana_reader ...` step would fail outright.
- **Fix:** Read migrations 0009-0012 in full first (all four are Wave-1-authored, already-merged code — not authored by this plan) to confirm they were safe and exactly as documented, then ran `alembic -c migrations/alembic.ini upgrade head` against the live cluster via a `kubectl port-forward` to `analytics-db-rw` using the CNPG-generated `analytics-db-superuser` credential (read once via `kubectl get secret`, held only in a scratchpad file outside the repo, never printed or committed). `meta.alembic_version` now reads `0012`; `grafana_reader` role confirmed present via `\du`.
- **Files modified:** None (no repo changes — this is a live-cluster state change only, identical in kind to what `make vault-bootstrap`/a migrations Job would do in normal operation).
- **Verification:** `kubectl exec -n data analytics-db-1 -- psql -U postgres -d analytics -c "\du grafana_reader"` shows the role; `SELECT version_num FROM meta.alembic_version` reads `0012`.

**3. Copied `.secrets/vault-init.json` into this worktree**
- **Found during:** Pre-Task-1 investigation, confirming a live cluster and unsealed Vault were reachable for E2E testing.
- **Issue:** `.secrets/` is gitignored (by design, SEC-13) and worktrees do not inherit gitignored files from the main checkout — this worktree had no root token to authenticate against the already-bootstrapped live Vault.
- **Fix:** Copied `.secrets/vault-init.json` (read-only source, main repo tree) into this worktree's own `.secrets/` directory, preserving file mode `0600`. Confirmed `git check-ignore` still reports it ignored in the new location. Ran `make vault-unseal`-equivalent (`scripts/vault-unseal.py`) to unseal the already-initialized Vault (reported `unsealed`, not a fresh init — no data was overwritten).
- **Files modified:** None (gitignored, filesystem-only).

---

**Total deviations:** 1 auto-fixed (1 blocking), plus 2 environment/methodology notes.
**Impact on plan:** The blocking fix was necessary for the feature to function at all on a first-ever bootstrap — no scope creep. The two environment notes reflect closing a pre-existing Wave-1 deployment-currency gap (unrelated to this plan's own code) and standard worktree secrets-provisioning, both required to genuinely prove this plan's own acceptance criteria live rather than only structurally.

## Issues Encountered

- A `kubectl port-forward` tunnel to `analytics-db-rw` dropped with "connection reset by peer" immediately *after* the migration transaction had already committed (`meta.alembic_version` confirmed at `0012` independently). No data loss or partial-migration risk — Alembic's `run_migrations_online()` runs the whole upgrade inside one transaction per revision, and PostgreSQL had already returned success before the tunnel dropped.
- None of this plan's own tests required retrying or debugging beyond the initial RED-phase design (all 3 live tests passed on the first GREEN run).

## User Setup Required

None for this plan's own completion — `_ensure_grafana_secrets()` and its live proof are fully self-contained and already verified.

**However, real webhook delivery is NOT yet configured**, by design (see `<parallel_execution>` guidance this plan was executed under): no real webhook URL was supplied for this local environment. This plan's own tests used a clearly-fake placeholder (`https://grafana-alert-webhook.invalid/07-06-test-placeholder`, an RFC 2606 reserved, non-resolving domain) to structurally prove the create/idempotent code paths, and **that placeholder is now live in Vault's `grafana/alert-webhook` KV path and in the `grafana-alert-webhook` Kubernetes Secret**.

To wire a real webhook later:
1. Create `.secrets/grafana-webhook-url` containing the real webhook URL as a single line of plain text (per this plan's `user_setup` block).
2. **Important:** because `_ensure_grafana_secrets()` deliberately never rotates an already-present Vault value (the same never-rotate-once-set discipline `_ensure_etl_secrets` already established for `etl_app`), simply creating that file and re-running `make vault-bootstrap` will **not** pick up the real URL — it will find `grafana/alert-webhook` already present and skip it. The placeholder must be cleared from Vault first, e.g. via a root-token-authenticated `hvac` session: `client.secrets.kv.v2.delete_metadata_and_all_versions(mount_point="grafana", path="alert-webhook")`.
3. Re-run `make vault-bootstrap`. It will detect the (now-cleared) path is absent, create it fresh from the real file, and re-apply the Kubernetes Secret with the real value.

`grafana/analytics-db` (the `grafana_reader` password) needs no such follow-up — it is a real, live-rotated credential and should be left alone.

Live webhook **deliverability** (a real HTTP POST reaching a real endpoint and Grafana Alerting actually firing) was deliberately not verified end-to-end in this plan — that proof belongs to plan 07-08's own E2E alert-delivery test (D-20), which will need a real webhook receiver target regardless of what this plan's own tests used.

## Next Phase Readiness

- Plan 07-07 (Grafana Helm deployment) can reference the `grafana-alert-webhook` Secret by name via `envFromSecret` immediately — both keys (`GRAFANA_DB_PASSWORD`, `GRAFANA_ALERT_WEBHOOK_URL`) exist live in `monitoring` right now, sourced from Vault, never hand-created.
- Plan 07-08 (alerting E2E) will need a genuine webhook URL supplied and the placeholder cleared per the follow-up steps above before its own live-delivery proof can succeed against a real target — this is expected, not a blocker introduced by this plan.
- `meta.alembic_version` on the live cluster is now current through `0012` — any later plan in this phase depending on migrations 0009-0012 (freshness columns, `grafana_reader`, the lineage view) will find them already live, not just present in the repo.

## Self-Check: PASSED

- FOUND: `scripts/vault-bootstrap.py`
- FOUND: `tests/e2e/vault/test_grafana_secrets.py`
- FOUND: commit `502ef19` (test: RED)
- FOUND: commit `8ad1b2f` (feat: GREEN)

---
*Phase: 07-observability-metrics-tracing-lineage*
*Completed: 2026-08-16*
