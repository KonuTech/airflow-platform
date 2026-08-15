---
phase: 07-observability-metrics-tracing-lineage
plan: 01
subsystem: database
tags: [postgresql, alembic, lineage, freshness, pydantic, config-sync, grafana]

# Dependency graph
requires:
  - phase: 06-universal-csv-engine-schema-contracts-normalization
    provides: meta.schema_versions, meta.ingestion_runs.schema_version_id FK, ConfigRegistry.sync()
provides:
  - meta.v_customers_lineage (OBS-07 lineage view over the full ingestion_runs/files/batches/config_versions/schema_versions chain)
  - meta.datasets.expected_frequency/freshness_warn_after/freshness_fail_after (nullable interval columns, OBS-01/OBS-09 foundation)
  - grafana_reader PostgreSQL role (SELECT-only, scoped to meta.datasets/files/ingestion_runs + the lineage view)
  - dataplat.config.model.FreshnessConfig, threaded through ConfigRegistry.sync() into meta.datasets
  - the freshness-breach SQL condition (FRESHNESS_BREACH_QUERY), proven and pinned for plan 07-07's Grafana alert rule to reuse verbatim
affects: [07-06-vault-bootstrap-grafana-secrets, 07-07-grafana-alerting-dashboards]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Freshness as nullable meta.datasets columns fed by an opt-in configs/datasets/*.yaml block, not a separate SLA table"
    - "Grafana-facing least-privilege role created via a plain Alembic migration (CREATE ROLE ... op.execute()) since CNPG postInitApplicationSQL cannot reach an already-running cluster"
    - "Lineage as one wide SQL view per target table, joining embedded lineage columns out to the full meta.* chain"

key-files:
  created:
    - migrations/versions/0010_meta_datasets_freshness.py
    - migrations/versions/0011_grafana_reader_role.py
    - migrations/versions/0012_meta_v_customers_lineage.py
    - tests/integration/test_lineage_view.py
    - tests/integration/test_freshness_query.py
  modified:
    - packages/dataplat/src/dataplat/config/model.py
    - packages/dataplat/src/dataplat/config/registry.py
    - configs/datasets/customers.yaml
    - tests/integration/test_config_registry.py
    - tests/integration/test_migrations.py
    - migrations/versions/0009_meta_schema_versions.py

key-decisions:
  - "FreshnessConfig gets no Python-level warn_after<=fail_after validator — both are opaque PostgreSQL interval literals a naive string comparison cannot safely order; ordering is enforced by Postgres at query time in the freshness condition instead (matches CONTEXT.md D-08's own reasoning)"
  - "meta.v_customers_lineage deliberately excludes error_detail (raw exception JSONB) from its SELECT list — a Security Domain finding from 07-RESEARCH.md, verified absent both in the migration's CREATE VIEW text and in the fetched row's keys at test time"

patterns-established:
  - "A migration whose downgrade() drops a table that another table has a live FK into must null the referencing column first, or a session where any other code path already populated that FK cannot ever re-upgrade past it"

requirements-completed: [OBS-01, OBS-07, OBS-09]

# Metrics
duration: ~30min
completed: 2026-08-15
---

# Phase 7 Plan 1: Lineage View, Freshness Foundation & grafana_reader Role Summary

**`meta.v_customers_lineage` (OBS-07, all named columns proven via a real `run_ingest()` call) plus `meta.datasets` freshness columns wired end-to-end from `customers.yaml` through `ConfigRegistry.sync()` (OBS-01/OBS-09), behind a new least-privilege `grafana_reader` role — no Helm chart, no live cluster, no Grafana required.**

## Performance

- **Duration:** ~30 min
- **Completed:** 2026-08-15
- **Tasks:** 3 (all `type="auto"`, no checkpoints)
- **Files modified:** 11 (5 created, 6 modified)

## Accomplishments

- `alembic upgrade head` now creates `meta.v_customers_lineage` (OBS-07's full column set: source file, object path, checksum, batch, ingestion timestamp, DAG/run/task ID, processor version, schema version, config version — in one query), three new nullable `meta.datasets` freshness columns, and the `grafana_reader` role — verified live against a throwaway PostgreSQL 18, including a 3x sequential downgrade/re-upgrade cycle (0012→0011→0010→0009, each step exiting 0)
- `FreshnessConfig` (new, opt-in) flows from `configs/datasets/customers.yaml`'s new `freshness:` block through `ConfigRegistry.sync()`'s widened `_resolve_dataset_id` upsert into `meta.datasets` — proven by a real `sync()` call against a real database returning genuine `datetime.timedelta` values, not a raw-INSERT-seeded row
- The freshness-breach SQL condition structurally distinguishes "never configured" (`expected_frequency IS NULL`, permanently excluded) from "configured and stale" (including correct cold-start behavior for a dataset with zero prior file history, via `COALESCE(MAX(f.discovered_at), d.created_at)`) — the exact query text is pinned in `tests/integration/test_freshness_query.py` for plan 07-07's Grafana alert rule to reuse verbatim
- `grafana_reader` exists, has `LOGIN` and no superuser/createrole attributes, and holds `SELECT` on exactly `meta.datasets`/`meta.files`/`meta.ingestion_runs`/`meta.v_customers_lineage` — never `normalized.customers` directly (the view surfaces that data under its own owner's privileges)

## Task Commits

Each task was committed atomically:

1. **Task 1: Three migrations — freshness columns, grafana_reader role, the lineage view** - `f9b4692` (feat)
2. **Task 2: FreshnessConfig — opt-in config block, threaded through sync() into meta.datasets** - `440bfa7` (feat)
3. **Task 3: Integration tests — lineage view, freshness structural distinction, config-sync write path** - `7f1cb93` (test; also carries the migration 0009 bug fix this task's own verification surfaced)

_No separate plan-metadata commit in this worktree — SUMMARY.md is committed as part of the final metadata commit below (worktree mode)._

## Files Created/Modified

- `migrations/versions/0010_meta_datasets_freshness.py` - three nullable `interval` columns on `meta.datasets`
- `migrations/versions/0011_grafana_reader_role.py` - `CREATE ROLE grafana_reader LOGIN` + schema USAGE + table SELECT grants, no password (set later out-of-band via Vault bootstrap)
- `migrations/versions/0012_meta_v_customers_lineage.py` - `CREATE VIEW meta.v_customers_lineage`, granted to `etl_app` and `grafana_reader`
- `migrations/versions/0009_meta_schema_versions.py` - **bug fix** (see Deviations): `downgrade()` now nulls `meta.ingestion_runs.schema_version_id` before dropping `meta.schema_versions`
- `packages/dataplat/src/dataplat/config/model.py` - new `FreshnessConfig`, added as `DatasetConfig.freshness: FreshnessConfig | None`
- `packages/dataplat/src/dataplat/config/registry.py` - `ConfigRegistry._resolve_dataset_id` widened to upsert the three freshness columns via `%s::interval` placeholders
- `configs/datasets/customers.yaml` - new `freshness: {expected_frequency: "1 day", warn_after: "2 hours", fail_after: "6 hours"}` block
- `tests/integration/test_lineage_view.py` - drives a real `run_ingest()`, asserts every OBS-07-named column
- `tests/integration/test_freshness_query.py` - SQL-only proof of the NULL-vs-configured distinction and cold-start fallback; pins the exact breach-query text
- `tests/integration/test_config_registry.py` - new `test_sync_persists_freshness_config_to_meta_datasets`
- `tests/integration/test_migrations.py` - new `test_grafana_reader_role_exists_and_is_select_only`

## Decisions Made

- **No Python-level ordering validator on `FreshnessConfig.warn_after`/`fail_after`** — both are opaque PostgreSQL interval literals; a naive Python string comparison cannot safely prove `warn_after <= fail_after` without a full interval parser, so the plan's own reasoning (documented in the model's docstring) explicitly rejected adding an unenforceable validator. Ordering is enforced by Postgres at query time in the freshness condition instead.
- **`error_detail` deliberately excluded from `meta.v_customers_lineage`** — a Security Domain finding carried over from 07-RESEARCH.md: the raw exception JSONB column must never reach a dashboard/view without the same redaction discipline the logging layer already applies. Verified two ways: `grep -n "error_detail"` against the migration file returns no match, and the integration test asserts the fetched row has no such key.
- **Task 3's lineage test builds its own `PipelineContext` with `CsvSource(dataset_id=...)` instead of reusing `_make_ctx` verbatim** — `_make_ctx` deliberately omits `dataset_id` (skips schema resolution), but this test needs `schema_version_id` genuinely populated to prove the view's `schema_version`/`schema_hash` columns are not vacuously NULL, mirroring `test_run_ingest.py`'s own precedent test for the identical reason.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] migration 0009's `downgrade()` could not safely reverse once any code path had populated a real `schema_version_id`**
- **Found during:** Task 3, while verifying the plan's own `<verification>` section command (`pytest tests/integration/test_lineage_view.py tests/integration/test_freshness_query.py tests/integration/test_config_registry.py tests/integration/test_migrations.py`) and the full `tests/integration` suite
- **Issue:** `migrations/versions/0009_meta_schema_versions.py`'s `downgrade()` dropped `meta.schema_versions` (via `op.drop_table`) without first nulling `meta.ingestion_runs.schema_version_id`. This defect predates this plan, but was never triggered because no test file alphabetically ordered before `test_migrations.py` had ever populated a real `schema_version_id`. This plan's own `test_lineage_view.py` (Task 3) is the first to do so — it deliberately sets `CsvSource(dataset_id=...)` to prove `meta.v_customers_lineage`'s `schema_version`/`schema_hash` columns are genuinely non-NULL (OBS-07's literal wording). Once that row existed, `test_migrations.py`'s pre-existing `test_0006_downgrade_restores_the_plain_index_and_reupgrade_restores_the_constraint` (which downgrades to revision `"0005"`, passing through 0009, then re-upgrades to `head` in a `finally` block) crashed on re-upgrade: `op.create_foreign_key` failed with `psycopg.errors.ForeignKeyViolation: Key (schema_version_id)=(1) is not present in table "schema_versions"`, since the table had been dropped-and-recreated empty while a live row still referenced the old value. This cascaded into `test_grafana_reader_role_exists_and_is_select_only` also failing (the aborted re-upgrade never reached migrations 0010–0012).
- **Fix:** `migrations/versions/0009_meta_schema_versions.py`'s `downgrade()` now runs `op.execute("UPDATE meta.ingestion_runs SET schema_version_id = NULL")` before dropping the FK constraint and the table — reverting to migration 0004's own original nullable/unconstrained shape, which is the correct semantic for "downgrade past the table that would otherwise be referenced."
- **Files modified:** `migrations/versions/0009_meta_schema_versions.py`
- **Verification:** Re-ran the plan's own 4-file combined invocation (18/18 passing) and the full `tests/integration` suite (87/87 passing, up from 2 failures before the fix); also re-ran `tests/property` (5/5) and `tests/unit` (388/388) to confirm no wider regression.
- **Committed in:** `7f1cb93` (part of the Task 3 commit, since Task 3's own verification is what surfaced it)

**2. [Rule 3 - Blocking] ruff lint failures on newly-written/modified files**
- **Found during:** Task 3, running `ruff check`/`ruff format --check` against every touched file per CLAUDE.md's mandatory lint gate
- **Issue:** Several `E501`/`W505` line-length violations (max 100 chars) in `migrations/versions/0009_*.py`, `migrations/versions/0011_*.py`, `tests/integration/test_config_registry.py`, `tests/integration/test_freshness_query.py`; an unsorted import block and two `F811` "redefinition of unused `env`" findings in `tests/integration/test_lineage_view.py` (a known false-positive pattern for cross-module pytest fixture re-export, already handled elsewhere in this codebase via an inline `# noqa: F811` comment — `tests/e2e/vault/test_airflow_backend.py` is the established precedent this fix follows verbatim)
- **Fix:** Wrapped/shortened the long lines; ran `ruff check --fix` for the import sort; added the same `# noqa: F811 -- pytest fixture-injection param name, not a real redefinition` comment this codebase already uses for the identical situation
- **Files modified:** `migrations/versions/0009_meta_schema_versions.py`, `migrations/versions/0011_grafana_reader_role.py`, `tests/integration/test_config_registry.py`, `tests/integration/test_freshness_query.py`, `tests/integration/test_lineage_view.py`
- **Verification:** `ruff check` and `ruff format --check` both clean on every touched Python file; `mypy packages/dataplat/src packages/csv-processor/src` clean (61 source files, no issues)
- **Committed in:** `7f1cb93` (folded into the Task 3 commit) and `f9b4692` (Task 2's own files were already clean)

**3. [Note, not a fix] Plan's stated `-m integration` acceptance-criteria flag does not select any test in `tests/integration/`**
- **Found during:** Task 3, attempting to literally run the plan's acceptance criteria commands (e.g. `pytest tests/integration/test_lineage_view.py -m integration -x -q`)
- **Issue:** No test under `tests/integration/` — new or pre-existing — carries `@pytest.mark.integration`; that marker is only applied to tests living OUTSIDE `tests/integration/` that need Docker (e.g. `tests/property/test_determinism.py`), matching `pyproject.toml`'s own marker description ("excluded from the offline gate") and the Makefile's `test-integration` target (`pytest tests/integration -q`, no `-m` flag). Verified empirically against an untouched, pre-existing file (`test_objectstore.py -m integration` → `5 deselected`, exit 5) before concluding this is a directory-based, not marker-based, convention already established project-wide — not something introduced or that should be "fixed" by retrofitting markers onto this whole test directory.
- **Resolution:** Verified every file using the actually-correct, equivalent command form (no `-m integration`), matching `07-RESEARCH.md`'s own Test Map (which likewise omits the flag) and the Makefile's `test-integration` target. All acceptance criteria's underlying intent — each file passes, individually and combined — is satisfied and proven live.
- **Files modified:** None (verification-command interpretation only)

---

**Total deviations:** 2 auto-fixed (1 bug, 1 blocking/lint), plus 1 verification-command clarification (no file change)
**Impact on plan:** The bug fix is necessary for correctness (a migration that cannot safely reverse is a real defect); the lint fixes are required by CLAUDE.md's mandatory gate. No scope creep — all three stayed within files this plan's tasks already touch or are directly, causally responsible for exposing.

## Issues Encountered

- A background sanity-check run of `pytest tests/policy -q -m "not manifests"` (not part of this plan's declared verification, run for extra confidence) stalled with no CPU progress after the first ~18 collected items — almost certainly blocked on a network-dependent step (e.g. a secret-scanning binary/tool expecting egress unavailable in this sandbox) rather than anything related to this plan's changes. Terminated rather than waited on, since it is outside this plan's actual acceptance criteria; the plan's own required verification (the 4 integration test files, `tests/unit -k config`) all passed cleanly.

## User Setup Required

None - no external service configuration required. `grafana_reader`'s password is deliberately NOT set by this plan (no credential literal anywhere) — it is set out-of-band by a later plan's Vault-bootstrap script extension.

## Next Phase Readiness

- OBS-07 is fully delivered: `meta.v_customers_lineage` answers "where did this row come from" by SQL alone, proven against a genuinely published row.
- The OBS-01/OBS-09 data foundation is real and proven end-to-end (config → `ConfigRegistry.sync()` → `meta.datasets`), ready for plan 07-07 to wire a Grafana panel/alert on top of the exact, already-proven `FRESHNESS_BREACH_QUERY` SQL text — unchanged, no re-derivation needed.
- `grafana_reader` exists and is scoped correctly; plan 07-06 (Vault bootstrap) still needs to set its password and materialize the Kubernetes Secret Grafana's Helm values will reference.
- No blockers for downstream plans in this phase.

---
*Phase: 07-observability-metrics-tracing-lineage*
*Completed: 2026-08-15*
