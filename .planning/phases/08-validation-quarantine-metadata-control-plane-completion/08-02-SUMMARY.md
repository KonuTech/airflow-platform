---
phase: 08-validation-quarantine-metadata-control-plane-completion
plan: 02
subsystem: orchestration
tags: [airflow, s3, integrity, load-10, psycopg, taskflow]

# Dependency graph
requires:
  - phase: 04-vertical-slice-csv-to-analytical-postgresql
    provides: "airflow/dags/_common/kpo.py precedent (the one prior sanctioned DAG-folder exception), meta.files schema (migrations/versions/0002_meta_files.py), get_or_create_dataset SQL shape (dataplat.metadata.postgres)"
  - phase: 05-vault-secrets-workload-identity
    provides: "Airflow's Vault-backed AIRFLOW__SECRETS__BACKEND=VaultBackend wiring, reused here for the analytics_db_default Connection lookup"
provides:
  - "airflow/dags/_common/integrity_gate.py: list_matched_keys (S3Hook.list_keys wrapper, since S3KeySensor pushes no XCom key list) and integrity_gate (extension/empty/two-HEAD-stability/real-GET-hash checks, in that order)"
  - "_reject_file: the one sanctioned Airflow-side write to meta.files, with a real empty-content hash for the empty-file case and a deterministic per-(object_uri, reason) sentinel hash for every other rejection, so content_sha256 is never null"
  - "A second, narrowly-scoped test_dag_thinness.py exemption (import AND SQL-string checks) for integrity_gate.py, symmetric with kpo.py's precedent but independently scoped"
affects: [08-12, load-10, load-11, quarantine]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Airflow-side pre-pod-launch integrity gate: plain @task functions in the scheduler/worker process performing S3 HEAD/GET checks, never a KubernetesPodOperator pod, for checks that can be resolved before any pod launch is justified"
    - "Deterministic sentinel hash for a NOT-NULL/UNIQUE content_sha256 column when the real content is unknown/ambiguous by construction (sha256(PREFIX + object_uri:reason)), scoped per (object_uri, reason) for idempotent ON CONFLICT re-resolution"

key-files:
  created:
    - airflow/dags/_common/integrity_gate.py
    - tests/unit/test_integrity_gate.py
  modified:
    - tests/policy/test_dag_thinness.py

key-decisions:
  - "Used airflow.sdk.bases.hook.BaseHook (not the deprecated airflow.hooks.base.BaseHook, and not PostgresHook -- apache-airflow-providers-postgres is not installed in this repo's Airflow dependency set, confirmed by import failure) to resolve the analytics_db_default DSN"
  - "Added a second, independently-scoped _EXEMPT_FROM_SQL_CHECK frozenset to test_dag_thinness.py (previously only an import-check exemption existed) so integrity_gate.py's structurally-necessary raw INSERT SQL does not trip the DAG-folder SQL-string policy test that has zero exemptions today"

patterns-established:
  - "A DAG-folder file that legitimately needs both the import exemption AND the SQL-string exemption gets added to BOTH frozensets explicitly and independently -- one exemption never implies the other, so a future file that only needs the import exemption doesn't silently inherit an SQL carve-out"

requirements-completed: [LOAD-10]

duration: 35min
completed: 2026-08-17
---

# Phase 08 Plan 02: LOAD-10 Pre-Pod-Launch File-Integrity Gate Summary

**Airflow-side `integrity_gate.py` (extension, empty-file, two-HEAD stability D-21, real GET+SHA256 checksum D-22) rejects a bad file before any `KubernetesPodOperator` pod launches, and every rejection path lands its own `meta.files` row via a narrow inline `psycopg` INSERT with a real or deterministic-sentinel `content_sha256` (D-20).**

## Performance

- **Duration:** ~35 min
- **Tasks:** 2 completed
- **Files modified:** 3 (2 created, 1 modified)

## Accomplishments

- `list_matched_keys` resolves the exact S3 key set a batch prefix currently matches (`S3Hook.list_keys(..., apply_wildcard=True)`), the concrete answer to "how does a DAG learn which keys to gate" since `S3KeySensor` pushes no XCom key list (verified directly against the pinned provider's installed source).
- `integrity_gate` checks, in short-circuiting order: wrong extension (no network call), empty file (one HEAD, real `sha256(b"")` hash), object instability between two HEAD calls five seconds apart (D-21), and a genuinely unreadable/unhashable object during a real streamed GET+SHA256 (D-22).
- `_reject_file` closes D-20 completely: every rejection path -- including the three where the real object bytes are unknown or ambiguous -- writes a real, non-null `content_sha256` to a new `meta.files` row, using a deterministic `sha256(INTEGRITY_GATE_REJECTED: + "{bucket}/{key}:{reason}")` sentinel when the real bytes cannot be known, scoped so a repeated identical failure idempotently resolves onto the same row while a different reason is a genuinely new row.
- 9 unit tests (8 required behaviors, one parametrized into 2 cases) with `S3Hook`/`psycopg` fully mocked and `time.sleep` patched -- suite runs in well under 1 second, no real network I/O anywhere.

## Task Commits

1. **Task 1: integrity_gate.py -- list_matched_keys, checks, D-20 sentinel-hash rejection write** - `956bfdd` (feat)
2. **Task 2: Unit tests -- every check path, list_matched_keys, sentinel-hash rejection, fully mocked** - `240d6b4` (test)

## Files Created/Modified

- `airflow/dags/_common/integrity_gate.py` - `list_matched_keys`, `integrity_gate`, and `_reject_file` (LOAD-10 gate + D-20 rejection bookkeeping)
- `tests/unit/test_integrity_gate.py` - 9 tests covering every check path, the sentinel-hash property, and `list_matched_keys`
- `tests/policy/test_dag_thinness.py` - added `integrity_gate.py` to the import-check exemption (alongside `kpo.py`/`tracing_kpo.py`) and a new, independently-scoped SQL-string-check exemption

## Decisions Made

- **`BaseHook` import path.** The plan flagged `PostgresHook` vs `BaseHook.get_connection(...).get_uri()` as "verify before choosing". Confirmed empirically: `apache-airflow-providers-postgres` is not installed in this repo's Airflow dependency set (`pyproject.toml` only lists `apache-airflow-providers-cncf-kubernetes` and `apache-airflow-providers-amazon`; `import airflow.providers.postgres` fails). Used `airflow.sdk.bases.hook.BaseHook` (the non-deprecated import, confirmed against the installed `apache-airflow==3.3.0`; the older `airflow.hooks.base.BaseHook` emits a `DeprecatedImportWarning`).
- **SQL-string policy exemption.** `test_dag_thinness.py`'s `test_no_raw_sql_strings` had zero exemptions before this plan and scans every file under `airflow/dags/**` unconditionally. `integrity_gate.py`'s `_reject_file` structurally requires a literal `INSERT INTO` SQL string (the plan's own stated "one sanctioned exception to ADR-0004"), which would otherwise fail that test. Added a second, narrowly-scoped `_EXEMPT_FROM_SQL_CHECK` frozenset containing only this one file, deliberately independent of `_EXEMPT_FROM_IMPORT_CHECK` so a future file needing only the import exemption does not silently inherit an SQL carve-out it was never granted.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added a missing SQL-string-check exemption for integrity_gate.py**
- **Found during:** Task 1 (writing `integrity_gate.py`'s `_reject_file`)
- **Issue:** The plan's own acceptance criteria only mentioned adding `integrity_gate.py` to `_EXEMPT_FROM_IMPORT_CHECK`, but `tests/policy/test_dag_thinness.py::test_no_raw_sql_strings` scans the entire `airflow/dags/**` tree for `INSERT INTO`/`SELECT `/`UPDATE ` literals with zero exemptions today. `_reject_file`'s structurally-necessary raw SQL INSERT would fail that test, blocking the plan's own stated verification command (`pytest tests/policy/test_dag_thinness.py -x`).
- **Fix:** Added a second, independently-scoped `_EXEMPT_FROM_SQL_CHECK` frozenset (documented distinctly from the import-check exemption, so the two carve-outs never implicitly grant each other) containing only `airflow/dags/_common/integrity_gate.py`.
- **Files modified:** `tests/policy/test_dag_thinness.py`
- **Verification:** `pytest tests/policy/test_dag_thinness.py -x` passes (3/3)
- **Committed in:** `956bfdd` (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Necessary to make the plan's own stated verification command pass; no scope creep -- the exemption is scoped to exactly the one file the plan's own design requires it for.

## Issues Encountered

None caused by this plan's changes. A full `pytest tests/policy` run (broader than
this plan's own stated verification scope) showed 4 pre-existing failures unrelated
to this plan's diff (`git diff --stat` confirms only `integrity_gate.py`,
`test_dag_thinness.py`, and `test_integrity_gate.py` were touched):
`test_dag_line_budget.py::test_csv_ingest_customers_stays_under_150_lines` and three
`test_manifest_validation_fails_closed.py` cases, the latter three failing because
`tools/bin/kubeconform` is not installed in this worktree (`run
tools/k8s/install_kubeconform.sh` or `make manifests` first). Out of scope per this
plan's file list and verification block; left untouched.

## User Setup Required

None - no external service configuration required. (`analytics_db_default` is an existing Airflow Connection from prior phases; this plan does not create it, only reads it via `BaseHook.get_connection`.)

## Next Phase Readiness

- `integrity_gate.py` exports both `list_matched_keys` and `integrity_gate`, fully unit-tested and independent of any other Wave-1 work in this phase -- ready for plan 08-12 to wire `.expand(key=list_matched_keys(...))` over `integrity_gate` into the two existing DAGs.
- No live-cluster verification was performed in this plan (by design -- this is a standalone module wired into a DAG only in plan 08-12); `analytics_db_default`'s actual Vault-backed resolution and a real `meta.files` rejection row have not yet been proven against a live cluster. That proof belongs to plan 08-12 once the gate is actually wired into `csv_ingest_customers`.

---
*Phase: 08-validation-quarantine-metadata-control-plane-completion*
*Completed: 2026-08-17*

## Self-Check: PASSED

- FOUND: airflow/dags/_common/integrity_gate.py
- FOUND: tests/unit/test_integrity_gate.py
- FOUND: .planning/phases/08-validation-quarantine-metadata-control-plane-completion/08-02-SUMMARY.md
- FOUND commit: 956bfdd (Task 1)
- FOUND commit: 240d6b4 (Task 2)
