---
phase: 11-ci-cd-completion-operations
plan: 10
subsystem: testing
tags: [chaos-testing, qual-15, malformed-csv, encoding-detection, oom, execution-timeout, duplicate-batch, live-verification-blocked]

# Dependency graph
requires:
  - phase: 11-ci-cd-completion-operations
    plan: 09
    provides: tests/e2e/chaos/conftest.py scaffolding (kubectl/s3_client/analytics_connection re-exports), the `chaos` pytest marker
provides:
  - tests/e2e/chaos/test_malformed_csv.py, test_invalid_encoding.py, test_duplicate_batch.py, test_oom.py, test_task_timeout.py — code-complete, lint/format/mypy-clean, NOT live-verified this session (see Deviations)
  - airflow/dags/chaos_probe.py — a new, permanent, three-DAG throwaway-probe fixture (discover/stage/publish chain, undersized-memory publish, tiny-execution_timeout publish) that a future session can trigger without editing the production DAG files
affects: ["a future session re-attempting this plan's own live verification", "a possible /gsd:debug session investigating the cluster CPU-starvation incident this plan found"]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "A dedicated, permanent, never-scheduled probe DAG (chaos_probe.py) as the only way to get a real, DB-queryable Airflow task instance under a custom resource limit / execution_timeout, without editing a production DAG file or relying on `airflow tasks test` (which explicitly does not persist task-instance state)"
    - "Reusing an edge-case corpus fixture's own generated field VALUES (not its literal file bytes) when its declared header is schema-incompatible with every dataset actually configured on the live cluster — documented explicitly as a deviation from literal byte-for-byte reuse, with the reasoning inline"
    - "Racing two independent, real psycopg connections against MetadataRepository.claim_ingestion_run's own literal SQL (a synthetic, isolated PENDING run row, not a real uploaded file) to prove a concurrency-safe ledger constraint deterministically, without depending on timing against a live-scheduled production DagRun"

key-files:
  created:
    - airflow/dags/chaos_probe.py
    - tests/e2e/chaos/test_malformed_csv.py
    - tests/e2e/chaos/test_invalid_encoding.py
    - tests/e2e/chaos/test_duplicate_batch.py
    - tests/e2e/chaos/test_oom.py
    - tests/e2e/chaos/test_task_timeout.py
  modified:
    - tests/e2e/chaos/conftest.py (re-exports `airflow_metadata_connection` from tests.e2e.slice.conftest)
    - .planning/phases/11-ci-cd-completion-operations/deferred-items.md (new "Plan 11-10" section: the live cluster CPU-starvation incident)

key-decisions:
  - "Targeted `csv_ingest_orders`/`orders` for test_malformed_csv.py (not `customers`): `csv_ingest_customers` had a large, pre-existing `stage` backlog occupying its own `max_active_runs=1` budget for hours at this plan's own execution time — the identical live-cluster finding test_pod_crash.py's own module docstring already documents (11-09-PLAN.md), worked around the identical way"
  - "Reconstructed (not literally re-uploaded) 17_malformed_rows.csv's/06_windows1250.csv's own declared malformation/encoding shape onto customers.yaml's/orders.yaml's real column headers: neither corpus fixture's own header matches any dataset actually configured on this live cluster, and CsvSource.inspect() would reject a genuinely mismatched header as a whole-file BREAKING schema change (IncompatibleSchemaError) before RaggedRowGuard/encoding detection ever ran — verified by reading packages/csv-processor/src/csv_processor/source.py directly, not assumed"
  - "Added a new, permanent airflow/dags/chaos_probe.py (three throwaway DAGs) rather than editing csv_ingest_customers.py or relying on `airflow tasks test`: confirmed live that `airflow tasks test --help` states it runs 'without checking for dependencies or recording its state in the database', so it cannot produce the DB-queryable task-instance state test_oom.py/test_task_timeout.py's own acceptance criteria require; editing the production DAG's own resources/execution_timeout would revert 10-07-PLAN.md's real OOM fix"
  - "test_duplicate_batch.py races two independent connections against an isolated, synthetic PENDING meta.ingestion_runs row (file_id/batch_id both NULL) rather than a real uploaded file's real discovered row: a real file's own row would almost certainly be claimed by the live production pipeline before this test's own two threads could ever race for it, making the assertion a test of timing luck rather than of the ledger constraint itself"
  - "Did NOT mark QUAL-15 complete, and did NOT claim any of this plan's 5 test files 'pass live' — a sustained (>90 minute, non-self-resolving within this session) live cluster CPU-starvation incident, confirmed unrelated to this plan's own new files (zero etl-namespace pods ever ran before the incident was first observed), blocked every attempt to run even the simplest test (test_duplicate_batch.py, no DAG trigger needed) past its own session-scoped autouse DAG-unpause fixture"

patterns-established:
  - "When a live cluster health incident blocks all verification for a sustained period, exhaust static verification first (py_compile, ruff check, ruff format --check, mypy, and the relevant tests/policy/* suite) before committing code as 'correct, live-verification-blocked' — every one of those passed clean for this plan's own 6 new/modified files"

# Metrics
duration: ~210min
completed: 2026-08-23
---

# Phase 11 Plan 10: Chaos II — Data/Resource Chaos Scenarios Summary

**Wrote all 5 remaining QUAL-15 chaos scenarios (malformed CSV, invalid encoding, OOM, task timeout, duplicate batch) plus a new permanent throwaway-probe DAG, all lint/format/mypy-clean and carefully researched against the real schema/migrations/production code — but could not live-verify any of them this session because of a sustained, pre-existing, independently-confirmed live-cluster CPU-starvation incident that blocked even the simplest test's own setup fixture.**

## Performance

- **Duration:** ~210 min (dominated by live-cluster research/investigation and the CPU-starvation incident itself, not by writing the code)
- **Completed:** 2026-08-23
- **Tasks:** 2 planned (both code-complete; NEITHER live-verified — see Deviations)
- **Files created/modified:** 8 (6 new: 1 DAG + 5 tests; 2 modified: conftest.py, deferred-items.md)

## Accomplishments

- `airflow/dags/chaos_probe.py` — a new, permanent, three-DAG throwaway-probe fixture
  (`chaos_probe_discover_stage_publish_customers`, `chaos_probe_oom_publish_customers`,
  `chaos_probe_timeout_publish_customers`), built entirely from `_common/kpo.py`'s existing
  `common_kpo_kwargs` helper — the identical pattern every task in `csv_ingest_customers.py`
  already uses. Confirmed live-mountable and syntactically valid (`python3 -m py_compile`,
  `airflow/dags/*.py`-scoped `tests/policy/test_dag_thinness.py` pass clean) but its actual
  live parse/registration by the real DAG processor could not be confirmed this session (the
  DAG processor pod was itself part of the CPU-starvation incident, restarting repeatedly).
- `tests/e2e/chaos/test_malformed_csv.py` — reconstructs `17_malformed_rows.csv`'s own two
  structural malformation types (`field-count-below-header`/`field-count-above-header`) onto
  `orders.yaml`'s real 4-column header, live-drives `csv_ingest_orders`, and asserts exactly 2
  `RAGGED_ROW` rejects + exactly 8 good rows loaded.
- `tests/e2e/chaos/test_invalid_encoding.py` — reshapes `06_windows1250.csv`'s own generated
  cp1250 field values onto `customers.yaml`'s real 5-column header, drives the new
  `chaos_probe_discover_stage_publish_customers` DAG, calls the real production
  `csv_processor.detect.encoding.detect_encoding` directly against the uploaded bytes for the
  confidence-score proof, and confirms every diacritic-bearing `name` value round-trips exactly
  in `normalized.customers`.
- `tests/e2e/chaos/test_duplicate_batch.py` — races two independent, real `etl_app` connections
  against `MetadataRepository.claim_ingestion_run`'s own literal SQL for a single isolated PENDING
  run row, proving the `idempotency_key` UNIQUE-constraint-backed claim mechanism is
  concurrency-safe under genuine PostgreSQL MVCC contention.
- `tests/e2e/chaos/test_oom.py` — triggers `chaos_probe_oom_publish_customers` (a real `publish
  --dataset customers` at the exact 256Mi limit 10-07-PLAN.md's own live sweep found insufficient),
  confirms a genuine `OOMKilled` container exit and a clean Airflow `failed` task state, and scopes
  its "zero corrupted rows" assertion precisely to whatever `meta.ingestion_runs` row the killed
  pod's own `k8s_pod_name` claimed.
- `tests/e2e/chaos/test_task_timeout.py` — triggers `chaos_probe_timeout_publish_customers` (a
  real `publish --dataset customers` with `execution_timeout=5s`, `retries=1`), confirms both
  attempts time out and the task reaches `failed` at `try_number=2` well under a bound that would
  catch a genuinely-hanging task.
- **Live-diagnosed and thoroughly documented a sustained (>90 minute, non-self-resolving within
  this session) cluster CPU-starvation incident** (`deferred-items.md`'s new "Plan 11-10"
  section): `docker stats` showed 276-481% CPU per kind node container throughout; the real
  Airflow scheduler pod cycled between `1/2 Running` (repeated liveness-probe timeouts) and a
  genuine `CrashLoopBackOff` (17-19 restarts observed); two separate, spaced attempts to run even
  the simplest new test (`test_duplicate_batch.py`, no DAG trigger needed) both failed identically
  at the shared `kubectl` fixture's own hardcoded 30s subprocess timeout. Independently confirmed
  NOT caused by this plan's own new files: `kubectl get pods -n etl` showed zero running pods at
  every point this was checked, before any `chaos_probe` DAG was ever triggered.

## Task Commits

1. **Task 1: malformed_csv + invalid_encoding + duplicate_batch + the new chaos_probe DAG** —
   see commit list below (code-complete, NOT live-verified — see Deviations)
2. **Task 2: oom + task_timeout** — see commit list below (code-complete, NOT live-verified — see
   Deviations)

_Note: NEITHER task satisfies its own plan-declared `<verify>` criterion
(`uv run --group cluster pytest tests/e2e/chaos -q -m cluster` passing) this session — see
"Deviations from Plan" and "Next Phase Readiness" below for the full, honest accounting, following
the exact precedent 11-09-SUMMARY.md's own key-decisions already established for this identical
situation._

## Files Created/Modified

- `airflow/dags/chaos_probe.py` — three throwaway, manually-triggered `dataplat` CLI probe DAGs;
  not declared in this plan's own `files_modified` frontmatter (deviation, documented below)
- `tests/e2e/chaos/test_malformed_csv.py` — VALID-01/VALID-03 structural-rejection proof, targets
  `orders`
- `tests/e2e/chaos/test_invalid_encoding.py` — CSV-02/CSV-03 encoding-detection proof, targets
  `customers` via the new probe DAG
- `tests/e2e/chaos/test_duplicate_batch.py` — LOAD-08-adjacent concurrent-claim-race proof
- `tests/e2e/chaos/test_oom.py` — ORCH-04/META-03 OOM regression proof
- `tests/e2e/chaos/test_task_timeout.py` — ORCH-04 execution_timeout/retry proof
- `tests/e2e/chaos/conftest.py` — re-exports `airflow_metadata_connection`; not declared in this
  plan's own `files_modified` frontmatter (deviation, documented below)
- `.planning/phases/11-ci-cd-completion-operations/deferred-items.md` — new "Plan 11-10" section

## Decisions Made

See `key-decisions` in the frontmatter above for the five decisions with rationale: targeting
`orders` for the malformed-CSV test, reconstructing (not literally re-uploading) two corpus
fixtures' own declared shapes, adding a new permanent probe DAG instead of editing production code
or relying on `airflow tasks test`, racing an isolated synthetic row for the duplicate-batch proof,
and not claiming live-pass status given the sustained platform incident.

## Deviations from Plan

### Scope additions (Rule 3 — blocking issue, no other viable path)

**1. [Rule 3] Added `airflow/dags/chaos_probe.py`, not declared in this plan's `files_modified`**
- **Found during:** Task 1, while designing `test_invalid_encoding.py` and Task 2's `test_oom.py`/
  `test_task_timeout.py`
- **Issue:** `test_oom.py`/`test_task_timeout.py` need a real, DB-queryable Airflow task instance
  running under a custom memory limit / `execution_timeout`. Live-confirmed `airflow tasks test
  --help`: "This will run a task without checking for dependencies or recording its state in the
  database" — this CLI path cannot satisfy the plan's own acceptance criteria ("the Airflow task
  instance reaches a clean failed/up_for_retry state"). Editing `csv_ingest_customers.py` itself to
  reintroduce an undersized `publish` resource limit would revert 10-07-PLAN.md's own real,
  permanent fix. `csv_ingest_customers` also carried a large pre-existing `stage` backlog occupying
  its `max_active_runs=1` budget for hours, blocking `test_invalid_encoding.py`'s own need for a
  fresh, promptly-processed customers-dataset run.
- **Fix:** Added a new, permanent, never-scheduled DAG file with three minimal probe DAGs, built
  entirely from the existing `_common/kpo.py` helper — zero new business logic, zero changes to any
  production DAG.
- **Files modified:** `airflow/dags/chaos_probe.py` (new)
- **Verification:** `python3 -m py_compile`, `ruff check`/`ruff format --check` (clean), and the
  `airflow/dags/*.py`-scoped `tests/policy/test_dag_thinness.py`/`test_dag_line_budget.py` suites
  (clean — the file is not in the named line-budget list at all, so no budget applies; the generic
  thinness/import scan passed). **Live DAG registration/parse by the real DAG processor was NOT
  confirmed this session** (see "Issues Encountered").
- **Committed in:** see commit list below

**2. [Rule 3] Added `airflow_metadata_connection` re-export to `tests/e2e/chaos/conftest.py`, not
declared in this plan's `files_modified`**
- **Found during:** Task 2, writing `test_oom.py`/`test_task_timeout.py`
- **Issue:** Both tests must query `task_instance.state`/`try_number` directly against the real
  Airflow metadata database — the only DB-queryable proof of a clean terminal task state. That
  fixture already exists in `tests/e2e/slice/conftest.py` but was not yet re-exported into
  `tests/e2e/chaos/conftest.py` (a sibling directory, per that module's own established
  re-export convention for exactly this reason).
- **Fix:** Added one import line, following the exact same re-export pattern already used for
  `analytics_connection`/`analytics_owner_connection`/`slice_fixtures_dir`.
- **Files modified:** `tests/e2e/chaos/conftest.py`
- **Verification:** `ruff check`/`mypy` clean.
- **Committed in:** see commit list below

---

**Total deviations:** 2 Rule-3 scope additions (both narrow, both following an already-established
in-repo convention, neither touching a production DAG's own behavior). **Not a deviation, but the
dominant finding of this plan's own execution:** the live cluster CPU-starvation incident
documented in full in `deferred-items.md`'s "Plan 11-10" section, which is why none of this
session's 5 test files could be live-verified — see "Issues Encountered" and "Next Phase
Readiness" below.

## Issues Encountered

- **The live cluster CPU-starvation incident** (full write-up: `deferred-items.md`'s "Plan 11-10"
  section). Summary: sustained (>90 minutes, non-self-resolving within this session) 276-481%
  CPU/node saturation on the shared kind cluster, most likely rooted in `csv_ingest_customers`'s
  own pre-existing `stage` backlog (independently, repeatedly documented in this project's own
  `STATE.md` history as a recurring characteristic, never before observed to cascade into a
  genuine scheduler `CrashLoopBackOff`). This blocked every attempt to run even the simplest new
  test (`test_duplicate_batch.py`) past its own session-scoped `_unpause_slice_dags` autouse
  fixture — two separate, spaced attempts both failed identically at the shared `kubectl` fixture's
  hardcoded 30s subprocess timeout. Confirmed, via `kubectl get pods -n etl` showing zero running
  pods at every check performed before the incident was first observed, that this plan's own new
  files did not cause it.
- **Consequence: `chaos_probe.py`'s own live registration by the real DAG processor could not be
  confirmed.** Every `kubectl exec ... airflow dags list`/`list-import-errors` attempt (several,
  spaced across the session, up to a 280s timeout) either timed out or never returned within this
  session's own time budget. `python3 -m py_compile` and the full `tests/policy/test_dag_thinness.py`
  suite both passed clean against the new file, and its constructs (`KubernetesPodOperator`, `@dag`,
  `@task`, `common_kpo_kwargs`) are byte-for-byte identical in shape to what `csv_ingest_customers.py`
  and `smoke_kubernetes_pod.py` already use successfully in production — high confidence it parses
  correctly, but this is inference from static review, not a live-confirmed fact, and is flagged as
  such honestly here.

## User Setup Required

None for the code itself. **A live cluster health check is required before this plan's own
remaining work (live verification) can proceed** — see "Next Phase Readiness" below.

## Next Phase Readiness

**What's ready:**
- All 5 test files plus the new `chaos_probe.py` DAG are code-complete, well-researched against
  the real schema/migrations/production source (not assumed), and pass every static check this
  session could actually run: `python3 -m py_compile`, `ruff check` (0 errors across all 6 new/
  modified files), `ruff format --check` (clean), `mypy` (0 errors across the 5 test files +
  conftest.py), and the relevant `tests/policy/*` suites.
- `deferred-items.md`'s "Plan 11-10" section gives a future session (or a dedicated `/gsd:debug`
  session) a complete, timestamped account of the cluster incident, its live diagnostic evidence,
  and an explicit recommendation to confirm `airflow-scheduler` reaches a genuine, sustained `2/2
  Ready` before attempting this plan's own live verification again.

**Blockers for a fully "done" plan 11-10 / QUAL-15:**
- **None of this session's 5 new test files have been live-verified.** `uv run --group cluster
  pytest tests/e2e/chaos -q -m cluster` — this plan's own `<verification>` block — has not been
  run to completion this session, let alone confirmed passing for the full 9-file suite (this
  plan's 5 + plan 11-09's 4).
- **`chaos_probe.py`'s live DAG registration is unconfirmed** (see "Issues Encountered" above) —
  the very first live step any future session must take.
- **QUAL-15 remains "Pending"** in REQUIREMENTS.md, exactly as plan 11-09 left it — this plan does
  not mark it complete, matching that plan's own established precedent for an identical situation.
- Recommended immediate next step for whichever session resumes this work: confirm
  `airflow-scheduler` shows a genuine, sustained `2/2 Ready` (not merely `1/2 Running`, which this
  session repeatedly observed to be a false-recovery signal), THEN run
  `uv run --group cluster pytest tests/e2e/chaos/test_duplicate_batch.py -q -m cluster` first (the
  simplest, no-DAG-trigger test) as a canary before attempting the other 4.

## Self-Check: PASSED

- `airflow/dags/chaos_probe.py` — FOUND
- `tests/e2e/chaos/test_malformed_csv.py` — FOUND
- `tests/e2e/chaos/test_invalid_encoding.py` — FOUND
- `tests/e2e/chaos/test_duplicate_batch.py` — FOUND
- `tests/e2e/chaos/test_oom.py` — FOUND
- `tests/e2e/chaos/test_task_timeout.py` — FOUND
- `tests/e2e/chaos/conftest.py` (modified, `airflow_metadata_connection` present) — FOUND
- `.planning/phases/11-ci-cd-completion-operations/deferred-items.md` ("## Plan 11-10" section) —
  FOUND

---
*Phase: 11-ci-cd-completion-operations*
*Completed: 2026-08-23*
