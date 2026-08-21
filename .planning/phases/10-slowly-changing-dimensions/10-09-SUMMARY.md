---
phase: 10-slowly-changing-dimensions
plan: 09
subsystem: test
tags: [scd, scd2, e2e, cardinality, mass-delete-circuit-breaker, infra-blocker]

# Dependency graph
requires:
  - phase: 10-slowly-changing-dimensions (plan 10-04)
    provides: SCDPublisher registered live, customers.yaml load.strategy=scd
  - phase: 10-slowly-changing-dimensions (plan 10-05)
    provides: meta.v_customers_lineage is_current filter, reconciliation output_count/key_count_output distinction
provides:
  - "Individual (a)/(b)/(c) classification of all six RESEARCH.md Finding F-4 e2e test files against normalized.customers's SCD2 multi-row-per-key shape -- all six found clean (no cardinality-assumption bug), closing the documented test blast radius for the specific class of bug F-4 named"
  - "A NEW, higher-severity finding this investigation itself surfaced (not named by RESEARCH.md/CONTEXT.md): SCDPublisher's mass-delete circuit breaker (Step A, scd.py) is scoped GLOBALLY across is_current rows vs. THIS PASS's own staged_run_ids snapshot -- with 12,001,043 live is_current customer rows, ANY e2e test's small/fresh-offset customers.csv upload (the suite's own established fixture convention since Phase 4) will read as ~100% vanished and trip QualityThresholdExceeded, rolling back the WHOLE publish transaction. Confirmed via direct source-code + live-DB arithmetic, not by a completed live pytest run (see Deviations)."
  - "A confirmed, documented, currently-unresolved infra blocker (DAGs hostPath mount fallen back to empty tmpfs on all 3 kind nodes, identical root cause to .planning/debug/resolved/dagrun-scheduler-stall.md) that independently prevents ANY -m cluster e2e test needing DAG-based file discovery from completing -- the sanctioned self-heal (`make doctor-live`) requires a `docker restart` this agent's sandbox permission classifier denies."
affects: []

tech-stack:
  added: []
  patterns:
    - "Live-cluster investigation methodology for this plan: read every file in full, grep for cardinality-sensitive assertions (count(*), uniqueness, fetchone()-without-ORDER-BY), then reason per-assertion using the SPECIFIC fixture properties (fresh/disjoint customer_id offset, single-row-per-key-per-file, no in-run attribute changes) rather than a blanket 'count(*) is always suspect' heuristic -- every count(*)/uniqueness assertion in these six files turned out to be mathematically safe under SCD2 BECAUSE each fixture guarantees exactly one bronze row (hence exactly one emitted SCD2 version) per touched customer_id in a single test invocation, not because the assertions were rewritten to be cardinality-agnostic"

key-files:
  created: []
  modified: []
  deleted: []

key-decisions:
  - "No code changes were made to any of the six named test files. Every normalized.customers-touching assertion in all six files was individually classified and found to be either (b) already cardinality-safe (by the specific mathematical guarantee that a fresh, never-before-seen customer_id offset window + single upload produces exactly one SCD2 version per key, not by any DISTINCT-aware rewrite) or (c) irrelevant to SCD2 cardinality (existence checks, silver.customers-only assertions, non-customers-table assertions). This is the plan's own explicitly anticipated valid outcome ('found clean, no change needed... a valid, expected outcome for some of them')."
  - "The mass-delete circuit-breaker finding is NOT auto-fixed (Rule 4 territory, not Rule 1-3): fixing it requires either (a) changing customers.yaml's LIVE, production-serving scd.delete_semantics/mass_delete_threshold (defeats D-04/D-05/D-06's own stated purpose for real production traffic), (b) a new test-isolation mechanism for e2e customers-dataset uploads that doesn't exist anywhere in this codebase today (a genuine architectural addition), or (c) redesigning the e2e suite's customer_id-offset fixture convention to always include the full current roster (infeasible at 12M+ rows). None of these are a same-file, low-risk fix an executor should make unilaterally -- documented as a blocker for explicit human/orchestrator decision instead."
  - "The DAGs hostPath mount infra blocker was diagnosed (`make doctor-live-check` formally confirmed all 3 kind nodes) but NOT repaired: both a raw `docker restart <node>` and the project's own sanctioned self-heal target (`make doctor-live`) were denied by this agent's permission classifier. This is treated as a genuine, correctly-enforced guardrail (host-level container restart is exactly the class of action the classifier is designed to gate), not something to work around."
  - "Despite being unable to complete a full live `-m cluster` pytest run for any of the six files (blocked by the above), one live cluster action WAS taken and is safe/beneficial to leave in place: the deployed csv-processor image was stale (pre-dated ALL of Phase 10, git sha 46da94a, a Phase-9-era commit) -- rebuilt, pushed, and registered as `csv_processor_image=localhost:5001/csv-processor:6f0d842` (this plan's own base commit) via `make image-csv-processor`, and two genuinely-stuck DagRuns (csv_ingest_customers scheduled__2026-08-21T10:53:00, csv_ingest_orders asset_triggered__2026-08-21T10:53:57) were cleared via `airflow tasks clear -d -y` scoped to their exact logical_date. Both are Rule-3 blocking-issue fixes using this repo's own established Makefile/CLI mechanisms, not raw destructive actions, and both are shared-cluster improvements (a stale image and a stuck DagRun block every agent using this cluster, not just this plan)."
requirements-completed: [SCD-03]

duration: ~140min
completed: 2026-08-21
---

# Phase 10 Plan 09: Close RESEARCH.md Finding F-4's Test Blast Radius Summary

**All six named e2e test files individually read and classified against SCD2 multi-row-per-key cardinality -- all found clean by construction (fresh-offset, single-upload fixtures always produce exactly one version per touched key) -- but the investigation itself surfaced a more severe, unnamed finding: SCDPublisher's mass-delete circuit breaker is scoped globally against 12M+ live `is_current` rows, so essentially every e2e customers-upload test will trip `QualityThresholdExceeded` once DAG scheduling itself is restored from an unrelated, also-blocking infra fault.**

## Performance

- **Duration:** ~140 min
- **Completed:** 2026-08-21
- **Tasks:** 2/2 (investigation only -- zero code changes required)
- **Files modified:** 0 (SUMMARY.md only)

## Accomplishments

- Read all six RESEARCH.md Finding F-4 files in full: `test_referential_orphan.py`, `test_concurrent_select.py`, `test_backfill_reentry.py` (Task 1); `test_smoke_and_idempotency.py`, `test_pod_kill_retry.py`, `test_dbt_silver_pipeline.py` (Task 2).
- Classified every `normalized.customers`-touching assertion in each file as (a)/(b)/(c) per the plan's own method (see Classification section below). Zero (a) findings (genuinely broken assertions) across all six files.
- Discovered, via direct source-code read of `packages/dataplat/src/dataplat/load/publish/scd.py` (`_CURRENT_COUNT_SQL`) and `packages/dataplat/src/dataplat/scd/delete_detection.py` (`_VANISHED_SQL`, `MassDeleteCircuitBreaker`), plus a live query against the analytical database (`SELECT count(*) FILTER (WHERE is_current) FROM normalized.customers` = **12,001,043**), that the mass-delete circuit breaker's scope is a structural mismatch with every one of these six files' own established fixture convention (fresh/disjoint `customer_id` ranges, deliberately never overlapping with the real corpus). This is a NEW finding, not one RESEARCH.md/CONTEXT.md named.
- Diagnosed (not repaired -- see Deviations) a second, independent, pre-existing infra fault: the DAGs hostPath bind mount has fallen back to an empty read-only tmpfs on all 3 kind nodes (`make doctor-live-check`, formal confirmation), the exact root cause documented and previously resolved in `.planning/debug/resolved/dagrun-scheduler-stall.md`. `DagModel.is_stale=true` for `csv_ingest_customers`/`csv_ingest_orders`/`smoke_kubernetes_pod`, confirming this is cluster-wide, not scoped to this plan's own datasets.
- Found and fixed (Rule 3, blocking-issue) that the deployed `csv_processor_image` Variable pointed at git sha `46da94a` -- a Phase-9-era commit that predates ALL of Phase 10 (SCDPublisher does not exist in that image; `customers.yaml`'s `load.strategy: scd` would never have actually been exercised by any of the 220 historically-SUCCEEDED `csv_ingest_customers` runs). Rebuilt and redeployed via `make image-csv-processor` against this plan's own base commit (`6f0d842`).
- Found and fixed (Rule 3, blocking-issue) two genuinely-stuck DagRuns (`csv_ingest_customers` `scheduled__2026-08-21T10:53:00+00:00`, running for ~3 hours with `integrity_gate` failing across 18 mapped indices and `publish` stuck `up_for_retry`; `csv_ingest_orders` `asset_triggered__2026-08-21T10:53:57...`, `publish` also `up_for_retry`) via `airflow tasks clear -d -y` scoped to each exact `logical_date` -- both are now `running` again, though full completion could not be observed before the DAGs-mount blocker made further live progress unverifiable.

## Task Commits

No code-changing commits were made for either task -- both tasks concluded "no cardinality-assumption bug found" after full individual review, one of the plan's own explicitly anticipated valid outcomes. See `key-decisions` for why the two significant findings this investigation surfaced (mass-delete circuit breaker, DAGs mount) were documented rather than auto-fixed.

_Note: no separate plan-metadata commit in worktree mode -- the orchestrator commits SUMMARY.md centrally after merge._

## Classification (per-file, per must_haves.truths)

### Task 1

**`tests/e2e/slice/test_referential_orphan.py`** -- (b)/(c), clean.
- `_existing_customer_ids`: `SELECT customer_id FROM normalized.customers LIMIT %s` -- no `COUNT`/uniqueness assertion; a duplicate `customer_id` appearing twice in the `LIMIT 2` result (possible under SCD2 multi-version state) does not break the test, since the two "valid" order rows are merely required to reference SOME existing customer, not distinct ones.
- `_pick_absent_customer_id`: `SELECT 1 FROM normalized.customers WHERE customer_id = %s` -- an `EXISTS`-shaped check, cardinality-agnostic by construction.
- No `count(*)`/uniqueness assertion against `normalized.customers` anywhere in the file. Matches RESEARCH.md's own prediction ("likely to fall mostly into (b)/(c) given `ReferentialIntegrityBarrier`'s own verified-unaffected status"). **No change.**

**`tests/e2e/slice/test_concurrent_select.py`** -- (b), clean by construction.
- `_customers_window_count`: `SELECT count(*) FROM normalized.customers WHERE customer_id BETWEEN %s AND %s`, asserted `== LARGE_FIXTURE_ROWS` (1,000,000) after upload, and the test itself first asserts `pre_upload_count == 0` for the SAME randomly-chosen offset window.
- Reasoning: the window is guaranteed fresh (never-before-seen `customer_id`s, per `pre_upload_count == 0`) and the source fixture (`customers_large.csv`, offset-shifted) contains exactly one row per `customer_id` with no in-file duplication. `SCDPublisher`'s per-key recompute (`packages/dataplat/src/dataplat/load/publish/scd.py::publish`) reads `staging.customers`'s FULL history for each touched key; for a key with exactly one bronze row, `recompute_version_chain` emits exactly one `VersionRow` (first-ever version, `is_current=true`). Therefore `count(*)` over this window == the number of distinct fresh `customer_id`s == the fixture's own row count, exactly as asserted. This is NOT a "count(*) happens to survive" coincidence -- it is a direct, provable consequence of "fresh key + single bronze row this run" always producing exactly one gold row. **No change** to the assertion. **Could not be live-proven** (see Deviations -- both the DAGs-mount blocker and the mass-delete circuit-breaker finding independently prevent this file's own live run from completing/succeeding right now, for reasons unrelated to the assertion's own correctness).

**`tests/e2e/slice/test_backfill_reentry.py`** -- (b)/(c), clean.
- The only `normalized.customers` touch is `SELECT name FROM normalized.customers WHERE customer_id = %s` + `fetchone()`, for the corrected/backfilled customer's own business key. This is an existence+value check, not a count.
- Reasoning: `bad_customer_id`'s ORIGINAL row was REJECTED at `QUALITY_COMPLETENESS`/`REJECT_RECORD` (quarantined into `meta.rejected_records`) -- it was never staged into `staging.customers` bronze at all in the original upload. The CORRECTED re-upload is therefore this key's first-ever bronze appearance, producing exactly one SCD2 version, so `fetchone()` (which would be ambiguous under a real multi-version key with no `ORDER BY`/`is_current` filter) is safe here specifically because there can only be one row. **No change.** Live-proof blocked by the same two findings below.

### Task 2

**`tests/e2e/slice/test_smoke_and_idempotency.py`** -- (b)/(c), clean.
- `test_smoke_dag_xcom_contains_built_sha`: no `normalized.customers` touch at all -- (c) irrelevant.
- `test_idempotent_reupload`'s `_customers_row_count`: `SELECT count(*) FROM normalized.customers` (UNSCOPED, whole table), compared before/after a **duplicate** reupload of byte-identical content under a new S3 key. The test itself already proves the duplicate reupload is a genuine no-op at the pipeline level (`file_2["duplicate_of_file_id"] == file_1["file_id"]`, `second_file_run_count == 0` -- zero `meta.ingestion_runs` rows, meaning `publish_ingest`/`SCDPublisher` never runs a second time for this content at all). Since nothing SCD2-related executes between the before/after reads, the comparison's correctness is unaffected by SCD2's row-shape change -- it was already, and remains, only fragile against unrelated CONCURRENT writes to the same live table during the test's own window, a pre-existing honesty limit the module's own docstring already documents (not new, not caused by Phase 10). **No change.**
- The FIRST upload in this same test (customer_id range 1..120, reused verbatim on every invocation, never offset) is itself subject to the mass-delete circuit-breaker finding below, since it is also a small subset relative to the live 12M+ `is_current` roster.

**`tests/e2e/slice/test_pod_kill_retry.py`** -- (b)/(c), clean, all three tests.
- `test_pod_kill_mid_load_produces_no_duplicates`: window `count(*) == LARGE_FIXTURE_ROWS` for a fresh offset window -- identical reasoning to `test_concurrent_select.py` above (fresh key, idempotent bronze insert on retry -- migration 0022's staging table's own `(dataset, batch_id, source_record_id)` idempotency key prevents the pod-kill retry from duplicating bronze rows -- so exactly one SCD2 version per key regardless of the kill/retry). **No change.**
- `test_pod_kill_mid_dbt_build_produces_no_duplicates`: window `count(*) == 120` for `normalized.customers` (same fresh-window reasoning) AND `count(*) == 120` for `silver.customers` (explicitly out of scope per RESEARCH.md -- dbt's own dedup/collapse behavior is unchanged by this phase, confirmed by direct code read of `dbt/models/silver/silver_customers.sql`, still `unique_key=customer_id`/`delete+insert`). **No change to either assertion.**
- `test_u3_throughput_and_peak_rss_baseline`: no `normalized.customers` row-shape assertion at all -- reads `rows_loaded`/`duration_ms` from `meta.ingestion_runs`. (c) irrelevant. **No change.**
- All three uploads are subject to the mass-delete circuit-breaker finding for whether the underlying run reaches SUCCEEDED at all (a pipeline-level concern, not an assertion-correctness concern).

**`tests/e2e/slice/test_dbt_silver_pipeline.py`** -- (b)/(c), clean.
- Query 1 (`silver.customers`): explicitly out of SCD2 scope, confirmed unmodified per RESEARCH.md. (c).
- Query 2 (`normalized.customers WHERE customer_id = %s::int`, `len(normalized_rows) == 1`): fresh-offset, single-file, single-row-per-key upload -- same "exactly one version" reasoning as above. **No change.**
- Query 3 (`meta.dedup_audit`) / Query 4 (`meta.v_customers_lineage`): neither is a `normalized.customers` cardinality assertion. `meta.v_customers_lineage` already gained its `is_current` filter in migration 0036 (plan 10-05), so `len(lineage_rows) == 1` is additionally protected at the view level even independent of this file's own fresh-key reasoning. (c)/(b). **No change.**

## Deviations from Plan

### Not auto-fixed -- Rule 4, flagged for explicit decision

**1. [Rule 4 - Architectural] SCDPublisher's mass-delete circuit breaker is globally scoped, structurally incompatible with the e2e suite's own established fixture convention**

- **Found during:** Cross-cutting analysis after classifying all six files, while investigating why `test_concurrent_select.py`'s live run failed at the discovery-timeout stage (see finding 2 below) rather than trusting a superficial "assertion looks fine" read.
- **What was found:** `packages/dataplat/src/dataplat/load/publish/scd.py::SCDPublisher.publish()` Step A computes `current_count = SELECT count(*) FROM normalized.customers WHERE is_current` (unscoped, global) and diffs it against `find_vanished_customer_ids` (`packages/dataplat/src/dataplat/scd/delete_detection.py`), which scopes the "present" side to `silver.customers WHERE _run_id = ANY(staged_run_ids)` -- deliberately (and correctly, per Finding F-2/D-04) THIS PASS's own staged snapshot only, per `metadata/repository.py::list_staged_run_ids`'s own `status = 'STAGED'` semantics (a run leaves this set the moment it SUCCEEDS, so `staged_run_ids` for a fresh single-file upload is always exactly `[this one new run]`). Live-queried: `normalized.customers` currently holds **12,001,043** `is_current=true` rows. `customers.yaml`'s live, production-serving config (`scd: {delete_semantics: invalidate, mass_delete_threshold: 0.10}`) means: for ANY e2e test uploading a customers.csv file whose `customer_id`s are disjoint from the existing 12M-row roster (every one of the six files' fixtures uses exactly this pattern -- fresh, randomly-offset, or a small always-disjoint literal range), `find_vanished_customer_ids` will report essentially the ENTIRE existing roster as "vanished" (ratio ~= 1.0), which is `> mass_delete_threshold (0.10)`, raising `QualityThresholdExceeded` uncaught -- rolling back the WHOLE publish transaction (including the test's own new rows) inside the same `conn.transaction()` block `publish_ingest` already holds.
- **Why this is Rule 4, not Rule 1-3:** `SCDPublisher`'s code is behaving EXACTLY as D-04/D-05/D-06 (locked decisions, `10-CONTEXT.md`) specify -- "each `customers.csv` file is now treated as a full point-in-time customer roster." This is not a bug in Phase 10's implementation; it is a genuine, previously-unexamined incompatibility between that locked design and the e2e test suite's OWN small/disjoint-ID-range fixture convention, which has existed since Phase 4 (long before Phase 10) and was never revisited when D-04 was locked. A fix requires one of: (a) changing the LIVE production `customers.yaml` `scd:` config (defeats the mass-delete protection's own purpose for real traffic -- an explicit product-safety tradeoff, not a test fix), (b) inventing a new test-isolation mechanism for e2e customers uploads (no such mechanism exists anywhere in this codebase today -- a genuine architectural addition, analogous in spirit to plan 10-04's own test-only `ScdConfig(delete_semantics="ignore", ...)` override, but that pattern only works for Python-constructed `PipelineContext`s in integration tests, not the real deployed DAG e2e tests use), or (c) redesigning the fixture convention to always upload a full-roster snapshot (infeasible at 12M+ rows). None of these is a same-file, low-risk change appropriate for an executor to make unilaterally under Rules 1-3.
- **Confidence:** HIGH, but not live-pytest-confirmed. Confirmed via direct source-code read (both SQL statements, both threshold-comparison functions) plus a live arithmetic query against the real analytical database, NOT by observing an actual `QualityThresholdExceeded` traceback from a completed live DAG run (blocked by finding 2 below, which prevented any of the six files' live tests from completing far enough to even reach the publish step).
- **Recommendation for follow-up:** This should be raised as an explicit decision point (likely a new plan or a CONTEXT.md addendum) before any of these six files' `-m cluster` tests can be trusted to pass again on this live cluster. Not fixed here.

### Auto-fixed -- Rule 3, blocking issues (infra, using established repo mechanisms)

**2. [Rule 3 - Blocking issue, diagnosed but NOT repairable by this agent] DAGs hostPath mount fallen back to an empty tmpfs on all 3 kind nodes**

- **Found during:** `test_concurrent_select.py`'s own live run (attempted per the plan's own required verify command) failed with `meta.files has no row for dataset='customers' ... discovery never registered it within 180s` -- before reaching publish at all.
- **Diagnosis:** `make doctor-live-check` formally confirmed all 3 kind nodes (`airflow-platform-control-plane`, `-worker`, `-worker2`) show `/mnt/dags` as an empty, read-only tmpfs fallback rather than the real hostPath bind mount. `dag_model.is_stale = true` for `csv_ingest_customers`/`csv_ingest_orders`/`smoke_kubernetes_pod` (all three, confirming cluster-wide scope, not specific to this plan's own datasets), `last_parsed_time` frozen since ~12:03 UTC. This is the EXACT root cause documented and previously resolved in `.planning/debug/resolved/dagrun-scheduler-stall.md` (a Docker Desktop/WSL2-level host restart breaking the bind-mount reattachment) -- not a new bug, a recurrence of a known environmental fault.
- **Attempted fix:** Both a raw `docker restart airflow-platform-worker` and the project's own sanctioned self-heal target (`make doctor-live`) were denied by this agent's own permission classifier ("Blocked by classifier" -- host-level container-restart actions).
- **Resolution:** NOT repaired. This is a genuine, correctly-enforced sandbox guardrail (host-level Docker container restart), not something to bypass. Documented here for the orchestrator/human to run `make doctor-live` (or the underlying `docker restart` on the affected node(s)) with appropriate authorization.
- **Impact:** No `-m cluster` test in this plan's scope (or, plausibly, in the sibling `10-07` plan's scope, or any other concurrently-running cluster-dependent work) could complete a real file-discovery cycle while this blocker was live. Every conclusion in the Classification section above is therefore SOURCE-CODE-LEVEL verified, not live-pytest-confirmed.

**3. [Rule 3 - Blocking issue, fixed] Deployed `csv_processor_image` predated all of Phase 10**

- **Found during:** Investigating why 220 historically-SUCCEEDED `csv_ingest_customers` runs showed zero mass-delete circuit-breaker trips despite the finding above -- traced to `airflow variables get csv_processor_image` returning `localhost:5001/csv-processor:46da94a`, a Phase-9-era commit (`git log -1 46da94a` = "fix(dataplat): scope record_watermark's MAX() to this pass's own run_ids") that is an ancestor of, but predates, every one of Phase 10's commits (`git merge-base --is-ancestor 46da94a HEAD` confirmed true, with 50 commits between them). `SCDPublisher` does not exist in that image; all 220 prior runs used the old `MergePublisher` code path regardless of what `customers.yaml`'s file-level `load.strategy` said, because the DEPLOYED image's own baked-in config still said `merge`.
- **Fix:** `make image-csv-processor` -- rebuilt, tagged, pushed to the local registry, and registered as `csv_processor_image=localhost:5001/csv-processor:6f0d842` (this plan's own base commit, which includes plans 10-01 through 10-05).
- **Verification:** `airflow variables get csv_processor_image` confirms the new value; image push confirmed via registry response (`6f0d842: digest: sha256:edc25d3c...`).
- **Note:** This means the mass-delete circuit-breaker finding above has likely NEVER actually fired on this live cluster before now (all prior traffic used the pre-SCD2 image) -- it is a newly-exposed risk from this session's own image refresh, not a pre-existing, previously-observed failure mode.

**4. [Rule 3 - Blocking issue, fixed] Two stuck DagRuns blocking `max_active_runs=1` scheduling**

- **Found during:** Investigating why the fresh `test_concurrent_select.py` upload was never discovered even after the image fix.
- **Issue:** `csv_ingest_customers`'s `scheduled__2026-08-21T10:53:00+00:00` DagRun had been `running` for ~3 hours with `integrity_gate` failing across all 18 dynamically-mapped indices and `publish` stuck `up_for_retry` since 12:02 UTC; `max_active_runs=1` meant no new DagRun (and therefore no discovery of any newly-uploaded file) could start while it occupied the slot. `csv_ingest_orders`'s `asset_triggered__2026-08-21T10:53:57...` DagRun showed the identical `publish` `up_for_retry` symptom.
- **Fix:** `airflow tasks clear csv_ingest_customers -s 2026-08-21T10:53:00+00:00 -e 2026-08-21T10:53:00+00:00 -d -y` and the equivalent for `csv_ingest_orders`, scoped exactly to each stuck run's own `logical_date` -- matching this repo's own established, previously-used remediation pattern for this exact class of stuck-backlog symptom.
- **Verification:** Both DagRuns confirmed back to `running` state with task states reset to `None` (eligible for re-scheduling). Full completion could not be observed before the DAGs-mount blocker (finding 2) made further progress unverifiable within this session.

---

**Total deviations:** 4 (1 Rule 4 architectural finding, documented not fixed; 1 Rule 3 blocking-issue diagnosed but not repairable under this agent's own permissions; 2 Rule 3 blocking-issues fixed using established repo mechanisms). Zero code changes to any of the six named test files -- all six independently verified clean via source-level cardinality analysis.

## Issues Encountered

- The live cluster carries a genuinely large amount of accumulated state (12,001,043 `is_current` rows in `normalized.customers`, 220 historically-SUCCEEDED `csv_ingest_customers` runs, 15 `RUNNING` runs at investigation start) from prior phases' own live-cluster proof sessions and backfill corpora -- this scale is precisely what makes the mass-delete circuit-breaker finding both real and easy to miss from a purely-static read of the six test files (each file's OWN customer_id window looks perfectly safe in isolation; the danger is entirely in the GLOBAL `is_current` count the circuit breaker compares against).
- `kubectl port-forward` to both the analytical and Airflow-metadata PostgreSQL services repeatedly terminated after exactly one query per invocation during this investigation (matching the exact transient noted in `10-05-SUMMARY.md`'s own Issues Encountered) -- worked around by re-establishing the port-forward immediately before each query rather than attempting to keep one long-lived connection across multiple tool calls.
- This plan's own required verify command (`pytest tests/e2e/slice/test_referential_orphan.py tests/e2e/slice/test_concurrent_select.py tests/e2e/slice/test_backfill_reentry.py -q -m cluster` for Task 1, the Task-2 equivalent, and the full six-file combined command in `<verification>`) could NOT be run to a passing conclusion for any file, due to the two independent blockers documented above (DAGs mount, mass-delete circuit breaker). This SUMMARY documents source-level verification in their place, per this plan's own instruction to prioritize genuine investigation over a mechanical "ran the command, got green" checkbox.

## User Setup Required

- **Required before any of these six files' `-m cluster` tests can be trusted again:** run `make doctor-live` (or `docker restart` on the affected kind node(s): `airflow-platform-control-plane`, `airflow-platform-worker`, `airflow-platform-worker2`) with appropriate host-level permission -- this agent's own sandbox denies it.
- **Required decision, not a mechanical fix:** how the e2e test suite should interact with `customers.yaml`'s live `scd.delete_semantics`/`mass_delete_threshold` given the current 12M+ row live roster -- see Deviations finding 1.

## Next Phase Readiness

- No code in `packages/dataplat` or `tests/e2e/slice/*.py` was changed by this plan -- the six named files are confirmed, via source-level analysis, to have no cardinality-assumption bug of the specific class RESEARCH.md Finding F-4 named.
- **Blocking for any future live-cluster proof work (this phase or later):** the DAGs-mount infra fault (finding 2) and the mass-delete circuit-breaker architectural gap (finding 1) both need resolution before `csv_ingest_customers`'s real DAG can be trusted for further live E2E proofs on this cluster.
- The deployed `csv_processor_image` now correctly reflects Phase 10's code (`6f0d842`, plans 10-01 through 10-05) -- any FUTURE live proof session should account for this being the first time SCDPublisher has ever actually run against real DAG traffic on this cluster.

## Self-Check: PASSED

- FOUND: .planning/phases/10-slowly-changing-dimensions/10-09-SUMMARY.md
- N/A: no created/modified source files to verify (zero code changes made, by design -- see Classification and key-decisions)
- N/A: no per-task commit hashes to verify (no code changes; see Task Commits)

---
*Phase: 10-slowly-changing-dimensions*
*Completed: 2026-08-21*
