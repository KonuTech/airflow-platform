---
phase: 11-ci-cd-completion-operations
plan: 14
subsystem: docs
tags: [runbooks, operations, obs-06, minio, postgresql, cnpg, vault, chaos-testing]

# Dependency graph
requires:
  - phase: 11-ci-cd-completion-operations
    provides: "11-09's chaos tests (test_minio_unavailable.py, test_database_unavailable.py, test_vault_unavailable.py) and 11-13's 15 real-incident/existing-feature runbooks + test_runbooks_structure.py"
provides:
  - "The final 3 of README §89's 18 operational runbooks (MinIO/PostgreSQL/secret unavailable)"
  - "docs/runbooks/ now contains exactly 18 files -- OBS-06 fully, structurally complete"
  - "tests/policy/test_runbooks_structure.py's permanent 18-file count + exact-name-set regression guards"
affects: []

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "When a live chaos-test precondition (existing referenced data) cannot be satisfied because
      shared-cluster state is genuinely empty/blocked, the fault-injection mechanism can still be
      reproduced manually (same kubectl commands the fixture/test use) against a disjoint fixture
      that does not depend on the missing precondition -- captures the same real symptom text
      without requiring the full automated test to pass end-to-end"
    - "A pod-log watcher script (kubectl get -l ... | kubectl logs -f) started BEFORE the fault is
      injected, not after, is required to capture a KPO pod's own stdout/stderr before
      on_finish_action=delete_pod removes it -- there is no remote logging configured on this
      platform, matching wait-for-files-stuck-task.md's own established finding"

key-files:
  created:
    - docs/runbooks/minio-unavailable.md
    - docs/runbooks/postgresql-unavailable.md
    - docs/runbooks/secret-unavailable.md
  modified:
    - tests/policy/test_runbooks_structure.py

key-decisions:
  - "The live cluster's analytics DB genuinely had 0 rows in normalized.customers/meta.ingestion_runs
    at execution time (a rebuild-from-raw from plan 11-12 dropped the schema and the reload is
    blocked by a live, separately-tracked Kyverno/Docker-Hub rate-limit issue a sibling parallel
    agent is fixing this same wave). The real chaos tests' own _existing_customer_ids precondition
    (>=20 rows) could not be satisfied, so all 3 automated tests failed at that assertion before
    ever reaching their own fault-injection code. Worked around by manually reproducing each test's
    own exact fault-injection/recovery mechanism (kubectl scale/annotate commands, identical to what
    the tests' own fixtures execute) against csv_ingest_orders using disjoint synthetic order_ids
    that do not require existing customers to observe -- capturing genuine live symptom text and
    recovery behavior without needing the full pipeline to reach SUCCEEDED."
  - "postgresql-unavailable.md's live-captured symptom text comes from a 'stage' task pod, not
    literally 'discover' (the test's own designated first DB-touching task) -- csv_ingest_orders'
    max_active_runs=1 was occupied by a large pre-existing accumulated backlog DagRun for the
    entire session, so a dedicated fresh trigger for this scenario could not get a discover pod in
    reasonable time. Both tasks call the identical csv_processor.cli._build_common() ->
    psycopg_pool.ConnectionPool.open(wait=True) connection path, so the captured failure signature
    (PoolTimeout / Connection refused to port 5432) is the same either way -- documented explicitly
    in the runbook's own provenance note rather than silently mis-attributed."
  - "secret-unavailable.md was written from a live root-token vault kv get against a genuinely
    nonexistent path (etl/nonexistent-workload-secret), not from a DAG run at all -- no dedicated
    chaos test exists for this scenario (per 11-RESEARCH.md's own §89 source-mapping table), and the
    scenario's own diagnostic (SEC-08's audit log distinguishing a missing-path read from Vault
    itself being sealed) does not require pipeline execution to demonstrate."
  - "Task 2's TDD split has no natural RED state: Task 1 already committed the 3 missing runbook
    files before Task 2's new count/set assertions were written, so the 18-file precondition those
    assertions check was already true at write time. Committed as a single test commit with the
    reasoning stated in the commit message, following 11-13's own precedent for a test-only task
    with no separate <implementation> block."

requirements-completed: [OBS-06]

# Metrics
duration: ~75min
completed: 2026-08-23
---

# Phase 11 Plan 14: Final 3 Operational Runbooks (18/18) + Structural Completeness Guard Summary

**The final 3 of README §89's 18 operational runbooks (MinIO/PostgreSQL/secret unavailable), each
written from genuinely live-reproduced fault-injection evidence captured against the real cluster
(not from chaos-test source code alone), completing OBS-06 with a permanent 18-file structural
regression guard.**

## Performance

- **Duration:** ~75 min
- **Completed:** 2026-08-23T15:56:00Z
- **Tasks:** 2
- **Files modified:** 4 (3 new runbooks + 1 extended policy test)

## Accomplishments

- `docs/runbooks/minio-unavailable.md` — live-captured the exact `EndpointConnectionError` /
  `NewConnectionError` / `ConnectionRefusedError [Errno 111]` chain from `list_matched_keys`
  (`dags/_common/integrity_gate.py:111`) while `deployment/minio` was genuinely scaled to zero
  replicas, then live-confirmed clean recovery: a fresh trigger's `list_matched_keys` succeeded,
  `discover` registered the file in `meta.files` with `duplicate_of_file_id IS NULL`.
- `docs/runbooks/postgresql-unavailable.md` — live-captured a real `psycopg_pool.PoolTimeout: pool
  initialization incomplete after 30.0 sec` / `connection to server at "10.96.100.222", port 5432
  failed: Connection refused` from a DB-touching KPO pod while the analytical CNPG `Cluster` was
  genuinely hibernated (`cnpg.io/hibernation: "on"`), then live-confirmed zero-data-loss recovery
  via `meta.ingestion_runs`' unchanged row count immediately after un-hibernation.
- `docs/runbooks/secret-unavailable.md` — live-captured `vault kv get etl/nonexistent-workload-secret`
  returning `No value found at etl/data/nonexistent-workload-secret` (exit 2) against an otherwise
  healthy, unsealed Vault, and live-confirmed the audit log's own distinguishing signature (a
  missing-path read produces a `response`-type entry with no `data` key at all, vs. a present
  `response.data.data` field for a successful read) — the diagnostic that separates this scenario
  from `vault-unavailable.md`'s own sealed-Vault case.
- `tests/policy/test_runbooks_structure.py` extended with `test_docs_runbooks_contains_exactly_18_
  files` and `test_the_full_verified_scenario_set_is_covered_by_filename`, using an explicit
  hard-coded 18-item filename list transcribed from 11-RESEARCH.md's own verified §89 list — a
  future accidental rename or deletion now fails loudly by name, not just by count.

## Task Commits

Each task was committed atomically:

1. **Task 1: 3 chaos-derived runbooks from live reproduction** - `9d13486` (docs)
2. **Task 2: Extend structural policy test to the full 18-file set** - `8e593a0` (test)

**Plan metadata:** commit to follow (docs: complete plan)

## Files Created/Modified

- `docs/runbooks/minio-unavailable.md` - live-captured MinIO-scale-to-zero fault/recovery
- `docs/runbooks/postgresql-unavailable.md` - live-captured CNPG hibernation fault/recovery
- `docs/runbooks/secret-unavailable.md` - live-captured missing-path vs. sealed-Vault distinction
- `tests/policy/test_runbooks_structure.py` - full 18-file count + exact-name-set regression guards

## Decisions Made

See `key-decisions` in frontmatter above for the full reasoning on:
1. Working around the live analytics DB's empty state (a currently-blocked, separately-tracked
   platform issue) by manually reproducing each test's own exact fault-injection mechanism against
   a disjoint fixture rather than running the full automated test to completion.
2. `postgresql-unavailable.md`'s symptom capture coming from a `stage` task pod rather than
   `discover`, documented explicitly as a provenance note in the runbook itself.
3. `secret-unavailable.md` being written from a direct Vault interaction rather than a DAG run,
   matching the scenario's own "no dedicated chaos test exists" nature per 11-RESEARCH.md.
4. Task 2's TDD split having no natural RED state, committed as a single test commit with the
   reasoning stated inline.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Worked around an empty live analytics DB blocking all 3 chaos tests' own precondition**
- **Found during:** Task 1, first attempt to run the 3 chaos tests live per the plan's own action text
- **Issue:** `uv run --group cluster pytest tests/e2e/chaos/test_minio_unavailable.py tests/e2e/chaos/
  test_database_unavailable.py tests/e2e/chaos/test_vault_unavailable.py -q -m cluster -v` failed
  all 3 tests identically at `_existing_customer_ids`'s own precondition assertion
  (`assert len(customer_ids) == 20`) — direct `psql` query confirmed `normalized.customers` and
  `meta.ingestion_runs` both genuinely had 0 rows on the live cluster. Root cause: plan 11-12's own
  `rebuild-from-raw` dropped the schema as part of its own live proof, and the reload is blocked by
  a separately-tracked, currently-being-fixed Kyverno/Docker-Hub anonymous-rate-limit issue
  (documented in `deferred-items.md`'s Plan 11-12 entry) that a sibling parallel worktree agent is
  actively fixing this same wave (unrelated files: `kubernetes/kyverno-policy.yaml`,
  `airflow/dags/_common/kpo.py`).
- **Fix:** Rather than force a destructive/long-running cluster mutation to restore
  `normalized.customers` (which would itself depend on the same blocked Kyverno fix to complete,
  and risks colliding with the sibling agent's own concurrent live verification), manually
  reproduced each test's own exact fault-injection and recovery mechanism (the identical `kubectl
  scale deployment/minio`, `kubectl annotate cluster analytics-db cnpg.io/hibernation=on/-`, and
  `vault kv get` commands the tests/fixtures themselves execute) against `csv_ingest_orders` using
  freshly-generated, disjoint synthetic `order_id`s that do not require pre-existing customer data
  to exercise the specific fault being tested (MinIO/DB unreachability manifests identically
  regardless of whether the uploaded file's rows later pass referential-integrity validation).
  Captured real pod logs live via a `kubectl logs -f` watcher started before each fault was
  injected (matching this platform's own "no remote logging configured" constraint, already
  established in `wait-for-files-stuck-task.md`).
- **Files modified:** `docs/runbooks/minio-unavailable.md`, `docs/runbooks/postgresql-unavailable.md`
  (both cite this live-capture methodology explicitly in their own provenance sections)
- **Verification:** Both scenarios' full fault→symptom→recovery cycle was independently confirmed
  live: MinIO scenario confirmed `list_matched_keys` failing then a fresh trigger succeeding with
  `meta.files` correctly populated; PostgreSQL scenario confirmed the pool-timeout symptom and
  `meta.ingestion_runs`' row count unchanged (no data loss) after un-hibernation.
- **Committed in:** `9d13486` (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking — worked around a pre-existing, separately-tracked,
currently-being-fixed platform blocker without touching the blocker itself or performing any
destructive/irreversible cluster mutation).
**Impact on plan:** The plan's own D-41/T-11-37 requirement ("written from... observed behavior")
is fully satisfied — all symptom text, error signatures, and recovery confirmations in all 3
runbooks were genuinely captured live against the real cluster, not read only from chaos-test
source code. The only deviation from the plan's literal action text ("run the 3 chaos tests... with
`-v`") is that the full automated pytest suites were run first (and their failure output is itself
genuine, captured live evidence of the current blocked state), then the same fault mechanisms were
reproduced manually once the tests' own precondition proved unsatisfiable on this session's live
cluster. No scope creep; no plan file was modified to work around this.

## Issues Encountered

- `csv_ingest_customers`'s DAG concurrency (`max_active_runs=1`) was occupied for the entire session
  by a large pre-existing accumulated backlog `DagRun` (dozens of mapped `integrity_gate`/`stage`
  tasks from earlier E2E sessions), preventing a dedicated fresh manual trigger from starting in
  reasonable time. Worked around by using `csv_ingest_orders` instead (confirmed free of any
  running `DagRun` at the time) for all live reproductions.
- The PostgreSQL-unavailable reproduction's own dedicated fresh trigger similarly queued behind
  `csv_ingest_orders`' own in-flight recovery `DagRun` from the MinIO scenario (itself carrying a
  large accumulated `raw/orders/*.csv` backlog). Rather than wait an unbounded amount of time for
  that backlog to fully drain, captured the DB-unavailable symptom from one of that same backlog's
  own in-flight `stage` tasks — a real, live DB-touching failure, explicitly documented in the
  runbook as coming from `stage` rather than `discover` (see key-decisions above).
- Both cluster mutations performed for this plan (`deployment/minio` scale, `cluster/analytics-db`
  hibernation annotation) were restored immediately after capturing evidence, each confirmed
  `Available`/`Ready` again before proceeding, matching the automated tests' own `finally`-block
  restoration guarantee.

## Known Stubs

None — all 3 runbooks are fully written from genuine live evidence, no placeholder content.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- OBS-06 is now fully and correctly delivered: `docs/runbooks/` contains exactly 18 files, matching
  the verified README §89 list exactly, each with the 5 required headings, structurally proven by
  `tests/policy/test_runbooks_structure.py`'s own permanent regression guard.
- No blockers introduced for any other Phase 11 plan. The pre-existing, separately-tracked platform
  blockers this plan worked around (empty analytics DB pending the sibling agent's Kyverno fix;
  `csv_ingest_customers`/`csv_ingest_orders` accumulated backlogs) remain exactly as documented in
  `STATE.md`'s existing Blockers/Concerns and `deferred-items.md`'s Plan 11-12 entry — this plan
  neither worsened nor resolved them, and performed no destructive mutation of any pre-existing
  state (only transient, immediately-reversed fault-injection on infrastructure the automated chaos
  tests already exercise identically).

---
*Phase: 11-ci-cd-completion-operations*
*Completed: 2026-08-23*

## Self-Check: PASSED

- All 5 claimed files (`docs/runbooks/minio-unavailable.md`, `docs/runbooks/postgresql-
  unavailable.md`, `docs/runbooks/secret-unavailable.md`, `tests/policy/test_runbooks_structure.py`,
  this `SUMMARY.md`) confirmed present via `ls`.
- Both commit hashes (`9d13486`, `8e593a0`) confirmed present via `git log --oneline --all`.
