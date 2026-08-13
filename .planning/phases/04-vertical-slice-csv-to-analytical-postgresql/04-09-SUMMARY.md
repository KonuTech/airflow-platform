---
phase: 04-vertical-slice-csv-to-analytical-postgresql
plan: 09
subsystem: tooling
tags: [minio, boto3, psycopg, kubectl-port-forward, s3, cli, developer-tooling, make]

# Dependency graph
requires:
  - phase: 04-02
    provides: "airflow/dags/csv_ingest_customers.py -- the deferred S3KeySensor -> discover -> ingest DAG this demo waits on, and the raw/customers/ object-key convention"
  - phase: 04-07
    provides: "kubernetes/rbac-etl.yaml and the confirmed csv_ingest_customers DAG shape (S3KeySensor 30s poke, dataset=customers) this script's docstring cites"
provides:
  - "scripts/ingest-demo.py -- upload FILE to s3://raw/customers/, poll meta.ingestion_runs (joined through meta.files by content sha256) until terminal, print a human-readable receipt; never calls any Airflow CLI-trigger equivalent (D-15)"
  - "make ingest-demo FILE=<path> -- the one-command developer demo (D-14)"
  - "make cluster-verify now collects tests/e2e/cluster AND tests/e2e/slice (plan 04-08) in one target"
  - "a documented, reproduced-live finding that kubectl port-forward to analytics-db-rw in this project's WSL2/kind environment serves exactly one real connection per tunnel -- and the one-tunnel-per-connection pattern that works around it"
affects: [phase-04-completion-verification]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Content-addressed polling: a script outside the cluster identifies 'its own' ingestion outcome by joining meta.ingestion_runs to meta.files on the uploaded file's sha256 content hash, never on object key -- a byte-identical re-run resolves to the same governing run immediately, which is the intended demonstration of 'a re-run produces zero additional rows', not a bug."
    - "One kubectl port-forward tunnel per connection to analytics-db-rw from a host-side script (not one long-lived tunnel reused across many polls) -- matches tests/e2e/cluster/test_postgres_topology.py's own existing convention, and is required (not merely nicer) in this environment: a reused tunnel's second connection reliably fails."
    - "time.monotonic() deadline loop for a bounded live-cluster wait, never a blind sleep; prints last-observed diagnostic state on timeout and always exits nonzero."

key-files:
  created:
    - scripts/ingest-demo.py
  modified:
    - Makefile
    - .planning/phases/04-vertical-slice-csv-to-analytical-postgresql/deferred-items.md

key-decisions:
  - "Run location: scripts/ingest-demo.py runs from the developer's own host (like tests/e2e/cluster/), reaching analytics-db-rw via kubectl port-forward, never a direct in-cluster DSN -- resolves 04-CONTEXT.md's open design point in favor of the already-proven tests/e2e/cluster/ connection pattern."
  - "Terminal status set narrowed to {SUCCEEDED, FAILED}: traced dataplat.pipeline.run._skipped_receipt and confirmed 'SKIPPED_DUPLICATE'/'SKIPPED_CONCURRENT' are Receipt/XCom-only presentation labels that dataplat never writes back to meta.ingestion_runs.status -- a DB-polling script can never actually observe those two strings."
  - "duration_ms: meta.ingestion_runs.duration_ms is never persisted by finalize_publication (pre-existing gap, out of this plan's scope, logged in deferred-items.md) -- the receipt query works around it with COALESCE against started_at/finished_at, both of which ARE persisted."
  - "One fresh kubectl port-forward tunnel per poll check, not one long-lived tunnel reused across the whole timeout window -- see Deviations below."

requirements-completed: [QUAL-06]

# Metrics
duration: 40min
completed: 2026-08-13
---

# Phase 04 Plan 09: Developer ingest demo Summary

**`make ingest-demo FILE=<path>` uploads to MinIO and polls the live analytical Postgres (content-hash-joined `meta.ingestion_runs`/`meta.files` over a fresh-per-check `kubectl port-forward` tunnel) until a terminal status, with zero CLI-trigger shortcuts around the DAG's sensor.**

## Performance

- **Duration:** ~40 min
- **Started:** 2026-08-13T16:48:07Z (worktree base checkout)
- **Completed:** 2026-08-13T17:26:49Z
- **Tasks:** 2 completed
- **Files modified:** 3 (1 created, 2 modified)

## Accomplishments

- `scripts/ingest-demo.py`: a standalone, ruff-clean (`select = ["ALL"]`, only the repo's standard `scripts/**` carve-outs), mypy-clean CLI that uploads a local CSV to `s3://raw/customers/`, then polls `meta.ingestion_runs` (joined through `meta.files` by content sha256) on the live analytical PostgreSQL cluster until a terminal status, printing a human-readable receipt (`run_id`/`status`/`rows_loaded`/`duration_ms`/`report_uri`).
- `grep -n "dags trigger\|dags_trigger\|trigger_dag" scripts/ingest-demo.py` returns zero matches (D-15) — verified live, including catching a self-referential trap where an earlier docstring draft named the forbidden grep pattern verbatim and tripped its own check.
- `make ingest-demo` / `make cluster-verify` wired into the Makefile, both verified against the live cluster and against the full `tests/policy` suite (zero regressions: 116 passed / 2 pre-existing failures already logged by four prior plans / 10 deselected — identical counts to 04-07's own baseline run).
- Live-debugged and fixed a real bug in this plan's own new polling code: a reused `kubectl port-forward` tunnel to `analytics-db-rw` fails its second real connection in this project's WSL2/kind environment (reproduced deterministically, 5/5, with a bare manual script). Redesigned to one fresh tunnel per poll check, matching `tests/e2e/cluster/test_postgres_topology.py`'s own existing (and, it turns out, load-bearing) convention.

## Task Commits

Each task was committed atomically:

1. **Task 1: scripts/ingest-demo.py — upload, poll, print the receipt** - `8cd1e1a` (feat)
2. **Task 2: make ingest-demo and the cluster-verify extension** - `d7d6b6e` (feat)

**Plan metadata:** (this commit, immediately following) `docs(04-09): complete developer ingest demo plan`

## Files Created/Modified

- `scripts/ingest-demo.py` - Upload/poll/receipt CLI (726 lines); never calls any Airflow CLI-trigger equivalent
- `Makefile` - `FILE ?=` variable, `ingest-demo` target (guarded on `FILE`), `cluster-verify` extended to `tests/e2e/cluster tests/e2e/slice`
- `.planning/phases/04-vertical-slice-csv-to-analytical-postgresql/deferred-items.md` - Appended `## From Plan 04-09` with two out-of-scope discoveries (see below)

## Decisions Made

See `key-decisions` in the frontmatter for the four substantive ones (run location, terminal-status set, `duration_ms` workaround, one-tunnel-per-connection). All were open design points this plan's own `<action>` text explicitly delegated ("decide and document") or gaps found while implementing against the real, live schema rather than an assumed one.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Long-lived `kubectl port-forward` tunnel reused across poll checks failed its second real connection**

- **Found during:** Task 1, live verification of the poll loop against the real cluster (before the task's commit — the plan's own acceptance criteria require a live round trip, so this was caught during required verification, not left for later).
- **Issue:** The first implementation opened one `kubectl port-forward` tunnel to `analytics-db-rw` for the whole `--timeout` window and reused it for a fresh `psycopg` connection every 5s poll. Live testing showed the tunnel dies after exactly one real (data-carrying) connection in this project's WSL2/kind environment (`kubectl`'s own stderr: `read: connection reset by peer`, `error: lost connection to pod`); every subsequent connection attempt over the same tunnel got "connection refused". Reproduced deterministically 5/5 with a standalone manual script (see `scripts/ingest-demo.py`'s `_poll_for_receipt` docstring for the full trace).
- **Fix:** First iteration added a retry-and-re-establish-the-tunnel loop (worked, but noisy and inefficient — roughly every other iteration paid the "die then retry" cost). Simplified to the final design: one fresh `kubectl port-forward` tunnel per poll check, opened and torn down each time — matching `tests/e2e/cluster/test_postgres_topology.py`'s own `_cluster_connection`/`_port_forwarded_postgres` convention (which, not by coincidence, never attempts a second connection per tunnel either). Zero connection errors across two subsequent live runs (20s and 30s windows, ~4 and ~6 poll cycles).
- **Files modified:** `scripts/ingest-demo.py` (within Task 1, before its commit).
- **Verification:** Live re-run against the real cluster, clean output, correct timeout diagnostic, correct nonzero exit.
- **Committed in:** `8cd1e1a` (Task 1 commit — the fix landed in the same commit; no broken version was ever committed).

**2. [Rule 1 - Bug] Module docstring's own D-15 prohibition text tripped the acceptance criteria's own grep check**

- **Found during:** Task 1, running the plan's own acceptance-criteria grep command as part of verification.
- **Issue:** The first draft's docstring named the prohibition by quoting `airflow dags trigger` and the literal grep pattern (`"dags trigger\|dags_trigger\|trigger_dag"`) it must satisfy — both self-matched, so `grep -n "dags trigger\|dags_trigger\|trigger_dag" scripts/ingest-demo.py` returned nonzero matches against the docstring itself, even though no actual trigger call exists anywhere in the file.
- **Fix:** Rewrote the PROHIBITION paragraph to describe the forbidden action in prose (Airflow's "classic manual-run subcommand") without ever spelling out the three literal substrings the mechanical check scans for, while still citing D-15 explicitly and explaining why (a repository policy check enforces this).
- **Files modified:** `scripts/ingest-demo.py` (within Task 1, before its commit).
- **Verification:** `grep -n "dags trigger\|dags_trigger\|trigger_dag" scripts/ingest-demo.py` exits 1 (zero matches).
- **Committed in:** `8cd1e1a` (Task 1 commit).

---

**Total deviations:** 2 auto-fixed (both Rule 1, both in this plan's own new file, both caught and fixed during required live/mechanical verification before any commit).
**Impact on plan:** Both fixes are internal to `scripts/ingest-demo.py`'s own first-draft correctness; neither touched any file outside this plan's scope, and both are now documented in the script's own docstrings so a future editor understands why the code is shaped the way it is. No scope creep.

## Issues Encountered

- **`tests/fixtures/csv/01_simple.csv`** (the plan's own acceptance-criteria example path) does not exist in this worktree — it is generated by `make fixtures`, not committed (only `corpus.yaml`/`CORPUS.sha256` are). Rather than invoking the corpus generator (out of this plan's scope, and `make fixtures` without `FAST=1` is documented as slow), live verification used a small scratch CSV file created outside the repo (`/tmp/...`, never committed) with the same shape (`customer_id,name,country,birth_date,event_ts`). This exercises identical code paths (upload is content-agnostic; the DAG that would parse the CSV's business content is not currently reachable in this session regardless — see below).
- **No DAG is currently registered on the shared live cluster** (`airflow dags list` → "No data found"; `meta.datasets`/`meta.files`/`meta.ingestion_runs` all report 0 rows). Traced to a live-infrastructure/deployment-lifecycle gap, not a code defect: `helm/values/local/airflow.yaml`'s `dags`-mount wiring (plan 04-02) is correct and merged, but the currently-running `airflow` Helm release on the shared cluster predates it (`kubectl get deploy airflow-dag-processor -o jsonpath='{.spec.template.spec.volumes}'` shows no `dags` volume). Deliberately NOT fixed by this plan: a `helm upgrade` would restart the scheduler/dag-processor/api-server/triggerer pods on the SAME cluster plan 04-08 is concurrently using for its own live E2E session, and is outside this plan's `files_modified` scope regardless. Full detail and a suggested owner are logged in `deferred-items.md`.
- **Consequence for this plan's own acceptance criteria:** the `status=SUCCEEDED` receipt path (Task 1's first acceptance-criteria bullet) could not be exercised live in this session. Everything else was: `--help`, the nonexistent-`--file` guard (exit 2, no upload attempted, verified before any credential resolution), the D-15 grep check, live MinIO upload (confirmed via a follow-up `list_objects_v2` read showing all four test uploads present with correct size), and the full poll-until-timeout diagnostic path (confirmed clean and correct across two live runs). `make ingest-demo` was verified through the same chain via its Makefile wiring (confirmed progressing identically once `PYTHONUNBUFFERED=1` was used to see interim output under an externally-bounded test window — the script's own default 300s timeout is longer than is practical to wait out in this session, and Python block-buffers stdout when not attached to a TTY, which was itself a testing-methodology red herring, not a script bug, before being diagnosed).

## User Setup Required

None - no external service configuration required. (When a developer next has exclusive use of the live cluster with `csv_ingest_customers` actually registered, `make ingest-demo FILE=<path>` is ready to use as-is.)

## Next Phase Readiness

- `scripts/ingest-demo.py` and `make ingest-demo`/`make cluster-verify` are complete, committed, and ready. No further code changes are anticipated to be needed once the live cluster's Airflow release picks up the already-merged DAG-mount wiring.
- **Blocker for a fully-live demonstration (not for this plan's own completion):** the live cluster's `airflow` Helm release needs re-converging (`helm upgrade`/`make stage-airflow`) before `csv_ingest_customers` is actually schedulable — see `deferred-items.md`. This phase's own end-to-end verification pass (or whichever plan next has exclusive use of the cluster) is the natural place to do that and then run `make ingest-demo FILE=...` for the full `status=SUCCEEDED` proof.
- `meta.ingestion_runs.duration_ms` is never persisted by `finalize_publication` (pre-existing, unrelated to this plan, logged in `deferred-items.md` with a suggested one-line fix for whoever next touches `dataplat/pipeline/run.py`/`metadata/postgres.py`).

---
*Phase: 04-vertical-slice-csv-to-analytical-postgresql*
*Completed: 2026-08-13*

## Self-Check: PASSED

- FOUND: `scripts/ingest-demo.py`
- FOUND: commit `8cd1e1a` (Task 1)
- FOUND: commit `d7d6b6e` (Task 2)
- FOUND: commit `02bef2f` (SUMMARY.md + deferred-items.md)
- FOUND: `ingest-demo:` target in `Makefile`
- FOUND: `tests/e2e/cluster tests/e2e/slice` in `Makefile`'s `cluster-verify` recipe
- FOUND: `## From Plan 04-09` section in `deferred-items.md`
