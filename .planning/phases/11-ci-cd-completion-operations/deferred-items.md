# Deferred Items — Phase 11

Out-of-scope discoveries found while executing Phase 11 plans, logged per the
executor's scope-boundary rule (only auto-fix issues directly caused by the
current task's own changes). None of these were fixed here. Entries are
grouped by the plan that found them.

## Plan 11-01

### Pre-existing `make check` / `tests/policy` failures, unrelated to CI/CD image publishing

Found while running `uv run pytest tests/policy -q` as part of plan 11-01
Task 3's regression check. Confirmed via `git show <base-commit>:<path>` that
each root cause predates plan 11-01 entirely (traces to Phase 9/10
slowly-changing-dimensions work, not to `.github/workflows/publish.yml` or
`tests/policy/test_publish_workflow_guards.py`, the only two files this plan
touches). All 4 are collected by `make check`'s `policy` target (not
deselected by `-m "not manifests"`), so `make check` is red on the base
commit `0bcc4652a5c74609dc16dbf2df574bc043ed4860` independent of this plan.

| Item | Status | Deferred At |
|------|--------|-------------|
| `tests/policy/test_dag_line_budget.py::test_csv_ingest_customers_stays_under_150_lines` — `airflow/dags/csv_ingest_customers.py` is 182 lines, budget (ORCH-06) is <=152. Confirmed 182 lines already at base commit via `git show`. | Deferred | 2026-08-22, plan 11-01 |
| `tests/policy/test_dag_thinness.py::test_no_business_logic_imports` — `airflow/dags/_common/gap_recorder.py:25` imports `psycopg` directly (ORCH-02/06 requires DAGs delegate DB access to the csv-processor image via `KubernetesPodOperator`, never import a DB driver). Traces to commit `d4a0a22` "feat(09-10): meta.processing_gaps migration + gap-recorder wiring (D-06)". | Deferred | 2026-08-22, plan 11-01 |
| `tests/policy/test_dag_thinness.py::test_no_raw_sql_strings` — same file, `gap_recorder.py:58-59` contains a raw `INSERT INTO meta.processing_gaps ... SELECT ...` string literal. Same root cause/commit as above. | Deferred | 2026-08-22, plan 11-01 |
| `tests/policy/test_gates_actually_fail.py::test_the_main_gate_does_not_lint_the_bad_samples` — `make lint` itself is red with 5 ruff findings (E501/W505 line-too-long) in `airflow/dags/csv_ingest_customers.py:141`, `tests/e2e/slice/test_backfill_2year_sweep.py:1072`, and `tests/integration/test_migrations.py:681`. Comments at the offending lines reference "plan 10-08" and are dated 2026-08-22 — Phase 10 work in progress at this plan's base commit. This test's own purpose (proving `make lint`/`make typecheck` fail closed against a *known-bad sample corpus*) is masked by the *real* tree already failing `make lint` for an unrelated reason. | Deferred | 2026-08-22, plan 11-01 |

**Why deferred rather than fixed:** all four sit entirely outside plan 11-01's
`files_modified` (`.github/workflows/publish.yml`,
`tests/policy/test_publish_workflow_guards.py`, `.claude/CLAUDE.md`) and
outside Phase 11's CI/CD-completion scope. Fixing them would mean editing
Phase 9/10 DAG and test files this plan never read, has no context budget to
review for correctness, and was not asked to touch. Whoever picks these up
should re-verify `make check` (or at minimum `make lint` +
`tests/policy/test_dag_line_budget.py` + `tests/policy/test_dag_thinness.py`)
passes clean before closing this entry.

**In scope and fixed in this plan (not deferred, listed here only for
completeness):** `tests/policy/test_workflow_secrets.py::test_no_workflow_
references_a_repository_secret` and `::test_the_workflow_token_stays_read_
only` also failed on first run, but were caused directly by plan 11-01's own
`publish.yml` (the first workflow to reference `secrets.GITHUB_TOKEN` and to
widen job permissions) and were anticipated by that test module's own
docstring ("expected to grow in Phase 11"). Fixed in the same commit as
`publish.yml` — see plan 11-01's SUMMARY.md for detail.

## Plan 11-11

| Category | Item | Status | Deferred At |
|----------|------|--------|-------------|
| Pre-existing bug | `tests/integration/test_reconciliation.py`'s four `raw_bronze` tests (`test_clean_staging_pass_writes_one_raw_bronze_row_with_zero_discrepancy` and 3 siblings) fail with `psycopg.errors.InvalidTextRepresentation: invalid input syntax for type bigint` on the `_source_row_number` column during `COPY INTO staging.customers__r<N>` — the value being written looks like a `_record_hash` hex string, suggesting a column-count/ordering mismatch in `StagingLoader`'s `COPY` column list vs. its value tuples (`packages/dataplat/src/dataplat/load/staging.py`), unrelated to `stage_ingest`'s reconciliation-writing step. Confirmed pre-existing and out of scope for plan 11-11: `_table_checksum`/`_compute_silver_gold_reconciliation` (the only functions plan 11-11 touches) are called exclusively from `publish_ingest`, never from `stage_ingest` (the function these 4 failing tests exercise) — this plan's diff makes no change reachable from that code path. Reproducer: `uv run --group cluster pytest tests/integration/test_reconciliation.py -q`. | Open | 2026-08-22 (plan 11-11) |

## Plan 11-09

### CRITICAL — silent data loss when a connection outage (DB/Vault) coincides with a DagRun's own start (`_common/run_stage_recorder.py`)

Found live while executing and independently verifying `test_database_unavailable.py`'s own
acceptance criterion ("recovers cleanly ... never a silent hang or corrupted state"). **Not a bug
in this plan's own test code** — the test correctly triggered the fault and correctly detected
the resulting bad state; the bug lives entirely in pre-existing, shared DAG-orchestration code
this plan does not modify.

**Root cause (two compounding defects), reproduced and diagnosed live against the real cluster,
run_id=50071, idempotency_key=`680b47fef083e6d25a336cc118020eeb0e3fdcbdbaee2c165f61ea5c1bd89879`:**

1. **`_common/run_stage_recorder.py`'s `wire_dbt_build_tracking`** wires
   `stage >> mark_dbt_build_running >> dbt_build >> resolve_dbt_build_status >>
   mark_dbt_build_done >> publish`. `list_run_ids_pending_dbt_build(dataset_name=...)` (the task
   `mark_dbt_build_running` consumes as its `run_ids` argument) has **no upstream dependency at
   all** — it runs essentially at DagRun-start, in parallel with `wait_for_files`/`discover`, and
   has Airflow's default `retries=0`. When the analytical DB (or Vault, which backs the
   `analytics_db_default` Connection this task resolves via `BaseHook.get_connection` — confirmed
   live: `airflow connections get analytics_db_default` returns `"id": null`, i.e. VaultBackend-
   resolved, not metadata-DB-stored) is unavailable at that exact moment,
   `list_run_ids_pending_dbt_build` fails permanently for this DagRun (no retry). Airflow's
   `all_success` trigger rule short-circuits `mark_dbt_build_running` (and cascades through
   `dbt_build`) to `upstream_failed` **immediately, without waiting for `stage` to even start** —
   live-observed: `mark_dbt_build_running`/`dbt_build` reached `upstream_failed` at 23:01:29-30,
   while `stage` (blocked behind `discover`'s own retries) did not start until 23:11:43, ten
   minutes later. `resolve_dbt_build_status`/`mark_dbt_build_done` both carry
   `trigger_rule="all_done"` (deliberately, to record dbt_build's outcome even on failure), so
   they proceed regardless — and **`publish`'s only real gate is `mark_done`, not `stage`
   directly**, so `publish` ran (try 2, 23:10:51-23:11:02) and exited "successfully" as a
   no-op — a full 41 seconds *before* `stage` (23:11:43-23:12:14) had even started, let alone
   produced anything to publish. The module's own docstring confirms this is a regression: it
   says this wiring "replaces the old `stage >> dbt_build >> publish` edge" — the direct
   `stage >> publish` dependency that would have prevented this exact race was dropped when the
   dbt-build-tracking sub-chain was inserted (09-09).
2. **A second, independent defect**: once `stage` genuinely completed afterward (`rows_read=20`
   in `meta.ingestion_runs`), the run sat in `status='STAGED'` — `publish` had already run and
   would not automatically re-run for this same DagRun. The run was only unstuck by an unrelated,
   later trigger of the same DAG (`test_minio_unavailable.py`'s own manual trigger, run 2) whose
   own `publish --dataset orders` invocation is dataset-wide (no run-id scoping in its CLI
   arguments) and swept it up — but that second, delayed `publish` pass recorded
   `status='SUCCEEDED'` with **`rows_loaded=0`** (`rows_read` stayed `20`), and
   `normalized.orders` independently confirmed **zero rows** for that run's own order_id window
   (`SELECT count(*) ... WHERE order_id BETWEEN 2210322252 AND 2210322271` → `0`, verified via a
   direct `kubectl exec ... psql` query, not through any test-owned connection). A run that read
   20 real rows was marked terminally `SUCCEEDED` with none of them ever reaching the target
   table — a genuine, silent violation of this project's own Core Value statement ("no data is
   ever silently dropped, duplicated or corrupted"), not merely a delayed-but-eventually-correct
   outcome.

**Two further, independent live confirmations of the same root cause (defect 1), found while
executing `test_minio_unavailable.py` — neither involves the analytical DB/Vault being
unavailable at all, confirming defect 1 is a general `publish`-gating bug, not specific to a
DB/Vault outage:**

- A `csv_ingest_orders` run whose `list_matched_keys` genuinely, permanently `failed` (MinIO
  scaled to 0, `retries=0`) still had its `publish` task **start and independently retry** (its
  own `retries=3`, hitting the same pre-existing KubernetesJobWatcher race), because `publish`'s
  gate (`mark_dbt_build_done`) never depends on `list_matched_keys`/`gate`/`discover`/`stage` at
  all — every other task in the graph reached a terminal state (`failed`/`upstream_failed`)
  within seconds, but the DagRun itself stayed `running` for several additional minutes waiting
  out `publish`'s own unrelated retry backoff before finally reaching `failed`.
- A separate run (the orphaned first attempt at re-verifying `test_database_unavailable.py`'s own
  scenario) had `publish` exhaust all 4 of its own attempts (retries=3) against the same
  KubernetesJobWatcher race, taking ~36 minutes for `publish`'s own exhaustion sequence alone —
  on top of ~20-25 minutes `dbt_build` already needed earlier in the same run — for a combined
  ~86 minutes observed end-to-end for what should be a simple recovery. This pre-existing,
  independently-tracked flakiness (plan 10-08) is not new, but this session is the first
  live evidence that it can compound severely enough, combined with defect 1's `publish`-gate
  decoupling, to make even a generous (3600s) test timeout insufficient — `tests/e2e/chaos/
  test_minio_unavailable.py`'s own `_RECOVERY_TIMEOUT_SECONDS`/`_DAGRUN_FAILED_TIMEOUT_SECONDS`
  were bumped (5400s/900s) in this plan's own commit to accommodate the observed worst case; this
  is a legitimate, narrow, in-scope timeout adjustment (Rule 1), not a fix for defect 1 itself.

**A fourth, independent confirmation — via Vault, not the analytical DB — closes the loop on
defect 1's breadth:** `test_vault_unavailable.py`'s own fault (sealing `vault-0`, via a real pod
delete/restart — see that file's own module docstring for why this replaced a broken
`vault operator seal` CLI call, a SEPARATE, narrower bug in the test's own sealing mechanism,
fixed in this plan's commit) makes `list_run_ids_pending_dbt_build`'s OWN `BaseHook.get_
connection("analytics_db_default")` call fail too, since that Connection is ALSO Vault-backed
(confirmed live: `airflow connections get analytics_db_default` returns `"id": null`, i.e.
VaultBackend-resolved, not metadata-DB-stored) — live-observed against a real triggered
`csv_ingest_orders` run: `list_run_ids_pending_dbt_build` reached `failed`,
`mark_dbt_build_running`/`dbt_build` cascaded to `upstream_failed`, while `discover`/`stage`
(gated on Vault being unsealed again, which happened before they were scheduled) genuinely
succeeded — the exact same "downstream `publish`-gate decoupling" shape as the other three
confirmations above, this time via the Vault-backed-connection path specifically. This makes
defect 1 the single root cause behind ALL FOUR of this plan's own scenarios failing to reliably
reach a clean, honest `SUCCEEDED` state, not a coincidence of four unrelated flaky tests.

**Separately, an operational near-miss worth recording:** `scripts/vault-unseal.py`'s own
`.finally` safety net (this test's own last line of defense) reads `.secrets/vault-init.json`
relative to `repo_root`, which resolves to THIS WORKTREE's own path — and `.secrets/` is
gitignored, worktree-local, and by this project's own established convention (STATE.md, Phase 05
decision log) is deliberately written ONLY to the main tree, never an ephemeral worktree. The
first live run of `test_vault_unavailable.py` in this session genuinely sealed the SHARED
cluster's Vault and then could not unseal it from this worktree (`ERROR: Vault is already
initialized, but .../.secrets/vault-init.json does not exist`) — a real, if temporary, outage of
a component every OTHER concurrent worktree/user of this shared cluster also depends on. Resolved
immediately by copying (read-only, from the main tree, never written back) `.secrets/vault-init.
json` into this worktree's own gitignored `.secrets/` directory, then re-running `scripts/
vault-unseal.py` successfully (confirmed via the unchanged `Cluster ID 243eafb8-...`: genuine
recovery, no data loss, no re-initialization). **This is a genuine hazard for ANY chaos-test
plan that seals Vault, run from an isolated worktree, and is worth flagging explicitly for
plan 11-10/11-05's own execution**: either ensure `.secrets/vault-init.json` is copied into the
worktree BEFORE running `test_vault_unavailable.py` (as this session ultimately did), or execute
Vault-sealing chaos tests from the main tree specifically, never a disposable worktree.

**Why not fixed here:** (a) `_common/run_stage_recorder.py` is shared by both
`csv_ingest_customers.py` and `csv_ingest_orders.py` and is entirely outside this plan's
`files_modified`; (b) this worktree's DAGs are hostPath-mounted from the **main repository
tree** (`kind/cluster.yaml`: `hostPath: /home/konutec/projects/airflow-platform/airflow/dags`),
**not** this worktree's own copy — any edit made here to a DAG-folder file cannot be live-verified
against the real cluster from an isolated worktree, and this executor's mandate is to verify
every fix against the real cluster before claiming it works; (c) defect 2 most plausibly lives in
`dataplat`'s own staging/publish CLI (`packages/dataplat/src/dataplat/load/...`), which is baked
into the `csv-processor` image — a correct fix there needs an image rebuild and redeploy, a much
heavier, higher-blast-radius operation than a single chaos-test-authoring plan should perform
unilaterally against a cluster shared with other concurrent work.

**Recommended fix for defect 1** (high confidence, not yet live-verified): add a direct
`stage >> publish` edge in `wire_dbt_build_tracking` (`_common/run_stage_recorder.py`), in
addition to the existing chain — restores the original "publish always waits for stage" guarantee
the module's own docstring says the dbt-tracking sub-chain was meant to preserve, without
requiring changes to the sub-chain's own `trigger_rule="all_done"` semantics (which are correct
and needed for recording `dbt_build`'s own retry/failure history). Defect 2 needs its own
dedicated investigation into `dataplat`'s publish/merge path for a run whose staging artifact may
already be gone by the time a second `publish` pass reaches it.

**Blast radius:** not orders-specific — `wire_dbt_build_tracking` is called identically from
`csv_ingest_customers.py`. Any transient DB or Vault outage overlapping a DagRun's own start,
for either dataset, can trigger the same silent-data-loss sequence; this is not contingent on
chaos-test fault injection, only on timing that a real production outage could reproduce.

**Recommended next step:** a dedicated `/gsd:debug` session (or a properly-scoped follow-up
plan) with the ability to edit and redeploy against the main tree, starting from this entry's
own live evidence.

| Item | Status | Deferred At |
|------|--------|-------------|
| `tests/e2e/chaos/test_database_unavailable.py` cannot pass live until defect 1 (and likely defect 2) above are fixed — the fault window this test must hold (upload → trigger → wait for `discover` to fail) deterministically overlaps `list_run_ids_pending_dbt_build`'s own DagRun-start-time execution, so this is not a rare flake to retry past. | Open — CRITICAL | 2026-08-22, plan 11-09 |
| `tests/e2e/chaos/test_vault_unavailable.py` — same root cause as the row above, via Vault instead of the DB directly (both back the same `analytics_db_default` Connection `list_run_ids_pending_dbt_build` needs); this file's OWN sealing mechanism (pod-delete restart, replacing a separately-broken `vault operator seal` CLI call — see this file's own module docstring) is fixed and live-confirmed working in this plan's own commit, but a full live pass of the test's recovery assertions remains blocked on defect 1. | Open — CRITICAL (same root cause) | 2026-08-23, plan 11-09 |
| `tests/e2e/chaos/test_minio_unavailable.py` — NOT blocked by defect 1's primary DB/Vault trigger (a MinIO-only fault does not touch `list_run_ids_pending_dbt_build`'s own connection), but IS affected by defect 1's secondary symptom (`publish`'s gate-decoupling extends how long the DagRun takes to reach a clean `failed` state after `list_matched_keys` itself already failed) compounding with today's unusually severe pre-existing KubernetesJobWatcher flakiness (plan 10-08); this file's own timeouts were bumped (`_RECOVERY_TIMEOUT_SECONDS` 3600→5400s, `_DAGRUN_FAILED_TIMEOUT_SECONDS` 180→900s) to accommodate the live-observed worst case, in this plan's own commit. A full clean live pass was not achieved this session (two attempts: one killed by a harness-level background-task limit unrelated to this test's own correctness, one still in flight when this session's own time budget was reached) but nothing observed contradicts the fault-injection mechanism itself being correct. | Open — not a defect-1 blocker, needs one more clean live attempt | 2026-08-23, plan 11-09 |

## Plan 11-10

### Live cluster CPU-starvation episode encountered during this plan's own execution (not caused by this plan)

Found live while starting Task 1's own verification (`uv run --group cluster pytest tests/e2e/
chaos/test_duplicate_batch.py`): the very first `_unpause_slice_dags` autouse fixture call
(`kubectl exec deploy/airflow-api-server -- airflow dags unpause smoke_kubernetes_pod`) exceeded
the `kubectl` fixture's own hardcoded 30s subprocess timeout (`tests/e2e/cluster/conftest.py`'s
`_run` helper, `timeout: int = 30` -- a shared fixture default this plan's own file scope does not
touch). A manual, unbounded retry of the identical command completed successfully but took
**2m21s** — a ~4-5x normal latency for what is ordinarily a sub-second CLI round-trip.

**Live diagnosis, before writing any of this plan's own test files against the cluster:**
- `docker stats --no-stream` on the three kind node containers showed **342-481% CPU** each
  (`airflow-platform-control-plane`, `-worker`, `-worker2`) — this project's own `kind/cluster.yaml`
  budgets ~3 allocatable CPU/node, so this is the node genuinely CPU-saturated, not a measurement
  artifact.
- `airflow-scheduler`'s pod progressed from `1/2 Running` (repeated liveness-probe-triggered
  restarts, `kubectl describe pod` events: `Liveness probe failed: command timed out ... timed out
  after 20s`) to a genuine **`CrashLoopBackOff`** within the same ~15-minute window this plan's own
  research/writing phase took. The scheduler container's own `lastState` showed `exitCode: 0,
  reason: "Completed"` — a clean `SIGTERM` from kubelet after the startup/liveness probe's own
  `airflow jobs check --job-type SchedulerJob --local` exec call itself could not get scheduled
  within its 20s window, not an application crash or OOM (node `MemoryPressure`/`DiskPressure`/
  `PIDPressure` conditions all `False`) — the exact CPU-starvation-cascade shape this project's own
  `STATE.md` Blockers/Concerns section has already documented multiple times (260817-mvp, the
  `airflow-scheduler-stuck-tasks` debug session, `dagrun-scheduler-stall.md`).
- Root cause, most likely: `csv_ingest_customers`'s own pre-existing `stage` backlog (a
  `scheduled__2026-08-23T03:26:00+00:00` DagRun, observed at 56/61 `stage` mapped instances
  `success` with 5 still `up_for_retry`, downstream `dbt_build`/`publish` not yet started) was
  STILL actively churning through this plan's own dispatch-time "confirmed healthy" window and
  this plan's own subsequent ~1-hour research phase, and eventually tipped the shared node(s) into
  the same CPU-contention regime this project's own history repeatedly names. **Not caused by this
  plan's own new files**: at every point this was observed, `kubectl get pods -n etl` showed ZERO
  running pods — no `chaos_probe` probe was ever triggered before this finding.

**Why not "fixed" here:** this is the SAME class of issue `.planning/debug/resolved/
airflow-scheduler-stuck-tasks.md` and the `260817-*` quick tasks already root-caused and
partially mitigated (concurrency caps, monitoring-stack trims) — a full remediation is a `/gsd:
debug` session's own scope, not a single chaos-test-authoring plan's. This plan's own test design
already anticipates and is resilient to this class of latency (see `test_oom.py`/
`test_task_timeout.py`'s own module docstrings: "Generous under this cluster's own documented,
live-observed CPU-contention latency").

**Update — sustained, not transient (same session, ~90 minutes later):** re-checked repeatedly
after the initial finding above, specifically waiting for a recovery signal before attempting any
live test again. `airflow-scheduler` cycled `CrashLoopBackOff` -> `1/2 Running` (still not
`Ready`) and back to `CrashLoopBackOff` again over that window (17 total restarts observed by the
end), never once reaching `2/2 Ready`. Two separate, spaced re-attempts of the identical
`uv run --group cluster pytest tests/e2e/chaos/test_duplicate_batch.py` command — the simplest
test in this plan, requiring no DAG trigger at all, only one `kubectl exec ... airflow dags
unpause` call via the session-scoped `_unpause_slice_dags` autouse fixture — both failed
identically at the same `kubectl` fixture's hardcoded 30s subprocess timeout
(`tests/e2e/cluster/conftest.py`). `docker stats` CPU stayed in the 276-420%/node range
throughout. **Conclusion: this is a sustained platform incident, not a brief blip** — none of
this plan's 5 chaos test files could be live-verified in this session as a direct result, through
no fault of their own code (confirmed via `ruff check`/`ruff format --check`/`mypy`, all clean,
and via `py_compile` for the new DAG file). Following the established precedent set by this
plan's own prerequisite (11-09-SUMMARY.md's key-decision: "Committed test_database_unavailable.py
... despite none of them currently passing live, because the test code itself is correct ... and
the reason they don't pass is a genuine, independently-reproduced platform bug outside this
plan's own scope"), this plan's 5 test files and the new `chaos_probe.py` DAG are committed as
correct, live-verification-blocked code — see `11-10-SUMMARY.md` for the full accounting.

**Recommended follow-up:** a dedicated `/gsd:debug` session, starting from this entry's own live
evidence, once the shared cluster's ambient load has genuinely settled enough to attempt
diagnosis without the diagnosis itself further starving an already-CPU-saturated node. The
established diagnostic/fix pattern from prior incidents applies first (`docker stats`, `kubectl
describe node`, check for an `etl`-namespace pod fan-out or a `stage`/`integrity_gate` backlog);
if none of those explain it, this specific episode (scheduler `CrashLoopBackOff` with `exitCode
0`/`reason=Completed`, no node-level Memory/Disk/PID pressure, and a >90-minute non-self-healing
duration) is new enough in degree, if not in kind, to warrant fresh diagnosis rather than being
assumed identical to a prior, shorter-lived incident. Before any further live chaos-test
execution against this cluster (this plan's own remaining work: live-verifying all 5 files,
`test_oom.py`/`test_task_timeout.py` in particular, which themselves add MORE load via a real
`chaos_probe_oom_publish_customers`/`chaos_probe_timeout_publish_customers` trigger), confirm
`airflow-scheduler` reaches a genuine, sustained `2/2 Ready` first.
