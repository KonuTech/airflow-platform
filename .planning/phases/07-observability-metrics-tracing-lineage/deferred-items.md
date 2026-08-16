# Deferred Items — Phase 7

Out-of-scope discoveries found during plan execution. Not auto-fixed (SCOPE BOUNDARY:
only fix issues directly caused by the current task's changes). Logged here per the
executor's deviation rules.

## From plan 07-02

Found while running `mypy` against `tests/unit/test_pipeline_errors.py` as a diligence
check beyond this plan's own declared acceptance criteria (Task 1 only requires
`mypy packages/dataplat/src/dataplat/observability`; Task 2 has no mypy criterion at
all — `make typecheck`'s `TYPECHECK_PATHS` is `packages/dataplat/src
packages/csv-processor/src tools`, which never includes `tests/`).

| File | Line (current) | Issue |
|------|-----------------|-------|
| `tests/unit/test_pipeline_errors.py` | 44-48 | `PipelineContext(config=..., metadata=None, objects=None, db=None, log=None)` — the `# type: ignore[arg-type]` comments guarding these placeholders use a malformed-per-mypy ignore-code syntax, so the underlying `arg-type` errors show through instead of being suppressed. |
| `tests/unit/test_logging_config.py` | 101-103 | `metrics.increment(...)` is annotated `-> None`; asserting `metrics.increment(...) is None` trips mypy's `func-returns-value` check. Pre-existing — the signature was already `-> None` before this plan's Task 1 rewrote the function body, confirmed identical via `git stash`. |

Verified pre-existing, not introduced by this plan: `git stash` back to the pre-07-02
state and re-running `mypy tests/unit/test_pipeline_errors.py` reproduces the exact same
12 errors (same messages, only line numbers shift, since this plan's docstring additions
push the block down 6 lines). This is the identical bug class already logged in
`.planning/phases/06-universal-csv-engine-schema-contracts-normalization/deferred-items.md`
(plan 06-02: `tests/integration/test_staging_loader.py` lines 155-158, same
malformed-ignore-comment pattern) — a repo-wide, still-unfixed gap in how `# type:
ignore[code] -- trailing comment` is written across several `tests/` files, not something
plan 07-02 introduced.

Not auto-fixed: neither file is in plan 07-02's `files_modified` list for this reason
(`tests/unit/test_pipeline_errors.py` was touched only to fix a real regression --
`RaggedRowGuard.apply()` now reads `ctx.config.dataset` for D-04's metric labels, so
`_make_context()`'s `config=None` placeholder had to become a `SimpleNamespace` with a
real `.dataset` attribute; that fix is a Rule 1 auto-fix, verified live via
`pytest tests/unit -k pipeline -q`, 6/6 passing). Both files' tests pass at runtime under
`pytest` regardless of the mypy gap (`pytest tests/unit tests/regression -q --no-cov`:
388/388 passing). A future cleanup pass should fix the `# type: ignore` comment syntax
across every affected file at once, matching 06-02's own recommendation.

## From plan 07-04

Found while running `ruff format --check .` (repo-wide) as a diligence check beyond this
plan's own declared acceptance criteria (Task 1-3's acceptance criteria only name specific
files/targets; none names a repo-wide format check).

| File | Issue |
|------|-------|
| `packages/csv-processor/src/csv_processor/cli.py` | `ruff format --check .` reports it would be reformatted (one multi-line expression collapses to a single line under the pinned formatter). |
| `tests/unit/detect/test_encoding.py` | `ruff format --check .` reports it would be reformatted (a multi-line string-literal concatenation collapses to one line). |

Verified pre-existing, not introduced by this plan: `git diff 1a619c4 --stat -- <both paths>`
(`1a619c4`, this plan's own worktree base / wave-1-merged commit) shows zero diff for either
file across all of plan 07-04's commits -- neither file is in Task 1/2/3's `files_modified`
list, and neither was read or edited during this plan's execution. Both were last modified by
a Phase 6 plan (`13b17a4`, encoding-coverage work), entirely unrelated to Phase 7's
observability/tracing scope.

Not auto-fixed: out of this plan's declared file scope (SCOPE BOUNDARY). `ruff check .`
(lint, not format) reports zero issues repo-wide, so this is purely a formatter-drift gap
(likely a ruff version/formatter-behavior change between when these two files were last
touched and the currently-pinned `ruff==0.16.2`), not a functional defect. A future cleanup
pass should run `ruff format .` once, repo-wide, to resettle both files.

## From plan 07-05

Found while running `mypy tests/integration/test_run_ingest.py` as a diligence check
beyond this plan's own declared acceptance criteria (Task 3's mypy gate, via
`<verification>`, only covers `uv run mypy packages/dataplat/src/dataplat` -- the whole
`packages/dataplat/src/dataplat` tree, never `tests/`).

| File | Line (current) | Issue |
|------|-----------------|-------|
| `tests/integration/test_run_ingest.py` | 550 | `run_module.StagingLoader` (pre-existing crash-simulation test, plan 04-05) — `Module "dataplat.pipeline.run" does not explicitly export attribute "StagingLoader"` (`strict = true`'s `no_implicit_reexport`; `run.py` imports `StagingLoader` without re-exporting it). |
| `tests/integration/test_run_ingest.py` | 622 | `run_module.resolve_publisher` (same pre-existing crash-simulation test area) — identical `no_implicit_reexport` finding for `resolve_publisher`. |

Verified pre-existing, not introduced by this plan: `git stash` back to the pre-07-05
state (this plan's own prior commit, `e076c1d`) and re-running `mypy
tests/integration/test_run_ingest.py` reproduces the exact same 2 errors (identical
messages, only line numbers shift by 6 -- from 544/616 to 550/622 -- since this plan's
Task 3 adds 6 new import lines above them). Same bug class already logged above (plan
07-02) and in `06-universal-csv-engine-schema-contracts-normalization/deferred-items.md`
(plan 06-02) — `no_implicit_reexport` flagging a module attribute accessed via its
importing module rather than its defining module, a repo-wide pattern this plan's own new
unit test (`tests/unit/test_run_ingest_trace.py`) deliberately avoided by importing
`metrics`/`tracing` directly rather than through `run_module.metrics`/`run_module.tracing`.

Not auto-fixed: this pre-existing crash-simulation test code is untouched by plan 07-05
(only new imports and one new test function were added to this file; the two flagged
lines sit inside `test_crash_between_staging_and_publish_leaves_no_partial_state_and_retry_succeeds`
and its neighbor, both plan-04-05-authored, outside this plan's own declared scope). The
whole file's tests still pass at runtime regardless of the mypy gap (`pytest
tests/integration/test_run_ingest.py -q`: 9/9 passing, including this plan's new test). A
future cleanup pass should decide once, repo-wide, whether `dataplat.pipeline.run` should
gain an explicit `__all__` re-exporting `StagingLoader`/`resolve_publisher`/`metrics`/
`tracing`, or whether every such test-side reference should import directly from each
symbol's owning module instead — matching 06-02's and 07-02's own same recommendation.

## From plan 07-08

Found while running `tests/e2e/observability/test_trace_propagation.py` (Task 2) live
against the cluster: a genuine, pre-existing cluster resource-exhaustion condition,
entirely unrelated to OBS-10/TRACEPARENT propagation, that blocks `csv_ingest_customers`
from scheduling new task pods at all.

**Root cause, fully diagnosed:** two `etl`-namespace `ingest-*` pods
(`ingest-qp3ougwy`, `ingest-qgw33dq0` — `dag_id=csv_ingest_customers`, `task_id=ingest`,
`run_id=scheduled__2026-08-16T0607000000-913ad3735`, `map_index` 10 and another) have
their `base` container (the real `csv-processor` work container) already `terminated`
with `exitCode: 0`/`reason: Completed` since `2026-08-16T06:19:0{1,3}Z`, but their
`airflow-xcom-sidecar` container (the tiny `alpine`-based XCom-file server KPO's
`do_xcom_push=True` adds) is STILL `running` 6+ hours later, with 0 restarts. Because at
least one container is still running, Kubernetes counts the WHOLE pod's resource
requests — including the already-exited `base` container's `500m` CPU / `1Gi` memory —
against the node's allocatable budget for as long as the pod itself never reaches a
terminal phase. Both worker nodes' kubelet-reported `Allocatable.cpu` is deliberately
capped at `3` (not the host's `12`; `9` total across the 3-node cluster, matching
CLAUDE.md's own framing), so these two pods alone permanently consume roughly a third of
one node's entire CPU budget, and combined with ordinary DAG/monitoring-stack load pushed
both nodes to 91-97% allocated — leaving no room for genuinely new pods
(`csv-ingest-customers-wait-for-files-*`/`csv-ingest-customers-resolve-window-*`,
KubernetesExecutor's own per-TaskInstance worker pods, `250m` CPU each) to ever schedule.
`kubectl describe pod` on the pending pods confirms the scheduler's own reason verbatim:
`FailedScheduling ... 2 Insufficient cpu`. `dag_run` history for `csv_ingest_customers`
shows 4 consecutive `failed` runs immediately before this plan's own session started,
consistent with this having degraded the whole DAG for hours already, not something this
session's own activity caused.

**Verified pre-existing, not introduced by this plan:** the stuck pods' own timestamps
(`06:19:0{1,3}Z`, ~5-6 hours before this plan's Task 2 first ran) predate this plan's
entire session. `resolve_window` (an independent `@task` with zero declared dependencies
on `wait_for_files`/`discover`/`ingest`) is ALSO stuck `Pending`, proving this is a pure
Kubernetes pod-scheduling capacity problem, not a DAG logic or dependency defect this
plan's own code could have caused.

**Not auto-fixed:** deleting the two stuck pods is a `kubectl delete pod` in the `etl`
namespace — outside plan 07-08's own declared file/mutation scope (its threat model and
`webhook_receiver` fixture explicitly authorize mutating only the `monitoring` namespace's
throwaway `webhook-receiver-*` resources, a dedicated `meta.datasets` test row, and the
`grafana/alert-webhook` Vault/K8s-Secret pair — never arbitrary `etl`-namespace pods from
an unrelated historical DagRun), and the executor's own worktree instructions explicitly
forbid touching any pod/resource outside that declared set. This is a Rule 4 (architectural
decision, or at minimum an infrastructure-operations decision) call for the orchestrator or
a human, not an in-scope auto-fix.

**Impact on this plan:** `test_trace_propagation.py` (OBS-10's live end-to-end proof) is
fully implemented, lints/type-checks cleanly, collects cleanly, and its every polling
helper/assertion was reviewed against the exact source code it exercises
(`airflow/dags/_common/tracing_kpo.py`'s `TracingKubernetesPodOperator.build_pod_request_obj`,
`packages/dataplat/src/dataplat/pipeline/run.py`'s `run_ingest` span-context capture,
`packages/dataplat/src/dataplat/metadata/postgres.py`'s `claim_ingestion_run` — all three
already unit/integration-tested in plans 07-04/07-05, per those plans' own SUMMARY.md
files; this session additionally re-ran `pytest tests/unit -k "trace or tracing" -q`
live, 21/21 green). Two live attempts against the cluster in this session could not
complete: first at the committed test's own standard 180s timeout (matching every
sibling e2e test's own convention), then a second, deliberately generous 900s (15
-minute) diagnostic retry -- run with a temporarily-widened timeout, reverted before
this file's own commit -- which ALSO failed, confirming this is not merely a slow
transient blip but a genuinely durable resource-exhaustion state for at least the
15-minute window this session could observe it. `csv_ingest_customers` cannot schedule
ANY new task pod at all while the two stuck `ingest-*` pods persist. See
`07-08-SUMMARY.md`'s own Deviations section for the full account and the recommended
next step (clear the two named stuck pods, then re-run
`uv run pytest tests/e2e/observability/test_trace_propagation.py -m cluster -x -q`).

**Recommended fix** (for whoever picks this up): `kubectl -n etl delete pod ingest-qp3ougwy
ingest-qgw33dq0`, then separately investigate why `airflow-xcom-sidecar` did not exit once
its `base` container completed — this is the general mechanism (not specific to these two
pods) that should be root-caused, since it will recur for every future `ingest` task
attempt until fixed, silently bleeding cluster capacity over time.
