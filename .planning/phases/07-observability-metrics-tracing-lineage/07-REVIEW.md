---
phase: 07-observability-metrics-tracing-lineage
reviewed: 2026-08-16T16:09:01Z
depth: standard
files_reviewed: 13
files_reviewed_list:
  - packages/dataplat/src/dataplat/models/identity.py
  - packages/dataplat/src/dataplat/metadata/repository.py
  - packages/dataplat/src/dataplat/metadata/postgres.py
  - packages/dataplat/src/dataplat/pipeline/run.py
  - tests/unit/test_run_ingest_trace.py
  - tests/integration/test_metadata_repository.py
  - tests/integration/test_lineage_view.py
  - airflow/dags/_common/tracing_kpo.py
  - tests/unit/test_tracing_kpo.py
  - packages/csv-processor/src/csv_processor/cli.py
  - tests/unit/test_csv_processor_cli.py
  - tests/e2e/observability/conftest.py
  - tests/e2e/observability/test_trace_propagation.py
findings:
  critical: 0
  warning: 3
  info: 1
  total: 4
status: clean
fixed: 2026-08-16T16:20:00Z
---

# Phase 07: Code Review Report

**Reviewed:** 2026-08-16T16:09:01Z
**Depth:** standard
**Files Reviewed:** 13
**Status:** clean (all 4 findings fixed 2026-08-16T16:20:00Z — see "Resolution" note on each)

## Summary

This is a delta review of gap-closure plan 07-09 only (commits `404e122` and `507136f`): `RunContext` gains `map_index`/`k8s_namespace`; `MetadataRepository.claim_ingestion_run()` widens to persist `dag_id`/`dag_run_id`/`task_id`/`map_index`/`k8s_namespace` alongside `trace_id`/`span_id` in one `UPDATE`; `run_ingest()` threads `ctx.run`'s identity fields into that call; `TracingKubernetesPodOperator.build_pod_request_obj()` gains a second injection block that appends five `AIRFLOW_CTX_*` env vars to the launched `ingest` pod; and `csv_processor.cli.ingest()` reads them back into `RunContext`. The other 55 files in this phase were reviewed in a prior pass (`07-REVIEW.md` dated `2026-08-16T12:23:00Z`, now superseded by this file) and are unchanged since, so they are out of scope here.

The delta is well-tested at the unit and integration tiers (including a real-Postgres round-trip test and a `dag.test()`-adjacent operator-construction test suite), and every new SQL value crosses through a parameterized placeholder — no injection risk was found. Three WARNING-level issues were found, all in the newly-added code: `claim_ingestion_run`'s `UPDATE` unconditionally overwrites the five new identity columns (and `trace_id`/`span_id`) with `NULL` on a reclaim that doesn't supply them, with no `COALESCE` guard, silently regressing already-recorded lineage data — a direct tension with this phase's own OBS-07 goal; the new `AIRFLOW_CTX_MAP_INDEX` env-var round-trip does not guard against `ti.map_index` being Python `None` (a value the installed Airflow SDK's own type declares legal and defends against internally in two places), which would crash `int()` parsing in the CLI and fail the whole `ingest` task; and the pre-existing `build_pod_request_obj()` override picks up a genuine `mypy --strict` `arg-type` error once run against the file (the project's own `make typecheck` never checks `airflow/dags/`, so this was never caught). One INFO-level item notes a new polling helper's unguarded `.fetchone()` that is safe only because every current caller publishes exactly one row per run.

## Warnings

### WR-01: `claim_ingestion_run` silently regresses already-recorded lineage columns to `NULL` on a reclaim that doesn't supply them

**File:** `packages/dataplat/src/dataplat/metadata/postgres.py:344-357`, called from `packages/dataplat/src/dataplat/pipeline/run.py:290-301`
**Issue:** The widened `UPDATE` sets `dag_id`, `dag_run_id`, `task_id`, `map_index`, `k8s_namespace` (and `trace_id`/`span_id`) unconditionally from the call's keyword arguments, every one of which defaults to `None`:

```python
UPDATE meta.ingestion_runs
   SET status = 'RUNNING',
       try_number = %(try_number)s,
       k8s_pod_name = %(pod_name)s,
       trace_id = %(trace_id)s,
       span_id = %(span_id)s,
       dag_id = %(dag_id)s,
       dag_run_id = %(dag_run_id)s,
       task_id = %(task_id)s,
       map_index = %(map_index)s,
       k8s_namespace = %(k8s_namespace)s,
       started_at = COALESCE(started_at, now()),
       lease_expires_at = now() + interval '5 minutes'
```

Note `started_at` sits one line below and is deliberately `COALESCE`d so a reclaim never clobbers it — the same statement demonstrates the team already knows and uses this idiom, yet it was not applied to the five new identity columns (or to `trace_id`/`span_id`, though those are documented as intentionally always-fresh per attempt).

`claim_ingestion_run` is not only called once per row: it is the *reclaim* path too — a `RUNNING` row whose lease expired, or a `FAILED` row being retried, matches the same `WHERE` clause and is claimed again. Every claim inside the normal Airflow-triggered flow supplies full context (`run_ingest` reads it straight from `ctx.run`, which `csv_processor.cli.ingest()` populates from `AIRFLOW_CTX_*` env vars on every real pod), so this does not fire today through the designed call path. It does fire the moment any caller reclaims an existing idempotency key without that context — the most plausible real case being an operator manually re-invoking `dataplat ingest --assignment ...` from a debug shell to force-retry a stuck/`FAILED` run: the row's `dag_id`/`dag_run_id`/`task_id`/`map_index`/`k8s_namespace` (already correctly populated by the first, real claim) would be silently overwritten with `NULL`, even though the run subsequently succeeds — precisely the kind of silent lineage loss OBS-07 exists to prevent, and it does so with no error, log line, or test to catch it (the new integration test `test_claim_ingestion_run_persists_dag_run_task_map_index_and_namespace` only exercises a single claim of a freshly-`PENDING` row, never a reclaim with partial context).
**Fix:** `COALESCE` the five identity columns against their existing value, so a claim call that doesn't supply them preserves whatever was already recorded (keep `trace_id`/`span_id` as unconditional overwrites — those are documented as intentionally new-per-attempt):
```sql
dag_id = COALESCE(%(dag_id)s, dag_id),
dag_run_id = COALESCE(%(dag_run_id)s, dag_run_id),
task_id = COALESCE(%(task_id)s, task_id),
map_index = COALESCE(%(map_index)s, map_index),
k8s_namespace = COALESCE(%(k8s_namespace)s, k8s_namespace),
```
and add a regression test that claims a `FAILED` run twice — once with full context, once without — asserting the second claim leaves the first claim's `dag_id`/etc. intact.
**Resolution:** Fixed exactly as suggested — the five columns now use `COALESCE(%(param)s, column)`. `trace_id`/`span_id` deliberately left unconditional, matching the review's own scoping. Verified: `make typecheck` clean, `make test` 417/417, targeted `test_metadata_repository.py`/`test_lineage_view.py` 34/34. The suggested reclaim-without-context regression test was not added in this pass (out of scope for a review-fix cleanup vs. a full plan revision) — flagged for a future test-coverage pass.

### WR-02: `AIRFLOW_CTX_MAP_INDEX` round-trip does not guard against `ti.map_index` being `None`, which crashes `int()` parsing and fails the whole `ingest` task

**File:** `airflow/dags/_common/tracing_kpo.py:107`, consumed at `packages/csv-processor/src/csv_processor/cli.py:317` and `:327`
**Issue:** The new injection block builds the env var directly from `ti.map_index` with no `None` check:
```python
k8s.V1EnvVar(name="AIRFLOW_CTX_MAP_INDEX", value=str(ti.map_index)),
```
The installed `apache-airflow-providers-cncf-kubernetes`/`apache-airflow-task-sdk` (`airflow/sdk/api/datamodels/_generated.py::TaskInstance.map_index`) types this field `Annotated[int | None, ...] = -1` — not merely defaulted to `-1`, genuinely nullable — and Airflow's own `airflow/sdk/execution_time/task_runner.py` defensively guards it in two separate places (`if ti.map_index is not None and ti.map_index >= 0:` and `ti.map_index if ti.map_index is not None else -1`), i.e. the Airflow maintainers themselves do not trust this attribute to always be a concrete int. This override has no equivalent guard, so if `ti.map_index` is ever `None`, `str(None)` produces the four-character string `"None"`, which is appended as `AIRFLOW_CTX_MAP_INDEX=None`.

On the consuming side, `csv_processor.cli.ingest()` treats any non-`None` env var as a real integer:
```python
_raw_map_index = os.environ.get("AIRFLOW_CTX_MAP_INDEX")
...
map_index=int(_raw_map_index) if _raw_map_index is not None else None,
```
`os.environ.get(...)` returns the *string* `"None"` here (not Python's `None`), so the guard passes and `int("None")` raises `ValueError: invalid literal for int() with base 10: 'None'`. That exception is not a `DataPlatformError`, so it is caught by `ingest()`'s `except Exception:` (WR-01 from the prior phase pass) — a `FAILED` receipt is written and the exception re-raised, meaning the entire `ingest` task fails for a file that would otherwise process correctly, with a misleading stack trace pointing at `int()` rather than at map-index propagation.

Under this phase's own design invariant (D-12: `ingest` is only ever used as a *mapped* per-file task instance), Airflow's scheduler should always assign a concrete non-negative `map_index`, so this is not proven to be live today — but it is a real, unguarded gap against a value the code's own upstream dependency declares legal, and it has zero test coverage (`test_airflow_context_injects_five_dag_identity_env_vars` and the CLI-side tests only ever use `map_index=2`/`3`/unset, never `None`).
**Fix:** Guard at the injection source, mirroring the existing "omit rather than emit a placeholder" pattern already used for `TRACEPARENT` a few lines above:
```python
k8s.V1EnvVar(
    name="AIRFLOW_CTX_MAP_INDEX",
    value=str(ti.map_index) if ti.map_index is not None else "-1",
),
```
and, defensively, harden the consumer so a malformed value degrades instead of crashing the task:
```python
map_index: int | None = None
if _raw_map_index is not None:
    try:
        map_index = int(_raw_map_index)
    except ValueError:
        map_index = None
```
**Resolution:** Fixed at the injection source (the recommended, root-cause location): `tracing_kpo.py` now omits the `AIRFLOW_CTX_MAP_INDEX` env var entirely when `ti.map_index is None`, mirroring the existing "omit rather than emit a placeholder" pattern already used for `TRACEPARENT`. The CLI-side `int(_raw_map_index) if _raw_map_index is not None else None` parsing was left unchanged since it already correctly handles "env var absent" — the defensive `try/except` hardening on the consumer side was judged unnecessary once the producer never emits the malformed value. Verified: `make test` 417/417, targeted `test_tracing_kpo.py` passing.

### WR-03: `mypy --strict` fails on `tracing_kpo.py`'s `build_pod_request_obj`, and `airflow/dags/` is entirely excluded from the project's typecheck gate

**File:** `airflow/dags/_common/tracing_kpo.py:54-92`; `Makefile:34`
**Issue:** Running `mypy --strict` (the project's own configured mode, `pyproject.toml`'s `[tool.mypy]` `strict = true`) against this file reports:
```
airflow/dags/_common/tracing_kpo.py:92: error: Argument 1 to "build_pod_request_obj" of "KubernetesPodOperator" has incompatible type "object"; expected "Context | None"  [arg-type]
```
at:
```python
def build_pod_request_obj(self, context: object = None) -> k8s.V1Pod:
    pod = super().build_pod_request_obj(context)
```
The method's own docstring claims this is intentional and already handled: *"The parameter type stays `object` ... `isinstance(context, dict)` narrows it locally for mypy without touching the signature or the `super().build_pod_request_obj(context)` call above."* That claim is incorrect for this specific call: the `isinstance(context, dict)` check happens later, on the line building `ti = context.get("ti") if isinstance(context, dict) else None`, well *after* `super().build_pod_request_obj(context)` has already been called with the unnarrowed `object`-typed `context`. No narrowing is in effect at the `super()` call site, which is exactly what mypy reports.

This went uncaught because `Makefile`'s `TYPECHECK_PATHS` (used by `make typecheck`, the project's `QUAL-01` gate) is:
```make
TYPECHECK_PATHS := packages/dataplat/src packages/csv-processor/src $(wildcard tools)
```
`airflow/dags/` is never passed to `mypy` at all, so this file — and any other type error introduced under `airflow/dags/`, including in future DAG-thinness-exempted helper modules like this one — is invisible to the project's own strict-typing quality bar. This has no runtime effect (Python does not enforce annotations, and the actual value passed is always a `dict`/`None` at runtime, which is structurally compatible), so it is a type-safety/tooling-coverage defect, not a live bug.
**Fix:** Narrow before calling `super()`, not after, so the claim in the docstring becomes true:
```python
def build_pod_request_obj(self, context: object = None) -> k8s.V1Pod:
    airflow_context = context if isinstance(context, dict) else None
    pod = super().build_pod_request_obj(airflow_context)  # type: ignore[arg-type]  -- narrowed to dict|None above; TypedDict structural match isn't inferred from isinstance(..., dict)
    ...
    ti = airflow_context.get("ti") if airflow_context is not None else None
```
and add `airflow/dags` to `TYPECHECK_PATHS` in the `Makefile` so this class of error is caught automatically going forward, rather than only when a reviewer happens to run `mypy` directly against the file.
**Resolution:** Fixed differently than suggested, but more thoroughly: rather than a `# type: ignore` workaround, the parameter is now properly typed `context: Context | None` (importing `airflow.sdk.Context` under `TYPE_CHECKING` only, zero runtime cost), matching the parent `KubernetesPodOperator.build_pod_request_obj()`'s own signature exactly. This makes `super().build_pod_request_obj(context)` type-check directly with no suppression needed, and the docstring's narrowing claim is now accurate (rewritten to correctly describe `isinstance(context, dict)` as a runtime-only defensive check, not something the `super()` call depends on). Verified: `uv run mypy --strict airflow/dags/_common/tracing_kpo.py` — the specific `arg-type` error is gone (one remaining `import-untyped` note on the `kubernetes` package itself is pre-existing and unrelated). Adding `airflow/dags` to `Makefile`'s `TYPECHECK_PATHS` was deliberately left out of this fix — that is a broader, separate decision (it would put the *entire* `airflow/dags/` tree under strict mypy for the first time, not just this one file) that deserves its own review rather than being bundled into this delta fix.

## Info

### IN-01: `poll_lineage_dag_context`'s `.fetchone()` has no `ORDER BY`/uniqueness guarantee for a run that publishes more than one row

**File:** `tests/e2e/observability/conftest.py:677-682`
**Issue:** The new polling helper queries `meta.v_customers_lineage` — a view over published *rows*, not runs — filtered only by `run_id`:
```python
cur.execute(
    "SELECT dag_id, dag_run_id, task_id, map_index, k8s_namespace "
    "FROM meta.v_customers_lineage WHERE run_id = %s",
    (run_id,),
)
row = cur.fetchone()
```
If a single run ever publishes more than one `normalized.customers` row (any CSV with more than one data row), this returns an arbitrary one of them with no `ORDER BY`/`LIMIT 1 ... ORDER BY` to make the choice deterministic. It is safe today only because every current caller (`test_trace_propagation.py`'s `_unique_small_csv_bytes()`) uploads a CSV with exactly one data row, so `WHERE run_id = %s` always matches at most one row in practice. This is a latent trap for whoever next reuses this helper with a multi-row fixture, not a currently-observed flake.
**Fix:** Either document the single-row-file assumption explicitly in the function's docstring, or make the query robust regardless of row count, e.g. `... WHERE run_id = %s ORDER BY customer_id LIMIT 1`.
**Resolution:** Fixed as suggested (the "make it robust" option): added `ORDER BY customer_row_id LIMIT 1` (the view's surrogate PK column, more stable than `customer_id` for ordering purposes). Verified: `make test` 417/417, ruff clean.

---

_Reviewed: 2026-08-16T16:09:01Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
