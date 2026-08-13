---
phase: 04-vertical-slice-csv-to-analytical-postgresql
reviewed: 2026-08-13T23:21:59Z
depth: standard
files_reviewed: 63
files_reviewed_list:
  - Makefile
  - airflow/dags/_common/__init__.py
  - airflow/dags/_common/kpo.py
  - airflow/dags/csv_ingest_customers.py
  - airflow/dags/smoke_kubernetes_pod.py
  - configs/datasets/customers.yaml
  - docker/csv-processor/Dockerfile
  - docs/spikes/U1-smoke-xcom.md
  - helm/values/ci/airflow.yaml
  - helm/values/ci/minio.yaml
  - helm/values/local/airflow.yaml
  - helm/values/local/minio.yaml
  - kubernetes/rbac-etl.yaml
  - migrations/versions/0006_normalized_customers_business_key_unique.py
  - migrations/versions/0007_staging_schema.py
  - migrations/versions/0008_grant_schema_usage_to_etl_app.py
  - packages/csv-processor/pyproject.toml
  - packages/csv-processor/src/csv_processor/cli.py
  - packages/dataplat/src/dataplat/cli.py
  - packages/dataplat/src/dataplat/config/model.py
  - packages/dataplat/src/dataplat/config/registry.py
  - packages/dataplat/src/dataplat/discovery.py
  - packages/dataplat/src/dataplat/load/publish/merge.py
  - packages/dataplat/src/dataplat/load/publish/registry.py
  - packages/dataplat/src/dataplat/load/staging.py
  - packages/dataplat/src/dataplat/metadata/postgres.py
  - packages/dataplat/src/dataplat/metadata/repository.py
  - packages/dataplat/src/dataplat/models/assignment.py
  - packages/dataplat/src/dataplat/models/identity.py
  - packages/dataplat/src/dataplat/models/receipt.py
  - packages/dataplat/src/dataplat/pipeline/protocol.py
  - packages/dataplat/src/dataplat/pipeline/run.py
  - packages/dataplat/src/dataplat/storage/objectstore.py
  - pyproject.toml
  - scripts/etl-secrets.sh
  - scripts/ingest-demo.py
  - scripts/stages/75-etl.sh
  - setup.cfg
  - tests/e2e/slice/__init__.py
  - tests/e2e/slice/conftest.py
  - tests/e2e/slice/test_concurrent_select.py
  - tests/e2e/slice/test_pod_kill_retry.py
  - tests/e2e/slice/test_smoke_and_idempotency.py
  - tests/fixtures/slice-corpus.yaml
  - tests/integration/test_config_registry.py
  - tests/integration/test_discover_files.py
  - tests/integration/test_metadata_repository.py
  - tests/integration/test_migrations.py
  - tests/integration/test_objectstore.py
  - tests/integration/test_publish_merge.py
  - tests/integration/test_run_ingest.py
  - tests/integration/test_staging_loader.py
  - tests/policy/test_dag_line_budget.py
  - tests/policy/test_dag_thinness.py
  - tests/policy/test_no_manual_kubectl_surgery.py
  - tests/unit/conftest.py
  - tests/unit/test_assignment_document.py
  - tests/unit/test_batching_config.py
  - tests/unit/test_config_hashing.py
  - tests/unit/test_dag_structure.py
  - tests/unit/test_discovery.py
  - tests/unit/test_publisher_registry.py
  - tests/unit/test_resolve_window.py
findings:
  critical: 2
  warning: 6
  info: 3
  total: 11
status: issues_found
---

# Phase 4: Code Review Report

**Reviewed:** 2026-08-13T23:21:59Z
**Depth:** standard
**Files Reviewed:** 63
**Status:** issues_found

## Summary

This review covers the full vertical-slice deliverable — DAGs, the `dataplat`/`csv_processor` packages implementing discover→claim→stage→publish, migrations, Helm/RBAC/Docker infra, and the accompanying unit/integration/E2E/policy test suites — with particular attention to `metadata/repository.py`/`postgres.py` and `pipeline/run.py` as requested.

The overall design is unusually disciplined: idempotent upserts are used correctly and consistently, the staging/publish split correctly separates checkpointed work from the atomic barrier, `MergePublisher`'s `INSERT ... ON CONFLICT` (not literal `MERGE`) is the right call given documented PostgreSQL concurrency behavior, and the six E2E-driven fixes mentioned in the task context (schema grants, ingress body-size, DAG pause state, MinIO policy, heartbeat env override, `duration_ms` persistence) all verify correctly in the current code.

That said, tracing the run-lifecycle state machine end to end surfaced two genuine correctness defects that undermine the platform's own stated core value ("no data is ever silently dropped... corrupted"; "can be traced, explained... and trusted"):

1. A race condition where the heartbeat thread can silently revert a `SUCCEEDED` run's status back to `RUNNING` after the publish transaction has already committed.
2. A non-deterministic duplicate-file lookup (`LIMIT 1` with no `ORDER BY`) that, once two or more files share identical content — an explicitly designed-for and tested scenario — can cause a legitimate, not-yet-ingested file to be silently and permanently excluded from every future discovery pass.

Six further Warnings and three Info items are listed below, mostly clustered around the run's status-lifecycle (`FAILED` is never actually written anywhere), a docstring/implementation mismatch on the "always write a receipt" contract, an unused/never-set `try_number` source, unvalidated identifiers reaching raw SQL/filesystem paths, and a couple of stale comments/docstrings that no longer match the code they describe.

## Critical Issues

### CR-01: Heartbeat thread can silently revert a `SUCCEEDED` run's status back to `RUNNING`

**File:** `packages/dataplat/src/dataplat/pipeline/run.py:190-197` (unconditional heartbeat write) interacting with `packages/dataplat/src/dataplat/pipeline/run.py:291-331` (commit → DROP TABLE → stop-signal window) and `packages/dataplat/src/dataplat/metadata/postgres.py:401-429` (`update_ingestion_run_status` has no status guard)

**Issue:** `_heartbeat_loop`'s body is:

```python
while not stop_event.wait(interval_seconds):
    ctx.metadata.update_ingestion_run_status(
        run_id=run_id, status="RUNNING",
        lease_expires_at=datetime.now(tz=UTC) + _LEASE_DURATION,
        rows_read=progress.rows_read, rows_parsed=progress.rows_parsed,
    )
```

`stop_event.wait(interval_seconds)` returns `False` (letting the loop body run once more) whenever the interval elapses *before* `stop_event.set()` is called — even if `set()` happens a moment later. In `run_ingest`, `stop_heartbeat.set()` is only called inside the `finally` block (line 330), which runs *after* the publish transaction has already committed `status='SUCCEEDED'` (the `with ctx.db.connection() as conn, conn.transaction():` block ending around line 322) *and* after the trailing `DROP TABLE IF EXISTS {staging_result.staging_table}` (line 328) has executed. If the heartbeat's timer elapses during that window, the thread performs one more `update_ingestion_run_status(status="RUNNING", lease_expires_at=<now+5min>, ...)` call. The generated SQL in `postgres.py` (`UPDATE meta.ingestion_runs SET status = %s, ... WHERE run_id = %s`) carries **no `WHERE status = ...` guard**, so this write unconditionally overwrites the just-committed `'SUCCEEDED'` status back to `'RUNNING'` with a fresh 5-minute lease.

This is not a purely theoretical window: `airflow/dags/csv_ingest_customers.py`'s `ingest` task explicitly sets `DATAPLAT_HEARTBEAT_INTERVAL_SECONDS=2` (`_INGEST_EXTRA_ENV_VARS`), which increases the chance of the timer landing inside the (normally short, but non-zero) commit→drop-table→stop-signal window.

**Impact:** `meta.ingestion_runs.status` — the platform's own audit-trail system of record — can end up reading `'RUNNING'` for a run that actually succeeded. Because `claim_ingestion_run`'s `WHERE` clause treats `status = 'RUNNING' AND lease_expires_at < now()` as reclaimable, once the phantom lease expires a subsequent `discover_files`/retry pass can re-claim and fully re-execute the run — re-staging and re-publishing already-loaded data — and `finalize_publication`'s second write then overwrites `rows_loaded`/`duration_ms` with the second run's numbers (likely near-zero, since `MergePublisher`'s own `_record_hash IS DISTINCT FROM` guard suppresses the republish), corrupting the historical audit trail for a run that genuinely succeeded with real data.

**Fix:** make the heartbeat's write self-guarding so it can never regress a terminal status, e.g. a dedicated repository method with a `WHERE ... AND status = 'RUNNING'` clause:

```python
# metadata/postgres.py
def heartbeat_ingestion_run(
    self, *, run_id: int, lease_expires_at: datetime, rows_read: int, rows_parsed: int,
) -> None:
    with self._pool.connection() as conn:
        conn.execute(
            """
            UPDATE meta.ingestion_runs
               SET lease_expires_at = %s, rows_read = %s, rows_parsed = %s
             WHERE run_id = %s AND status = 'RUNNING'
            """,
            (lease_expires_at, rows_read, rows_parsed, run_id),
        )
```

and have `_heartbeat_loop` call this instead of the generic `update_ingestion_run_status(status="RUNNING", ...)`. A stray post-terminal write then becomes a silent no-op instead of a status regression.

### CR-02: Non-deterministic duplicate lookup can silently and permanently exclude a legitimate file from ingestion

**File:** `packages/dataplat/src/dataplat/metadata/postgres.py:162-178` (`find_file_by_content_hash`, no `ORDER BY`) consumed by `packages/dataplat/src/dataplat/discovery.py:178-217`

**Issue:** `find_file_by_content_hash`'s SQL is:

```sql
SELECT file_id FROM meta.files
 WHERE dataset_id = %s AND content_sha256 = %s
 LIMIT 1
```

There is no `ORDER BY`. PostgreSQL's own documentation is explicit that without one, which row `LIMIT 1` returns is unspecified whenever more than one row matches. `discover_files` (`discovery.py:178-217`) relies on an implicit assumption that when *re-discovering* an object whose own `meta.files` row already exists, this lookup returns *that same row* — this is exactly what the "rediscovery correction" comment at `discovery.py:196-217` depends on to clear a wrongly self-referential `duplicate_of_file_id`:

```python
if duplicate_of_file_id is not None and file_id == existing_file_id:
    duplicate_of_file_id = None
    file_id = metadata.create_file(..., duplicate_of_file_id=None)
```

That assumption only holds when at most one row shares a given `content_sha256`. Once a *second* file with identical content genuinely exists — an explicitly designed-for, tested scenario (D-13's duplicate detection; see `tests/integration/test_discover_files.py::test_duplicate_content_is_skipped`) — `find_file_by_content_hash` can, with no guarantee either way, return a *different* duplicate's `file_id` instead of the object's own row on a later rediscovery pass. If that happens for a file whose ingestion run is still `PENDING` (not yet processed), `discover_files` incorrectly marks it `duplicate_of_file_id = <some other file>` and `continue`s (`discovery.py:227`, "D-13 skip policy: no batch, no run, no assignment document") *before* ever re-offering its already-created `PENDING` run as a Dynamic-Task-Mapping candidate. Because the underlying rows and query plan don't change between passes, nothing about this codebase causes the situation to self-correct on a later attempt — the file's `meta.ingestion_runs` row can stay `PENDING` forever, its data silently never loaded, with no error raised anywhere.

**Impact:** this directly contradicts the platform's stated Core Value ("no data is ever silently dropped, duplicated, or corrupted") for a scenario (multiple arrivals of byte-identical content) the platform is explicitly built and tested to handle.

**Fix:** make the lookup deterministic by always resolving to the earliest-created ("true original") row:

```python
row = conn.execute(
    """
    SELECT file_id FROM meta.files
     WHERE dataset_id = %s AND content_sha256 = %s
     ORDER BY file_id ASC
     LIMIT 1
    """,
    (dataset_id, content_sha256),
).fetchone()
```

This restores the invariant `discover_files`'s self-correction logic already assumes: rediscovering an object's own row and looking up a genuine duplicate's "original" both resolve to the same, stable file consistently across calls.

## Warnings

### WR-01: `ingest()`'s "receipt on every exit path" contract is violated for non-`DataPlatformError` exceptions

**File:** `packages/csv-processor/src/csv_processor/cli.py:199-201` (docstring claim), `:207-263` (try/except), `:247` (`except DataPlatformError:`)

**Issue:** the docstring states: *"A `Receipt` is written to the XCom path on every exit path, success or failure."* The actual handler only catches `DataPlatformError`:

```python
try:
    ...
    receipt = run_ingest(ctx, heartbeat_interval_seconds=heartbeat_interval_seconds)
    _write_xcom(receipt)
except DataPlatformError:
    _write_xcom(Receipt(run_id=doc.run_id if doc is not None else -1, status="FAILED", ...))
    raise
finally:
    if pool is not None:
        pool.close()
```

A cast failure inside `MergePublisher`'s publish `INSERT` (see WR-04 below) surfaces as a raw `psycopg.errors.DataError`, not a `DataPlatformError` — as would any other unexpected exception (network error not wrapped by `StorageError`, `MemoryError`, etc.). Every one of those exits the process with **no** Receipt/XCom written at all, contradicting the documented "every exit path" contract. (Operationally this is non-fatal — Airflow still observes the pod's non-zero exit and marks the task failed via `get_logs=True` output — but the structured Receipt this command promises never gets produced for this whole failure class.)

**Fix:** broaden the except clause (or add a second one) so a Receipt is genuinely always written:

```python
except DataPlatformError:
    _write_xcom(Receipt(run_id=doc.run_id if doc is not None else -1, status="FAILED", ...))
    raise
except Exception:
    _write_xcom(Receipt(run_id=doc.run_id if doc is not None else -1, status="FAILED", ...))
    raise
```

### WR-02: No code path ever writes `meta.ingestion_runs.status = 'FAILED'`

**File:** `packages/dataplat/src/dataplat/metadata/postgres.py:331` (`claim_ingestion_run`'s `WHERE` clause references `'FAILED'` as reclaimable) and `packages/dataplat/src/dataplat/pipeline/run.py` (`run_ingest` never writes it)

**Issue:** `claim_ingestion_run`'s `WHERE status IN ('PENDING', 'FAILED')` clause is written as if some code path transitions a row to `'FAILED'`, but nothing in this codebase does. `run_ingest`'s own docstring says explicitly: *"This function catches nothing: a run-fatal exception... propagates OUT of run_ingest uncaught... The only thing this function ever guarantees on every exit path... is that its own heartbeat thread is stopped."* When staging or publish raises, the row is left at `status='RUNNING'` with a live 5-minute lease — indistinguishable from a genuinely in-progress run until the lease naturally expires. Even after Airflow's own `retries=3` are exhausted and the task is permanently failed, the row still reads `'RUNNING'`, never `'FAILED'`. `scripts/ingest-demo.py:96-108`'s own comment already confirms this gap explicitly: *"'FAILED' is a legitimate persisted value too... even though no call site in this phase's code currently sets it."*

**Impact:** `meta.ingestion_runs` — the audit surface this platform's traceability promise rests on — cannot currently distinguish "genuinely still running" from "permanently abandoned after exhausting retries" without also cross-referencing `lease_expires_at` against wall-clock time and Airflow's own task-instance history.

**Fix:** wrap `run_ingest`'s work (or its caller, `csv_processor.cli.ingest()`) so a run-fatal exception updates `status='FAILED'` (with `error_type`/`error_message`, both already tracked as updatable fields in `_INGESTION_RUN_UPDATABLE_FIELDS`) before re-raising.

### WR-03: `attempt`/`try_number` is always `1` — the environment variable it reads is never set

**File:** `packages/csv-processor/src/csv_processor/cli.py:229`; `packages/dataplat/src/dataplat/models/identity.py:105`; `airflow/dags/_common/kpo.py` (the only place `env_vars` is built for these pods)

**Issue:** `RunContext.attempt` (passed to `claim_ingestion_run` as `try_number`) is resolved via:

```python
attempt=int(os.environ.get("AIRFLOW_TASK_TRY_NUMBER", "1")),
```

No `KubernetesPodOperator` invocation in this codebase ever sets `AIRFLOW_TASK_TRY_NUMBER` — `common_kpo_kwargs()` in `_common/kpo.py` builds exactly four env vars (`DATAPLAT_DB_DSN`, `DATAPLAT_S3_ACCESS_KEY`, `DATAPLAT_S3_SECRET_KEY`, `DATAPLAT_S3_ENDPOINT_URL`) plus, for `ingest` only, `DATAPLAT_HEARTBEAT_INTERVAL_SECONDS`. This means `attempt` — and therefore `meta.ingestion_runs.try_number` — will always record `1`, even on a task's third real Airflow retry, degrading the audit trail's accuracy.

**Fix:** add a Jinja-templated env var to the `ingest` task's KPO invocation:

```python
k8s.V1EnvVar(name="AIRFLOW_TASK_TRY_NUMBER", value="{{ ti.try_number }}")
```

### WR-04: A single row that fails to cast at publish time aborts the entire file's publish

**File:** `packages/dataplat/src/dataplat/load/publish/merge.py:50-71` (casts at line 57: `customer_id::int, ..., birth_date::date, event_ts::timestamptz`); `packages/dataplat/src/dataplat/load/staging.py` (business columns staged as unchecked TEXT); `packages/csv-processor/src/csv_processor/source.py` (structural CSV parsing only — field count, never field content — is validated anywhere upstream)

**Issue:** `StagingLoader` deliberately stores every business column as unvalidated TEXT (its own "Pitfall 9" comment: *"A COPY never fails on a bad date/number here — that becomes a later, set-based validation pass"*). No such validation pass exists anywhere between staging and publish in this phase's `run_ingest` pipeline — `RaggedRowGuard` (the only stage wired in) checks field *count*, never field *content*. `MergePublisher`'s `_PUBLISH_SQL` then performs hard `::int`/`::date`/`::timestamptz` casts on **every** staged row inside one `INSERT ... SELECT` statement. A single malformed value anywhere in the file — an empty `birth_date`, a non-numeric `customer_id`, an unparsable `event_ts` (exactly the "real-world messy CSV" inputs this platform's README names as its target problem) — aborts the whole statement, so the *entire file's* publish fails, not just the offending row. Because staging reproduces the identical malformed value on every retry, all of Airflow's `retries=3` fail identically, permanently blocking that file's ingestion until a human manually intervenes; there is no partial-success or quarantine path in this phase.

**Fix (scoped to this phase):** catch the cast failure around the publish `INSERT`, surface it as a clear, actionable `DataPlatformError` naming the offending row where feasible, rather than a raw `psycopg.errors.DataError`. Full mitigation (per-row validation before publish, quarantine routing) is already anticipated as later-phase work — this finding is to make sure the current failure mode is at least diagnosable, not to demand the full feature now.

### WR-05: SQL identifiers built from an unvalidated config field (`DatasetConfig.dataset`)

**File:** `packages/dataplat/src/dataplat/config/model.py:130` (`dataset: str`, no format constraint); consumed unquoted at `packages/dataplat/src/dataplat/load/staging.py:163,169,182-185,224-225` and via the `staging_table` parameter at `packages/dataplat/src/dataplat/load/publish/merge.py:117-118`

**Issue:** `StagingLoader.load()` builds `staging_table = f"staging.{ctx.config.dataset}__r{ctx.run.run_id}"` and interpolates it unquoted into `DROP TABLE`, `CREATE UNLOGGED TABLE`, and `COPY ... FROM STDIN` statements; `MergePublisher.publish()` interpolates the same value into `INSERT ... FROM {staging_table}`. `DatasetConfig.dataset` is a bare `str` with no character-set/pattern validator, so nothing at the config-loading boundary stops a `configs/datasets/*.yaml` document from containing a `dataset` value that breaks the generated SQL, or — should config provenance ever become less trusted than "hand-authored, git-committed YAML" (a change the codebase's own forward-looking comments anticipate for later phases/multi-dataset support) — enables SQL injection into these statements.

**Fix:** add a Pydantic field validator restricting `dataset` to a safe identifier pattern:

```python
from pydantic import field_validator
import re

_DATASET_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")

class DatasetConfig(BaseModel):
    ...
    @field_validator("dataset")
    @classmethod
    def _dataset_is_a_safe_identifier(cls, value: str) -> str:
        if not _DATASET_NAME_RE.fullmatch(value):
            raise ValueError(f"dataset must match {_DATASET_NAME_RE.pattern!r}, got {value!r}")
        return value
```

### WR-06: `--dataset` CLI argument builds a filesystem path with no traversal guard

**File:** `packages/csv-processor/src/csv_processor/cli.py:129-152`, specifically `Path(f"configs/datasets/{dataset}.yaml")` at line 150

**Issue:** `discover`'s `--dataset` option is interpolated directly into a filesystem path with no validation:

```python
config = load_config(
    Path(f"configs/datasets/{dataset}.yaml"),
    defaults_path=Path("configs/defaults.yaml"),
)
```

Not reachable via the current wiring — `airflow/dags/csv_ingest_customers.py:126` always passes the literal `"customers"` — but the CLI is a real, directly-invokable entrypoint (`ENTRYPOINT ["dataplat"]`; `docker run <image> dataplat discover --dataset <value>`) running as the `csv-processor` service account with mounted DB/S3 credentials. A value such as `--dataset "../../../etc/passwd"` would attempt to resolve outside `configs/datasets/`. Shares its root cause with WR-05 (`dataset` carries no format constraint anywhere).

**Fix:** validate the CLI's own `--dataset` value before constructing the path (the same pattern as WR-05's proposed fix would also close this, once it fires early enough — but the CLI itself should not rely solely on the Pydantic model, since the path is built *before* `load_config`/model validation runs):

```python
import re

_DATASET_ARG_RE = re.compile(r"^[a-z][a-z0-9_]*$")

@click.option("--dataset", required=True, callback=lambda ctx, param, value: (
    value if _DATASET_ARG_RE.fullmatch(value)
    else (_ for _ in ()).throw(click.BadParameter("must be a safe identifier"))
))
```

## Info

### IN-01: `Receipt.rows_deduplicated`'s docstring doesn't capture what the field actually measures

**File:** `packages/dataplat/src/dataplat/models/receipt.py:32` (docstring: *"Number of rows collapsed by deduplication"*); actual computation at `packages/dataplat/src/dataplat/pipeline/run.py:338-346`

**Issue:** `run.py`'s own inline comment candidly explains `rows_deduplicated = max(rows_parsed - rows_affected, 0)` also counts rows suppressed as no-op republishes of already-identical content (via `MergePublisher`'s `_record_hash IS DISTINCT FROM` guard), not only genuine within-batch `DISTINCT ON` collapses: *"This phase does not separately track 'collapsed by DISTINCT ON...' from 'suppressed as a no-op write by the WHERE guard'... a finer split is Phase 9's... territory."* `Receipt.rows_deduplicated`'s public docstring — the one place a Receipt consumer would look — doesn't carry that caveat, so a large `rows_deduplicated` figure could be misread as "many duplicate records in this file" when it may simply mean "this file's content hasn't changed since last time."

**Fix:** copy `run.py`'s caveat into `Receipt.rows_deduplicated`'s docstring.

### IN-02: `_build_common()` doesn't close the pool if `pool.open()` raises after `create_pool()` succeeds

**File:** `packages/csv-processor/src/csv_processor/cli.py:71-87`

**Issue:**

```python
pool = create_pool(dsn)
pool.open(wait=True)
```

If `create_pool(dsn)` succeeds but `pool.open(wait=True)` then raises, the locally-created `ConnectionPool` is discarded without `.close()` ever being called — its background worker/connections aren't explicitly torn down. Low real-world impact given the short-lived, single-shot CLI/pod lifecycle (process exit reclaims everything), but a `try`/`except`-and-close is the more correct pattern.

**Fix:**

```python
pool = create_pool(dsn)
try:
    pool.open(wait=True)
except Exception:
    pool.close()
    raise
```

### IN-03: Stale comment in `scripts/ingest-demo.py` contradicts the current (already-fixed) implementation

**File:** `scripts/ingest-demo.py:111-117`

**Issue:** the comment reads:

```
# `r.duration_ms` is never actually populated by `finalize_publication`
# (dataplat/metadata/postgres.py only sets status/finished_at/rows_loaded/
# report_uri) -- COALESCE to a value computed from started_at/finished_at
```

`PostgresMetadataRepository.finalize_publication` (`packages/dataplat/src/dataplat/metadata/postgres.py:388-399`) *does* set `duration_ms` — per the task context, this was one of the fixes already landed during this phase's live E2E verification. The comment (and the `deferred-items.md` entry it references) is now stale and could mislead a future reader into believing the gap still exists.

**Fix:** update the comment to note `duration_ms` is now persisted directly, and that the `COALESCE` fallback is retained only as a defensive presentational fallback (e.g. for a row that was never finalized), not as a workaround for a known persistence gap.

---

_Reviewed: 2026-08-13T23:21:59Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
