# Phase 4: Vertical Slice — CSV to Analytical PostgreSQL - Pattern Map

**Mapped:** 2026-08-13
**Files analyzed:** 33 (26 explicit in RESEARCH.md's Recommended Project Structure + 7 implied by CONTEXT.md decisions/RESEARCH.md's Validation Architecture table)
**Analogs found:** 26 / 33 (7 have no in-repo structural analog — this is the project's first Airflow DAG work and first strategy-registry code; each is flagged below with the best available substitute)

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `airflow/dags/smoke_kubernetes_pod.py` | controller (DAG) | batch | `packages/dataplat/src/dataplat/cli.py` (convention only) | no structural analog — first DAG in repo |
| `airflow/dags/csv_ingest_customers.py` | controller (DAG) | event-driven | `packages/dataplat/src/dataplat/cli.py` (convention only) + `metadata/postgres.py` (typed-CRUD discipline) | no structural analog — first DAG in repo |
| `packages/dataplat/src/dataplat/load/staging.py` | service | streaming | `packages/dataplat/src/dataplat/pipeline/engine.py` | role+flow match |
| `packages/dataplat/src/dataplat/load/publish/merge.py` | service | CRUD | `packages/dataplat/src/dataplat/load/publish/protocol.py` + `metadata/postgres.py` | exact contract + SQL-idiom match |
| `packages/dataplat/src/dataplat/load/publish/registry.py` | provider | transform | — | no analog — first registry in repo |
| `packages/dataplat/src/dataplat/sources/registry.py` | provider | transform | — | no analog — first registry in repo |
| `packages/dataplat/src/dataplat/metadata/repository.py` (extend) | service (Protocol) | CRUD | itself (`create_ingestion_run`, `update_ingestion_run_status`) | self-extension, exact |
| `packages/dataplat/src/dataplat/metadata/postgres.py` (extend) | service | CRUD | itself (`get_or_create_dataset`, `update_ingestion_run_status`) | self-extension, exact |
| `packages/dataplat/src/dataplat/cli.py` (extend) | controller | request-response | itself (`cli` group / `--version`) | self-extension, exact |
| `packages/dataplat/src/dataplat/config/model.py` (extend, implied by D-13) | model | transform | itself (`SourceConfig`/`LoadConfig`) | self-extension, exact |
| `packages/dataplat/src/dataplat/models/*.py` (new pydantic `AssignmentDocument`, implied) | model | transform | `packages/dataplat/src/dataplat/config/model.py` | role-match, location undetermined |
| `migrations/versions/0006_normalized_customers_business_key_unique.py` | migration | batch | `migrations/versions/0005_normalized_customers.py` | exact — same table |
| `kubernetes/rbac-etl.yaml` | config | batch | `kubernetes/namespaces.yaml` | role-match — only K8s manifest in repo |
| `helm/values/local/airflow.yaml` (extend) | config | batch | itself | self-extension, exact |
| `helm/values/ci/airflow.yaml` (extend) | config | batch | `helm/values/local/airflow.yaml` (paired file) | exact — D-06 divergence-axis rule |
| `tools/corpus/manifest.py` (extend) | model | transform | itself (`PickColumn`/`DecimalColumn`) | self-extension, exact |
| `tools/corpus/generators.py` (extend) | utility | transform | itself (`_pick_renderer`/`_decimal_renderer`) | self-extension, exact |
| `tests/fixtures/slice-corpus.yaml` | config | batch | `tests/fixtures/corpus.yaml` | exact — same manifest format |
| `tests/e2e/slice/__init__.py` | test | n/a | `tests/e2e/cluster/__init__.py` | exact |
| `tests/e2e/slice/conftest.py` | test | event-driven | `tests/e2e/cluster/conftest.py` | exact |
| `tests/e2e/slice/test_pod_kill_retry.py` | test | event-driven | `tests/e2e/cluster/test_airflow_workloads.py` | role-match |
| `tests/e2e/slice/test_concurrent_select.py` | test | CRUD | `tests/e2e/cluster/test_airflow_workloads.py` (`metadata_connection`) | role-match |
| `tests/e2e/slice/test_idempotent_reupload.py` | test | CRUD | `tests/e2e/cluster/test_minio_buckets.py` | role-match |
| `tests/unit/test_dag_structure.py` | test | transform | — (style: `tests/policy/test_no_postgres_csv_parsing.py`) | no structural analog — first `DagBag` test |
| `tests/unit/test_batching_config.py` | test | transform | `tests/unit/test_config_hashing.py` | role-match (style) |
| `tests/unit/test_resolve_window.py` | test | transform | `tests/unit/test_config_hashing.py` | role-match (style) |
| `tests/policy/test_dag_thinness.py` | test | transform | `tests/policy/test_no_postgres_csv_parsing.py` | exact |
| `tests/policy/test_dag_line_budget.py` | test | transform | `tests/policy/test_no_postgres_csv_parsing.py` | role-match |
| `tests/integration/test_discover_files.py` | test | CRUD + file-I/O | `tests/integration/test_metadata_repository.py` | exact |
| `tests/integration/test_publish_merge.py` | test | CRUD | `tests/integration/test_metadata_repository.py` | exact |
| `configs/datasets/customers.yaml` (extend, D-13) | config | batch | itself | self-extension, exact |
| `Makefile` (extend, D-14 `ingest-demo`) | utility | batch | itself (`minio-creds`, `cluster-verify`, `FAST` variable) | self-extension, exact |
| `scripts/ingest-demo.*` (new, implied, path/language TBD) | utility | event-driven | `scripts/wait-for.sh` + `scripts/minio-credentials.sh` | role-match, implied file |

---

## Pattern Assignments

### Group A — Airflow DAG files (no structural analog — first DAG work in this repo)

Both DAG files have **no structural in-repo analog**: `airflow/dags/` currently holds only `.gitkeep`. Per the orchestrator's guidance, treat `cli.py` as the **convention** analog (docstring style, typing, error-handling discipline) and RESEARCH.md's own already-verified code (Patterns 1-5, Code Examples) as the **structural** template, since it was independently verified this session against live official docs and the live cluster.

#### `airflow/dags/smoke_kubernetes_pod.py` (controller, batch)

**Convention analog:** `packages/dataplat/src/dataplat/cli.py`

**Docstring-first-then-code discipline** (`cli.py` lines 1-25): every module opens with a docstring naming *why* the file exists, which requirement it satisfies, and which later file extends it — copy this shape for the DAG's own module docstring (name ORCH-01/ORCH-06/U1 explicitly, and that this file must stay a permanent regression fixture, not a throwaway).

**Structural template:** RESEARCH.md's own Pattern 5 KPO code block (already-verified against `apache-airflow-providers-cncf-kubernetes` docs this session) — `on_finish_action="delete_succeeded_pod"` explicit override, `do_xcom_push=True` explicit, `container_resources` mandatory (ORCH-09), `namespace="etl"` (not `data-etl` — Pitfall 6), `service_account_name="csv-processor"`.

**Error-handling convention to copy** (`errors.py` lines 39-72): row/task-fatal conditions in DAG-adjacent Python (if any helper functions are added) raise `DataPlatformError` subclasses with structured `context`, never a bare `Exception` — but note ORCH-06 forbids this DAG file from containing real logic at all, so this convention mostly applies to any `_common/` helper, not the DAG body itself.

#### `airflow/dags/csv_ingest_customers.py` (controller, event-driven)

**Convention analog:** `packages/dataplat/src/dataplat/cli.py` (docstring/typing discipline) + `packages/dataplat/src/dataplat/metadata/postgres.py` (typed-CRUD-only discipline — the `discover_files` task must call `MetadataRepository` methods, never hand-written SQL, mirroring how `postgres.py` itself is the only place SQL text is assembled).

**Structural template — S3KeySensor** (RESEARCH.md Pattern 4, already verified against `apache-airflow-providers-amazon` docs this session):
```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

wait_for_files = S3KeySensor(
    task_id="wait_for_files",
    bucket_name="raw",
    bucket_key="customers/*.csv",
    wildcard_match=True,
    aws_conn_id="minio_default",
    deferrable=True,
    poke_interval=30,               # D-02
)
```

**Structural template — `resolve_window`'s `logical_date=None` guard** (RESEARCH.md Code Examples, ORCH-05):
```python
@task
def resolve_window(dag_run=None) -> dict[str, str | None]:
    if dag_run is None or dag_run.logical_date is None:
        return {"logical_date": None, "data_interval_start": None, "data_interval_end": None}
    return {
        "logical_date": dag_run.logical_date.isoformat(),
        "data_interval_start": dag_run.data_interval_start.isoformat(),
        "data_interval_end": dag_run.data_interval_end.isoformat(),
    }
```

**Import convention (STACK.md gotcha, still current for 3.3.0):** `from airflow.sdk import dag, task` — never `airflow.decorators`/`airflow.models`. Enforced later by `tests/policy/test_dag_thinness.py`.

---

### Group B — New Publisher/Source strategy layer

#### `packages/dataplat/src/dataplat/load/staging.py` (service, streaming)

**Analog:** `packages/dataplat/src/dataplat/pipeline/engine.py` (explicitly named by the orchestrator as the closest analog for a concrete `StreamingStage`-shaped class)

**Core pattern to copy — the concrete-stage shape** (`engine.py` lines 38-63, `RaggedRowGuard.__init__`/class attrs):
```python
class RaggedRowGuard(StreamingStage):
    name = "ragged_row_guard"

    def __init__(self, *, field_delimiter: str = ",") -> None:
        self._field_delimiter = field_delimiter
```
`StagingLoader` should follow the same shape: a `name: str` class attribute, constructor-injected configuration (staging table name pattern, chunk size), never module-level mutable state.

**Sequencing loop it plugs into** (`engine.py` lines 108-144, `run_streaming`): every stage's `apply(ctx, chunk) -> StageResult` return value threads into the next stage and yields `(first_ordinal, StageResult)` per chunk — `staging.py`'s chunked `COPY` write is a consumer of this generator's output (one `COPY` per yielded chunk), not a reimplementation of chunk sequencing.

**Observability threading to copy** (`engine.py` lines 103-104, 135): `metrics.increment(...)` inside `apply()` and `tracing.start_span(...)` around each chunk — `staging.py`'s per-chunk heartbeat log line (PITFALLS B4) should sit at the same call depth, using `dataplat.observability.logging.get_logger()` (see Shared Patterns).

**Protocol contract it implements:** `packages/dataplat/src/dataplat/pipeline/protocol.py` lines 66-91 (`StreamingStage`) — `name: str` attribute + `apply(ctx, chunk) -> StageResult`, never raising for a row-level problem.

**Corrected staging-table SQL (Pitfall 2 — do not copy ARCHITECTURE.md's `ON COMMIT DROP` verbatim):**
```sql
DROP TABLE IF EXISTS staging.customers__r8123;   -- idempotent-retry cleanup (C5)
CREATE UNLOGGED TABLE staging.customers__r8123 ( ... all business columns TEXT ... );
-- ... chunked COPY ...
-- explicit DROP TABLE after the publish transaction commits (NOT ON COMMIT DROP — invalid syntax)
```

#### `packages/dataplat/src/dataplat/load/publish/merge.py` (service, CRUD)

**Contract analog (exact, explicitly named by the orchestrator):** `packages/dataplat/src/dataplat/load/publish/protocol.py`

**Protocol this class implements** (`protocol.py` lines 38-70):
```python
class Publisher(Protocol):
    name: str

    def publish(
        self,
        ctx: PipelineContext,
        staging_table: str,
        conn: Connection[Any],
    ) -> PublishResult:
        ...
```
Docstring at lines 55-59 is load-bearing: `conn` carries an already-open transaction `publish()` must **never** commit/rollback itself — the caller (the `ingest` CLI subcommand, see Group D) owns the transaction boundary so watermark/run-status updates land in the same transaction as the data (META-03).

**Return type analog** (`protocol.py` lines 23-35): `PublishResult(rows_affected: int, outcome: str)` — `merge.py` populates this from `cur.rowcount` after the `INSERT ... ON CONFLICT` executes; no `MERGE ... RETURNING merge_action()` needed this phase per the protocol's own docstring (lines 6-9).

**SQL-idiom analog (the closest existing `ON CONFLICT` in this codebase):** `packages/dataplat/src/dataplat/metadata/postgres.py` lines 77-109, `get_or_create_dataset`:
```python
row = conn.execute(
    """
    INSERT INTO meta.datasets (dataset_name) VALUES (%s)
    ON CONFLICT (dataset_name) DO UPDATE
        SET dataset_name = EXCLUDED.dataset_name
    RETURNING dataset_id
    """,
    (dataset_name,),
).fetchone()
```
This is the same `INSERT ... ON CONFLICT DO UPDATE ... RETURNING` shape `merge.py`'s publish statement needs, extended with a real `WHERE` guard on `DO UPDATE` (RESEARCH.md Pattern 1's full SQL) — copy the parameterization discipline (every value via `%s`, never string interpolation) verbatim; the docstring at lines 80-94 explaining *why* a no-op upsert beats read-then-write is the same reasoning `merge.py`'s docstring should restate for its own `WHERE _record_hash IS DISTINCT FROM ...` guard.

**Barrier-stage shape (secondary structural analog):** `packages/dataplat/src/dataplat/pipeline/protocol.py` lines 94-116, `BarrierStage` — publication runs once per run (never checkpointed, ARCHITECTURE.md Q4.3), which is the same "runs once, after every chunk is staged" shape as `BarrierStage.apply(ctx) -> StageResult`, even though `Publisher` is the actual contract `merge.py` implements (per ADR-0008, these are sibling shapes, not the same Protocol).

**Migration precondition:** `normalized.customers.customer_id` currently carries only a plain, non-unique index (`migrations/versions/0005_normalized_customers.py` lines 90-96, `ix_customers_customer_id`) — `ON CONFLICT (customer_id)` will fail with `there is no unique or exclusion constraint matching the ON CONFLICT specification` until migration `0006` (Group F) lands first.

#### `packages/dataplat/src/dataplat/load/publish/registry.py` and `packages/dataplat/src/dataplat/sources/registry.py` (provider, transform)

**No analog** — confirmed by repo-wide grep: no `dict[str, Protocol]`-shaped registry exists anywhere in this codebase yet. `config/model.py` only *names* `SOURCE_REGISTRY`/`PUBLISHER_REGISTRY` in docstrings (lines 35, 74) as a forward reference; nothing implements them.

**What to copy instead (per RESEARCH.md's own Don't-Hand-Roll guidance):** a plain module-level `dict[str, Publisher]` / `dict[str, Source]` constant — no entry-points plugin machinery (that's explicitly rejected as premature generality until a second `Source`/`Publisher` exists). The closest available *convention* for a validated module-level constant is `metadata/postgres.py` lines 30-62, `_INGESTION_RUN_UPDATABLE_FIELDS` — a `frozenset`/`dict` built once at import time, named with a leading underscore only when private, referenced by every call site instead of re-declared:
```python
_INGESTION_RUN_UPDATABLE_FIELDS = frozenset({...})
```
`registry.py` should mirror this shape:
```python
PUBLISHER_REGISTRY: dict[str, Publisher] = {
    "merge": MergePublisher(),
}
```
Raise `dataplat.errors.ConfigurationError` (see Shared Patterns) when a config names a strategy key absent from the registry — this is exactly the failure mode `errors.py`'s `ConfigurationError` docstring (lines 76-81) already reserves: "a config names a source/deduplication/publisher strategy key that has no registry entry."

---

### Group C — `MetadataRepository` extension

#### `packages/dataplat/src/dataplat/metadata/repository.py` (extend)

**Analog:** itself — the file's own `create_ingestion_run` method (lines 117-153) is the template for the new `get_or_create_ingestion_run`; `update_ingestion_run_status` (lines 155-170) is the template for `claim_ingestion_run`.

**Docstring cross-reference convention to copy** (repository.py lines 155-170):
```python
def update_ingestion_run_status(self, *, run_id: int, status: str, **fields: object) -> None:
    """Update `meta.ingestion_runs.status` and any additional named columns.

    Maps to ``UPDATE meta.ingestion_runs SET status = ..., ... WHERE
    run_id = ...``. Implementations must validate `fields`' keys against
    a fixed allow-list of real `meta.ingestion_runs` column names before
    using them to shape the `SET` clause — never build it from
    unchecked caller-supplied keys.
    ...
    """
    ...
```
Every method here states "Maps to `<SQL shape>`" in its docstring — `get_or_create_ingestion_run`/`claim_ingestion_run` should do the same, naming the exact `INSERT ... ON CONFLICT` / `UPDATE ... WHERE` shape from RESEARCH.md Pattern 2, so the Protocol stays the single source of truth for what SQL a conforming implementation must run.

**PLR0913 noqa convention** (repository.py line 117): `def create_ingestion_run(  # noqa: PLR0913 -- matches ingestion_runs' identity/FK column set` — reuse this exact comment shape if either new method's keyword-only parameter count trips the same lint rule.

**Critical distinction to encode (Pitfall 5):** these are **two different SQL statements with two different jobs** — `get_or_create_ingestion_run` (discovery-time, called by the DAG's `discover_files` task, must tolerate re-runs) is a no-op `DO UPDATE`; `claim_ingestion_run` (pod-startup-time, must enforce exclusivity) is a conditional `UPDATE ... WHERE status IN (...)`. Never collapse them into one method — see Group D's code example for how the pod calls only the second one.

#### `packages/dataplat/src/dataplat/metadata/postgres.py` (extend)

**Analog:** itself — same two methods as above, concrete implementations.

**`get_or_create_ingestion_run`'s SQL-idiom source** (postgres.py lines 77-109, `get_or_create_dataset` — copy this shape exactly, extended per RESEARCH.md Pattern 2 part 1):
```python
row = conn.execute(
    """
    INSERT INTO meta.ingestion_runs (idempotency_key, dataset_id, file_id, batch_id,
                                      config_version_id, processor_version,
                                      processor_image_digest, status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, 'PENDING')
    ON CONFLICT (idempotency_key) DO UPDATE
        SET idempotency_key = EXCLUDED.idempotency_key
    RETURNING run_id, status
    """,
    (...),
).fetchone()
```

**`claim_ingestion_run`'s dynamic-SET-clause discipline source** (postgres.py lines 231-259, `update_ingestion_run_status`): the allow-list-then-assemble pattern —
```python
unknown_fields = sorted(set(fields) - _INGESTION_RUN_UPDATABLE_FIELDS)
if unknown_fields:
    raise ValueError(...)
...
query = "UPDATE meta.ingestion_runs SET " + set_clause + " WHERE run_id = %s"  # noqa: S608
```
`claim_ingestion_run` does not need this exact dynamic-`SET` machinery (its `SET` clause is fixed, per RESEARCH.md Pattern 2 part 2), but it must copy the same **`# pragma: no cover` / `if row is None: raise RuntimeError(...)`** narrowing convention every other method here uses (e.g. lines 106-108, 142-144, 176-178) — except `claim_ingestion_run`'s `row is None` case is a **real, expected outcome** (already-claimed or already-succeeded run), not an invariant violation, so it must `return None` rather than raise, per RESEARCH.md Pattern 2 part 2's comment:
```python
# row is None and a SUCCEEDED row exists ⇒ SKIPPED_DUPLICATE, exit 0
# row is None and a live-leased RUNNING row exists ⇒ CONCURRENT_RUN, exit 0
```

**Constructor/pool convention** (postgres.py lines 65-75): every method opens one connection from `self._pool.connection()` — never constructs its own pool. Both new methods must follow this unchanged.

---

### Group D — CLI extension

#### `packages/dataplat/src/dataplat/cli.py` (extend)

**Analog:** itself — the file's own docstring (lines 22-24) names this exact extension point: *"This phase's only subcommand is the `--version` flag on the group itself; `ingest` (Phase 4) and later subcommands attach to the same `cli` group and inherit this boundary and the one-time logging configuration for free."*

**Attachment point to copy** (cli.py lines 58-61):
```python
@click.group(no_args_is_help=True)
@click.version_option(version=resolve_version(), prog_name="dataplat")
def cli() -> None:
    """Dataplat -- the source-agnostic ETL platform core's command line."""
```
The new subcommand is `@cli.command()` decorated, in a new or existing module, imported so `cli.py`'s `main()` (unchanged) picks it up — never a second click group, never a second `main()`.

**Inherited catch-once error boundary** (cli.py lines 93-127) — do **not** add a second `try/except DataPlatformError` inside the `ingest` subcommand's callback; any run-fatal condition raises `DataPlatformError` (or a new subclass — see Shared Patterns) and lets `main()`'s single boundary catch it:
```python
except DataPlatformError as exc:
    log.error(
        "dataplat command failed",
        error_type=type(exc).__name__,
        error_message=str(exc),
        **exc.context,
    )
    return 1
```

**Sketch already drafted (RESEARCH.md Code Examples — use as the literal starting shape):**
```python
@cli.command()
@click.option("--assignment", required=True)
def ingest(assignment: str) -> None:
    ctx = build_pipeline_context(assignment_uri=assignment)
    claimed = ctx.metadata.claim_ingestion_run(...)
    if claimed is None:
        write_receipt(status="SKIPPED_DUPLICATE_OR_CONCURRENT")
        return
    run_id, _ = claimed
    heartbeat = start_heartbeat_thread(ctx, run_id)
    try:
        staging_table = stage_chunks(ctx, run_id)
        with ctx.db.connection() as conn, conn.transaction():
            publisher = PUBLISHER_REGISTRY[ctx.config.load.strategy]
            result = publisher.publish(ctx, staging_table, conn)
            ctx.metadata.update_ingestion_run_status(
                run_id=run_id, status="SUCCEEDED",
                finished_at=..., rows_loaded=result.rows_affected,
            )
        conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
    finally:
        heartbeat.stop()
    write_receipt(run_id=run_id, status="SUCCEEDED", rows_loaded=result.rows_affected, ...)
```

---

### Group E — Config model + dataset config extension (D-13)

#### `packages/dataplat/src/dataplat/config/model.py` (extend, implied)

Not named explicitly in RESEARCH.md's Recommended Project Structure, but **required**: D-13 adds a duplicate-file-content policy field to `configs/datasets/customers.yaml`, and every model in this file is `ConfigDict(extra="forbid", frozen=True)` (lines 43, 62, 79, 104) — an unrecognized YAML key fails validation, so the new key needs a matching field before `configs/datasets/customers.yaml` can be extended.

**Analog:** itself — `SourceConfig` (lines 31-48) is the closest existing model by subject matter (it already owns `change_semantics`, a sibling "how the source signals state" field):
```python
class SourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    type: str
    bucket: str
    path: str
    change_semantics: str
```
Add the new field as a plain `str` (registry-resolved key, matching this file's own stated convention at lines 10-15: *"Strategy/source fields ... are plain `str`, resolved through string-keyed registries elsewhere ... never a Python `Enum`"*), e.g. `duplicate_policy: str` defaulting or required per the planner's call — not a bespoke `Literal["skip"]`, to stay consistent with every sibling field in this file.

**Loader this flows through unchanged:** `packages/dataplat/src/dataplat/config/loader.py` lines 39-71, `load_config` — merge-then-`model_validate`-then-wrap-as-`ConfigurationError` requires no change; a new field is picked up automatically once `model.py` declares it.

#### `configs/datasets/customers.yaml` (extend, D-13)

**Analog:** itself (full file, 27 lines) — add the new key inside the existing `source:` block (mirroring `change_semantics`'s placement) or as a new top-level block, matching this file's own comment-header convention (lines 1-12) that names which Pydantic model and which `ARCHITECTURE.md` section the shape derives from.

---

### Group F — Migration

#### `migrations/versions/0006_normalized_customers_business_key_unique.py`

**Analog (exact — same table):** `migrations/versions/0005_normalized_customers.py`

**Docstring convention to copy** (0005 lines 1-25): names *what* is created, *which CONTEXT.md/RESEARCH.md decision* drove the shape, and explicitly documents *why the previous migration's choice no longer applies* — 0005's own docstring (lines 17-20) says the plain index was deliberate "since a target row's uniqueness constraint here would fight ... `MERGE`"; `0006`'s docstring must explain the reversal (LOAD-09 rejects `MERGE`, so that reasoning no longer holds) rather than silently contradicting it.

**Revision-chaining convention** (0005 lines 32-36, matching 0004's lines 27-31):
```python
revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None
```

**GRANT convention** (0005 line 97, 0001 lines 58/94): every table-creating migration issues an explicit `op.execute("GRANT ... TO etl_app")` — `0006` alters an existing table rather than creating one, so no new `GRANT` is needed, but confirm no grant regression (dropping and recreating the index/constraint does not touch table-level grants in PostgreSQL, so this is a non-issue, worth a one-line docstring note rather than silent omission).

**Content already drafted (RESEARCH.md Pattern 1 — verified against `0005`'s actual index name, `ix_customers_customer_id`, confirmed live):**
```python
def upgrade() -> None:
    op.drop_index("ix_customers_customer_id", table_name="customers", schema="normalized")
    op.create_unique_constraint(
        "uq_customers_customer_id", "customers", ["customer_id"], schema="normalized"
    )

def downgrade() -> None:
    op.drop_constraint("uq_customers_customer_id", "customers", schema="normalized", type_="unique")
    op.create_index("ix_customers_customer_id", "customers", ["customer_id"], schema="normalized")
```

---

### Group G — Kubernetes RBAC + Helm values wiring

#### `kubernetes/rbac-etl.yaml` (new)

**Analog (role-match — only existing K8s manifest in the repo):** `kubernetes/namespaces.yaml`

**Comment-header convention to copy** (namespaces.yaml lines 1-12): every object gets a one-line purpose comment, and the file's own header states an invariant it protects (there: "No Helm release ... may pass `--create-namespace`"). `rbac-etl.yaml`'s header should state its own invariant: this Role/RoleBinding grants exactly `get/list/watch/create/delete` on `pods` and `get` on `pods/log` in namespace `etl`, to the Airflow scheduler's real ServiceAccount (verify the name live — Pitfall 7 — never assume it), and nothing broader (V4 Access Control: no `cluster-admin`, no wildcard verb/resource).

**Structural shape to copy** (namespaces.yaml lines 13-36): plain `---`-separated multi-document YAML, `apiVersion`/`kind`/`metadata.name` at top level — `rbac-etl.yaml` needs four documents in this same flat style: `ServiceAccount` (`csv-processor`, namespace `etl`), `Role` (namespace `etl`), `RoleBinding` (scheduler SA → the Role), and optionally a second minimal `Role`/`RoleBinding` pair if the `csv-processor` SA itself needs any in-namespace permission (verify against RESEARCH.md — this phase's KPO pod does not create further pods, so likely none needed beyond running as that SA).

#### `helm/values/{local,ci}/airflow.yaml` (extend)

**Analog:** itself, paired — the two files are already required to "diverge on EXACTLY three axes" (both files' own header comments, line 1-3 in each) with any additional axis requiring an explicit, argued exception (local.yaml lines 5-15's own worked example for the `executor` axis). The DAG-mount addition is a **fourth candidate divergence axis** and must be added identically to both files unless a similarly explicit argument is written — do not let CI silently diverge.

**Insertion shape (RESEARCH.md Pattern 3, cross-checked against the chart's documented `extraVolumes`/`extraVolumeMounts` keys this session):**
```yaml
dags:
  persistence:
    enabled: false
  gitSync:
    enabled: false

extraVolumes:
  - name: dags
    hostPath: { path: /mnt/dags, type: Directory }

extraVolumeMounts:
  - name: dags
    mountPath: /opt/airflow/dags
    readOnly: true
```
`/mnt/dags` is the exact `containerPath` `kind/cluster.yaml` already declares (lines 86-88, 133-136, 163-166 — confirmed identical on control-plane and both workers) — do not invent a different path.

**Existing resource-block convention this addition must match** (local.yaml lines 169-180, `workers.kubernetes.resources` — already the first per-task-pod sizing block in this file, added specifically because "Phase 4's DAGs are the first thing to actually schedule one of these"): the new `workers.kubernetes.extraVolumeMounts`/`extraVolumes` keys (Assumption A1 in RESEARCH.md — verify the exact key name against the live chart before relying on it) sit alongside this existing `workers.kubernetes:` block, not as a new top-level stanza.

**CI-specific constraint to preserve** (ci/airflow.yaml lines 11-13): `tests/policy/test_manifest_resources.py` sums CI's rendered container requests against the 4 CPU / 16 GB runner budget — confirm the DAG-mount addition introduces no new resource-bearing container (a `hostPath` volume mount does not, but double-check after rendering).

---

### Group H — Corpus generator extension (D-05/D-08, Faker-style fixtures)

#### `tools/corpus/manifest.py` (extend)

**Analog:** itself — `PickColumn` (lines 144-155) and `DecimalColumn` (lines 158-178) are the two existing `ColumnSpec` variants closest to what "realistic-looking" data needs; both are frozen, slotted dataclasses with a `kind: Literal[...]` discriminator:
```python
@dataclass(frozen=True, slots=True)
class PickColumn:
    values: tuple[str, ...]
    kind: Literal["pick"] = "pick"
```
A new composite-name or date-offset `ColumnSpec` variant must follow this exact shape: frozen, `slots=True`, a fixed `Literal` `kind` field, added to the `ColumnSpec` union (line 203: `ColumnSpec = ZeroPaddedIntColumn | PickColumn | DecimalColumn | RepeatColumn`).

**Three places a new `ColumnSpec` variant must be registered (miss any one and it silently fails at a different layer):**
1. `_COLUMN_KEY_ORDER` dict (lines 102-109) — the known-keys-per-kind allow-list used for `_reject_extra_keys` error messages.
2. `_parse_column` (lines 976-1018) — the `if kind == "...":` dispatch that builds the dataclass from validated YAML.
3. `tools/corpus/generators.py`'s `_renderer_for` (lines 437-445) — the `isinstance` dispatch that builds the per-row `Renderer` closure.

#### `tools/corpus/generators.py` (extend)

**Analog:** itself — `_pick_renderer` (lines 477-486) and `_decimal_renderer` (lines 489-516) are the exact shape a new renderer must copy: a closure-returning function that does any one-time setup (list/bounds conversion) **outside** the returned `_render` closure, so per-row cost is O(1) arithmetic:
```python
def _pick_renderer(spec: PickColumn) -> Renderer:
    values = spec.values
    count = len(values)

    def _render(rng: random.Random, row_index: int) -> str:
        del row_index
        return values[min(int(rng.random() * count), count - 1)]

    return _render
```

**Determinism rules that bind any new renderer (module docstring lines 10-37, restated because they are easy to violate silently):**
- **R2**: consume randomness *only* via `rng.random()` — never `rng.choice`/`rng.randint`/`rng.sample` (documented as version-unstable). A composite name column (first+last) is two independent `_pick_renderer`-style draws, each its own `rng.random()` call, never a single `rng.choice(names_list)`.
- **R10**: no value passes through `float` — a date/timestamp `ColumnSpec` must render via integer day-offset arithmetic (mirroring `_decimal_renderer`'s "convert bounds once via `Decimal`, do integer arithmetic per row" pattern at lines 497-502), never per-row `datetime` object construction.
- **R1**: every fixture's stream is derived once via `stream_for(master_seed, name)` (lines 126-138) — a new renderer must accept the already-derived `rng: random.Random` as a parameter, never seed its own.

#### `tests/fixtures/slice-corpus.yaml` (new manifest, separate from `tests/fixtures/corpus.yaml`)

**Analog:** `tests/fixtures/corpus.yaml`'s own tabular-fixture shape (lines 66-90, fixture `01_simple.csv`):
```yaml
version: 1
master_seed: "airflow-platform/corpus/v1"

fixtures:
  - name: "01_simple.csv"
    covers: [CSV-04, CSV-07]
    generator: tabular
    encoding: utf-8
    bom: false
    delimiter: ","
    quotechar: '"'
    line_terminator: "\n"
    header: [id, name, amount]
    rows: 20
    row_spec:
      id: { kind: zero_padded_int, width: 6, start: 1 }
      name: { kind: pick, values: ["Kowalski", "Nowak", "Wiśniewski", "Wójcik"] }
      amount: { kind: decimal, min: "100.00", max: "99999.99", scale: 2 }
    expect:
      detected_encoding: utf-8
      detected_delimiter: ","
      header_row_index: 0
      data_rows: 20
      rejected_rows: 0
```
`slice-corpus.yaml` needs (per D-06/D-07) two fixture entries in this same shape — one `rows: ~50-200` (fast, every-CI-run) and one `rows: 1_000_000` with `profile: large` (spike-only, skipped by `--fast`, mirroring `corpus.yaml`'s own `profile: large` fixture) — driven through the *same* `generate_corpus()`/`load_manifest()` functions via `python -m tools.corpus generate --manifest tests/fixtures/slice-corpus.yaml --out <dir>` (no code change needed in `tools/corpus/__main__.py`, which already accepts `--manifest`/`--out` as flags per lines 47-48, 64).

**Header block convention** (corpus.yaml lines 1-13): every manifest states, in a comment, why it is a *separate* file from its sibling — `slice-corpus.yaml`'s header should state explicitly that it holds realistic uniform E2E data, not edge-case pathologies, so a future reader does not merge the two.

---

### Group I — `tests/e2e/slice/` (D-09..D-12)

#### `tests/e2e/slice/__init__.py`

**Analog (exact):** `tests/e2e/cluster/__init__.py` — empty marker file.

#### `tests/e2e/slice/conftest.py`

**Analog (exact, explicitly named by the orchestrator):** `tests/e2e/cluster/conftest.py`

**Skip-with-reason autouse fixture to copy verbatim in shape** (conftest.py lines 79-102):
```python
@pytest.fixture(scope="session", autouse=True)
def _require_cluster(kubectl_context: str) -> None:
    kubectl_bin = shutil.which("kubectl")
    if kubectl_bin is None:
        pytest.skip("kubectl not found on PATH — tests/e2e/cluster/ needs a live cluster")
    proc = subprocess.run(
        [kubectl_bin, "--context", kubectl_context, "get", "nodes", "-o", "name"],
        capture_output=True, text=True, check=False, timeout=10,
    )
    if proc.returncode != 0:
        pytest.skip(f"no live cluster reachable at context '{kubectl_context}' ... run `make cluster-up` first")
```
`tests/e2e/slice/conftest.py` needs the equivalent, plus (since `tests/e2e/slice/` also needs a live analytical-PostgreSQL connection and a live MinIO) either import this fixture directly (pytest fixtures are inheritable from a parent `conftest.py` if `tests/e2e/slice/` is *not* independently collected — verify collection scope) or re-derive it identically; do not invent a second, differently-worded skip message.

**`s3_client` factory to reuse near-verbatim** (conftest.py lines 182-227) — same `admin`/`app` credential selector, same `S3_ENDPOINT_URL` env-var override, same path-style addressing:
```python
return boto3.client(
    "s3",
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name="us-east-1",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)
```

**`kubectl`/`kubectl_json` helpers to reuse verbatim in shape** (conftest.py lines 105-149) — `tests/e2e/slice/test_pod_kill_retry.py` needs `kubectl` (for `kubectl delete pod`, D-09) and `kubectl_json` (to find the running pod's name before killing it).

#### `tests/e2e/slice/test_pod_kill_retry.py` (D-09, D-10, D-11)

**Analog (role-match):** `tests/e2e/cluster/test_airflow_workloads.py`

**Poll-until-connected pattern to copy** (test_airflow_workloads.py lines 152-196, `_port_forwarded_postgres`) — the exact "spawn subprocess, poll with a deadline, never `sleep N` blindly" shape D-11 requires:
```python
deadline = time.monotonic() + 30
connected = False
while time.monotonic() < deadline:
    if proc.poll() is not None:
        ...
        raise AssertionError(msg)
    with contextlib.suppress(OSError), socket.create_connection(...):
        connected = True
        break
    time.sleep(0.5)
if not connected:
    msg = f"... never accepted a connection within 30s"
    raise AssertionError(msg)
```
D-11's own polling target differs (poll `meta.ingestion_runs.rows_read`/`lease_expires_at` via a live query, not a socket connect) but the **shape** — `time.monotonic()` deadline loop, short `time.sleep(0.5)` between polls (not a single long `sleep N`), explicit timeout failure message — is exactly this pattern, reapplied. `scripts/wait-for.sh` (see Shared Patterns) is the shell-side sibling of this same discipline.

**`kubectl delete pod` mechanism (D-09 — a real kill, not a self-kill):** use the `kubectl` fixture from `conftest.py`:
```python
kubectl("-n", "etl", "delete", "pod", pod_name, "--wait=false")
```
issued once the poll confirms the pod is genuinely mid-load (`rows_read > 0` and `< total_rows`), against the ~1M-row fixture (D-06) specifically, so there is a real window to hit.

#### `tests/e2e/slice/test_concurrent_select.py` (D-12)

**Analog (role-match):** `tests/e2e/cluster/test_airflow_workloads.py`'s `metadata_connection` fixture (lines 199-218) — a live `psycopg.Connection` via `kubectl port-forward`, torn down unconditionally in a `finally`. `test_concurrent_select.py` needs **two** such connections open concurrently (one driving/observing the publish, one polling `normalized.customers` row counts) — extend the single-connection fixture into a pair, or open a second connection directly with `psycopg.connect(...)` reusing `_read_app_secret`/`_port_forwarded_postgres`'s pattern (lines 146-196) against the **analytical** cluster's Secret name instead of `airflow-db`.

#### `tests/e2e/slice/test_idempotent_reupload.py` (D-07, LOAD-03/QUAL-09)

**Analog (role-match):** `tests/e2e/cluster/test_minio_buckets.py`

**Upload-then-observe pattern to copy** (test_minio_buckets.py lines 53-67, `test_round_trip_through_s3_uri`):
```python
app = s3_client("app")
bucket, key = "raw", "e2e/minio-buckets/round-trip.txt"
payload = b"..."
try:
    app.put_object(Bucket=bucket, Key=key, Body=payload)
    got = app.get_object(Bucket=bucket, Key=key)["Body"].read()
    assert got == payload
finally:
    admin.delete_object(Bucket=bucket, Key=key)
```
`test_idempotent_reupload.py` uploads the **same bytes** (the D-07 small fixture) to two different keys under `raw/customers/`, waits for both DAG runs to reach a terminal state (poll `meta.ingestion_runs`, never sleep — same discipline as `test_pod_kill_retry.py`), then asserts `normalized.customers` row count is unchanged after the second upload and `meta.files.duplicate_of_file_id` is set on the second file's row (D-13).

---

### Group J — `tests/unit/` (structural, no live dependencies)

#### `tests/unit/test_dag_structure.py`

**No structural analog** (first `DagBag`-based test in the repo). Style to copy from `tests/policy/test_no_postgres_csv_parsing.py` (module docstring stating an honest limitation, one test per named assertion, assertion messages that name the offending file). RESEARCH.md's Validation Architecture table (`ORCH-01`, `ORCH-04`, `ORCH-09` rows) already names the exact assertions needed: `DagBag(dag_folder="airflow/dags", include_examples=False)`, `import_errors == {}`, every task has `retries` set, every KPO task has `container_resources` set.

#### `tests/unit/test_batching_config.py` and `tests/unit/test_resolve_window.py`

**Analog (style):** `tests/unit/test_config_hashing.py` — plain `test_*` functions (no test classes), small inline fixture data built as a Python dict literal at module scope (lines 24-39), `tmp_path`-based tests only where filesystem I/O is genuinely needed:
```python
def test_hashing_the_same_config_twice_returns_the_same_hash() -> None:
    cfg = _customers_config()
    first_hash, _ = hash_config(cfg)
    second_hash, _ = hash_config(cfg)
    assert first_hash == second_hash
```
`test_resolve_window.py` tests the pure function from RESEARCH.md's Code Examples directly (no Airflow `DagRun` needed beyond a minimal stand-in/mock for the `dag_run=None` and `dag_run.logical_date=None` branches) — same plain-function-per-case shape, no class.

---

### Group K — `tests/policy/`

#### `tests/policy/test_dag_thinness.py`

**Analog (exact):** `tests/policy/test_no_postgres_csv_parsing.py` — copy this file's entire shape: `REPO_ROOT` resolved once from `Path(__file__).resolve().parents[N]`, a `_candidate_files()` walker with an `EXCLUDED_DIRS` frozenset, a compiled regex built from assembled fragments where relevant, one `violations: list[str]` accumulator, and the mandatory anti-vacuity test:
```python
def test_the_scan_actually_reaches_files() -> None:
    """A scanner that walks nothing passes for the wrong reason."""
    assert _candidate_files(), "policy scan found no candidate files — the walk is broken"
```
ORCH-02's forbidden patterns for `airflow/dags/*.py` are different (no `csv.reader`, no `psycopg`/SQL execution, no `pydantic` model construction/validation inside a DAG file — business logic markers, not a single regex) — this will likely need several named `FORBIDDEN_*` patterns rather than `test_no_postgres_csv_parsing.py`'s single one, but the file-walking/violation-accumulation/anti-vacuity skeleton copies directly.

#### `tests/policy/test_dag_line_budget.py`

**Analog (role-match):** `tests/policy/test_no_postgres_csv_parsing.py`'s file-walking skeleton, simplified — this test only needs `len(path.read_text().splitlines())` per DAG file against ORCH-06's 150-line ceiling, no regex at all. `tests/policy/test_manifest_resources.py`'s framing (a numeric budget asserted against real rendered/real-file output, module docstring stating the "honest limit" of what is and is not proven) is the secondary style reference for how to word the budget assertion's failure message.

---

### Group L — `tests/integration/`

#### `tests/integration/test_discover_files.py` and `tests/integration/test_publish_merge.py`

**Analog (exact):** `tests/integration/test_metadata_repository.py` + `tests/integration/conftest.py`

**Fixture composition to copy** (test_metadata_repository.py lines 80-88):
```python
@pytest.fixture
def repository(migrated_dsn: str) -> Iterator[PostgresMetadataRepository]:
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        yield PostgresMetadataRepository(pool)
    finally:
        pool.close()
```
`test_publish_merge.py` additionally needs a populated staging table before it can call `publisher.publish(...)` — seed it with direct SQL the same way `test_metadata_repository.py`'s own `_insert_config_version` helper (lines 33-66) seeds a `meta.config_versions` row it doesn't own the repository method for: a small `_seed_staging_rows(dsn, ...)` helper using `psycopg.connect(dsn)` directly, not through any repository method (there isn't one for staging writes at the integration-test level).

**`migrated_dsn`/`s3_client` fixtures to reuse, not reimplement** (conftest.py lines 81-171): `test_discover_files.py` needs both (`discover_files` touches S3 for listing and `meta.files` for recording) — depend on the existing session-scoped `migrated_dsn` and `s3_client` fixtures from `tests/integration/conftest.py` directly; do not write a third MinIO/PostgreSQL container-spinning fixture.

**Full-round-trip assertion style to copy** (test_metadata_repository.py lines 91-147, `test_full_slice_round_trip`) — construct the whole chain end to end in one test function, asserting each intermediate ID/status as it goes, rather than one assertion per test function; `test_publish_merge.py`'s the analogous concurrency test PITFALLS C1 recommends (two overlapping batches, same business key, both attempting the same `ON CONFLICT` path) should follow this same "build the real scenario, assert the real outcome" style rather than mocking any part of the transaction.

---

### Group M — Developer demo workflow (D-14, D-15, D-16)

#### `Makefile` (extend)

**Analog:** itself — `minio-creds` (lines 150-152) and `cluster-verify` (lines 154-166) are the two closest existing targets: both need a live cluster, both use `$(RUN_CLUSTER)` (never `$(RUN)`) because they touch boto3/psycopg, both have a `## D-XX: ...` comment suffix naming the decision that drove them:
```makefile
minio-creds:                   ## D-14: print live MinIO credentials, shell-sourceable [plan 02-04]
	@set -a; . helm/versions.env; set +a; \
	KUBECTL_CONTEXT="kind-$$CLUSTER_NAME" scripts/minio-credentials.sh show
```

**`FILE=<path>` variable-argument convention** (Makefile lines 36-43, the `FAST ?=` pattern for `make fixtures FAST=1`):
```makefile
FAST ?=
FIXTURES_FAST  := $(if $(FAST),--fast,)
```
`make ingest-demo FILE=<path>` should declare `FILE ?=` the same way, and fail loudly with a clear message if unset (no existing target has an *unconditionally required* variable to copy — this is a new-but-small pattern, consistent with `stage-%`'s own explicit-failure style at Makefile lines 209-213: `if [ -z "$$script" ]; then echo "ERROR: ..." >&2; exit 1; fi`).

**`.PHONY` list update:** add `ingest-demo` to the existing `.PHONY:` block (lines 45-48).

#### `scripts/ingest-demo.*` (new, implied by D-14/D-16, exact filename/language left to the planner per CONTEXT.md's "Claude's Discretion" note)

**Poll-not-sleep analog:** `scripts/wait-for.sh` (full file, 57 lines) — every wait in this repo is a bounded, explicit-timeout wait, never a bare `sleep`:
```bash
wait_for_deploy_available() {
  local namespace="$1"
  local deploy="$2"
  _kubectl_wait -n "${namespace}" wait --for=condition=Available \
    --timeout="${WAIT_DEPLOY_TIMEOUT:-180s}" "deploy/${deploy}"
}
```
D-16's "poll `meta.ingestion_runs` with a timeout, not a blind sleep" requirement is the same discipline applied to a SQL poll instead of a `kubectl wait` — if the target is implemented in Python (likely, since it needs both `boto3` PutObject and a `psycopg` poll — both already pinned dependencies), copy `tests/e2e/cluster/test_airflow_workloads.py`'s `time.monotonic()`-deadline loop shape (lines 175-188) instead of this shell version; if implemented in shell, copy `wait-for.sh`'s bounded `kubectl wait --timeout=` idiom plus a small polling loop around `psql`/`kubectl exec ... psql`.

**Dev-script header/usage convention:** `scripts/minio-credentials.sh` (full file) — shebang, a comment block naming the decision (`D-14`) and the exact invariant the script protects, a `usage()` function printing to stderr and exiting `2` on bad invocation, credentials/values never appearing in `argv` (T-02-16's precedent — if the demo script ever needs a credential, resolve it the same way, never as a CLI argument).

**Upload step:** reuse `S3ObjectStore`-adjacent `boto3` construction (`packages/dataplat/src/dataplat/storage/objectstore.py` lines 96-112, `S3ObjectStore.__init__`) or the `s3_client` fixture's plain-`boto3.client(...)` call (`tests/e2e/cluster/conftest.py` lines 220-227) — never `mc` (not installed, not vendored, explicitly rejected in RESEARCH.md's Don't-Hand-Roll table).

---

## Shared Patterns

### Parameterized SQL only, never string interpolation
**Source:** `packages/dataplat/src/dataplat/metadata/postgres.py` (every method) — the one deliberate exception is `update_ingestion_run_status`'s dynamic `SET` clause (lines 244-256), and even there only **column names** (pre-validated against `_INGESTION_RUN_UPDATABLE_FIELDS`) are assembled dynamically; every **value** still crosses via a `%s` placeholder.
**Apply to:** `load/staging.py`, `load/publish/merge.py`, `metadata/postgres.py`'s two new methods, `tests/integration/test_discover_files.py`'s/`test_publish_merge.py`'s seed helpers.
```python
conn.execute("INSERT INTO meta.files (...) VALUES (%s, %s, ...)", (value1, value2, ...))
```

### `DataPlatformError` catch-once boundary
**Source:** `packages/dataplat/src/dataplat/errors.py` (full file) + `packages/dataplat/src/dataplat/cli.py` lines 93-127.
**Apply to:** every new run-fatal raise site in `load/staging.py`, `load/publish/merge.py`, the `ingest` CLI subcommand. Row-level problems (a malformed staged row, a duplicate business key inside one batch) are **not** run-fatal — they become part of a `PublishResult`/receipt, never a raised exception (QUAL-03). Only genuinely run-aborting conditions (staging table creation fails, the claim upsert errors for a reason other than "already claimed") raise `DataPlatformError` (reuse `StorageError`, or add a narrowly-scoped new subclass only at its first real raise site, per `errors.py`'s own "a subclass with no raise site is dead code" rule, lines 6-7).

### Single connection-pool factory, never constructed ad hoc
**Source:** `packages/dataplat/src/dataplat/storage/db.py` (full file, `create_pool`).
**Apply to:** the `ingest` CLI subcommand's `build_pipeline_context` helper — call `create_pool(dsn)` exactly once per pod invocation; `load/staging.py`/`load/publish/merge.py` receive an already-open `psycopg.Connection`/pool, never build their own.

### `INSERT ... ON CONFLICT DO UPDATE ... RETURNING` no-op-upsert idiom
**Source:** `packages/dataplat/src/dataplat/metadata/postgres.py` lines 77-109 (`get_or_create_dataset`) and `packages/dataplat/src/dataplat/config/registry.py` lines 217-226 (`_resolve_dataset_id` — the identical idiom, independently reused, with matching docstring reasoning at lines 24-30/192-208 about why `SELECT ... FOR UPDATE` cannot serialize a first-ever insert).
**Apply to:** `get_or_create_ingestion_run` (Group C), `merge.py`'s publish statement (Group B) — extended with a real `WHERE` guard on `DO UPDATE`, which neither existing use case needs but `merge.py` does (RESEARCH.md Pattern 1).

### Structured logging via the one `configure()`/`get_logger()` seam
**Source:** `packages/dataplat/src/dataplat/observability/logging.py` (full file).
**Apply to:** every per-chunk heartbeat log line in `load/staging.py` (PITFALLS B4 — silence during a long `COPY` is indistinguishable from a hang), every DAG task's own logging (Airflow's task logger, not this seam, inside DAG files — but any `dataplat`/`csv_processor` code the DAG calls into uses this seam exclusively). Never `print()`, never a fresh `logging.getLogger(...)` call site.

### Poll with an explicit deadline, never a bare `sleep`
**Source:** `scripts/wait-for.sh` (shell side, `kubectl wait --timeout=`) and `tests/e2e/cluster/test_airflow_workloads.py` lines 152-196 (`_port_forwarded_postgres`, Python side, `time.monotonic()` deadline loop).
**Apply to:** `tests/e2e/slice/test_pod_kill_retry.py` (D-11), `tests/e2e/slice/test_idempotent_reupload.py`, `scripts/ingest-demo.*` (D-16), any DAG-side wait — this is explicitly flagged in PITFALLS.md (line ~2168) as a permanent-flakiness trap if violated.

### Live-cluster / live-Docker skip-with-reason, session-scoped, `autouse=True`
**Source:** `tests/e2e/cluster/conftest.py` lines 79-102 (`_require_cluster`) and `tests/integration/conftest.py` lines 51-78 (`_require_docker`) — same shape, two different preconditions.
**Apply to:** `tests/e2e/slice/conftest.py` (needs the cluster precondition; may also need `_require_docker`-style guards removed since this suite never spins up testcontainers).

### Pydantic `ConfigDict(extra="forbid", frozen=True)` for every config/contract model
**Source:** `packages/dataplat/src/dataplat/config/model.py` (every class).
**Apply to:** `config/model.py`'s new field (Group E), the new `AssignmentDocument` pydantic model (Security Domain V5 — validates the assignment JSON the pod reads from MinIO before any field is used to build SQL identifiers or object-store paths).

### Regex-over-source-files policy test skeleton
**Source:** `tests/policy/test_no_postgres_csv_parsing.py` (full file).
**Apply to:** `tests/policy/test_dag_thinness.py`, `tests/policy/test_dag_line_budget.py` — `REPO_ROOT` resolution, `EXCLUDED_DIRS`, `_candidate_files()`, one `violations: list[str]` accumulator, mandatory `test_the_scan_actually_reaches_files` anti-vacuity check.

---

## No Analog Found

Files with no close structural match in the codebase (planner should lean on RESEARCH.md's own already-verified patterns for these):

| File | Role | Data Flow | Reason |
|---|---|---|---|
| `airflow/dags/smoke_kubernetes_pod.py` | controller | batch | First Airflow DAG file in the repo — `airflow/dags/` held only `.gitkeep` before this phase. Use RESEARCH.md Pattern 5 (KPO settings, already verified against official docs this session) as the structural template; `cli.py` for docstring/error-handling convention only. |
| `airflow/dags/csv_ingest_customers.py` | controller | event-driven | Same reason. Use RESEARCH.md Patterns 1, 2, 4, 5 and the Code Examples section (`resolve_window`) — all independently verified this session. |
| `packages/dataplat/src/dataplat/load/publish/registry.py` | provider | transform | No `dict[str, Protocol]`-shaped registry exists anywhere in this codebase yet (confirmed by grep — `config/model.py` only names `PUBLISHER_REGISTRY` in a docstring). Use RESEARCH.md's Don't-Hand-Roll guidance: a plain module-level `dict`, no entry-points machinery. |
| `packages/dataplat/src/dataplat/sources/registry.py` | provider | transform | Same reason as above, for `SOURCE_REGISTRY`. |
| `tests/unit/test_dag_structure.py` | test | transform | First `DagBag`-based test in the repo. Use `tests/policy/test_no_postgres_csv_parsing.py` for the file-scanning/violation-accumulation style, and RESEARCH.md's Validation Architecture table for exactly which structural assertions are required. |
| `scripts/ingest-demo.*` | utility | event-driven | Not named in RESEARCH.md's Recommended Project Structure at all — implied by D-14/D-16 and explicitly left to "Claude's Discretion" in CONTEXT.md (exact filename, language, and implementation shape). `scripts/wait-for.sh` and `scripts/minio-credentials.sh` are the closest conventions to copy regardless of the final shape chosen. |
| `packages/dataplat/src/dataplat/models/*.py` (new `AssignmentDocument`) | model | transform | RESEARCH.md's Supporting-library table and Security Domain section both require this model but neither names its file path. `config/model.py` is the convention analog (`extra="forbid"`, `frozen=True`); the planner must decide whether it lives in a new `models/assignment.py`, inside `config/model.py` itself, or colocated with the `ingest` CLI subcommand's module. |

## Metadata

**Analog search scope:** `packages/dataplat/src/dataplat/**`, `packages/csv-processor/src/csv_processor/**`, `migrations/versions/**`, `tests/{unit,integration,policy,e2e}/**`, `tools/corpus/**`, `kubernetes/**`, `helm/values/**`, `configs/**`, `Makefile`, `scripts/**`, `airflow/dags/**` (empty except `.gitkeep`)
**Files scanned (read in full or via targeted offset/limit):** 38 existing files read; repo-wide `grep`/`find` sweeps for registry patterns, DagBag usage, and kind cluster DAG-mount wiring
**Pattern extraction date:** 2026-08-13
