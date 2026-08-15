# Phase 7: Observability, Metrics, Tracing & Lineage - Pattern Map

**Mapped:** 2026-08-15
**Files analyzed:** 34 (new + modified)
**Analogs found:** 30 exact/role-match / 34 (4 have no close analog — new mechanism categories for this codebase)

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `packages/dataplat/src/dataplat/observability/metrics.py` | utility (observability seam) | event-driven | `packages/dataplat/src/dataplat/observability/logging.py` | role-match (sibling seam, already real) |
| `packages/dataplat/src/dataplat/observability/tracing.py` | utility (observability seam) | event-driven | `packages/dataplat/src/dataplat/observability/logging.py` | role-match |
| `packages/dataplat/src/dataplat/observability/__init__.py` | config (package marker) | — | itself (unchanged) | exact |
| `packages/dataplat/pyproject.toml` | config | — | itself, `dependencies = [...]` block | exact |
| `packages/dataplat/src/dataplat/cli.py` | controller (CLI entrypoint) | request-response | itself, `main()` | exact |
| `packages/dataplat/src/dataplat/pipeline/run.py` | service (orchestration) | CRUD | itself, `run_ingest()` | exact |
| `packages/dataplat/src/dataplat/metadata/postgres.py` | service (repository) | CRUD | itself, `claim_ingestion_run`/`update_ingestion_run_status` | exact |
| `packages/dataplat/src/dataplat/config/model.py` | model | transform (validation) | itself, `BatchingConfig` + `DatasetConfig` model validators | exact |
| `packages/dataplat/src/dataplat/config/registry.py` | service (repository) | CRUD | itself, `_resolve_dataset_id` | exact |
| `configs/datasets/customers.yaml` | config | — | itself | exact |
| `airflow/dags/_common/tracing_kpo.py` (NEW) | middleware (operator subclass) | event-driven | `airflow/dags/_common/kpo.py` (package convention only) | role-match (no operator-subclass precedent exists — see No Analog Found) |
| `airflow/dags/csv_ingest_customers.py` | controller (DAG orchestration) | event-driven | itself | exact |
| `migrations/versions/0010_meta_datasets_freshness.py` (NEW) | migration | batch | `migrations/versions/0001…` (table+grant) — no pure add-column precedent | role-match |
| `migrations/versions/0011_grafana_reader_role.py` (NEW) | migration | batch | `migrations/versions/0008_grant_schema_usage_to_etl_app.py` | role-match (grant-only shape; `CREATE ROLE` itself has no precedent) |
| `migrations/versions/0012_meta_v_customers_lineage.py` (NEW) | migration | batch | `migrations/versions/0009_meta_schema_versions.py` (docstring/chaining) | role-match (`CREATE VIEW` itself has no precedent) |
| `helm/values/local/monitoring.yaml` (NEW) | config (Helm values) | — | `helm/values/local/vault.yaml` | role-match |
| `helm/values/ci/monitoring.yaml` (NEW) | config (Helm values) | — | `helm/values/ci/vault.yaml` | role-match |
| `helm/values/local/otel-collector.yaml` (NEW) | config (Helm values) | — | `helm/values/local/vault.yaml` | role-match |
| `helm/values/ci/otel-collector.yaml` (NEW) | config (Helm values) | — | `helm/values/ci/vault.yaml` | role-match |
| `helm/values/local/tempo.yaml` (NEW) | config (Helm values) | — | `helm/values/local/vault.yaml` | role-match |
| `helm/values/ci/tempo.yaml` (NEW) | config (Helm values) | — | `helm/values/ci/vault.yaml` | role-match |
| `helm/values/local/airflow.yaml` | config (Helm values) | — | itself | exact |
| `helm/values/ci/airflow.yaml` | config (Helm values) | — | itself | exact |
| `helm/versions.env` | config | — | itself | exact |
| `scripts/stages/85-monitoring.sh` (NEW) | utility (cluster bring-up stage) | batch | `scripts/stages/80-vault.sh` | exact |
| `scripts/vault-bootstrap.py` (`_ensure_grafana_secrets`) | service (script) | CRUD + file-I/O | itself, `_ensure_etl_secrets` | exact |
| `scripts/grafana-db-secret.sh` (NEW) | utility (script) | file-I/O | `scripts/airflow-metadata-secret.sh` | exact |
| `Makefile` (`image-airflow` target) | config (build script) | batch | itself, `image-csv-processor` | exact |
| `docker/airflow/Dockerfile` (NEW) | config (Dockerfile) | batch | `docker/csv-processor/Dockerfile` | role-match (simpler: extends a base image, no multi-stage uv build) |
| `tests/integration/test_lineage_view.py` (NEW) | test | CRUD | `tests/integration/test_config_registry.py` + `conftest.py` | exact |
| `tests/integration/test_freshness_query.py` (NEW) | test | CRUD | `tests/integration/test_config_registry.py` + `conftest.py` | exact |
| `tests/integration/test_metrics_otlp.py` (NEW) | test | event-driven | `tests/integration/test_config_registry.py` (structural only) | role-match (no OTLP-assertion precedent — see No Analog Found) |
| `tests/e2e/observability/{__init__,conftest}.py` (NEW) | test (fixtures) | — | `tests/e2e/vault/conftest.py` + `tests/e2e/slice/conftest.py` | exact |
| `tests/e2e/observability/test_trace_propagation.py` (NEW) | test (e2e, cluster) | event-driven | `tests/e2e/slice/test_pod_kill_retry.py` | exact |
| `tests/e2e/observability/test_alert_webhook_delivery.py` (NEW) | test (e2e, cluster) | event-driven | `tests/e2e/vault/test_rotation.py` | role-match (receiver-pod mechanism itself has no precedent — see No Analog Found) |

## Pattern Assignments

### `packages/dataplat/src/dataplat/observability/metrics.py` and `tracing.py` (utility, event-driven)

**Analog:** `packages/dataplat/src/dataplat/observability/logging.py` — the one seam in this same package that already went from concept to a real backend, so it is the structural template for "how a `dataplat.observability.*` module wires an external library while keeping one importable seam." The current no-op files themselves fix the exact public signature that must not change.

**Current no-op signature to preserve** (`metrics.py`, full file, lines 1-20):
```python
from __future__ import annotations


def increment(name: str, value: int = 1, **labels: str) -> None:
    """Record a counter increment. No-op until Phase 7 wires a real backend.
    ...
    """
```

**Current no-op signature to preserve** (`tracing.py`, full file, lines 1-25):
```python
from __future__ import annotations

import contextlib
from contextlib import AbstractContextManager


def start_span(name: str) -> AbstractContextManager[None]:  # noqa: ARG001 -- no-op today
    return contextlib.nullcontext()
```

**One-seam-one-configure() convention to copy** (`logging.py` lines 64-105 — `configure()` builds one global chain the caller invokes once per process; every submodule re-exports flat, callable names, never a class the caller must instantiate):
```python
def configure(*, in_cluster: bool, level: str = "INFO") -> None:
    ...
    structlog.configure(
        processors=[...],
        wrapper_class=structlog.make_filtering_bound_logger(level.upper()),
        logger_factory=structlog.PrintLoggerFactory(),
    )


bind_contextvars = structlog.contextvars.bind_contextvars
clear_contextvars = structlog.contextvars.clear_contextvars
get_logger = structlog.get_logger
```
Apply the same shape to `tracing.py`: a module-level `configure(*, otlp_endpoint: str | None, ...)` that builds one global `TracerProvider`/OTLP exporter (no-op/`NoOpTracerProvider` when unset — mirrors `logging.py`'s own "the same call site works unmodified... only `in_cluster` changes" framing from CONTEXT.md's own precedent), and `start_span()` becomes `trace.get_tracer(__name__).start_as_current_span(name)` instead of `contextlib.nullcontext()`. Same shape for `metrics.py`: a module-level `configure(*, otlp_endpoint: str | None)` building one global `MeterProvider`, and `increment()` becomes a lookup into a small counter-instrument cache keyed by `name`, bounded by D-04's `dataset`/`stage`/`status` label set only.

**Call sites that must not change** (`pipeline/engine.py` lines 22-28, 116-118, 148 — already threaded, Phase 3):
```python
from dataplat.observability import metrics, tracing
...
        metrics.increment("rows_rejected", len(rejected))
        metrics.increment("rows_kept", len(kept))
...
        with tracing.start_span("pipeline.run_streaming.chunk"):
```
D-04's bounded label set (`dataset`+`stage`+`status`) means these two `increment()` call sites need **new keyword arguments** added at the call site (`metrics.increment("rows_rejected", len(rejected), dataset=ctx.dataset_name, stage="ragged_row_guard", status="rejected")`), not just a backend swap — confirm during planning whether `ctx`/`PipelineContext` (imported at `pipeline/protocol.py`, referenced in `engine.py` line 35) already carries a `dataset_name` field reachable from `RaggedRowGuard.apply()`.

---

### `packages/dataplat/src/dataplat/cli.py` (controller, request-response) — trace-context extraction entrypoint

**Analog:** itself — the existing one-time-configuration call site.

**Exact call site to extend** (lines 109-111):
```python
    if not structlog.is_configured():
        configure(in_cluster=_log_json_enabled())
    log = get_logger()
```
RESEARCH.md Pattern 2's `_extract_incoming_trace_context()` (reads `os.environ["TRACEPARENT"]`, calls `opentelemetry.propagate.extract()` + `otel_context.attach()`) belongs immediately after this block, before `entry_points(group="dataplat.plugins")` loads at line 113 — same "once, near the top, before dispatching" placement `configure()` already uses, per this function's own docstring (lines 68-70: "Configures structured logging once, near the top, before dispatching... so every present and future subcommand inherits it").

---

### `packages/dataplat/src/dataplat/pipeline/run.py` (service, CRUD) — where `trace_id`/`span_id` get captured

**Analog:** itself — `run_ingest()`'s existing pattern for reading a pod-identity environment variable and threading it into the run-claim call.

**Exact pattern to mirror** (lines 252-259):
```python
    log = get_logger()
    start = time.monotonic()

    claimed = ctx.metadata.claim_ingestion_run(
        idempotency_key=ctx.run.idempotency_key,
        try_number=ctx.run.attempt,
        pod_name=os.environ.get("HOSTNAME", "unknown"),
    )
```
`trace_id`/`span_id` (already modeled on `RunContext`, `models/identity.py` lines 103-113, currently always `None`) should be captured the same way — either as new `claim_ingestion_run` kwargs (requiring `metadata/postgres.py` and the `MetadataRepository` protocol to widen), or via a follow-up `update_ingestion_run_status(run_id=run_id, status="RUNNING", trace_id=..., span_id=...)` call, since `trace_id`/`span_id` are already in `_INGESTION_RUN_UPDATABLE_FIELDS` (see next section) and need no schema change.

---

### `packages/dataplat/src/dataplat/metadata/postgres.py` (service/repository, CRUD)

**Analog:** itself — `trace_id`/`span_id` are already first-class, already-wired columns; this file needs zero new columns, only a caller passing real values instead of always `None`.

**Already-present update whitelist** (lines 33-61, `trace_id`/`span_id` at 47-48):
```python
_INGESTION_RUN_UPDATABLE_FIELDS = frozenset(
    {
        "schema_version_id",
        "dag_id",
        "dag_run_id",
        "task_id",
        "map_index",
        "try_number",
        ...
        "k8s_pod_name",
        "k8s_node_name",
        "trace_id",
        "span_id",
        "lease_expires_at",
        ...
    }
)
```

**Claim-time write pattern to extend** (lines 326-351 — `k8s_pod_name` set inside a single `UPDATE ... RETURNING`):
```python
    def claim_ingestion_run(
        self, *, idempotency_key: str, try_number: int, pod_name: str,
    ) -> tuple[int, str] | None:
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                UPDATE meta.ingestion_runs
                   SET status = 'RUNNING',
                       try_number = %(try_number)s,
                       k8s_pod_name = %(pod_name)s,
                       started_at = COALESCE(started_at, now()),
                       lease_expires_at = now() + interval '5 minutes'
                 WHERE idempotency_key = %(key)s
                   AND (...)
                RETURNING run_id, status
                """,
                {"try_number": try_number, "pod_name": pod_name, "key": idempotency_key},
            ).fetchone()
```
Adding `trace_id`/`span_id` params here (same shape as `pod_name`) is the more direct route than a second `update_ingestion_run_status` round trip — decide during planning which the codebase's own single-round-trip convention favors (this method already favors one write per state transition).

---

### `packages/dataplat/src/dataplat/config/model.py` (model, transform) — `FreshnessConfig`

**Analog:** itself — `BatchingConfig` (lines 119-132) is the simplest existing sibling model (one/few fields, `ConfigDict(extra="forbid", frozen=True)`, required-not-defaulted field), and `DatasetConfig`'s own cross-field `model_validator` (lines 391-413) is the pattern for the `warn_after <= fail_after` ordering check RESEARCH.md's Security Domain (V5) flags as needed.

**Simple-model template to copy** (`BatchingConfig`, lines 119-132):
```python
class BatchingConfig(BaseModel):
    """How many discovery units one ``discover_files`` call may hand to Dynamic Task Mapping.
    ...
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_units_per_run: int
```

**Cross-field validator template to copy** (lines 391-413):
```python
    @model_validator(mode="after")
    def _check_delimiter_does_not_collide_with_decimal_separator(self) -> DatasetConfig:
        """Reject a delimiter that is also the decimal separator (STACK.md §15)."""
        if (
            self.csv.delimiter is not None
            and self.normalization is not None
            and self.csv.delimiter == self.normalization.decimal_separator
        ):
            msg = (...)
            raise ValueError(msg)
        return self
```
`FreshnessConfig` is an **optional** nested field on `DatasetConfig` (`filename: FilenameMaskConfig | None = None` at line 385 is the exact precedent for an opt-in nested block — `freshness: FreshnessConfig | None = None` follows identically, matching D-08's "optional per dataset" requirement structurally, not via a sentinel).

---

### `packages/dataplat/src/dataplat/config/registry.py` (service, CRUD) — freshness columns ride `sync()`

**Analog:** itself — `_resolve_dataset_id`'s existing upsert is the exact widen point; RESEARCH.md's own Pattern 3 already drafted the diff.

**Exact upsert to widen** (lines 250-259):
```python
        row = cur.execute(
            """
            INSERT INTO meta.datasets (dataset_name) VALUES (%s)
            ON CONFLICT (dataset_name) DO UPDATE
                SET dataset_name = EXCLUDED.dataset_name
            RETURNING dataset_id
            """,
            (dataset_name,),
        ).fetchone()
        return int(_require_row(row, "meta.datasets insert returned no row")[0])
```
Becomes (per RESEARCH.md Pattern 3, same `INSERT ... ON CONFLICT DO UPDATE` shape, same `_require_row` narrowing):
```python
        row = cur.execute(
            """
            INSERT INTO meta.datasets (dataset_name, expected_frequency, freshness_warn_after, freshness_fail_after)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (dataset_name) DO UPDATE
                SET expected_frequency = EXCLUDED.expected_frequency,
                    freshness_warn_after = EXCLUDED.freshness_warn_after,
                    freshness_fail_after = EXCLUDED.freshness_fail_after
            RETURNING dataset_id
            """,
            (dataset_name, freq, warn_after, fail_after),
        ).fetchone()
```
Note `sync()`'s docstring/module docstring (lines 1-31) explains the row-lock-via-upsert reasoning (CR-03) — preserve it; widening the columns does not change the concurrency argument.

---

### `configs/datasets/customers.yaml` (config) — the `freshness:` block

**Analog:** itself. Follows the file's own established shape: top-level keys are grouped config blocks (`source:`, `deduplication:`, `load:`, `batching:`), each validated by a sibling `*Config` Pydantic model.

**Sibling block to mirror** (lines 38-39, `batching:` — smallest existing block, one required field):
```yaml
batching:
  max_units_per_run: 100
```
A new `freshness:` block sits at the same top level, e.g. `freshness: {expected_frequency: "1 day", warn_after: "2 hours", fail_after: "6 hours"}` — the file's own header comment (lines 1-21) documents that every top-level key here maps 1:1 to a `DatasetConfig` field, so this is purely additive, matching how `filename:`/`normalization:` are "deliberately absent... not an oversight" (lines 49-51) when a dataset has no need for them.

---

### `airflow/dags/_common/tracing_kpo.py` (NEW; middleware, event-driven)

**Analog:** `airflow/dags/_common/kpo.py` — same package, same "no business logic" constraint, same docstring convention (module purpose stated up front, referencing which policy test exempts it by name). No operator-subclass exists anywhere in this codebase yet (see No Analog Found) — RESEARCH.md's own Pattern 1 code block is the primary source, cross-checked against `kpo.py`'s conventions below.

**Docstring/scope convention to copy** (`kpo.py` lines 1-15):
```python
"""``common_kpo_kwargs`` -- the ONLY shared code between this phase's two DAGs.
...
nothing here parses CSV, validates a row, or writes to a database, so it does
not violate ORCH-02/ORCH-06's DAG-thinness rule. ``tests/policy/
test_dag_thinness.py`` exempts this file BY NAME from its import-based scan
for exactly this reason...
"""
```
`tracing_kpo.py` will need the same `test_dag_thinness.py` by-name exemption treatment if that policy test scans `_common/` by default — verify during planning.

**Env-var construction convention to match** (`kpo.py` lines 83-91 — `k8s.V1EnvVar(name=..., value=...)` list literal):
```python
        "env_vars": [
            k8s.V1EnvVar(name="DATAPLAT_DB_DSN", value="vault://etl/analytics-db#dsn"),
            ...
            *(extra_env_vars or []),
        ],
```
`build_pod_request_obj()`'s `pod.spec.containers[0].env.append(k8s.V1EnvVar(name="TRACEPARENT", value=...))` (RESEARCH.md Pattern 1) uses the identical `kubernetes.client.models.V1EnvVar` construction this file already imports as `k8s` — same import alias, same object shape, just appended post-hoc to an already-built pod instead of pre-built into `common_kpo_kwargs()`'s dict (Pitfall 2 explains why the two functions cannot merge).

---

### `airflow/dags/csv_ingest_customers.py` (controller, event-driven) — swap `ingest`'s operator class

**Analog:** itself.

**Exact site to change** (lines 137-144 — only the class name changes; `**common_kpo_kwargs(...)` stays untouched):
```python
    ingest = KubernetesPodOperator.partial(
        task_id="ingest",
        cmds=["dataplat"],
        retries=3,
        retry_exponential_backoff=True,
        max_active_tis_per_dag=5,
        **common_kpo_kwargs(resources=_INGEST_RESOURCES, extra_env_vars=_INGEST_EXTRA_ENV_VARS),
    ).expand(arguments=build_ingest_args(discover.output))
```
Becomes `TracingKubernetesPodOperator.partial(...)` (import added at the top alongside the existing `from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator` at line 49). Per D-12, `discover` (lines 123-130) stays a plain `KubernetesPodOperator` — only `ingest` becomes the trace root.

---

### `migrations/versions/0010_meta_datasets_freshness.py` (NEW; migration, batch)

**Analog:** `migrations/versions/0001_meta_datasets_config_versions.py` for the docstring/revision-header convention and grant-statement style; no existing migration does a pure `add_column`-only change (see No Analog Found), so the DDL body itself comes from RESEARCH.md Pattern 3 directly.

**Docstring/header convention to copy** (0001 lines 1-30):
```python
"""meta.datasets and meta.config_versions — the dataset registry and config history.
...
Revision ID: 0001
Revises:
Create Date: 2026-08-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None
```
Revision chain: `revision = "0010"`, `down_revision = "0009"`.

**DDL body** (RESEARCH.md Pattern 3, already verified against real column-naming conventions like `hash_version`):
```python
def upgrade() -> None:
    op.add_column("datasets", sa.Column("expected_frequency", sa.Interval(), nullable=True), schema="meta")
    op.add_column("datasets", sa.Column("freshness_warn_after", sa.Interval(), nullable=True), schema="meta")
    op.add_column("datasets", sa.Column("freshness_fail_after", sa.Interval(), nullable=True), schema="meta")
```
`downgrade()` mirrors with three `op.drop_column(...)` calls, matching 0006's reverse-of-upgrade convention (see below).

---

### `migrations/versions/0011_grafana_reader_role.py` (NEW; migration, batch)

**Analog:** `migrations/versions/0008_grant_schema_usage_to_etl_app.py` — the one existing migration that is pure `op.execute()` GRANT statements with no `create_table`/`add_column`, i.e. the closest shape to a role-creation-and-grant migration (still no `CREATE ROLE` precedent — see No Analog Found).

**Exact shape to mirror** (0008, full `upgrade()`/`downgrade()`, lines 40-49):
```python
def upgrade() -> None:
    """Grant `etl_app` USAGE on `meta` and `normalized` (table grants already exist, were inert)."""
    op.execute("GRANT USAGE ON SCHEMA meta TO etl_app")
    op.execute("GRANT USAGE ON SCHEMA normalized TO etl_app")


def downgrade() -> None:
    """Revoke the USAGE grants this migration added. Never drops the schemas."""
    op.execute("REVOKE USAGE ON SCHEMA normalized FROM etl_app")
    op.execute("REVOKE USAGE ON SCHEMA meta FROM etl_app")
```
`0011` follows identically: `op.execute("CREATE ROLE grafana_reader LOGIN")` + `GRANT USAGE ON SCHEMA meta, normalized TO grafana_reader` (per RESEARCH.md Pitfall 4 — `postInitApplicationSQL` cannot add a role to the already-running cluster, an Alembic migration is the only live-cluster-safe mechanism). Password is set separately, out-of-band, by `scripts/vault-bootstrap.py`'s `_ensure_grafana_secrets()` via `ALTER ROLE ... WITH PASSWORD` (mirrors `_ensure_etl_secrets`'s existing `etl_app` password rotation) — the migration itself must never embed a password literal (§81).

---

### `migrations/versions/0012_meta_v_customers_lineage.py` (NEW; migration, batch)

**Analog:** `migrations/versions/0009_meta_schema_versions.py` for the "this migration also closes/joins prior work" docstring convention (0009 closes 0004's deferred FK; 0012 joins columns 0002/0004/0005/0009 already created). The `CREATE VIEW` DDL itself has no precedent in this codebase (see No Analog Found) — use RESEARCH.md's Code Examples SQL directly, via `op.execute()` (Alembic has no `create_view` helper, matching 0008's pure-`op.execute()` style for non-`op.*`-helper DDL).

**Exact SQL to embed** (RESEARCH.md Code Examples, verified against migrations 0002/0003/0004/0005/0009's real column names):
```python
def upgrade() -> None:
    op.execute("""
        CREATE VIEW meta.v_customers_lineage AS
        SELECT
            c.id AS customer_row_id, c.customer_id, c._source_row_number,
            c._record_hash, c._record_hash_version, c._ingested_at,
            f.file_id, f.object_uri, f.content_sha256, f.hash_version AS file_hash_version,
            f.filename, f.business_date,
            b.batch_id, b.batch_key,
            r.run_id, r.idempotency_key, r.dag_id, r.dag_run_id, r.task_id, r.map_index,
            r.try_number, r.k8s_namespace, r.k8s_pod_name, r.trace_id, r.span_id,
            r.processor_version, r.processor_image_digest,
            r.started_at AS run_started_at, r.finished_at AS run_finished_at,
            cv.version AS config_version, cv.config_hash,
            sv.version AS schema_version, sv.schema_hash
        FROM normalized.customers c
        JOIN meta.ingestion_runs r ON r.run_id = c._run_id
        JOIN meta.files f ON f.file_id = c._file_id
        JOIN meta.batches b ON b.batch_id = c._batch_id
        JOIN meta.config_versions cv ON cv.config_version_id = r.config_version_id
        LEFT JOIN meta.schema_versions sv ON sv.schema_version_id = r.schema_version_id
    """)
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO etl_app")
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO grafana_reader")


def downgrade() -> None:
    op.execute("DROP VIEW meta.v_customers_lineage")
```
Must run after 0011 (revision chain `down_revision = "0011"`) since it grants to `grafana_reader`, which 0011 creates. `error_detail` is deliberately excluded from the SELECT list per RESEARCH.md's Security Domain finding (Information Disclosure risk) — do not add it without the same redaction discipline `logging.py`'s `_redact()` already applies elsewhere.

---

### Helm values: `monitoring.yaml` / `otel-collector.yaml` / `tempo.yaml` (NEW, config) × `{local,ci}`

**Analog:** `helm/values/local/vault.yaml` + `helm/values/ci/vault.yaml` — the most recent precedent for "a brand-new infrastructure chart this project didn't have before," including the local/ci profile-split header-comment convention and the persistent-PVC pattern D-17 requires.

**Profile-split header convention to copy** (`ci/vault.yaml` lines 1-4):
```yaml
# D-06: this file and helm/values/local/vault.yaml diverge on EXACTLY one
# permitted axis -- resource sizing -- because the CI profile shares a
# 4 CPU / 16 GB GitHub-hosted runner with every other pod the CI job
# renders. Every other key below is IDENTICAL in shape to the local profile.
```
For monitoring/otel-collector/tempo, the CI file will diverge on a **second**, already-anticipated axis: `tests/policy/test_values_profiles.py`'s `PERMITTED_AXES` (see Shared Patterns below) already names "monitoring enablement" as permitted — `_is_monitoring_enablement` matches any path containing `metrics` or `monitoring` segments. State this explicitly in the new CI values file's header comment, mirroring vault.yaml's own argued-reversal style.

**Persistence pattern to copy** (`local/vault.yaml` lines 46-62 — PVC-backed, sized per-service, explicit `ha.enabled: false` restated even when it's the chart default):
```yaml
  dataStorage:
    enabled: true
    size: 1Gi

  auditStorage:
    enabled: true
    size: 1Gi

  ha:
    enabled: false
```
Apply D-17/D-18 the same way: Prometheus/Tempo PVCs `enabled: true` with a size comment tying back to the retention target (`~15d`/`~7d`), not left to chart defaults.

**Resource-sizing local/ci split to copy** (`local/vault.yaml` lines 72-78 vs `ci/vault.yaml` lines 40-49 — identical key shape, smaller numbers in CI):
```yaml
  resources:
    requests:
      cpu: 250m
      memory: 512Mi
    limits:
      cpu: "1"
      memory: 1Gi
```
Per D-16 (monitoring stack template/lint-only in CI, never actually deployed there), the CI values files exist **only** so `helm template` + `kubeconform` (the existing five-chart CI "manifests" job, `.github/workflows/ci.yml` — search string "helm template both values profiles for all five pinned charts") can structurally validate them — no `scripts/stages/*.sh` entry installs them against a live CI cluster.

---

### `helm/values/local/airflow.yaml` and `helm/values/ci/airflow.yaml` (config) — OTel wiring on the existing chart

**Analog:** itself — two exact, already-flagged edit sites.

**Image reference to repoint** (`local/airflow.yaml` lines 38-40):
```yaml
airflowVersion: "3.3.0"
defaultAirflowRepository: apache/airflow
defaultAirflowTag: "3.3.0"
```
Per RESEARCH.md Pitfall 1/Open Question 1: repoint `defaultAirflowRepository`/`defaultAirflowTag` at the new `docker/airflow/Dockerfile`-built, locally-pushed image (`localhost:5001/airflow:<GIT_SHA>`, same registry pattern `image-csv-processor` already uses), in **both** profiles for consistency — but only set `[traces] otel_on: True`/`OTEL_EXPORTER_OTLP_ENDPOINT` in the **local** profile.

**Load-bearing pre-existing comment marking this exact edit site** (`local/airflow.yaml` lines 114-118 — written during Phase 5, explicitly deferring to this phase):
```yaml
# Phase 7 owns metrics (kube-prometheus-stack is not deployed until then, and
# enabling the OTel collector would disable statsd anyway) — the third D-06
# axis, disabled in both profiles for now.
statsd:
  enabled: false
```
Per D-02, `statsd.enabled` flips to `true` in **both** profiles now (Airflow's own metrics stay on StatsD regardless of environment — this is not a local/ci divergence axis), while `otelCollector.metricsEnabled` must stay `false`/unset everywhere (the chart's own StatsD-XOR-OTel mutual exclusion, `.claude/CLAUDE.md` §H) — only `otel_on`'s **tracing** config block is local-only.

---

### `helm/versions.env` (config)

**Analog:** itself — the single append point for every new chart version pin.

**Exact shape to extend** (full file, lines 14-24):
```
INGRESS_NGINX_CHART_VERSION=4.15.1
CNPG_OPERATOR_CHART_VERSION=0.29.0
CNPG_CLUSTER_CHART_VERSION=0.8.1
MINIO_CHART_VERSION=5.4.0
MINIO_IMAGE_TAG=RELEASE.2026-08-04T00-00-00Z
AIRFLOW_CHART_VERSION=1.22.0
AIRFLOW_IMAGE_TAG=3.3.0
VAULT_CHART_VERSION=0.34.0
```
Add `KUBE_PROMETHEUS_STACK_CHART_VERSION=88.2.0`, `OTEL_COLLECTOR_CHART_VERSION=0.169.0`, `TEMPO_CHART_VERSION=<resolve via `helm search repo tempo` per RESEARCH.md Assumption A4>` — `KEY=value` only, no quoting, no inline comments (this file's own header rule, enforced by `tests/policy/test_pinned_tool_versions_agree.py`).

---

### `scripts/stages/85-monitoring.sh` (NEW; utility, batch)

**Analog:** `scripts/stages/80-vault.sh` — the most recent "new component stage" precedent, including the same `helm_install`/`wait_for_pod_running` sourcing convention and numbered-after-dependency placement logic.

**Exact shape to copy** (full file, lines 1-46):
```bash
#!/usr/bin/env bash
#
# ...component-specific header comment explaining ordering and any
# readiness-wait deadlock avoidance...

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"
# shellcheck source=/dev/null
source "${repo_root}/scripts/helm-install.sh"
# shellcheck source=/dev/null
source "${repo_root}/scripts/wait-for.sh"

helm_bin="${repo_root}/tools/bin/helm"

"${helm_bin}" repo add hashicorp https://helm.releases.hashicorp.com >/dev/null 2>&1 || true
"${helm_bin}" repo update hashicorp >/dev/null

helm_install vault hashicorp/vault vault VAULT_CHART_VERSION vault hookOnly

wait_for_pod_running vault vault-0
```
Numbered `85` (after `80-vault.sh`) per D-16/Pattern 5's finding that Grafana's Vault-backed secret depends on `scripts/vault-bootstrap.py` having already run — though bootstrap itself is a separate manual step (`make vault-bootstrap`) like Vault's own, so ordering here is about the chart install only, not the secret material.

---

### `scripts/vault-bootstrap.py` (`_ensure_grafana_secrets`, NEW function) — service, CRUD + file-I/O

**Analog:** itself — `_ensure_etl_secrets` (lines 565-648) is the exact precedent RESEARCH.md's Pattern 5 names directly: read-then-skip-or-write against Vault KV, `kubectl exec` into the CNPG primary for a live `ALTER ROLE`, never touch a value already handed to a caller.

**Exact pattern to extend** (lines 599-631 — guard read, `secrets.token_hex` password, `kubectl exec` ALTER ROLE, write DSN/creds to Vault KV):
```python
    try:
        client.secrets.kv.v2.read_secret_version(mount_point="etl", path="analytics-db")
        print("secret etl/analytics-db: already present")
    except hvac.exceptions.InvalidPath:
        primary_pod = _kubectl_cluster_primary_pod(
            kubectl_context, namespace=_DATA_NAMESPACE, cluster=_ANALYTICS_CLUSTER,
        )
        password = secrets.token_hex(32)
        _kubectl_exec_psql(
            kubectl_context, namespace=_DATA_NAMESPACE, pod=primary_pod,
            database=_ANALYTICS_DATABASE,
            sql=f"ALTER ROLE {_ANALYTICS_APP_ROLE} WITH PASSWORD '{password}';",
        )
        encoded_password = quote(password, safe="")
        dsn = (
            f"postgresql://{_ANALYTICS_APP_ROLE}:{encoded_password}"
            f"@{_ANALYTICS_CLUSTER}-rw.{_DATA_NAMESPACE}:5432/{_ANALYTICS_DATABASE}"
        )
        client.secrets.kv.v2.create_or_update_secret(
            mount_point="etl", path="analytics-db", secret={"dsn": dsn},
        )
        print("secret etl/analytics-db: created")
```
`_ensure_grafana_secrets` follows identically for `grafana_reader`'s password AND (new step, RESEARCH.md Pattern 5's "one genuinely new step") a generic webhook URL read from wherever the operator provisions it (e.g. prompted once, or read from an existing `.secrets/` local file per this project's own `.secrets/` directory convention) written to `grafana/alert-webhook-url`. Call `_ensure_grafana_secrets(client, kubectl_context)` from `bootstrap()` (lines 799-800) alongside the existing two calls:
```python
    _ensure_etl_secrets(client, kubectl_context)
    _ensure_airflow_secrets(client, kubectl_context)
```

**Subprocess/stdin-only credential pattern to reuse** (`_kubectl_exec_psql`, lines 504-562 — SQL via `input=`, never argv, `-v ON_ERROR_STOP=1`): reuse this exact helper unmodified for the `ALTER ROLE grafana_reader ...` statement — no new helper needed.

---

### `scripts/grafana-db-secret.sh` (NEW; utility, file-I/O)

**Analog:** `scripts/airflow-metadata-secret.sh` — the one existing script whose entire purpose is "materialize a Kubernetes Secret from a value that lives somewhere else," which is exactly Pattern 5's "the raw credential still never appears in git... only the Secret's name does" requirement.

**`_apply_secret` helper to copy verbatim** (lines 88-106 — builds a Secret manifest with `stringData` in memory, pipes to `kubectl apply -f -` on stdin, values never touch argv):
```bash
_apply_secret() {
  local namespace="$1" name="$2"
  shift 2
  {
    printf 'apiVersion: v1\n'
    printf 'kind: Secret\n'
    printf 'metadata:\n'
    printf '  name: %s\n' "${name}"
    printf '  namespace: %s\n' "${namespace}"
    printf 'type: Opaque\n'
    printf 'stringData:\n'
    local kv key value
    for kv in "$@"; do
      key="${kv%%=*}"
      value="${kv#*=}"
      printf '  %s: %s\n' "${key}" "${value}"
    done
  } | _kubectl apply -f - >/dev/null
}
```

**`cmd_ensure`-style idempotent orchestration to copy** (lines 135-174 — read source values, derive target, `_apply_secret`, one `_secret_exists` guard per credential that must never silently rotate):
```bash
cmd_ensure() {
  local username password host port dbname
  username="$(_read_source_key username)"
  ...
  echo "==> deriving Secret ${TARGET_NAMESPACE}/${METADATA_SECRET} (key: connection) from ..."
  _apply_secret "${TARGET_NAMESPACE}" "${METADATA_SECRET}" "connection=${connection}"
}
```
`grafana-db-secret.sh` differs from this analog in **source**: rather than reading a Kubernetes Secret cross-namespace (this file's whole reason for existing), it reads a value already written to Vault KV by `_ensure_grafana_secrets` (via `vault kv get` or `hvac`, since the value now lives in Vault, not a Secret) and materializes a `kubernetes.io/basic-auth`-shaped Secret (`username`+`password` keys) that Grafana's `envFromSecret` values key references by name — the `_apply_secret`/`_kubectl` scaffolding is otherwise identical.

---

### `Makefile` (`image-airflow` target, config)

**Analog:** itself — `image-csv-processor` (lines 222-262).

**Exact target shape to copy** (lines 222-245, GIT_SHA-pinned build+tag+push, never `:latest`):
```makefile
GIT_SHA := $(shell git rev-parse --short HEAD)

image-csv-processor:            ## INFRA-08/U1: build, tag, push to the local registry, register the image for the DAG
	docker build \
	  --build-arg GIT_SHA=$$(git rev-parse --short HEAD) \
	  -t csv-processor:$$(git rev-parse --short HEAD) \
	  -f docker/csv-processor/Dockerfile .
	docker tag csv-processor:$(GIT_SHA) localhost:5001/csv-processor:$(GIT_SHA)
	docker push localhost:5001/csv-processor:$(GIT_SHA)
```
`image-airflow` mirrors this exactly, substituting `docker/airflow/Dockerfile` and image name `airflow`. `tests/policy/test_no_latest_image_tag.py` reads this recipe body directly (per the comment at lines 220-221) — a new target needs the same two inline `git rev-parse --short HEAD` calls (build-arg + `-t`), not a shortcut, to keep passing that policy test.

---

### `docker/airflow/Dockerfile` (NEW; config, batch)

**Analog:** `docker/csv-processor/Dockerfile` for repo-wide Dockerfile conventions (non-root `USER 1000`, OCI labels, never `:latest`) — but the new file is structurally **simpler**: no multi-stage `uv` build, since it only layers one `pip install` onto an already-built base image.

**Conventions to carry over** (csv-processor Dockerfile lines 113-121 — OCI labels tying the image back to the exact git commit, numeric non-root user):
```dockerfile
LABEL org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.source="https://github.com/KonuTech/airflow-platform" \
      org.opencontainers.image.version="${GIT_SHA}"

USER 1000
```

**New file's actual shape** (per RESEARCH.md Standard Stack "Installation" and Pitfall 1 — `docs/README.md` already documents `docker/airflow/`'s intended purpose: "Installs providers under Airflow's own constraints file — deliberately outside the uv workspace"):
```dockerfile
ARG GIT_SHA=unknown
FROM apache/airflow:3.3.0-python3.12

RUN pip install --no-cache-dir "apache-airflow[otel]==3.3.0" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.12.txt"

LABEL org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.source="https://github.com/KonuTech/airflow-platform" \
      org.opencontainers.image.version="${GIT_SHA}"
```
No `WORKDIR`/`ENTRYPOINT` override needed — the base image already sets both correctly; this file only adds one dependency layer + labels, unlike csv-processor's from-scratch multi-stage build.

---

### `packages/dataplat/pyproject.toml` (config)

**Analog:** itself — `[project] dependencies = [...]` (lines 10-18).

```toml
dependencies = [
  "PyYAML>=6",
  "psycopg[binary,pool]>=3.3.4,<4",
  "boto3>=1.43.68,<2",
  "pydantic>=2.13,<3",
  "structlog>=26,<27",
  "click>=8.4,<9",
  "hvac>=2.4,<3",
]
```
Add `"opentelemetry-sdk>=1.44,<2"`, `"opentelemetry-exporter-otlp-proto-http>=1.44,<2"` to this list — same `>=X,<Y` pinning shape every existing entry uses (never a bare unpinned name, never exact-pin `==`).

---

### Integration tests: `test_lineage_view.py`, `test_freshness_query.py` (test, CRUD)

**Analog:** `tests/integration/test_config_registry.py` + `tests/integration/conftest.py` — the established testcontainers-Postgres-against-real-migrations pattern.

**Fixture wiring to copy** (`test_config_registry.py` lines 36-44 — module-scoped fixture built on the shared `migrated_dsn` session fixture):
```python
@pytest.fixture(scope="module")
def registry(migrated_dsn: str) -> Iterator[ConfigRegistry]:
    with create_pool(migrated_dsn) as pool:
        yield ConfigRegistry(pool)
```

**Direct-SQL assertion helper convention to copy** (lines 47-59 — a plain `psycopg.connect` + `.execute()` + `.fetchall()` helper, not going through the library's own repository classes, when the test's whole point is verifying raw DB state):
```python
def _config_version_rows(dsn: str, dataset_name: str) -> list[tuple[int, int, object]]:
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            """
            SELECT cv.config_version_id, cv.version, cv.valid_to
              FROM meta.config_versions cv
              JOIN meta.datasets d ON d.dataset_id = cv.dataset_id
             WHERE d.dataset_name = %s
             ORDER BY cv.version
            """,
            (dataset_name,),
        ).fetchall()
    return [(row[0], row[1], row[2]) for row in rows]
```
`test_lineage_view.py` seeds one real ingested row (via `registry.sync()` + a direct `INSERT` chain matching `meta.v_customers_lineage`'s join tables, or by reusing `tests/integration/test_run_ingest.py`'s existing seed helpers if present — check during planning), then asserts `SELECT * FROM meta.v_customers_lineage WHERE customer_id = %s` returns every OBS-07-named column non-null. `test_freshness_query.py` seeds `meta.datasets` rows with/without `expected_frequency` set and asserts RESEARCH.md's Code Examples freshness `HAVING` query's result set excludes the `NULL` row and includes the stale one — pure SQL-only, no library code under test, matching the file's own "integration (SQL-only, testcontainers Postgres)" classification in RESEARCH.md's Test Map.

**Underlying `migrated_dsn` fixture** (`tests/integration/conftest.py`, `postgres_dsn` at line 81, `run_migrations` at line 109, `migrated_dsn` at line 134) — reuse unchanged; migrations 0010/0011/0012 run automatically through this fixture's existing `alembic upgrade head` invocation, no test-side change needed.

---

### `tests/e2e/observability/test_trace_propagation.py` (test, e2e/cluster) — OBS-10

**Analog:** `tests/e2e/slice/test_pod_kill_retry.py` — the established "real pod on the real cluster, `pytest.mark.cluster`, assert against actual pod state" shape.

**Marker + real-pod-action convention to copy** (lines 56, 232):
```python
pytestmark = pytest.mark.cluster
...
        delete = kubectl("-n", "etl", "delete", "pod", pod_name, "--wait=false")
```
`test_trace_propagation.py` triggers a real `csv_ingest_customers` DAG run (or invokes the `ingest` task directly), then uses the same `kubectl` fixture to `kubectl get pod <name> -o json` and assert a `TRACEPARENT` env var is present in `spec.containers[0].env` with a well-formed W3C value — matching D-19's "proof over prose" bar and RESEARCH.md's Phase Requirements → Test Map entry (`pytest tests/e2e/observability/test_trace_propagation.py -m cluster -x`).

**Polling-not-sleeping convention** (`tests/e2e/slice/conftest.py`, `poll_ingestion_run` at line 619 — Phase 4 D-11's established idiom, poll metadata rather than `time.sleep`): reuse this fixture (or its exact shape) to wait for the triggered run's `meta.ingestion_runs.trace_id` column to become non-null, then assert it matches the pod's injected `TRACEPARENT`.

---

### `tests/e2e/observability/test_alert_webhook_delivery.py` (test, e2e/cluster) — D-20

**Analog:** `tests/e2e/vault/test_rotation.py` — the closest existing "force a real state change, wait, assert an observable side effect, restore in `finally`" shape (D-03's "proof over prose" precedent D-20 explicitly extends).

**Force-condition / assert-observable-effect / restore-in-finally convention to copy** (lines 124-193, full test body):
```python
def test_rotating_minio_default_is_observed_with_no_restart(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    vault_root_client: hvac.Client,
) -> None:
    before_secret = vault_root_client.secrets.kv.v2.read_secret_version(...)
    ...
    try:
        vault_root_client.secrets.kv.v2.create_or_update_secret(...)  # force the condition
        after_read = _read_minio_default(kubectl)                     # observe the effect
        assert after_sanitized != before_sanitized, (...)
    finally:
        vault_root_client.secrets.kv.v2.create_or_update_secret(       # restore, unconditionally
            mount_point=_VAULT_MOUNT, path=_VAULT_PATH, secret={"conn_uri": original_conn_uri},
        )
```
`test_alert_webhook_delivery.py` follows the same shape: force a real freshness breach (e.g. `UPDATE meta.datasets SET expected_frequency = interval '1 second' WHERE ...` against a live-cluster Postgres, or insert a stale `meta.files` row), wait for Grafana Alerting to evaluate and fire (poll, not sleep — same `poll_ingestion_run`-style idiom), assert an HTTP POST arrived at the in-cluster receiver, and restore the original `meta.datasets` row in `finally`. The receiver-pod mechanism itself is genuinely new (see No Analog Found) — RESEARCH.md's own Wave-0-gap note flags it for early prototyping.

**Fixture conventions to reuse** (`tests/e2e/vault/conftest.py` — `kubectl`, lines 95-117; `vault_root_client`, lines 201-214; both session-scoped):
```python
@pytest.fixture(scope="session")
def kubectl(kubectl_context: str) -> Callable[..., subprocess.CompletedProcess[str]]:
    ...
    def _run(*args: str, timeout: int = 30) -> subprocess.CompletedProcess[str]:
        ...
    return _run
```
`tests/e2e/observability/conftest.py` should import/re-declare the same `kubectl`/`kubectl_context` shape (or share it via a higher-level conftest) rather than reinvent subprocess plumbing, plus a new fixture that deploys/tears-down the minimal receiver Pod+Service RESEARCH.md's Wave 0 Gaps section calls for.

---

## Shared Patterns

### One-seam, module-level `configure()` for observability backends
**Source:** `packages/dataplat/src/dataplat/observability/logging.py` lines 64-105
**Apply to:** `metrics.py`, `tracing.py`
Every `dataplat.observability.*` submodule exposes flat callables (`configure()`, `get_logger`/`start_span`/`increment`) — never a class instance the caller must construct and thread through every layer. `cli.py`'s `main()` (lines 109-111) is the one process-wide call site that invokes `configure()` once; `tracing.configure()`/`metrics.configure()` join it at the same call site.

### Env-var-driven pod identity, read once at task-run time
**Source:** `packages/dataplat/src/dataplat/pipeline/run.py` line 258 (`pod_name=os.environ.get("HOSTNAME", "unknown")`)
**Apply to:** trace-context extraction in `cli.py`/`run.py` (Pattern 2)
This codebase already has the exact idiom for "read a Kubernetes-pod-supplied environment fact once, thread it into the run record" — `TRACEPARENT` extraction is the same idiom, not a new one.

### Postgres `INSERT ... ON CONFLICT DO UPDATE ... RETURNING` for idempotent upserts
**Source:** `packages/dataplat/src/dataplat/config/registry.py` lines 250-259 (`_resolve_dataset_id`)
**Apply to:** freshness-column widening of the same method; any future single-row upsert
Never `SELECT ... FOR UPDATE` then a plain `INSERT` (documented race in this file's own module docstring) — the atomic upsert is this codebase's established concurrency-safety idiom.

### Alembic migration docstring convention: state what closes/joins prior work
**Source:** `migrations/versions/0008_grant_schema_usage_to_etl_app.py`, `0009_meta_schema_versions.py`
**Apply to:** `0010`, `0011`, `0012`
Every migration's module docstring names (a) what it creates, (b) which prior migration's deferred/incomplete work it closes (0009 explicitly closes 0004's deferred FK), and (c) the Revision ID/Revises/Create Date footer. `0012`'s lineage view is exactly this shape: it joins tables 0002/0003/0004/0005/0009 already created, closing no FK but completing OBS-07 the way 0009 completed the FK chain.

### Local/CI Helm values profile split — permitted-divergence axes
**Source:** `helm/values/local/vault.yaml` + `helm/values/ci/vault.yaml`; enforced by `tests/policy/test_values_profiles.py`'s `PERMITTED_AXES`
**Apply to:** every new `helm/values/{local,ci}/{monitoring,otel-collector,tempo}.yaml` pair
Two profiles must be **structurally identical** except along a small, explicitly-named, tested set of axes (replica counts, resource sizing, monitoring enablement, executor). `_is_monitoring_enablement` (the test's own helper) already anticipates this phase's charts by matching any `metrics`/`monitoring` path segment — confirm the new files' divergences fall inside an already-permitted axis, or extend `PERMITTED_AXES` (with the mandatory non-empty argument the test enforces) if a genuinely new axis is needed.

### `secrets.token_hex` + `kubectl exec ... ALTER ROLE` + Vault KV write, never a K8s Secret directly
**Source:** `scripts/vault-bootstrap.py` `_ensure_etl_secrets` (lines 565-648), `_kubectl_exec_psql` (lines 504-562)
**Apply to:** `_ensure_grafana_secrets` (new), and by extension `migrations/versions/0011`'s `grafana_reader` role
Password generation, live-cluster mutation and durable storage are three separate, composable steps in this codebase's established Vault-bootstrap idiom — never inlined into a migration, never passed via argv (`_kubectl_exec_psql` uses `input=`, not an argv element, specifically to keep the SQL — and thus the password inside its string literal — out of `ps`/`/proc/<pid>/cmdline`).

### Real-cluster, poll-not-sleep, restore-in-`finally` E2E proof
**Source:** `tests/e2e/vault/test_rotation.py` (full file); `tests/e2e/slice/test_pod_kill_retry.py` line 232; `tests/e2e/slice/conftest.py`'s `poll_ingestion_run` (line 619)
**Apply to:** `test_trace_propagation.py`, `test_alert_webhook_delivery.py`
D-19/D-20's "proof over prose" bar has one consistent shape across every phase that has needed it so far: `pytest.mark.cluster`, act on the real system (`kubectl delete pod`, a Vault KV write, a Postgres `UPDATE`), poll metadata/state for the expected reaction rather than sleeping a fixed duration, assert on the real observed value, and — where the test itself mutates shared state — restore it in a `finally` block with its own closing assertion.

## No Analog Found

Files/mechanisms with no close match in the codebase (planner should lean on RESEARCH.md's own drafted patterns instead):

| File / Mechanism | Role | Data Flow | Reason |
|---|---|---|---|
| `airflow/dags/_common/tracing_kpo.py` — the `KubernetesPodOperator` subclass mechanism itself (`build_pod_request_obj()` override) | middleware | event-driven | No custom Airflow operator subclass exists anywhere in this codebase today — every DAG uses provider operators directly. RESEARCH.md Pattern 1's code block is the primary source; `kpo.py` only supplies the surrounding package/docstring/env-var-object conventions, not the subclassing mechanism. |
| `migrations/versions/0010…` — pure `op.add_column`-only migration | migration | batch | Every existing migration either creates a table (0001/0002.../0009) or is grant-only (0008); none adds columns to an already-existing table with no other change. Low risk: `op.add_column` is a well-documented Alembic primitive, RESEARCH.md's Pattern 3 already drafted the exact three-column body. |
| `migrations/versions/0011…` — `CREATE ROLE` | migration | batch | No migration in this codebase creates a PostgreSQL role; every existing role (`etl_app`, `analytics_owner`) was created via CNPG's `initdb.postInitApplicationSQL` (chart-bootstrap-time), not Alembic. 0011 is the first role-creation migration, required specifically because the analytical cluster is already running (RESEARCH.md Pitfall 4). |
| `migrations/versions/0012…` — `CREATE VIEW` | migration | batch | No view exists anywhere in this schema today; every prior migration is `create_table`/`add_column`/grants. Alembic has no `op.create_view` helper — use `op.execute()`, matching 0008's precedent for non-helper DDL. |
| `helm/values/*/monitoring.yaml`, `otel-collector.yaml`, `tempo.yaml` — chart content itself (Grafana `additionalDataSources`, `alerting: {}` provisioning, OTel Collector receivers/exporters) | config | — | No chart in this repo today wires a Postgres datasource, alerting-as-code, or an OTLP receiver/exporter pipeline. `vault.yaml` supplies only the structural conventions (profile split, persistence, resource sizing) — the actual keys are new, sourced from RESEARCH.md's Pattern 4 (MEDIUM confidence — flagged there for a `helm show values` spot-check before use). |
| `tests/integration/test_metrics_otlp.py` — asserting a real value reached an OTLP collector | test | event-driven | No existing test in this codebase asserts against telemetry-pipeline delivery (only against Postgres state or live cluster/pod state). Needs either an in-process OTLP receiver double or a real OTel Collector reachable from the test process — mechanism to be decided during planning; `test_config_registry.py` only supplies the surrounding testcontainers-fixture shape. |
| `tests/e2e/observability/test_alert_webhook_delivery.py` — the in-cluster webhook-receiver Pod+Service | test (fixture) | event-driven | RESEARCH.md's own Wave 0 Gaps section flags this as "the most novel testing mechanism this phase introduces": Grafana's Alerting engine runs in-cluster and cannot reach a pytest-process-local listener, so the test needs a throwaway receiver Pod+Service deployed for the test's duration, with assertions via `kubectl exec`/log inspection — no existing test in this codebase deploys an ad-hoc workload into the cluster as part of its own setup (closest is `tests/e2e/cluster/*` which only *reads* already-deployed infrastructure). Recommend early prototyping, as RESEARCH.md itself advises. |

## Metadata

**Analog search scope:** `packages/dataplat/src/dataplat/` (observability, pipeline, metadata, config, cli), `airflow/dags/` (`_common/`, `csv_ingest_customers.py`), `migrations/versions/` (all 9 existing), `helm/values/{local,ci}/` (all 7 existing pairs), `scripts/` (all 15 top-level scripts + `scripts/stages/`), `docker/` (both Dockerfiles), `Makefile`, `packages/dataplat/pyproject.toml`, `tests/{integration,e2e}/` (all existing test modules + conftest fixtures), `tests/policy/test_values_profiles.py`.
**Files scanned:** ~55 read directly (full or targeted ranges); ~90 enumerated via Glob/grep across the directories above.
**Pattern extraction date:** 2026-08-15
