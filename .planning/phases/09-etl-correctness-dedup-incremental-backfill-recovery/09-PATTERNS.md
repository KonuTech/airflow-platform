# Phase 9: ETL Correctness — Dedup, Incremental, Backfill & Recovery - Pattern Map

**Mapped:** 2026-08-19
**Files analyzed:** 20 (new + modified)
**Analogs found:** 20 / 20

This phase is pure extension of already-proven mechanics (per RESEARCH.md's own
Summary: "not a build-from-scratch phase"). Every new file below has a byte-for-byte
structural template already committed in this repository — the discipline this
phase needs is reuse, not invention.

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `migrations/versions/00XX_meta_watermarks.py` | migration | CRUD (DDL) | `migrations/versions/0025_meta_run_stages.py` | exact |
| `migrations/versions/00XX_meta_reconciliation_results.py` | migration | CRUD (DDL) | `migrations/versions/0024_meta_dedup_audit_decisions.py` | exact |
| `migrations/versions/00XX_meta_v_run_recovery.py` | migration (view) | CRUD (DDL) | `migrations/versions/0030_fix_v_customers_lineage_dedup_audit_model_name.py` (+0012/0026) | exact |
| `packages/dataplat/src/dataplat/metadata/repository.py` (+`record_watermark`/`get_current_watermark`) | service (Protocol) | CRUD | `claim_run_stage`/`complete_run_stage`/`get_run_stage_status` (same file, lines 476-633) | exact |
| `packages/dataplat/src/dataplat/metadata/postgres.py` (+ implementations) | service (repository impl) | CRUD | `claim_run_stage`/`complete_run_stage` impl (same file, lines 458-566) | exact |
| `packages/dataplat/src/dataplat/pipeline/run.py` (`publish_ingest`, extend) | service (orchestration) | request-response / transactional | same function's existing publish transaction block (lines 824-923) | exact (self-extend) |
| `packages/dataplat/src/dataplat/load/staging.py` (`promote_to_durable_bronze`, extend) | service | CRUD | same method (lines 722-769) | exact (self-extend) |
| `airflow/dags/_common/run_stage_recorder.py` (new) | controller (Airflow task, DB write) | event-driven | `airflow/dags/_common/integrity_gate.py` (`_reject_file`, ADR-0004 exception) | exact |
| `dbt/macros/reconciliation_post_hook.sql` (new) | transform (dbt macro, post-hook) | event-driven | `dbt/macros/dedup_audit_post_hook.sql` | exact |
| `dbt/models/silver/schema.yml` (or per-model `.yml`, add `severity: warn` test) | config (dbt schema/tests) | transform | `dbt/models/silver/silver_customers.yml` | exact |
| `helm/values/local/monitoring.yaml` / `helm/values/ci/monitoring.yaml` (+D-19 alert rule) | config (alerting-as-code) | event-driven | same file's existing `gauge-failure-rate`/`gauge-runs-inflight` rules (lines 605-694) | exact |
| `configs/datasets/customers.yaml` / `orders.yaml` (+`reconciliation:`, −`deduplication:`) | config (dataset YAML) | CRUD | same files' existing `freshness:`/`quality:` optional blocks | exact |
| `packages/dataplat/src/dataplat/config/model.py` (+`ReconciliationConfig`, `deduplication` → Optional) | model (Pydantic config) | CRUD | `FreshnessConfig`/`QualityConfig`/`NormalizationConfig` optional-field pattern (same file) | exact |
| `tests/integration/test_watermarks.py` (new) | test | CRUD | `tests/integration/test_dbt_dedup_audit.py` (helpers) | role-match |
| `tests/integration/test_run_recovery_view.py` (new) | test | CRUD | `tests/integration/test_dbt_dedup_audit.py` (fixture/helper shape) | role-match |
| `tests/integration/test_reconciliation.py` (new) | test | CRUD | `tests/integration/test_dbt_dedup_audit.py` | role-match |
| `tests/integration/test_dbt_reconciliation.py` (new) | test | event-driven (dbt) | `tests/integration/test_dbt_dedup_audit.py` | exact |
| `tests/integration/test_batch_complete_control_totals.py` (new) | test | CRUD | `tests/unit/validate/test_batch_complete_marker.py` (existing presence-only precedent) | role-match |
| `tests/e2e/slice/test_backfill_2year_sweep.py` (new) | test | event-driven (live cluster) | `tests/e2e/slice/test_backfill_reentry.py` + `tests/e2e/slice/test_pod_kill_retry.py` | exact |
| `tests/e2e/slice/conftest.py` (extend, dbt_build poll helper) | test fixture/helper | event-driven | `_poll_mid_load_signal` (`tests/e2e/slice/test_pod_kill_retry.py` lines 85-138) | exact |
| `tests/fixtures/backfill-corpus.yaml` (new) + `tools/corpus/generators.py` (possible extension) | config (fixture manifest) / utility | batch | `tests/fixtures/slice-corpus.yaml` + `tools/corpus/generators.py` | role-match |

## Pattern Assignments

### `migrations/versions/00XX_meta_watermarks.py` (migration, CRUD)

**Analog:** `migrations/versions/0025_meta_run_stages.py` (full read)

**Structure to copy verbatim** (module docstring explaining *why*, `upgrade()`/`downgrade()` shape, grant discipline):
```python
# Source: migrations/versions/0025_meta_run_stages.py, lines 37-84
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "00XX"
down_revision = "<prior>"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create `meta.watermarks`, `etl_app`-only, one row per (dataset_id, target_key)."""
    op.create_table(
        "watermarks",
        sa.Column("watermark_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column("dataset_id", sa.BigInteger(), sa.ForeignKey("meta.datasets.dataset_id"), nullable=False),
        # D-03: single 'default' target_key per dataset this phase — forward-compat column,
        # unexercised (same "built but unexercised" pattern as _BATCH_COMPLETE).
        sa.Column("target_key", sa.Text(), nullable=False, server_default=sa.text("'default'")),
        sa.Column("cursor_value", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("dataset_id", "target_key", name="uq_watermarks_dataset_id_target_key"),
        schema="meta",
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON meta.watermarks TO etl_app")
    op.execute("GRANT SELECT ON meta.watermarks TO grafana_reader")
    # meta.watermark_history: append-only audit (D-04), same migration file.
    op.create_table(
        "watermark_history",
        sa.Column("watermark_history_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column("dataset_id", sa.BigInteger(), sa.ForeignKey("meta.datasets.dataset_id"), nullable=False),
        sa.Column("target_key", sa.Text(), nullable=False),
        sa.Column("old_value", sa.DateTime(timezone=True), nullable=True),
        sa.Column("new_value", sa.DateTime(timezone=True), nullable=True),
        sa.Column("run_id", sa.BigInteger(), sa.ForeignKey("meta.ingestion_runs.run_id"), nullable=True),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        schema="meta",
    )
    op.execute("GRANT SELECT, INSERT ON meta.watermark_history TO etl_app")
    op.execute("GRANT SELECT ON meta.watermark_history TO grafana_reader")


def downgrade() -> None:
    """Drop both tables (reverse order)."""
    op.drop_table("watermark_history", schema="meta")
    op.drop_table("watermarks", schema="meta")
```
**Key rules from the analog to follow:**
- `stage_name`/`target_key`-style vocabulary columns are always plain `sa.Text()`, never a native Postgres ENUM (established convention, cited in 0025's own docstring).
- `UNIQUE(dataset_id, target_key)` is the load-bearing constraint enabling an `ON CONFLICT` upsert, exactly like `UNIQUE(run_id, stage_name)` enables `claim_run_stage`.
- Docstring must explain *why*, not just *what* — cite the CONTEXT.md decision IDs (D-01..D-04) the way 0025 cites D-17/ARCHITECTURE.md.
- `dbt_app` gets **zero** grant here (this table is written by `publish_ingest`, running as `etl_app`) — mirror 0025's explicit "Deliberately nothing granted to dbt_app" comment.

---

### `migrations/versions/00XX_meta_reconciliation_results.py` (migration, CRUD)

**Analog:** `migrations/versions/0024_meta_dedup_audit_decisions.py` (full read)

**Grant pattern to copy exactly** (lines 118-124 of the analog):
```python
# Source: migrations/versions/0024_meta_dedup_audit_decisions.py
op.execute("GRANT USAGE ON SCHEMA meta TO dbt_app")  # only if not already granted — check 0021/0024 first, don't re-grant
op.execute("GRANT SELECT, INSERT ON meta.reconciliation_results TO dbt_app")  # INSERT-only: dbt post-hook only appends
op.execute("GRANT SELECT, INSERT ON meta.reconciliation_results TO etl_app")  # etl_app writes raw->bronze and silver->gold hops
op.execute("GRANT SELECT ON meta.reconciliation_results TO grafana_reader")
```
**Table shape** — grain is per-file-per-hop (D-24), columns should include: `reconciliation_id` (Identity PK), `file_id` (FK `meta.files.file_id`), `hop` (Text: `'raw_bronze'`/`'bronze_silver'`/`'silver_gold'`), `input_count`, `output_count`, `rejected_count` (D-22's quarantine-aware net-out), `dedup_count` (nets out `meta.dedup_audit` for the bronze→silver hop), `discrepancy` (nullable, non-zero flags an unexplained gap), `checked_at`. Reference `meta.rejected_records`/`meta.dedup_audit` via joined counts computed at write time (not FKs — same "not every reference needs a literal FK" precedent as `dedup_audit.min_run_id`/`max_run_id`, which the 0024 docstring explicitly calls out as deliberately FK-less because "dbt's own transaction writes these").

**Do NOT** implement D-22's accounting as a naive `input_count - output_count` diff — this is the one explicit correctness rule from CONTEXT.md (D-22) that a schema/migration alone cannot enforce; the migration's docstring should state the accounting formula explicitly (`discrepancy = input_count - (output_count + rejected_count + dedup_count)`), mirroring how 0024's docstring documents the schema-shape correction it makes concrete.

---

### `migrations/versions/00XX_meta_v_run_recovery.py` (migration, view)

**Analog:** `migrations/versions/0030_fix_v_customers_lineage_dedup_audit_model_name.py` (full read; also 0012/0026 establish the same pattern)

**Exact "drop + recreate whole view" pattern and grants to copy** (lines 154-167):
```python
# Source: migrations/versions/0030_..., adapted
def upgrade() -> None:
    op.execute(_CREATE_VIEW)  # first-ever creation this migration — no prior DROP needed
    op.execute("GRANT SELECT ON meta.v_run_recovery TO etl_app")
    op.execute("GRANT SELECT ON meta.v_run_recovery TO grafana_reader")


def downgrade() -> None:
    op.execute("DROP VIEW meta.v_run_recovery")
```
**View body** — see RESEARCH.md Architecture Pattern 3 (already verified against this repo's real column names) for the exact `LEFT JOIN meta.run_stages sl/db/pb ON ... AND stage_name = 'STAGE_LOAD'/'DBT_BUILD'/'PUBLISH'` shape and the `CASE WHEN ... THEN 'retry stage X' ... ELSE 'in progress' END AS next_action` column (D-15: always "retry", never "rollback" — literally spell that word out, don't imply it).
**Rationale to state in the docstring:** "Postgres has no `ALTER VIEW ... ADD COLUMN`" — copy 0030's own stated reason for the drop+recreate pattern verbatim as the precedent, even on first creation (documents the convention for whoever adds a 4th `run_stages` value later).

---

### `packages/dataplat/src/dataplat/metadata/repository.py` / `postgres.py` (+`record_watermark`/`get_current_watermark`)

**Analog:** `claim_run_stage`/`complete_run_stage`/`get_run_stage_status` — Protocol declarations at `repository.py` lines 476-633, implementations at `postgres.py` lines 458-566 (both fully read).

**Protocol declaration pattern** (docstring-heavy, `Maps to`` <SQL shape> `` inline, explicit `Args:`/`Returns:`):
```python
# Source: packages/dataplat/src/dataplat/metadata/repository.py, lines 566-586 (complete_run_stage), adapted shape
def record_watermark(
    self,
    *,
    conn: Connection[Any],
    dataset_id: int,
    target_key: str,
    cursor_value: datetime | None,
    run_id: int,
) -> None:
    """Advance meta.watermarks using GREATEST(); always logs to watermark_history.

    Maps to ``INSERT INTO meta.watermarks (dataset_id, target_key, cursor_value)
    VALUES (%s, %s, %s) ON CONFLICT (dataset_id, target_key) DO UPDATE
    SET cursor_value = GREATEST(meta.watermarks.cursor_value, EXCLUDED.cursor_value)
    RETURNING cursor_value`` followed by an INSERT into
    ``meta.watermark_history`` with the old/new pair — INCR-02's
    "``>=``, never ``>``" rule enforced structurally by ``GREATEST()``,
    not a conditional branch.

    MUST be called on the SAME ``conn``/transaction ``publish_ingest``
    already holds ``pg_advisory_xact_lock`` on (INCR-02, AP4 avoidance) —
    never a separate connection or a post-commit write.
    """
    ...
```
**Implementation pattern** — copy `claim_run_stage`'s impl shape (`postgres.py` lines 478-513): a single parameterized SQL statement via `conn.execute(...)`, `RETURNING` clause, `fetchone()`, defensive `None`-check. Note `record_watermark` takes `conn` as an explicit argument (unlike most repository methods, which open `self._pool.connection()` themselves) because it MUST run inside the caller's already-open publish transaction — this is a deliberate signature deviation from the rest of the Protocol; document it exactly like `finalize_publication` already does (same file, `conn: Connection[Any]` param, called from inside `publish_ingest`'s `with ctx.db.connection() as conn, conn.transaction():` block).

---

### `packages/dataplat/src/dataplat/pipeline/run.py` (`publish_ingest`, extend — watermark advance + silver→gold reconciliation)

**Analog:** the function's own existing publish transaction block (lines 824-923, fully read this session).

**Exact insertion point** — after `result = publisher.publish(ctx, source_table, conn)` (line 846), before the `for run_id, file_id, batch_id, report_uri in staged:` loop (line 851):
```python
# Source: packages/dataplat/src/dataplat/pipeline/run.py, lines 839-851 — insert between these two lines
publisher = resolve_publisher(ctx.config.load.strategy)
source_table = f"silver.{ctx.config.dataset}"
result = publisher.publish(ctx, source_table, conn)

# --- Phase 9 D-01/D-02/D-04 addition, SAME transaction, after publish ---
watermark_column = _watermark_column_for_dataset(ctx.config.dataset)  # config-driven, not hardcoded — orders uses order_date, customers uses event_ts
ctx.metadata.record_watermark(
    conn=conn,
    dataset_id=dataset_id,
    target_key="default",
    cursor_value=None,  # resolved server-side via SELECT max(<watermark_column>) FROM {source_table} inside record_watermark's SQL
    run_id=max(run_id for run_id, _, _, _ in staged),
)
# --- Phase 9 D-21 silver->gold reconciliation addition ---
ctx.metadata.record_reconciliation(
    conn=conn,
    hop="silver_gold",
    input_count=<count from source_table>,
    output_count=result.rows_affected,
    ...,
)

finished_at = datetime.now(tz=UTC)
duration_ms = int((time.monotonic() - start) * 1000)

for run_id, file_id, batch_id, report_uri in staged:
    ...
```
**Critical constraint from Pitfall 2 (RESEARCH.md):** `publisher.publish()` reads the ENTIRE cumulative `silver.<dataset>` table every call, never a per-run slice — design the watermark's `max(event_ts)` and the reconciliation input/output counts to compare against the **whole target table's current state** or `result.published_business_keys`, never a `WHERE _run_id = ...` filter (that SQL does not exist and changing `merge.py`'s `_PUBLISH_SQL` is explicitly out of scope).
**Comment discipline to match:** every non-obvious design choice in this file gets an inline comment citing the decision ID (see how lines 864-886 document the aggregate-attribution simplification) — do the same for watermark/reconciliation additions, citing D-01/D-02/D-04/D-21/D-22.

---

### `packages/dataplat/src/dataplat/load/staging.py` (`promote_to_durable_bronze`, extend — raw→bronze reconciliation)

**Analog:** the method itself (lines 722-769, fully read) — never-commits-or-rolls-back-`conn` ownership contract identical to `Publisher.publish`.

**Insertion point** — immediately after the existing `INSERT INTO {durable_table} ... SELECT ... FROM {staging_result.staging_table}` (lines 766-769), same `conn`, same transaction:
```python
# Source: packages/dataplat/src/dataplat/load/staging.py, lines 764-769 — extend after this INSERT
durable_table = f"staging.{ctx.config.dataset}"
column_list = ", ".join((*self._target_columns, *_LINEAGE_COLUMN_NAMES))
conn.execute(
    f"INSERT INTO {durable_table} ({column_list}) "
    f"SELECT {column_list} FROM {staging_result.staging_table}",
)
# --- Phase 9 D-21 raw->bronze reconciliation addition, SAME conn/transaction ---
ctx.metadata.record_reconciliation(
    conn=conn,
    file_id=ctx.run.file_id,
    hop="raw_bronze",
    input_count=staging_result.rows_read,
    output_count=staging_result.rows_parsed,
    rejected_count=staging_result.rows_rejected,  # D-22: already-known StagingResult field, quarantine-aware by construction
)
```
`StagingResult` (dataclass, same file lines ~171-197) already carries `rows_read`/`rows_parsed`/`rows_rejected`/`rejected_records` — D-22's "compare input against (output + rejected)" formula is available with zero new plumbing at this hop; this is the cheapest of the three reconciliation call sites to implement.

---

### `airflow/dags/_common/run_stage_recorder.py` (new, D-14's `DBT_BUILD` write)

**Analog:** `airflow/dags/_common/integrity_gate.py`, specifically `_reject_file` (lines 206-277, fully read) — the exact "ADR-0004 sanctioned exception: Airflow writes directly to `meta` via plain psycopg, never a `dataplat` import" pattern D-14 requires (`dbt_app` has zero grant on `meta.run_stages`, migration 0025).

**Structure to copy:**
```python
# Source: airflow/dags/_common/integrity_gate.py, lines 244-277 (_reject_file), adapted shape
"""D-14: the dbt_build task's own meta.run_stages write.

A third sanctioned exception to ADR-0004's "Airflow never writes to the
analytical database directly" rule, alongside `integrity_gate.py`'s
`_reject_file`. `dbt_app` has zero grant on `meta.run_stages` (migration
0025's own deliberate D-02 decoupling) — the DBT_BUILD stage-status write
must come from the Airflow side, plain psycopg, never a dataplat import.
"""

from __future__ import annotations

import psycopg
from airflow.sdk import task
from airflow.sdk.bases.hook import BaseHook

_ANALYTICS_DB_CONN_ID = "analytics_db_default"  # same Connection ID integrity_gate.py resolves


@task
def record_dbt_build_stage(run_id: int, status: str, pod_name: str) -> None:
    """Plain-psycopg claim/complete write for the DBT_BUILD run_stages row.

    Mirrors PostgresMetadataRepository.claim_run_stage/complete_run_stage's
    SQL shape (packages/dataplat/src/dataplat/metadata/postgres.py, lines
    478-513/539-556) exactly, duplicated as raw SQL here — never imported
    (ADR-0004).
    """
    dsn = BaseHook.get_connection(_ANALYTICS_DB_CONN_ID).get_uri()
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO meta.run_stages (run_id, stage_name, status, pod_name, started_at)
            VALUES (%s, 'DBT_BUILD', %s, %s, now())
            ON CONFLICT (run_id, stage_name) DO UPDATE
                SET status = EXCLUDED.status, finished_at = CASE WHEN EXCLUDED.status != 'RUNNING' THEN now() ELSE meta.run_stages.finished_at END
            """,
            (run_id, status, pod_name),
        )
```
**Precedent to follow exactly:** duplicate the SQL shape from `postgres.py`, never import `dataplat` into the DAG folder (module docstring's own stated rule, ADR-0004) — this is the THIRD instance of this exception (after `integrity_gate.py` and `kpo.py`/`tracing_kpo.py`), so cite that count in the new module's own docstring, matching `integrity_gate.py`'s own "a second, narrowly-scoped exception... alongside kpo.py/tracing_kpo.py" framing (line 3).

---

### `dbt/macros/reconciliation_post_hook.sql` (new, D-26 bronze→silver hop)

**Analog:** `dbt/macros/dedup_audit_post_hook.sql` (full read, 178 lines) — the direct, load-bearing template.

**Three hard-won lessons to copy verbatim (from the analog's own header, all independently re-verifiable):**
1. Accept `dataset_name` as a plain string, resolve via `meta.dataset_id_for_name(text)` (SECURITY DEFINER function, migration 0028) **inside** the macro's own INSERT — never a direct `SELECT` against `meta.datasets` (fails `dbt_app`'s least-privilege grant test).
2. Accept `source_schema`/`source_identifier`/`target_schema`/`target_identifier` as plain **strings**, never `{{ source(...) }}`/`{{ this }}` Relation objects passed as macro arguments — verified unreliable across `post_hook`'s two-pass Jinja evaluation.
3. Derive any "floor"/prior-state value from the audit table's **own history** (`coalesce(max(...), 0)`), never from a `run_query()` value captured into a `post_hook` config string.

**Macro signature and structure to mirror:**
```sql
{#
  reconciliation_post_hook.sql -- D-21/D-26's bronze->silver meta.reconciliation_results
  write, same "post-hook runs in the model's own transaction" contract as
  dedup_audit_post_hook.sql. Called from a silver model's own post_hook config.
#}
{% macro reconciliation_post_hook(dataset_name, source_schema, source_identifier, target_schema, target_identifier) %}
with bronze_count as (
    select count(*) as input_count
    from {{ source_schema }}.{{ source_identifier }}
),
silver_count as (
    select count(*) as output_count
    from {{ target_schema }}.{{ target_identifier }}
),
dedup_count as (
    select coalesce(sum(records_deduplicated), 0) as dedup_count
    from meta.dedup_audit
    where model_name = '{{ target_identifier }}'
)
insert into meta.reconciliation_results (
    dataset_id, hop, input_count, output_count, dedup_count, checked_at
)
select
    meta.dataset_id_for_name('{{ dataset_name }}'),
    'bronze_silver',
    bronze_count.input_count,
    silver_count.output_count,
    dedup_count.dedup_count,
    now()
from bronze_count, silver_count, dedup_count
{% endmacro %}
```
**Grant precedent (from `migrations/versions/0024...` lines 118-124):** `dbt_app` needs `GRANT SELECT, INSERT ON meta.reconciliation_results TO dbt_app` — INSERT-only, and `GRANT USAGE ON SCHEMA meta TO dbt_app` already exists from migration 0021/0024, **do not re-grant** (dead-weight duplicate statement, same caution the RESEARCH.md flags).

---

### `dbt/models/silver/schema.yml` or per-model `.yml` (D-26 dbt-test half, `severity: warn`)

**Analog:** `dbt/models/silver/silver_customers.yml` (full read, 76 lines) — the existing `contract: enforced: true` + `data_tests:` shape.

**Pattern to extend** (add a model-level singular test or `dbt_utils.expression_is_true`-style generic test with explicit `config: {severity: warn}`, additive alongside the existing `not_null`/`unique` tests at lines 35-37):
```yaml
# Source: dbt/models/silver/silver_customers.yml, lines 21-37 — extend this shape
version: 2

models:
  - name: silver_customers
    config:
      contract:
        enforced: true
      on_schema_change: fail
    tests:
      - dbt_utils.expression_is_true:
          expression: "1 = 1"  # placeholder: real expression compares bronze_row_count = silver_kept_count + silver_dropped_count via a source() macro call or a dedicated singular test
          config:
            severity: warn  # D-26: visible, non-blocking signal — never fails the build
    columns:
      - name: customer_id
        data_type: text
        constraints:
          - type: not_null
          - type: unique
        data_tests:
          - not_null
          - unique
      # ... unchanged
```
Consider a **singular test** (`dbt/tests/reconciliation_customers.sql`) instead if `dbt_utils.expression_is_true` cannot express the bronze/silver/dedup three-way comparison cleanly — either is additive to the macro (D-26 "both"), never a replacement.

---

### `helm/values/local/monitoring.yaml` / `helm/values/ci/monitoring.yaml` (D-19 alert rule on `meta.v_run_recovery`)

**Analog:** the file's own existing `gauge-runs-inflight`/`gauge-failure-rate`/`gauge-reject-rate` rules (lines 605-694, fully read) — Grafana native unified alerting-as-code under `grafana.alerting.rules.yaml`.

**Exact shape to copy** (Postgres-datasource variant, since `meta.v_run_recovery` is SQL not Prometheus — check the file for an existing Postgres-datasource rule as the more precise template before finalizing; the Prometheus-datasource shape below is the structural skeleton, `datasourceUid` must point at the Grafana Postgres datasource instead):
```yaml
# Source: helm/values/local/monitoring.yaml, lines 605-635 (gauge-runs-inflight), structural template
- uid: alert-run-recovery-exhausted
  title: "A run/stage has exhausted retries and needs manual retry"
  condition: B
  data:
    - refId: A
      relativeTimeRange: { from: 600, to: 0 }
      datasourceUid: <postgres-datasource-uid>  # meta.v_run_recovery is SQL, not a Prometheus metric
      model:
        refId: A
        rawSql: >
          SELECT count(*) FROM meta.v_run_recovery
          WHERE next_action LIKE 'retry stage%'
            AND run_status != 'SUCCEEDED'
        format: table
    - refId: B
      relativeTimeRange: { from: 600, to: 0 }
      datasourceUid: __expr__
      model:
        refId: B
        type: threshold
        expression: A
        conditions:
          - evaluator: { type: gt, params: [0] }
  noDataState: NoData
  execErrState: Error
  for: 5m  # Pitfall 6: time-based threshold, not a raw FAILED-status trigger — mirrors this exact `for: 5m` field
  labels:
    severity: warning
  annotations:
    summary: "meta.v_run_recovery reports a stage stuck in retry-needed state for 5m+."
```
**D-07 constraint to respect:** this is the ONLY alerting engine in the project — do not introduce a second mechanism; this is strictly "add one more rule to the existing `rules.yaml` list."

---

### `configs/datasets/customers.yaml` / `orders.yaml` (+`reconciliation:`, −`deduplication:`)

**Analog:** the files themselves (both fully read) — the `freshness:`/`quality:` opt-in block precedent (`customers.yaml` lines 41-70).

**New block to add** (orders only, per D-25 — customers omits it, no natural numeric column):
```yaml
# Source: configs/datasets/orders.yaml — add after the `batching:` block (line 57-58),
# mirroring customers.yaml's own `# freshness: is D-08's opt-in...` comment-then-block convention
reconciliation:
  sum_columns: [amount]
```
**Block to remove** (both files, D-28) — delete the entire `deduplication:` block:
```yaml
# Source: configs/datasets/customers.yaml lines 31-34 / orders.yaml lines 42-45 — REMOVE ENTIRELY
deduplication:
  strategy: business_key_latest
  keys: [customer_id]      # or [order_id] for orders
  order_by: [event_ts desc]  # or [order_date desc] for orders
```
**Critical dependency (Pitfall 4):** this YAML edit alone breaks Pydantic validation — `DatasetConfig.deduplication` must become `DeduplicationConfig | None = None` in `config/model.py` FIRST (or in the same commit), matching the `freshness`/`quality`/`normalization`/`filename` Optional-field precedent already established in that same model file.

---

### `packages/dataplat/src/dataplat/config/model.py` (+`ReconciliationConfig`; `deduplication` → Optional)

**Analog:** `FreshnessConfig`/`QualityConfig`/`NormalizationConfig`'s existing `X | None = None` pattern on `DatasetConfig` (same file, lines 525-528, fully read) — and `DeduplicationConfig` itself (lines 93-124, fully read) as the shape template for the new `ReconciliationConfig`.

**New model, mirroring `DeduplicationConfig`'s docstring density and `ConfigDict(extra="forbid", frozen=True)` convention:**
```python
# Source: packages/dataplat/src/dataplat/config/model.py, lines 93-124 (DeduplicationConfig), adapted shape
class ReconciliationConfig(BaseModel):
    """D-25: opt-in, dataset-conditional sum-check columns for VALID-05's reconciliation.

    Attributes:
        sum_columns: Numeric column names this dataset's reconciliation
            pass sums and compares source-vs-target on. Omitted (field
            absent, DatasetConfig.reconciliation is None) for a dataset
            with no natural numeric column to sum — customers.yaml does
            not set this; orders.yaml declares sum_columns: [amount].
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    sum_columns: list[str]
```
**Required change to the existing field** (Pitfall 4's exact fix):
```python
# Source: packages/dataplat/src/dataplat/config/model.py, lines 516-531 (DatasetConfig), current shape
deduplication: DeduplicationConfig             # BEFORE: required, no default
# AFTER (D-28):
deduplication: DeduplicationConfig | None = None
reconciliation: ReconciliationConfig | None = None  # D-25, new optional field, same convention

# and update `_check_deduplication_keys_are_business_key_columns` (grep for it —
# not read this session, exists per RESEARCH.md Pitfall 4) to guard:
#   if self.deduplication is not None: ... (existing body unchanged)
```
This is the exact same `X | None = None` shape as `freshness: FreshnessConfig | None = None` / `quality: QualityConfig | None = None` already on the same `DatasetConfig` class (lines 527-528) — no new pattern, direct copy of an existing one.

---

### `tests/integration/test_dbt_reconciliation.py` (new, D-26 bronze→silver hop)

**Analog:** `tests/integration/test_dbt_dedup_audit.py` (full file structure read, 267 lines) — direct structural mirror per RESEARCH.md's own explicit recommendation.

**Structure to copy:**
```python
# Source: tests/integration/test_dbt_dedup_audit.py, lines 1-38
"""Integration test proving VALID-05's bronze->silver reconciliation hop against a real `dbt build`."""

from __future__ import annotations

import psycopg
import pytest

from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.storage.db import create_pool

pytestmark = [pytest.mark.dbt, pytest.mark.integration]

# The reconciliation macro's own hop vocabulary — mirror _VALID_REASONS's
# closed-set assertion pattern.
_VALID_HOPS = frozenset({"raw_bronze", "bronze_silver", "silver_gold"})
```
Reuse `_get_or_create_config_version`/`_seed_ingestion_run` helpers verbatim (same file, lines 41-90) — the docstring "See `test_dbt_silver_dedup.py`'s identical helper" convention shows this repo already deliberately duplicates small helpers across dbt-marker test files rather than building a shared test-utility module; follow that precedent, don't introduce a new shared fixture module for this alone.

---

### `tests/e2e/slice/test_backfill_2year_sweep.py` (new, INCR-06/QUAL-11 live proof)

**Analog:** `tests/e2e/slice/test_backfill_reentry.py` (772 lines, grepped for CLI-invocation shape) + `tests/e2e/slice/test_pod_kill_retry.py` (461 lines, fully read for polling-helper shape).

**CLI invocation pattern to copy** (from `test_backfill_reentry.py`, verified live-proven):
```python
# Source: tests/e2e/slice/test_backfill_reentry.py, line ~392 pattern
f"airflow backfill create --dag-id {dag_id} --from-date {logical_date_iso} "
f"--to-date {to_date_iso} --reprocess-behavior completed"
# invoked via kubectl exec, per the same file's `kubectl_fn: Callable[..., subprocess.CompletedProcess[str]]` fixture parameter shape
```
**Sizing discipline (Pitfall 1, RESEARCH.md):** do NOT set `--from-date`/`--to-date` to span the genuine 2 calendar years — the DAGs' `schedule="*/1 * * * *"` means one DagRun per minute-tick; a literal 2-year window is ~1,051,200 DagRuns. Upload the full 2-year fixture corpus to MinIO ahead of time (a setup step), then use `--dry-run` first to size a short window (RESEARCH.md's own worked estimate: ~20-30 minutes / 15+ discover-call ticks per dataset) that drains the corpus via `discover_files`'s own content-hash-driven, date-agnostic selection — never rely on the backfill window's calendar span to "cover" the 2 years.

**Polling-helper pattern to copy** (`test_pod_kill_retry.py::_poll_mid_load_signal`, lines 85-138, fully read): same `deadline = time.monotonic() + timeout` / `while time.monotonic() < deadline: ... time.sleep(0.5)` loop shape, raising `AssertionError` with a message reporting the last-observed state on timeout — reuse this exact loop shape for polling `meta.v_run_recovery`'s `next_action = 'complete'` and for the idempotency re-run's "zero additional rows" row-count assertion.

---

### `tests/e2e/slice/conftest.py` (extend — D-18's `dbt_build` pod-kill poll helper)

**Analog:** `_poll_mid_load_signal` (`tests/e2e/slice/test_pod_kill_retry.py`, lines 85-138, fully read) — same file's docstring explicitly documents its own provenance chain (`on_progress` callback → heartbeat thread → `meta.ingestion_runs.rows_read`).

**New helper, same shape, different signal source** (D-14's `run_stages` write, not a `rows_read` heartbeat — `dbt_build` has no row-level heartbeat, RESEARCH.md Code Examples section):
```python
# Source: tests/e2e/slice/test_pod_kill_retry.py, lines 85-138 (_poll_mid_load_signal), adapted signal
def _poll_dbt_build_running_signal(
    conn: psycopg.Connection[Any],
    run_id: int,
    *,
    timeout: float,
) -> dict[str, Any]:
    """Poll meta.run_stages for D-18's dbt_build mid-flight signal: stage_name='DBT_BUILD', status='RUNNING'.

    Unlike _poll_mid_load_signal, there is no rows_read-style heartbeat for
    dbt_build (dbt's own execution is opaque to meta.ingestion_runs) -- the
    mid-flight detection condition is the D-14 run_stages write itself.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = conn.execute(
            "SELECT status, pod_name FROM meta.run_stages "
            "WHERE run_id = %s AND stage_name = 'DBT_BUILD'",
            (run_id,),
        ).fetchone()
        if row is not None and row[0] == "RUNNING" and row[1]:
            return {"status": row[0], "pod_name": row[1]}
        time.sleep(0.5)
    msg = f"never observed DBT_BUILD RUNNING for run_id={run_id!r} within {timeout}s"
    raise AssertionError(msg)
```

---

### `tests/fixtures/backfill-corpus.yaml` (new) + `tools/corpus/generators.py` (possible extension)

**Analog:** `tests/fixtures/slice-corpus.yaml` (partial read, header/structure) + `tools/corpus/generators.py`'s existing `ColumnSpec` kinds (`zero_padded_int`, `pick`, etc. — function names grepped, not all bodies read).

**Manifest header/structure to copy:**
```yaml
# Source: tests/fixtures/slice-corpus.yaml, lines 1-46 — structural template
version: 1
master_seed: "airflow-platform/backfill-corpus/v1"  # DELIBERATELY DIFFERENT from both existing master_seed strings

fixtures:
  # D-10's four combined requirements, all in ONE window, not isolated fixtures:
  #   - regular file-drop cadence (daily, ~730 files x 2 datasets over 2 years)
  #   - at least one deliberate schema-version-change boundary partway through
  #   - at least one deliberate missing file (gap, exercises D-06)
  #   - at least one out-of-order/late event relative to its neighbors
```
**Open question (RESEARCH.md OQ2, unresolved):** confirm during planning whether `tools/corpus/generators.py`'s existing generator functions already express "daily cadence over N days with an injected schema-version boundary + gap + late event," or whether new generator code is needed — this file's current session did not fully audit every generator function body. Reference `docs/adr/0005-fixture-corpus-generated-from-a-seed.md` for the seed-determinism rules (R1-R10) any new generator code must still satisfy.

## Shared Patterns

### Same-transaction write discipline (INCR-02, D-21, AP4 avoidance)
**Source:** `packages/dataplat/src/dataplat/pipeline/run.py` lines 827-846 (advisory lock + publish, one transaction)
**Apply to:** `record_watermark`, `record_reconciliation` (silver→gold hop) in `publish_ingest`; `record_reconciliation` (raw→bronze hop) in `promote_to_durable_bronze`
```python
with (
    tracing.start_span("pipeline.publish"),
    ctx.db.connection() as conn,
    conn.transaction(),
):
    conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"publish:{ctx.config.load.target}",))
    # every Phase 9 write below happens on THIS conn, inside THIS transaction — never a separate connection/commit
```

### ADR-0004 "Airflow writes directly to meta via plain psycopg" exception
**Source:** `airflow/dags/_common/integrity_gate.py` `_reject_file` (lines 206-277)
**Apply to:** `airflow/dags/_common/run_stage_recorder.py` (D-14's `DBT_BUILD` write) — the SQL shape is duplicated from `postgres.py`, never imported (`dataplat` is never imported into the DAG folder).

### `dbt_app` least-privilege grants (INSERT-only where it writes, SELECT-only elsewhere)
**Source:** `migrations/versions/0024_meta_dedup_audit_decisions.py` lines 118-124
**Apply to:** every new `meta.*` table migration this phase — `dbt_app` gets `GRANT SELECT, INSERT` only on `meta.reconciliation_results` (its one write target, via the post-hook macro), nothing on `meta.watermarks`/`meta.watermark_history`/`meta.run_stages` (all `etl_app`/Airflow-side-only per D-01/D-02/D-14's decoupling).

### Optional Pydantic config field, `X | None = None`
**Source:** `packages/dataplat/src/dataplat/config/model.py` — `freshness: FreshnessConfig | None = None` / `quality: QualityConfig | None = None` (`DatasetConfig`, lines 527-528)
**Apply to:** `deduplication: DeduplicationConfig | None = None` (D-28) and `reconciliation: ReconciliationConfig | None = None` (D-25) — both new fields on `DatasetConfig`, both following the exact same "opt-in, unexercised where it doesn't apply" convention already established for `filename`/`normalization`/`freshness`/`quality`.

### Drop + recreate whole SQL view (no `ALTER VIEW`)
**Source:** `migrations/versions/0030_fix_v_customers_lineage_dedup_audit_model_name.py` (and 0012/0026)
**Apply to:** `meta.v_run_recovery`'s own migration — even on first creation, document the "no `ALTER VIEW ... ADD COLUMN`" rationale so a future 4th `run_stages` value's migration follows the same convention.

### Time-based (not raw-status) alert threshold
**Source:** `helm/values/local/monitoring.yaml` — every existing rule's `for: 5m` field (lines 552, 597, 631, 662, 692)
**Apply to:** D-19's exhausted-retry alert — a `FAILED` `run_stages` row does not by itself mean retries are exhausted (Pitfall 6); gate on `for: 5m`-style persistence, exactly like every other rule in this file already does.

## No Analog Found

None — every file in this phase's scope has a direct, verified, previously-read structural template already committed in this repository (per RESEARCH.md's own Summary: "not a build-from-scratch phase"). The one item carrying genuine residual uncertainty is not a missing analog but an unresolved scope question — the fixture-corpus generator's current capability (RESEARCH.md Open Question 2) — noted inline under `tests/fixtures/backfill-corpus.yaml` above, not listed here since `tests/fixtures/slice-corpus.yaml` + `tools/corpus/generators.py` remain the correct starting analog regardless of how that question resolves.

## Metadata

**Analog search scope:** `packages/dataplat/src/dataplat/{metadata,pipeline,load,config}/`, `migrations/versions/`, `dbt/macros/`, `dbt/models/silver/`, `airflow/dags/_common/`, `helm/values/local/`, `configs/datasets/`, `tests/{integration,e2e/slice,fixtures}/`
**Files scanned (this session, full or targeted read):** `metadata/repository.py`, `metadata/postgres.py`, `pipeline/run.py`, `load/staging.py`, `load/publish/merge.py`, `config/model.py`, `discovery.py`, `migrations/versions/{0024,0025,0030}_*.py`, `dbt/macros/dedup_audit_post_hook.sql`, `dbt/models/silver/silver_customers.yml`, `airflow/dags/_common/integrity_gate.py`, `helm/values/local/monitoring.yaml`, `configs/datasets/{customers,orders}.yaml`, `tests/integration/test_dbt_dedup_audit.py`, `tests/e2e/slice/{test_pod_kill_retry,test_backfill_reentry}.py`, `tests/fixtures/slice-corpus.yaml`, `tools/corpus/generators.py` (function index only)
**Pattern extraction date:** 2026-08-19
