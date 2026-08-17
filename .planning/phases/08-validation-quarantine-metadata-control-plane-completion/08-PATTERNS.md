# Phase 8: Validation, Quarantine & Metadata Control-Plane Completion - Pattern Map

**Mapped:** 2026-08-17
**Files analyzed:** 29 (24 explicit from RESEARCH.md's Recommended Project Structure + 5 implied extension points Pattern 3/D-05 requires)
**Analogs found:** 26 / 29 (3 flagged "No Analog Found" — genuinely new architectural surfaces per RESEARCH.md's own Open Questions)

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `packages/dataplat/src/dataplat/validate/__init__.py` | package init | — | `packages/dataplat/src/dataplat/normalize/__init__.py` | exact |
| `packages/dataplat/src/dataplat/validate/registry.py` | config/registry | transform (config-not-code dispatch) | `packages/dataplat/src/dataplat/load/publish/registry.py` (`PUBLISHER_REGISTRY`) | exact |
| `packages/dataplat/src/dataplat/validate/completeness.py` | pipeline stage (StreamingStage) | streaming, row-scoped | `packages/dataplat/src/dataplat/pipeline/engine.py` (`RaggedRowGuard`) | exact |
| `packages/dataplat/src/dataplat/validate/uniqueness.py` | pipeline stage (StreamingStage or BarrierStage) | streaming or barrier | `packages/dataplat/src/dataplat/pipeline/engine.py` (`RaggedRowGuard`) for the streaming case; `pipeline/protocol.py`'s `BarrierStage` Protocol for the run-scoped case | role-match |
| `packages/dataplat/src/dataplat/validate/validity_range.py` | pipeline stage (StreamingStage) | streaming, row-scoped | `packages/dataplat/src/dataplat/pipeline/engine.py` (`RaggedRowGuard`) | exact |
| `packages/dataplat/src/dataplat/validate/pattern.py` | pipeline stage (StreamingStage) | streaming, row-scoped | `packages/dataplat/src/dataplat/pipeline/engine.py` (`RaggedRowGuard`) | exact |
| `packages/dataplat/src/dataplat/validate/referential.py` | pipeline stage (BarrierStage) | barrier, run-scoped + DB read | `packages/dataplat/src/dataplat/pipeline/protocol.py` (`BarrierStage` Protocol only — no concrete implementation exists yet) | role-match (protocol-only) |
| `packages/dataplat/src/dataplat/validate/circuit_breaker.py` | pipeline stage (BarrierStage) | barrier, run-scoped aggregate | `packages/dataplat/src/dataplat/pipeline/protocol.py` (`BarrierStage` Protocol only) | role-match (protocol-only) |
| `packages/dataplat/src/dataplat/quarantine/__init__.py` | package init | — | `packages/dataplat/src/dataplat/schema/__init__.py` | exact |
| `packages/dataplat/src/dataplat/quarantine/resolution.py` | service (batch-scoped state transition) | CRUD, transactional | `packages/dataplat/src/dataplat/metadata/postgres.py` (`finalize_publication`) | role-match |
| `packages/dataplat/src/dataplat/metadata/repository.py` (extend) | Protocol | CRUD | itself — extend existing method conventions | exact |
| `packages/dataplat/src/dataplat/metadata/postgres.py` (extend, implied) | service (Protocol impl) | CRUD | itself — `finalize_publication`/`create_file` conventions | exact |
| `packages/dataplat/src/dataplat/pipeline/run.py` (extend, implied by Pattern 3) | orchestration | transactional | itself — the existing publish-transaction block (lines 375-413) | exact |
| `packages/dataplat/src/dataplat/models/report.py` (extend, implied) | model | value object | itself — `ValidationResult`'s "minimal D-05 shape" docstring names this phase as the widening point | exact |
| `packages/dataplat/src/dataplat/models/record.py` (extend, implied) | model | value object | itself — `RejectedRecord` | exact |
| `packages/dataplat/src/dataplat/errors.py` (extend) | exception hierarchy | — | itself — module docstring names `QualityThresholdExceeded`/`PublicationError` as this phase's job | exact |
| `packages/dataplat/src/dataplat/config/model.py` (extend, `quality:` block) | config model | validation | `FreshnessConfig` (opt-in `X | None = None` block pattern) | exact |
| `migrations/versions/0014_meta_validation_results.py` | migration | DDL, batch | `migrations/versions/0009_meta_schema_versions.py` (text-not-enum convention) | exact |
| `migrations/versions/0015_meta_rejected_records.py` | migration | DDL, batch | `migrations/versions/0002_meta_files.py` (arrival/registry table shape) | exact |
| `migrations/versions/0016_normalized_orders.py` | migration | DDL, batch | `migrations/versions/0005_normalized_customers.py` | exact |
| `configs/datasets/customers.yaml` (extend, `quality:` block) | config | — | itself — existing `freshness:` opt-in block | exact |
| `configs/datasets/orders.yaml` (new) | config | — | `configs/datasets/customers.yaml` | exact |
| `airflow/dags/csv_ingest_customers.py` (extend: `integrity_gate` task, `outlets=`) | DAG | orchestration | itself | exact |
| `airflow/dags/csv_ingest_orders.py` (new) | DAG | orchestration | `airflow/dags/csv_ingest_customers.py` | exact |
| `airflow/dags/_common/integrity_gate.py` (new) | shared helper (Airflow-side, non-business-logic) | request-response (S3 HEAD) | `airflow/dags/_common/kpo.py` | role-match |
| `tests/unit/validate/*.py` (new dir) | test | unit | `tests/unit/normalize/test_boolean_null.py` | exact |
| `tests/integration/test_validation_persistence.py`, `test_referential_integrity.py`, `test_backfill_resolution.py` (new) | test | integration (testcontainers) | `tests/integration/test_publish_merge.py` + `tests/integration/conftest.py` | exact |
| `tests/dagtest/conftest.py` + `tests/dagtest/test_backfill_dagrun.py` (new tier) | test | integration (`dag.test()`, testcontainers) | `tests/integration/conftest.py` (testcontainers pattern) + `tests/unit/conftest.py` (`DagBag`/`sys.path` bootstrap) | **no close analog — new tier, flagged below** |
| `tests/e2e/slice/test_referential_orphan.py`, `test_backfill_reentry.py` (new) | test | e2e (cluster) | `tests/e2e/slice/test_smoke_and_idempotency.py` + `tests/e2e/slice/conftest.py` | role-match |

## Pattern Assignments

### `packages/dataplat/src/dataplat/validate/registry.py` (config/registry, transform)

**Analog:** `packages/dataplat/src/dataplat/load/publish/registry.py`

**Full pattern to copy verbatim (module is only 48 lines)**, adapted from `PUBLISHER_REGISTRY` to `VALIDATION_RULE_REGISTRY`:
```python
# Source: packages/dataplat/src/dataplat/load/publish/registry.py:1-48
from __future__ import annotations

from typing import TYPE_CHECKING

from dataplat.errors import ConfigurationError
from dataplat.load.publish.merge import MergePublisher

if TYPE_CHECKING:
    from dataplat.load.publish.protocol import Publisher

PUBLISHER_REGISTRY: dict[str, Publisher] = {"merge": MergePublisher()}


def resolve_publisher(strategy: str) -> Publisher:
    """Resolve a ``configs/datasets/*.yaml`` ``load.strategy`` key to its ``Publisher``.
    ...
    """
    try:
        return PUBLISHER_REGISTRY[strategy]
    except KeyError:
        msg = (
            "a config names a source/deduplication/publisher strategy key "
            f"that has no registry entry: {strategy!r}"
        )
        raise ConfigurationError(
            msg,
            context={"strategy": strategy, "known": sorted(PUBLISHER_REGISTRY)},
        ) from None
```

**Adaptation notes:**
- `VALIDATION_RULE_REGISTRY: dict[str, type[StreamingStage] | type[BarrierStage]]` maps `rule_type` (`"STRUCTURAL"`, `"QUALITY_COMPLETENESS"`, `"QUALITY_UNIQUENESS"`, `"QUALITY_VALIDITY_RANGE"`, `"QUALITY_PATTERN"`, `"REFERENTIAL"`) to a **class**, not an instance, since rules take per-rule config (thresholds, column names) at construction time — unlike `PUBLISHER_REGISTRY`, which registers a single stateless instance because `MergePublisher` takes no config. See RESEARCH.md Pattern 1's worked example (lines 234-248) for the exact registry literal.
- Raise `ConfigurationError` on an unknown `rule_type`, identical shape to `resolve_publisher`'s `KeyError` handling — this is the established "config names a registry key with no entry" idiom throughout the codebase (also used by `SOURCE_REGISTRY`/`DEDUP_REGISTRY`).

---

### `packages/dataplat/src/dataplat/validate/completeness.py`, `validity_range.py`, `pattern.py` (StreamingStage, row-scoped)

**Analog:** `packages/dataplat/src/dataplat/pipeline/engine.py` (`RaggedRowGuard`, lines 38-132)

**Imports pattern** (engine.py lines 22-35):
```python
from __future__ import annotations

from typing import TYPE_CHECKING

from dataplat.models.record import RejectedRecord, StageResult
from dataplat.observability import metrics, tracing
from dataplat.pipeline.protocol import StreamingStage

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

    from dataplat.models.record import RecordChunk
    from dataplat.models.report import ValidationResult
    from dataplat.pipeline.protocol import PipelineContext
```

**Core stage-shape pattern** (engine.py lines 38-132, `RaggedRowGuard`):
```python
class RaggedRowGuard(StreamingStage):
    """Rejects rows whose field count does not match the chunk's expected count."""

    name = "ragged_row_guard"

    def __init__(self, *, field_delimiter: str = ",") -> None:
        self._field_delimiter = field_delimiter

    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult:
        kept: list[tuple[str | bool | None, ...]] = []
        rejected: list[RejectedRecord] = []
        for i, row in enumerate(chunk.rows):
            if len(row) != chunk.expected_field_count:
                rejected.append(
                    RejectedRecord(
                        source_row_number=chunk.first_ordinal + i,
                        error_type="RAGGED_ROW",
                        error_message=f"expected {chunk.expected_field_count} fields, got {len(row)}",
                        raw_line=self._field_delimiter.join(...),
                    )
                )
                continue  # never pad or truncate
            kept.append(row)

        metrics.increment(
            "rows_rejected", len(rejected),
            dataset=ctx.config.dataset, stage=self.name, status="rejected",
        )
        metrics.increment(
            "rows_kept", len(kept),
            dataset=ctx.config.dataset, stage=self.name, status="kept",
        )
        return StageResult(chunk=chunk.replace(rows=tuple(kept)), rejected=rejected, findings=[])
```

**Adaptation notes:**
- `name` is a stable, human-readable class attribute (`"ragged_row_guard"` → e.g. `"completeness_rule"`, `"validity_range_rule"`, `"pattern_rule"`).
- `apply()` must NEVER raise for a row-level problem (QUAL-03 — `StreamingStage.apply`'s own Protocol docstring, `pipeline/protocol.py` lines 90-93). Every rule violation becomes a `RejectedRecord` in `StageResult.rejected` (for `REJECT_RECORD`/`QUARANTINE_RECORD` strategies) or a `ValidationResult` in `StageResult.findings` (for `WARN_AND_CONTINUE`/aggregate reporting) — **never both are mandatory; which one(s) a rule populates depends on its configured strategy (D-07)**.
- `chunk.replace(rows=tuple(kept))` is the sole functional-update mechanism (`RecordChunk.replace`, `models/record.py` lines 66-85) — never hand-construct a new `RecordChunk`.
- The `metrics.increment(..., dataset=ctx.config.dataset, stage=self.name, status=...)` call shape is D-04's bounded-label-set convention (dataset+stage+status only, never an unbounded identity like run_id) — copy verbatim for every new rule's metrics emission.
- Completeness/validity-range/pattern rules are naturally row-scoped (no cross-chunk state needed) — implement as `StreamingStage`, exactly like `RaggedRowGuard`. `UniquenessRule` needs a decision: within-chunk uniqueness is a `StreamingStage` (accumulate a per-chunk `set()`); cross-chunk/whole-file uniqueness needs a `BarrierStage` (see below) — RESEARCH.md leaves this as an open per-rule-scope call (Pattern 1's own text: "StreamingStage or BarrierStage per rule scope").

---

### `packages/dataplat/src/dataplat/validate/referential.py`, `circuit_breaker.py` (BarrierStage, run-scoped)

**Analog:** `packages/dataplat/src/dataplat/pipeline/protocol.py`'s `BarrierStage` Protocol (lines 106-129) — **no concrete `BarrierStage` implementation exists anywhere in the codebase today**; this phase writes the first ones. Use `RaggedRowGuard`'s general code shape (imports, `name` attribute, `metrics.increment` label conventions, `RejectedRecord`/`ValidationResult` construction) but the method signature is `apply(self, ctx: PipelineContext) -> StageResult` (no `chunk` parameter — see Protocol below), and the body issues real SQL against `ctx.db`/queries `normalized.customers`.

**Protocol to implement** (`pipeline/protocol.py` lines 106-129):
```python
class BarrierStage(Protocol):
    """A stage that runs once per run, after every chunk has been staged.

    Cross-batch deduplication, threshold evaluation, publication and
    reconciliation are barriers — each needs the whole run, not one chunk —
    so a barrier is never checkpointed (ARCHITECTURE.md Q4.3).
    """

    name: str

    def apply(self, ctx: PipelineContext) -> StageResult:
        """Apply this stage once, for the whole run."""
        ...
```

**RESEARCH.md's own worked example** (Pattern 2, `08-RESEARCH.md` lines 256-268):
```python
class ReferentialIntegrityBarrier(BarrierStage):
    name = "referential_integrity_customer_id"

    def apply(self, ctx: PipelineContext) -> StageResult:
        # Query staging.<dataset>__r<run_id> for distinct customer_id values,
        # anti-join against normalized.customers, mark orphans REFERENTIAL_ORPHAN
        # (D-16) with strategy QUARANTINE_RECORD from ctx.config.quality rules.
        ...
```

**For the DB-query shape inside `apply()`**, copy the parameterized-query, no-string-formatting-of-values convention from `MergePublisher.publish()` (`load/publish/merge.py` lines 92-120): `conn.execute(SQL, params)`, never an f-string with row content interpolated as a value (only identifiers like `staging_table` are ever `.format()`-interpolated, and only because they are built from `dataset`+`run_id`, never CSV content).

**Wiring point (implied file, `pipeline/run.py`):** Barrier stages run in `run_ingest`'s existing publish-transaction block — see Pattern 3 below.

---

### `packages/dataplat/src/dataplat/quarantine/resolution.py` (service, batch-scoped)

**Analog:** `packages/dataplat/src/dataplat/metadata/postgres.py`'s `finalize_publication` (lines 426-467) — the closest existing precedent for "a whole-batch-scoped write against `conn`, never per-row, executing inside the caller's already-open transaction."

**Core pattern to copy** (the "never opens its own connection, executes inside caller's transaction" contract):
```python
# Source: packages/dataplat/src/dataplat/metadata/postgres.py:426-467
def finalize_publication(
    self, *, conn: Connection[Any], run_id: int, file_id: int, batch_id: int,
    rows_loaded: int, finished_at: datetime, duration_ms: int,
    report_uri: str | None, schema_version_id: int | None = None,
) -> None:
    """The one method on this class that does NOT open its own connection
    from `self._pool` (META-03): it must land inside the same transaction
    as `Publisher.publish`'s own `INSERT ... ON CONFLICT`, so it executes
    against the caller-supplied `conn` and never commits or rolls it back.
    """
    conn.execute(
        "UPDATE meta.files SET status = 'PROCESSED' WHERE file_id = %s",
        (file_id,),
    )
    ...
```

**Adaptation notes for `resolve_rejected_records_for_batch`:**
- Same shape: accept `conn: Connection[Any]` inside the caller's already-open publish transaction (a backfill run's own publish transaction — this is what makes D-05's "linked to the new run_id" atomic with the backfill's own data landing).
- D-04's hard constraint ("no per-row manual state editing, ever") means this is the **only** write path to `resolution_type`/`resolved_by_run_id` — model it as a single `UPDATE meta.rejected_records SET resolution_type = %s, resolved_by_run_id = %s WHERE batch_id = %s AND resolution_type = 'PENDING'` (whole-batch `WHERE`, never a `WHERE ... rejected_record_id = %s`). Do not add any narrower-scoped variant "for debugging" (RESEARCH.md Anti-Patterns, explicit).
- `MetadataRepository.resolve_rejected_records_for_batch`'s exact target signature is already drafted in RESEARCH.md's Code Examples section (see `metadata/repository.py` pattern assignment below) — implement `quarantine/resolution.py` as the pure-logic layer this Protocol method calls into, OR implement the SQL directly in `metadata/postgres.py` and keep `quarantine/resolution.py` for the batch-selection logic (deciding *which* batch a backfill run resolves) — RESEARCH.md's project structure lists `resolution.py` as "resolution_type transition logic, called only from the publication-transaction path (D-04)," so the transition SQL itself likely belongs here, invoked by `postgres.py`'s Protocol implementation.

---

### `packages/dataplat/src/dataplat/metadata/repository.py` (extend Protocol)

**Analog:** itself — every existing method's docstring convention (`create_ingestion_run`, `finalize_publication`)

**Exact target signatures already drafted in RESEARCH.md** (`08-RESEARCH.md` lines 448-463, copy verbatim as the Protocol method stubs):
```python
class MetadataRepository(Protocol):
    ...
    def record_validation_results(
        self, *, run_id: int, results: list[ValidationResult]
    ) -> None: ...

    def record_rejected_records(
        self, *, run_id: int, file_id: int, rejected: list[RejectedRecord]
    ) -> None: ...

    def resolve_rejected_records_for_batch(
        self, *, batch_id: int, resolved_by_run_id: int, resolution_type: str
    ) -> int:
        """Whole-batch side effect only (D-04) -- no per-row variant exists."""
        ...
```
Follow the existing docstring convention exactly: explicit `Maps to` SQL-shape sentence, `Args:`/`Returns:`/`Raises:` sections, and explicit callouts of what does/doesn't hold a transaction (mirroring `finalize_publication`'s "the one method that does NOT open its own connection" callout, lines 440-446).

**Concrete PostgreSQL implementation location:** `metadata/postgres.py` (implied file, not explicitly listed in RESEARCH.md's structure but required — every `Protocol` method here has a `PostgresMetadataRepository` implementation in that file; follow `create_file`'s (line 114) or `finalize_publication`'s (line 426) exact style for the new three methods).

---

### `packages/dataplat/src/dataplat/pipeline/run.py` (extend, implied by Pattern 3 — the D-11 rollback point)

**Analog:** itself — the existing publish-transaction block, `pipeline/run.py` lines 375-413

**Exact block to extend** (this IS where `meta.validation_results`/`meta.rejected_records` writes and the circuit-breaker rollback must land, per D-11/Pitfall 2):
```python
# Source: packages/dataplat/src/dataplat/pipeline/run.py:375-413
with (
    tracing.start_span("pipeline.publish"),
    ctx.db.connection() as conn,
    conn.transaction(),
):
    # Single-writer publication per dataset (LOAD-09): every writer to this
    # target serializes on the SAME advisory-lock key before touching it.
    conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (f"publish:{ctx.config.load.target}",),
    )
    publisher = resolve_publisher(ctx.config.load.strategy)
    result = publisher.publish(ctx, staging_result.staging_table, conn)
    duration_ms = int((time.monotonic() - start) * 1000)
    # META-03: lands inside the SAME transaction as the Publisher's own
    # write -- the `with` block's exit commits both together, or rolls
    # back both together on any exception.
    ctx.metadata.finalize_publication(
        conn=conn, run_id=run_id, file_id=file_id, batch_id=batch_id,
        rows_loaded=result.rows_affected, finished_at=finished_at,
        duration_ms=duration_ms, report_uri=None,
        schema_version_id=staging_result.schema_version_id,
    )
```

**Adaptation notes (this is the phase's single most load-bearing wiring point — Pitfall 2 in RESEARCH.md):**
- Barrier stages (`ReferentialIntegrityBarrier`, `RejectionRateCircuitBreaker`) must run **inside this `with` block**, after `publisher.publish()` (referential integrity needs the staged rows already visible) but the circuit breaker's `QualityThresholdExceeded` raise must happen **before** `ctx.metadata.finalize_publication` commits — raising inside the `with conn.transaction():` block triggers psycopg's automatic rollback of everything in that transaction, including the `Publisher.publish()` INSERT and any `record_validation_results`/`record_rejected_records` calls issued earlier in the same block. This is the entire mechanism that makes D-11 ("nothing publishes on FAIL") true — do not persist validation/rejection rows via a separate, earlier-committed connection.
- `ctx.metadata.record_validation_results(...)` and `ctx.metadata.record_rejected_records(...)` calls belong in this same block, using the same `conn` — never `ctx.db.connection()` opening a second connection (that would be a separate, independently-committable transaction, breaking D-11).

---

### `packages/dataplat/src/dataplat/errors.py` (extend)

**Analog:** itself — module docstring already names the exact two classes and their doc shape

**Exact classes to add, already drafted in RESEARCH.md** (`08-RESEARCH.md` lines 470-486, copy verbatim as the starting shape):
```python
class QualityThresholdExceeded(DataPlatformError):
    """The run-level rejection-rate circuit breaker (D-10) tripped.

    Raised by RejectionRateCircuitBreaker when the aggregate rejected/total
    ratio exceeds the dataset's configured threshold -- causes the entire
    publication transaction to roll back (D-11).
    """


class PublicationError(DataPlatformError):
    """The publication transaction failed for a reason other than a quality threshold.

    Raised when Publisher.publish() itself fails (constraint violation,
    connection loss mid-transaction) -- distinct from QualityThresholdExceeded,
    which is a deliberate business-rule rollback, not an infrastructure failure.
    """
```
Insert after `IncompatibleSchemaError` (end of file, `errors.py` line 188), matching the existing linear addition order (each subclass added by the phase that first raises it, per the module's own docstring convention, lines 7-14).

---

### `packages/dataplat/src/dataplat/config/model.py` (extend, `quality:` block)

**Analog:** `FreshnessConfig` (lines 336-376) — the established "opt-in block, `X | None = None`, absence is load-bearing" pattern

**Pattern to copy** (opt-in block shape + `DatasetConfig` wiring):
```python
# Source: packages/dataplat/src/dataplat/config/model.py:336-376, 422-437
class FreshnessConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    expected_frequency: str
    warn_after: str | None = None
    fail_after: str | None = None


class DatasetConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    dataset: str
    config_schema_version: int
    source: SourceConfig
    deduplication: DeduplicationConfig
    load: LoadConfig
    batching: BatchingConfig
    columns: list[ColumnContract]
    filename: FilenameMaskConfig | None = None
    normalization: NormalizationConfig | None = None
    freshness: FreshnessConfig | None = None
    csv: CsvParsingConfig = Field(default_factory=CsvParsingConfig)
```

**Adaptation notes:**
- New `QualityConfig`/`QualityRuleConfig` block: `quality: QualityConfig | None = None` on `DatasetConfig` — BUT unlike `freshness`, D-09 requires `customers.yaml` to have a **real** `quality:` block (not fixture-only), so while the *type* stays optional (a dataset genuinely could have none), this phase's own `customers.yaml` populates it.
- Each rule entry needs `rule_type` (resolved through `VALIDATION_RULE_REGISTRY`, plain `str` — matching `SourceConfig.type`/`DeduplicationConfig.strategy`'s "string, never an enum, resolved through a registry" convention, module docstring lines 10-15) and `strategy` (one of `FAIL_FILE`/`REJECT_RECORD`/`QUARANTINE_FILE`/`QUARANTINE_RECORD`/`WARN_AND_CONTINUE`, D-07 — also plain `str`, not a `Literal`, following the same registry-not-enum reasoning UNLESS the codebase's `ColumnContract.type` precedent (a closed `Literal` because there's genuinely no registry dispatch for it — lines 32-46) applies better; since `strategy` dispatch IS a real extension point (D-07 names 5 fixed values today but the registry-preferred reasoning in RESEARCH.md Pattern 1 argues for extensibility), prefer plain `str` + a small strategy-dispatch table, mirroring `rule_type`'s own registry treatment, not `ColumnContract.type`'s closed-`Literal` treatment.
- The run-level rejection-rate threshold (D-10, Claude's discretion on naming) belongs on this same `quality:` block, e.g. `quality.rejection_rate_threshold: float | None = None` — same opt-in-nullable shape as `FreshnessConfig.warn_after`/`fail_after`.
- Cross-field validation: follow `_check_deduplication_keys_are_business_key_columns`'s `@model_validator(mode="after")` shape (lines 462+) if a `quality:` rule needs to validate against `columns:` (e.g. a `validity_range` rule naming a column that doesn't exist).

---

### `migrations/versions/0014_meta_validation_results.py`

**Analog:** `migrations/versions/0009_meta_schema_versions.py`

**Full pattern to copy** (table structure, text-not-enum convention, grant statement, revision header):
```python
# Source: migrations/versions/0009_meta_schema_versions.py:1-93
"""meta.schema_versions — schema versioning, hashing and evolution history (SCHEMA-03/04/05).
...
`derived_from` and `compatibility` are `sa.Text()`, app-validated, never a
native Postgres `ENUM` — no CHECK constraint or native enum type exists
anywhere in this project's migrations.

Revision ID: 0009
Revises: 0008
Create Date: 2026-08-15
"""
from __future__ import annotations
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.create_table(
        "schema_versions",
        sa.Column("schema_version_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column("dataset_id", sa.BigInteger(), sa.ForeignKey("meta.datasets.dataset_id"), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        ...
        # App-validated "CONTRACT" | "INFERRED" -- never a native enum.
        sa.Column("derived_from", sa.Text(), nullable=False),
        ...
        schema="meta",
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON meta.schema_versions TO etl_app")

def downgrade() -> None:
    op.drop_table("schema_versions", schema="meta")
```

**Adaptation notes for `meta.validation_results`:**
- Columns per Locked Table Shapes (RESEARCH.md lines 92-95): `run_id` (FK → `meta.ingestion_runs.run_id`), `rule_id`, `rule_type` (`sa.Text()`, app-validated against `{FILE,STRUCTURAL,SCHEMA,TYPE,QUALITY,REFERENTIAL}` — **never** `CREATE TYPE ... AS ENUM`, per RESEARCH.md's explicit "Don't Hand-Roll" table row and this migration's own docstring precedent), `severity` (`sa.Text()`), `outcome` (`sa.Text()`, app-validated `{PASS,PASS_WITH_WARNING,FAIL,QUARANTINE}`), `evaluated_count` (`sa.BigInteger()`), `failed_count` (`sa.BigInteger()`), `threshold` (`JSONB()`), `observed` (`JSONB()`).
- `GRANT SELECT, INSERT, UPDATE ON meta.validation_results TO etl_app` — **no `DELETE`**, matching D-04's no-per-row-edit constraint enforced at the database-privilege level too (RESEARCH.md Security Domain, V4 Access Control row).
- No partial-unique-index-for-current-row pattern needed here (unlike 0009's `schema_versions`/`config_versions` valid_from/valid_to pattern) — this is an append-only findings log, not a versioned-row table (D-12: plain, unpartitioned).

---

### `migrations/versions/0015_meta_rejected_records.py`

**Analog:** `migrations/versions/0002_meta_files.py`

**Full pattern to copy** (arrival-registry table shape, unique constraint, index, grant):
```python
# Source: migrations/versions/0002_meta_files.py:1-82
"""meta.files — the arrival registry, identity split between path and content.
...
Revision ID: 0002
Revises: 0001
"""
from __future__ import annotations
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0002"
down_revision = "0001"

def upgrade() -> None:
    op.create_table(
        "files",
        sa.Column("file_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column("dataset_id", sa.BigInteger(), sa.ForeignKey("meta.datasets.dataset_id"), nullable=False),
        ...
        sa.Column(
            "duplicate_of_file_id", sa.BigInteger(), sa.ForeignKey("meta.files.file_id"), nullable=True,
        ),
        sa.Column("status", sa.Text(), nullable=False),
        sa.UniqueConstraint("dataset_id", "object_uri", "content_sha256", name="uq_files_dataset_uri_content"),
        schema="meta",
    )
    op.create_index("ix_files_dataset_content_sha256", "files", ["dataset_id", "content_sha256"], schema="meta")
    op.execute("GRANT SELECT, INSERT, UPDATE ON meta.files TO etl_app")

def downgrade() -> None:
    op.drop_table("files", schema="meta")
```

**Adaptation notes for `meta.rejected_records`:**
- Columns per Locked Table Shapes (RESEARCH.md lines 97-100): `run_id` (FK → `meta.ingestion_runs.run_id`), `file_id` (FK → `meta.files.file_id`), `source_row_number` (`sa.BigInteger()`), `source_byte_offset` (`sa.BigInteger()`, nullable), `raw_line` (`sa.Text()`), `error_type` (`sa.Text()`), `error_column` (`sa.Text()`, nullable), `error_message` (`sa.Text()`), `rejected_at` (`sa.DateTime(timezone=True)`, `server_default=sa.text("now()")` — matches `normalized.customers._ingested_at`'s convention in migration 0005 line 82-87), **plus** `resolution_type` (`sa.Text()`, `nullable=False`, `server_default=sa.text("'PENDING'")`) and `resolved_by_run_id` (`sa.BigInteger()`, `sa.ForeignKey("meta.ingestion_runs.run_id")`, `nullable=True` — a direct nullable FK, **not** a deferred FK like migration 0004's `schema_version_id` pattern, because `meta.ingestion_runs` already exists by this migration — RESEARCH.md's own text explicitly says "no deferred-constraint dance needed" here, lines 100).
- Self-note on the self-referencing-table risk: `resolved_by_run_id` references `meta.ingestion_runs`, NOT `meta.rejected_records` itself — this is a simple FK to an already-existing table, unlike migration 0002's genuine self-FK (`duplicate_of_file_id → meta.files.file_id`). Do not model this as a self-FK by mistake.
- `GRANT SELECT, INSERT, UPDATE ON meta.rejected_records TO etl_app` — **no `DELETE`** (same D-04 rationale as `validation_results`).
- Consider an index on `(batch_id, resolution_type)` or `(file_id, resolution_type)` to support D-06's "operators query directly via SQL" access pattern and `resolve_rejected_records_for_batch`'s `WHERE batch_id = %s AND resolution_type = 'PENDING'` — mirrors migration 0002's `ix_files_dataset_content_sha256` composite-index precedent for a predictable query shape.

---

### `migrations/versions/0016_normalized_orders.py`

**Analog:** `migrations/versions/0005_normalized_customers.py` (full file, 103 lines — copy the six embedded lineage columns **verbatim**)

**Exact lineage-column block to copy verbatim** (migration 0005, lines 52-87 — D-17/RESEARCH.md's "Record-level lineage" section, lines 102-105, requires bit-for-bit reuse):
```python
# Source: migrations/versions/0005_normalized_customers.py:52-87
sa.Column("_run_id", sa.BigInteger(), sa.ForeignKey("meta.ingestion_runs.run_id"), nullable=False),
sa.Column("_file_id", sa.BigInteger(), sa.ForeignKey("meta.files.file_id"), nullable=False),
sa.Column("_batch_id", sa.BigInteger(), sa.ForeignKey("meta.batches.batch_id"), nullable=False),
sa.Column("_source_row_number", sa.BigInteger(), nullable=False),
sa.Column("_record_hash", sa.LargeBinary(), nullable=False),
sa.Column("_record_hash_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
sa.Column("_ingested_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
```

**Business columns per D-17** (replace `customers`' five business columns with `orders`' four):
```python
# Adapted from migrations/versions/0005_normalized_customers.py:46-51
sa.Column("order_id", sa.Integer(), nullable=False),
sa.Column("customer_id", sa.Integer(), nullable=False),  # D-16: FK-like business reference,
                                                           # NOT a DB-level FK to normalized.customers --
                                                           # an orphan order (D-16 default QUARANTINE_RECORD)
                                                           # must be able to publish rows referencing a
                                                           # not-yet-loaded customer without the whole INSERT
                                                           # failing; referential integrity is enforced by
                                                           # the ReferentialIntegrityBarrier application-side,
                                                           # never a Postgres FK constraint here.
sa.Column("order_date", sa.Date(), nullable=True),
sa.Column("amount", sa.Numeric(), nullable=True),
```

**Adaptation notes:**
- `op.execute("CREATE SCHEMA IF NOT EXISTS normalized")` from migration 0005 line 41 is **not needed again** — the schema already exists after 0005; this migration only needs `op.create_table`.
- `customer_id` gets a plain, non-unique index (`ix_orders_customer_id`), mirroring `ix_customers_customer_id` (migration 0005 lines 90-96) — needed by `ReferentialIntegrityBarrier`'s anti-join query performance, not for uniqueness (uniqueness is `order_id`'s job, enforced by publish-time MERGE logic, same reasoning as customers' own docstring, migration 0005 lines 17-20).
- `GRANT SELECT, INSERT, UPDATE ON normalized.orders TO etl_app` (migration 0005 line 97 pattern).

---

### `configs/datasets/orders.yaml` (new)

**Analog:** `configs/datasets/customers.yaml` (full file, 87 lines)

**Sections to copy verbatim in shape** (`source:`/`deduplication:`/`load:`/`batching:` blocks, lines 25-39):
```yaml
# Source: configs/datasets/customers.yaml:23-39, adapted per D-13..D-17
dataset: orders
config_schema_version: 1
source:
  type: csv
  bucket: raw
  path: orders/
  change_semantics: snapshot
  duplicate_policy: skip
deduplication:
  strategy: business_key_latest
  keys: [order_id]
  order_by: [order_date desc]
load:
  strategy: merge
  target: normalized.orders
batching:
  max_units_per_run: 100
```

**`columns:` block, adapted for D-17's minimal schema** (customers.yaml lines 62-87 shape):
```yaml
columns:
  - name: order_id
    type: string
    nullable: false
    required: true
    business_key: true
    description: "Natural business key for an order record"
  - name: customer_id
    type: string
    nullable: false
    required: true
    description: "References customers.customer_id -- checked by ReferentialIntegrityBarrier, D-16"
  - name: order_date
    type: date
    nullable: true
    required: true
    format: "%Y-%m-%d"
  - name: amount
    type: decimal
    nullable: true
    required: true
```

**Adaptation notes:**
- D-17 explicitly says "same config shape as `customers.yaml`" — no new capability. `filename:`/`normalization:` blocks stay absent (matching customers.yaml's own "no filename-mask delivery shape, no numeric/currency locale needs" precedent, lines 58-61) UNLESS `amount`'s `decimal` type needs a `normalization.decimal_separator` profile — check whether `customers.yaml`'s omission still holds for a `decimal`-typed column (customers.yaml has no `decimal` columns; `orders.amount` does — this may require adding a minimal `normalization:` block, a genuine deviation planning should confirm).
- New: a `quality:` block per D-16 (orphan handling) with a `REFERENTIAL` rule type entry naming `strategy: QUARANTINE_RECORD` and `error_type: REFERENTIAL_ORPHAN` — no existing precedent in `customers.yaml` (which gets its own new `quality:` block per D-09, but a different rule set: completeness/uniqueness/validity-range/pattern, not referential). Both configs' `quality:` blocks are new this phase; do not copy one into the other's shape assuming they're identical — `orders.yaml`'s block must include the `REFERENTIAL` entry; `customers.yaml`'s must not (it has nothing to reference).

---

### `airflow/dags/csv_ingest_orders.py` (new)

**Analog:** `airflow/dags/csv_ingest_customers.py` (full file, 163 lines — mirror its shape per D-14)

**Structure to copy verbatim, substituting `orders` for `customers`**:
```python
# Source: airflow/dags/csv_ingest_customers.py:42-163 (full DAG shape)
from __future__ import annotations
import logging
import pendulum
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import Asset, dag, task
from kubernetes.client import models as k8s
from _common.integrity_gate import integrity_gate  # NEW, D-18 (shared with customers DAG)
from _common.kpo import common_kpo_kwargs
from _common.tracing_kpo import TracingKubernetesPodOperator

log = logging.getLogger(__name__)

customers_asset = Asset("s3://normalized/customers")  # D-15, declared once (in customers.py or a shared module -- see Pattern 5 below)

_DISCOVER_RESOURCES = k8s.V1ResourceRequirements(...)
_INGEST_RESOURCES = k8s.V1ResourceRequirements(...)

@task
def resolve_window(dag_run=None) -> dict[str, str | None]:
    """ORCH-05 proof, identical for an Asset-triggered run -- csv_ingest_orders
    inherits the SAME logical_date=None risk as csv_ingest_customers (RESEARCH.md
    Pattern 5's caveat) and must carry the identical proof, not a new one."""
    ...

@task
def build_ingest_args(discovered: dict) -> list[list[str]]: ...

@task
def aggregate_receipts(receipts: list[dict]) -> None: ...

@dag(
    dag_id="csv_ingest_orders",
    schedule=[customers_asset],  # D-15: Asset-triggered, NOT a cron string
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["vertical-slice", "orders"],
)
def csv_ingest_orders() -> None:
    wait_for_files = S3KeySensor(
        task_id="wait_for_files", bucket_name="raw", bucket_key="orders/*.csv",
        wildcard_match=True, aws_conn_id="minio_default", deferrable=True,
        poke_interval=30, retries=2, retry_exponential_backoff=True,
    )
    resolve_window()
    gate = integrity_gate.partial(bucket="raw").expand(key=...)  # D-18, NEW vs customers DAG
    wait_for_files >> gate
    discover = KubernetesPodOperator(
        task_id="discover", cmds=["dataplat"], arguments=["discover", "--dataset", "orders"],
        retries=2, retry_exponential_backoff=True,
        **common_kpo_kwargs(resources=_DISCOVER_RESOURCES),
    )
    gate >> discover
    ingest = TracingKubernetesPodOperator.partial(
        task_id="ingest", cmds=["dataplat"], retries=3, retry_exponential_backoff=True,
        max_active_tis_per_dag=1,
        **common_kpo_kwargs(resources=_INGEST_RESOURCES, extra_env_vars=_INGEST_EXTRA_ENV_VARS),
    ).expand(arguments=build_ingest_args(discover.output))
    aggregate_receipts(ingest.output)

csv_ingest_orders()
```

**Adaptation notes:**
- `max_active_tis_per_dag=1` and the resource sizing comment (customers.py lines 142-154) documents a real, measured cluster-capacity constraint (kind worker node CPU headroom) — this constraint applies identically to a second DAG competing for the SAME node pool; do not casually raise it for `orders` without re-checking total concurrent-pod headroom across BOTH DAGs now running.
- D-18's `integrity_gate` task must be inserted into **both** DAGs (customers gets it added as a modification; orders gets it from day one) — see `integrity_gate.py` pattern assignment below for exact task shape.
- `outlets=[customers_asset]` goes on `csv_ingest_customers.py`'s `ingest` task (the modification to the existing file), not on anything in `csv_ingest_orders.py` — `orders`' own DAG only ever *consumes* the asset via `schedule=[customers_asset]`.

---

### `airflow/dags/_common/integrity_gate.py` (new)

**Analog:** `airflow/dags/_common/kpo.py` — the established "ONLY shared code between DAGs, never business logic" precedent (module docstring, lines 1-15) and its exemption from `tests/policy/test_dag_thinness.py`'s import-based business-logic scan.

**Pattern to copy (module docstring convention + the "genuinely no business logic" framing):**
```python
# Source: airflow/dags/_common/kpo.py:1-15 (docstring shape to mirror)
"""``integrity_gate`` -- the LOAD-10 pre-pod-launch file-integrity check (D-18).

This module exists solely to share the S3-HEAD-stability-check logic between
``csv_ingest_customers.py`` and ``csv_ingest_orders.py``
(``_common/`` is for shared KPO/task-builder helpers, "NEVER business logic").
Every value here is either a boto3/S3Hook call or a literal Airflow
connection id -- nothing here parses CSV, validates a row, or writes to the
analytical database except the narrow, explicit D-20 rejection-bookkeeping
call (see RESEARCH.md Pattern 4's Open Question — flag for planning
confirmation, not silently assumed).
"""
```

**Full worked example already drafted in RESEARCH.md** (`08-RESEARCH.md` Pattern 4, lines 300-325 — copy as the starting implementation):
```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import task

@task
def integrity_gate(bucket: str, key: str) -> dict[str, object]:
    hook = S3Hook(aws_conn_id="minio_default")
    client = hook.get_conn()
    first = client.head_object(Bucket=bucket, Key=key)
    time.sleep(STABILITY_CHECK_INTERVAL_SECONDS)  # short, e.g. 5s -- config, not hardcoded
    second = client.head_object(Bucket=bucket, Key=key)
    if (first["ContentLength"], first["ETag"]) != (second["ContentLength"], second["ETag"]):
        raise AirflowFailException(f"{key}: object not stable between HEAD checks")
    return {"content_length": first["ContentLength"], "etag": first["ETag"]}
```

**Adaptation notes:**
- `aws_conn_id="minio_default"` is the SAME connection ID `S3KeySensor` already uses in both DAGs (`csv_ingest_customers.py` line 115) — no new credential wiring (RESEARCH.md Pattern 4's own note).
- **The D-20 rejection write is the one genuinely open architectural decision** — RESEARCH.md flags this explicitly as Open Question #2 (lines 513-516) and as Pattern 4's own inline "Open Questions" callout. `common_kpo_kwargs`' `DATAPLAT_DB_DSN` env var (`vault://etl/analytics-db#dsn`, `_common/kpo.py` line 114) is only ever consumed inside a POD, never by the Airflow scheduler process — a narrow inline `psycopg` call from `integrity_gate.py` would need its own DSN resolution path, which has **no existing precedent anywhere in this codebase** (RESEARCH.md's own words: "Precedent search found no existing direct-DB-write code path from an Airflow DAG file — this is a genuinely new architectural surface, not a reuse of an existing one"). Flag this for the plan to resolve explicitly, per RESEARCH.md's own recommendation (lean toward the narrow inline exception).
- `tests/policy/test_dag_thinness.py` — check whether this new file needs the same by-name exemption `_common/kpo.py` has (see that module's docstring lines 11-14); if the D-20 write requires importing `psycopg` directly, this file may need to be added to that policy test's exemption list.

---

### `tests/unit/validate/*.py` (new dir)

**Analog:** `tests/unit/normalize/test_boolean_null.py`

**Full pattern to copy** (the `_make_context()`/`_chunk()` helper convention for testing a `StreamingStage` in isolation):
```python
# Source: tests/unit/normalize/test_boolean_null.py:1-40
from __future__ import annotations
from dataplat.models.identity import RunContext
from dataplat.models.record import RecordChunk
from dataplat.pipeline.protocol import PipelineContext

def _make_context() -> PipelineContext:
    """Build a placeholder ``PipelineContext`` -- only ``run`` is real."""
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=None,  # type: ignore[arg-type] -- unused by the code under test
        metadata=None,  # type: ignore[arg-type]
        objects=None,  # type: ignore[arg-type]
        db=None,  # type: ignore[arg-type]
        log=None,  # type: ignore[arg-type]
    )

def _chunk(
    rows: list[tuple[str | bool | None, ...]], *,
    first_ordinal: int = 0, expected_field_count: int = 4,
) -> RecordChunk:
    return RecordChunk(rows=tuple(rows), first_ordinal=first_ordinal, expected_field_count=expected_field_count)
```

**Adaptation notes:**
- Each new rule's config (thresholds, column names) means `config=None` may not suffice if a rule genuinely reads `ctx.config.quality` — construct a minimal real `DatasetConfig`/`QualityConfig` fixture where needed, rather than `None`, unlike `boolean_null`'s tests (which never touch `ctx.config`).
- `tests/unit/detect/` and `tests/unit/schema/` are the other sibling per-domain directories (`tests/unit/schema/test_evolution.py`) — `tests/unit/validate/` mirrors this exact per-domain layout (RESEARCH.md Wave 0 Gaps, line 433).
- Every new test package needs an `__init__.py` (ruff `INP001` under `select = ["ALL"]` rejects implicit namespace packages — `tests/unit/normalize/__init__.py`'s own docstring explains this exact reasoning, copy verbatim).

---

### `tests/integration/test_validation_persistence.py`, `test_referential_integrity.py`, `test_backfill_resolution.py`

**Analog:** `tests/integration/test_publish_merge.py` (full file pattern) + `tests/integration/conftest.py` (fixtures: `migrated_dsn`, `s3_client`)

**Pattern to copy** (real testcontainers Postgres, hand-built staging tables independent of `StagingLoader`, `_make_context()` convention, `_insert_config_version`-style local SQL helper):
```python
# Source: tests/integration/test_publish_merge.py:1-70
from __future__ import annotations
import psycopg
import pytest
from dataplat.load.publish.merge import MergePublisher
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.pipeline.protocol import PipelineContext
from dataplat.storage.db import create_pool

def _make_context() -> PipelineContext:
    """A fully placeholder ``PipelineContext``..."""
    ...

def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic `meta.config_versions` row directly via SQL.
    Duplicated locally rather than imported, matching this test suite's
    existing per-file helper convention."""
    ...
```

**Adaptation notes:**
- Depend on `migrated_dsn` (session-scoped, `alembic upgrade head` already applied — `conftest.py` lines 134-145), never re-run migrations per-test.
- For `test_referential_integrity.py`: hand-build both `staging.orders__r<n>` AND populate real rows in `normalized.customers` directly via SQL (matching this pattern's "hand-built staging tables... independent of `StagingLoader`'s own implementation" convention) to construct the orphan-vs-non-orphan race scenario Pitfall 5 describes.
- For `test_backfill_resolution.py`: call `resolve_rejected_records_for_batch` directly (the `dataplat`-level function a real backfill's publish path calls) — Pitfall 3 explicitly recommends this as the tier that proves resolution-state-transition logic WITHOUT Airflow involved at all.
- Mark every new file with `@pytest.mark.integration` per the existing marker convention (`pyproject.toml` line 194).

---

### `tests/dagtest/conftest.py` + `tests/dagtest/test_backfill_dagrun.py` (NEW tier — no close analog)

**Closest partial analogs (neither is a full match — this is a genuinely new test tier per RESEARCH.md Pitfall 3):**
1. `tests/integration/conftest.py` — for the testcontainers-Postgres-container lifecycle pattern (`_require_docker` skip-with-reason, session-scoped container fixture shape — copy the STRUCTURE, but this new conftest needs an Airflow **metadata** DB, not the analytical DB `tests/integration/conftest.py` provides; RESEARCH.md Wave 0 Gaps is explicit: "distinct from `tests/integration/conftest.py`'s *analytical* DB fixture — do not conflate the two databases, CLAUDE.md's own §4 constraint").
2. `tests/unit/conftest.py` — for the `DagBag`/`sys.path`/`AIRFLOW_VAR_*` bootstrap pattern (lines 49-67) — `dag.test()` needs the same `sys.path` setup this fixture already does, but ALSO needs `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` pointed at a real (testcontainers) metadata DB and `AIRFLOW__CORE__EXECUTOR=LocalExecutor`, which `tests/unit/conftest.py`'s `dagbag` fixture does not set up (it only parses DAGs structurally, never executes one).
3. CLAUDE.md's own "Testing Airflow 3 DAGs" section (project-committed, authoritative) — the `dag.test()` entry point and "mock `KubernetesPodOperator.execute`, assert on the constructed pod spec" pattern is documented there but has **zero existing implementation anywhere in this repo** (RESEARCH.md's own confirmed finding: "no existing `dag.test()` usage" in the codebase, Sources section line 562).

**No Analog Found — build from CLAUDE.md's documented guidance directly, not from a codebase precedent:**
```python
# CLAUDE.md's own documented pattern (project-committed, authoritative):
# - dag.test() is the Airflow 3 entry point; needs a metadata DB via a
#   session-scoped testcontainers PostgreSQL + AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
# - AIRFLOW__CORE__EXECUTOR=LocalExecutor
# - Mock the KPO: patch KubernetesPodOperator.execute and assert on the
#   constructed pod spec (image tag, SA name, resources, env)
```
Add the new `dagtest` pytest marker to `pyproject.toml`'s `markers` list (mirroring the `integration`/`cluster` marker precedent, `pyproject.toml` line 194): `"dagtest: needs a local Docker daemon (testcontainers PostgreSQL for Airflow metadata); excluded from the offline gate"`.

---

### `tests/e2e/slice/test_referential_orphan.py`, `test_backfill_reentry.py`

**Analog:** `tests/e2e/slice/conftest.py` (fixtures: `_require_cluster`, `s3_client`, `analytics_connection`) + an existing sibling test file's shape (`test_smoke_and_idempotency.py`/`test_pod_kill_retry.py` — both real cluster-driving E2E tests in the same directory)

**Pattern to copy:** `_require_cluster` skip-with-reason (imported from `tests/e2e/cluster/conftest.py`, never re-derived — `conftest.py`'s own docstring lines 1-13 states this explicitly), the `kubectl`/`kubectl_json` helper reuse, and the `role="etl_app"` connection fixture (matching what the real pipeline pods authenticate as, per `conftest.py`'s own docstring lines 20-27).

**Adaptation notes:**
- Mark `@pytest.mark.cluster` (existing marker, `pyproject.toml` line 192).
- These are the ONLY tests in this phase that drive a real `airflow dags backfill` CLI invocation and a real orphan-order race — everything else (unit, integration, dagtest) explicitly stops short of this per Pitfall 3's three-tier split.

## Shared Patterns

### Text-not-enum for every new enum-like column
**Source:** `migrations/versions/0009_meta_schema_versions.py` docstring (lines 5-11), cross-confirmed against all 13 prior migrations
**Apply to:** `meta.validation_results.rule_type`/`.outcome`/`.severity`, `meta.rejected_records.error_type`/`.resolution_type`
```sql
-- Every enum-like column: sa.Text(), app-validated via Pydantic Literal.
-- Never CREATE TYPE ... AS ENUM. Zero native enums exist in 13 prior migrations.
sa.Column("outcome", sa.Text(), nullable=False),
```

### Config-not-code registry dispatch, never an if/elif chain
**Source:** `packages/dataplat/src/dataplat/load/publish/registry.py` (full file)
**Apply to:** `validate/registry.py`'s `VALIDATION_RULE_REGISTRY`
```python
def resolve_publisher(strategy: str) -> Publisher:
    try:
        return PUBLISHER_REGISTRY[strategy]
    except KeyError:
        raise ConfigurationError(msg, context={"strategy": strategy, "known": sorted(PUBLISHER_REGISTRY)}) from None
```

### `StreamingStage.apply()` never raises for a row-level problem (QUAL-03)
**Source:** `packages/dataplat/src/dataplat/pipeline/protocol.py` lines 88-103 (Protocol docstring) + `pipeline/engine.py`'s `RaggedRowGuard` (concrete proof)
**Apply to:** every new `validate/*.py` `StreamingStage`
> "Must never raise for a row-level problem (QUAL-03): a malformed row becomes a `RejectedRecord` inside the returned `StageResult` instead of aborting the run."

### Publish-transaction is the ONLY place validation/rejection rows are written (D-11)
**Source:** `packages/dataplat/src/dataplat/pipeline/run.py` lines 375-413
**Apply to:** `pipeline/run.py`'s extension, `quarantine/resolution.py`, `metadata/postgres.py`'s new methods
> Every new persistence call for `meta.validation_results`/`meta.rejected_records` must execute against the SAME `conn` inside `with (tracing.start_span("pipeline.publish"), ctx.db.connection() as conn, conn.transaction()):` — never a separately-opened connection, which would defeat D-11's all-or-nothing rollback guarantee.

### No `DELETE` grant on quarantine tables (D-04 enforced at the DB-privilege level)
**Source:** `migrations/versions/0002_meta_files.py` line 76, `0005_normalized_customers.py` line 97 (the `GRANT SELECT, INSERT, UPDATE` — never `DELETE` — convention)
**Apply to:** `migrations/0014_meta_validation_results.py`, `0015_meta_rejected_records.py`
```sql
GRANT SELECT, INSERT, UPDATE ON meta.rejected_records TO etl_app
-- No DELETE grant: matches D-04's no-per-row-edit constraint at the
-- database-privilege level, not just application logic.
```

### `_common/*.py` is shared Airflow glue only, never business logic
**Source:** `airflow/dags/_common/kpo.py` module docstring (lines 1-15)
**Apply to:** `airflow/dags/_common/integrity_gate.py`
> "This module exists solely to remove boilerplate duplication... nothing here parses CSV, validates a row, or writes to a database" — the D-20 rejection-write decision (see Open Question above) is the one place this convention is genuinely under tension for the new file; resolve explicitly, do not silently violate it.

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| `packages/dataplat/src/dataplat/validate/referential.py`, `circuit_breaker.py` (concrete `BarrierStage` bodies) | pipeline stage | barrier, run-scoped | `BarrierStage` Protocol exists (`pipeline/protocol.py`) but has ZERO concrete implementations anywhere in the codebase today — this phase writes the first ones. Use `RaggedRowGuard`'s code-shape conventions (imports, `name`, metrics labels) plus `MergePublisher`'s parameterized-SQL discipline as the nearest composite analog; RESEARCH.md Pattern 2's own worked sketch (lines 256-268) is the most concrete starting point available. |
| `tests/dagtest/conftest.py`, `tests/dagtest/test_backfill_dagrun.py` | test (new tier) | integration (`dag.test()`) | Genuinely new test tier — RESEARCH.md's own confirmed finding is "no existing `dag.test()` usage" anywhere in this repo. Build from CLAUDE.md's documented "Testing Airflow 3 DAGs" guidance directly (KPO-mocking pattern, testcontainers metadata DB), not from a codebase precedent. Partial structural analogs are `tests/integration/conftest.py` (container lifecycle) and `tests/unit/conftest.py` (`DagBag`/`sys.path` bootstrap) — see Pattern Assignments above for what to borrow from each. |
| `airflow/dags/_common/integrity_gate.py`'s D-20 rejection-write path | shared helper (DB write) | request-response → DB write | RESEARCH.md's own Open Question #2 (lines 513-516): "Precedent search found no existing direct-DB-write code path from an Airflow DAG file — this is a genuinely new architectural surface, not a reuse of an existing one." Every other DAG-side write to the analytical DB happens inside a `dataplat`-image pod (KPO), never the Airflow process itself; this gate is the first proposed exception, and the plan must decide explicitly (narrow inline `psycopg` call vs. a minimal purpose-built KPO) rather than silently copying an existing pattern that doesn't exist. |

## Metadata

**Analog search scope:** `packages/dataplat/src/dataplat/` (all subpackages), `migrations/versions/` (all 13 prior revisions), `airflow/dags/` (both DAG files + `_common/`), `configs/datasets/customers.yaml`, `tests/unit/`, `tests/integration/`, `tests/e2e/slice/`
**Files scanned:** ~45 (full reads: `pipeline/protocol.py`, `pipeline/engine.py`, `pipeline/run.py` (partial), `models/record.py`, `models/report.py`, `errors.py`, `config/registry.py`, `config/model.py` (partial), `load/publish/registry.py`, `load/publish/merge.py`, `metadata/repository.py`, `metadata/postgres.py` (partial), migrations 0002/0004/0005/0009/0010, `configs/datasets/customers.yaml`, `airflow/dags/csv_ingest_customers.py`, `airflow/dags/_common/kpo.py`, `tests/integration/conftest.py`, `tests/integration/test_publish_merge.py` (partial), `tests/unit/conftest.py`, `tests/unit/test_dag_structure.py`, `tests/unit/normalize/test_boolean_null.py` (partial), `tests/e2e/slice/conftest.py` (partial); directory listings for the rest)
**Pattern extraction date:** 2026-08-17
