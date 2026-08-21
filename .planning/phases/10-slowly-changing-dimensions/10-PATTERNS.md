# Phase 10: Slowly Changing Dimensions - Pattern Map

**Mapped:** 2026-08-21
**Files analyzed:** 17 (7 new, 10 modified/replaced)
**Analogs found:** 17 / 17

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `packages/dataplat/src/dataplat/load/publish/scd.py` (NEW, `SCDPublisher`) | service (Publisher) | CRUD (recompute-and-replace) | `packages/dataplat/src/dataplat/load/publish/merge.py` (`MergePublisher`) | exact (same Protocol, same call site, same advisory-lock convention) |
| `packages/dataplat/src/dataplat/scd/recompute.py` (NEW) | utility (pure transform) | transform (ordered rows → version chain) | `packages/dataplat/src/dataplat/load/staging.py` lines 604-693 (per-row hash + enrichment loop) | role-match (closest existing "read ordered rows, compute derived columns" logic) |
| `packages/dataplat/src/dataplat/scd/delete_detection.py` (NEW) | service (barrier/circuit-breaker) | batch (snapshot diff) | `packages/dataplat/src/dataplat/validate/circuit_breaker.py` (`RejectionRateCircuitBreaker`) + `packages/dataplat/src/dataplat/validate/referential.py` (`ReferentialIntegrityBarrier`, for the anti-join shape) | exact (named as direct template in CONTEXT.md D-06) |
| `packages/dataplat/src/dataplat/scd/hashing.py` (NEW) | utility (hashing) | transform | `packages/dataplat/src/dataplat/load/staging.py` lines 653-672 (`_record_hash` computation) | role-match (the ONLY existing hash-computation precedent; `dataplat.normalize.unicode` is a normalization stage, not a hash helper — see Finding below) |
| `migrations/versions/0035_normalized_customers_scd2.py` (NEW) | migration | DDL + backfill | `migrations/versions/0006_normalized_customers_business_key_unique.py` (constraint swap) + `migrations/versions/0005_normalized_customers.py` (original table DDL) | exact (0006 is literally the constraint this migration supersedes) |
| `packages/dataplat/src/dataplat/load/publish/registry.py` (MODIFIED) | config/registry | — | itself (add one dict entry, same file) | exact |
| `packages/dataplat/src/dataplat/pipeline/run.py::_compute_silver_gold_reconciliation` (MODIFIED, ~line 293) | service | CRUD (aggregate reconciliation) | itself + `record_watermark`'s `_run_id = ANY(%s)` scoping (`metadata/repository.py` line 910) | exact (same function, needs run-scoping + multi-row-per-key awareness) |
| `packages/dataplat/src/dataplat/metadata/repository.py::record_reconciliation` (MODIFIED, ~line 990) | service | CRUD | itself | exact |
| Migration touching `meta.v_customers_lineage` (is_current filter) | migration | DDL (view redefinition) | `migrations/versions/0030_fix_v_customers_lineage_dedup_audit_model_name.py` (most recent "drop+recreate full view" precedent) | exact |
| `packages/dataplat/src/dataplat/scd/delete_detection.py`'s `MassDeleteCircuitBreaker` (same file as above) | service (BarrierStage) | batch | `packages/dataplat/src/dataplat/validate/circuit_breaker.py::RejectionRateCircuitBreaker` | exact (named template, CONTEXT.md D-06) |
| `configs/datasets/customers.yaml` (MODIFIED) | config | — | itself (existing file, add `signup_country` column + DELETE-semantics + circuit-breaker config keys) | exact |
| `packages/dataplat/src/dataplat/config/model.py` (MODIFIED — new config fields for DELETE semantics/circuit-breaker threshold) | model (Pydantic) | — | `QualityConfig`/`ReconciliationConfig` (opt-in nested `BaseModel` blocks, `extra="forbid", frozen=True`) | exact |
| `tests/integration/test_publish_merge.py` (REPLACE → `test_publish_scd.py`) | test | — | itself (structure: `_make_context()`, hand-built staging tables, `testcontainers` PostgreSQL) | exact (structural template for the new file) |
| `tests/integration/test_publish_ingest.py`, `test_run_ingest.py`, `test_reconciliation.py` (MODIFIED) | test | — | themselves | exact |
| `tests/integration/test_migrations.py` (MODIFIED — exists, verified) | test | — | itself, specifically `test_0006_customer_id_has_a_real_unique_constraint` (lines 273-325) and `test_gold_indexes_exist_and_business_key_uniqueness_is_unchanged` (line 720) | exact |
| `tests/e2e/slice/test_backfill_2year_sweep.py` (MODIFIED, extends) | test (e2e) | — | itself + `tools/corpus/dated_series.py` (`generate_dated_series`) | exact |
| `tools/corpus/dated_series.py` (MODIFIED — extend for attribute-change/late-correction/missing-customer/bad-snapshot fixtures) | utility (fixture generator) | batch | itself (existing anomaly-injection pattern: gap day, schema-change day, late event) | exact |

## Pattern Assignments

### `packages/dataplat/src/dataplat/load/publish/scd.py` (service, CRUD/recompute)

**Analog:** `packages/dataplat/src/dataplat/load/publish/merge.py`

**Imports pattern** (merge.py lines 33-42):
```python
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dataplat.load.publish.protocol import Publisher, PublishResult

if TYPE_CHECKING:
    from psycopg import Connection

    from dataplat.pipeline.protocol import PipelineContext
```

**Class/Protocol conformance pattern** (merge.py lines 75-132, full class) — `SCDPublisher` must implement the exact same `Publisher` Protocol shape:
```python
class MergePublisher(Publisher):
    name = "merge"

    def publish(
        self,
        ctx: PipelineContext,
        source_table: str,
        conn: Connection[Any],
    ) -> PublishResult:
        cursor = conn.execute(_PUBLISH_SQL.format(staging_table=source_table))
        published_business_keys = tuple(str(row[0]) for row in cursor.fetchall())
        return PublishResult(
            rows_affected=cursor.rowcount,
            outcome="PUBLISHED",
            published_business_keys=published_business_keys,
        )
```
**Critical divergence to document in the new module's docstring:** `MergePublisher.publish()` never opens its own connection or commits/rolls back (`conn` carries the caller's already-open transaction, `pg_advisory_xact_lock` already held by `publish_ingest`'s caller before `publisher.publish()` is invoked — see `pipeline/run.py` lines 1055-1074 below). `SCDPublisher.publish()` must follow the identical ownership split even though its body does far more (DELETE-detection sweep + per-key recompute), never taking its own lock and never committing.

**SQL identifier-interpolation discipline** (merge.py lines 44-49, T-04-01 threat model) — every dynamic SQL fragment in `scd.py` must be a config/run-derived IDENTIFIER, never row content:
```python
# The ONLY dynamic SQL fragment is `{staging_table}`, and it is interpolated as an
# IDENTIFIER only, never a value (T-04-01, this plan's threat model):
# `staging_table` is built by `StagingLoader` from `ctx.config.dataset` + a
# numeric `run_id`, never from CSV row content.
```

**Caller wiring pattern** (`pipeline/run.py` lines 1055-1074) — `SCDPublisher` is invoked exactly where `MergePublisher` is today, via `resolve_publisher`:
```python
with (
    tracing.start_span("pipeline.publish"),
    ctx.db.connection() as conn,
    conn.transaction(),
):
    conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (f"publish:{ctx.config.load.target}",),
    )
    publisher = resolve_publisher(ctx.config.load.strategy)
    source_table = f"silver.{ctx.config.dataset}"
    result = publisher.publish(ctx, source_table, conn)
```
**Load-bearing finding (Pitfall 1/F-1, RESEARCH.md):** unlike `MergePublisher`, `SCDPublisher.publish()` CANNOT do all its work against `source_table` (`silver.customers`) alone — it must also open a second read against `staging.customers` (migration 0022's durable bronze table) for the per-key recompute, since `silver.customers` collapses history to one row per key. Document this divergence explicitly; do not silently assume `source_table` is sufficient the way `merge.py`'s docstring implies.

**Error handling pattern:** No explicit try/except in `merge.py`/`merge_orders.py` — errors (`psycopg.errors.ExclusionViolation`, `QualityThresholdExceeded` from the mass-delete breaker) are allowed to propagate uncaught out of `publish()`, letting the caller's `conn.transaction()` context manager roll back the whole publish transaction (same "catches nothing" contract `publish_ingest`'s own docstring states, `pipeline/run.py` line 1015).

---

### `packages/dataplat/src/dataplat/load/publish/merge_orders.py` — secondary analog for the DELETE-detection anti-join style

**Why included:** its module docstring documents the exact three-valued-NULL-logic bugfix class (Pitfall 5, RESEARCH.md) the recompute's `LAG()`/hash-comparison logic must apply:
```python
# `normalized.orders.order_date IS NULL OR ...` (CR-01-adjacent finding,
# phase-08 code review, WR-04): unlike `merge.py`'s `event_ts` (declared
# `nullable: false`), `order_date` IS `nullable: true` (orders.yaml) --
# plain `EXCLUDED.order_date >= normalized.orders.order_date` is NULL
# (not TRUE) in three-valued SQL logic whenever the EXISTING row's
# order_date is NULL, so the whole `WHERE` clause would evaluate NULL and
# the row would be "locked but left unchanged" FOREVER
```
Apply the same discipline: use `IS DISTINCT FROM` for hash-change comparison (NULL-safe by design), and verify `name`/`country`/`event_ts` DB-level nullability before relying on ordering comparisons in `LAG() OVER (ORDER BY event_ts)`.

---

### `packages/dataplat/src/dataplat/scd/recompute.py` (utility, pure transform)

**Analog:** `packages/dataplat/src/dataplat/load/staging.py` (`StagingLoader.load()`, lines 600-693) — the closest existing "read ordered rows, derive columns, emit enriched rows" logic in this codebase, even though it's a streaming COPY loader, not a pure function.

**Core pattern to borrow** (staging.py lines 648-672) — hash computed once, per row, over normalized fields in declared column order:
```python
staged_row = (
    row[: len(self._target_columns)]
    if len(row) > len(self._target_columns)
    else row
)
record_hash = hashlib.sha256(
    "|".join(
        "" if field is None else str(field) for field in staged_row
    ).encode("utf-8"),
).digest()
```
**Design guidance (RESEARCH.md Assumption A2):** unlike `staging.py`, `recompute.py` should be written as a pure function accepting plain ordered records (not a live cursor/connection) — RESEARCH.md's own Wave-0 Gaps table explicitly calls for `tests/unit/test_scd_recompute.py` to be DB-free. Keep the SQL shape (Pattern 2 in RESEARCH.md, the `LEAD()`/change-point CTE) as the *illustrative* SQL sketch only if the recompute is done in SQL; if done in Python, mirror `staging.py`'s "compute once, in Python, never recomputed elsewhere" discipline instead.

---

### `packages/dataplat/src/dataplat/scd/delete_detection.py` (service, BarrierStage)

**Analog 1 (circuit breaker shape):** `packages/dataplat/src/dataplat/validate/circuit_breaker.py::RejectionRateCircuitBreaker` (full file read, 148 lines) — CONTEXT.md D-06 names this as the DIRECT implementation template.

**Constructor + apply() pattern** (circuit_breaker.py lines 41-129):
```python
class RejectionRateCircuitBreaker(BarrierStage):
    name = "rejection_rate_circuit_breaker"

    def __init__(
        self,
        *,
        threshold: float,
        total_rows_read: int,
        total_rows_rejected: int,
        rule_id: str = "rejection_rate_circuit_breaker",
    ) -> None:
        self._threshold = threshold
        self._total_rows_read = total_rows_read
        self._total_rows_rejected = total_rows_rejected
        self._rule_id = rule_id

    def apply(self, ctx: PipelineContext) -> StageResult:
        del ctx  # unused -- totals come from the constructor, see module docstring
        placeholder_chunk = RecordChunk(rows=(), first_ordinal=0, expected_field_count=0)
        if self._total_rows_read == 0:
            return StageResult(chunk=placeholder_chunk, rejected=[], findings=[...])  # trivial PASS
        ratio = self._total_rows_rejected / self._total_rows_read
        if ratio > self._threshold:
            raise QualityThresholdExceeded(msg, context={...})
        return StageResult(chunk=placeholder_chunk, rejected=[], findings=[...])  # PASS
```
Map directly: `total_rows_read`→`current_count` (currently-`is_current=true` customers), `total_rows_rejected`→`vanished_count`. RESEARCH.md's own `MassDeleteCircuitBreaker` code example (lines 478-497) already performs this exact mapping — treat it as the literal starting draft, not a fresh design.

**Analog 2 (run-scoped anti-join SQL shape):** `packages/dataplat/src/dataplat/validate/referential.py::ReferentialIntegrityBarrier` (full file read, 204 lines) — a `BarrierStage` whose `apply()` runs a parameterized anti-join and turns unmatched rows into structured findings:
```python
_ANTI_JOIN_SQL = """
SELECT s.customer_id, s._source_row_number, s.order_id
FROM   {staging_table} s
LEFT JOIN {target_table} t
       ON s.{staging_column}::int = t.{target_column}
WHERE  t.{target_column} IS NULL
"""
```
Structural template for the snapshot-diff query (RESEARCH.md Pattern 3):
```sql
WITH this_run_snapshot AS (
    SELECT DISTINCT customer_id
    FROM   silver.customers
    WHERE  _run_id = ANY(%(staged_run_ids)s)
),
vanished AS (
    SELECT c.customer_id
    FROM   normalized.customers c
    WHERE  c.is_current
      AND  c.customer_id NOT IN (SELECT customer_id FROM this_run_snapshot)
)
SELECT count(*) FROM vanished;
```
**Critical scoping requirement (Finding F-2/Pitfall 2):** the `silver.customers` read side of this diff MUST be scoped `WHERE _run_id = ANY(%(staged_run_ids)s)` — an unscoped whole-table read (which is what `_compute_silver_gold_reconciliation` and `MergePublisher`'s own `_PUBLISH_SQL` both currently do, by established convention) makes DELETE-detection permanently vacuous, since `silver.customers` never drops a business key it stops seeing. `staged_run_ids` is already computed by `publish_ingest` (`pipeline/run.py` line 1084: `staged_run_ids = [run_id for run_id, _, _, _ in staged]`) — thread it through, do not recompute independently.

**Reused run-scoping SQL precedent** (`metadata/repository.py` `record_watermark` docstring, lines 908-910):
```sql
INSERT INTO meta.watermarks (dataset_id, target_key, cursor_value)
VALUES (%s, %s, (SELECT max({watermark_column}::timestamptz)
                  FROM {source_table} WHERE _run_id = ANY(%s)))
```

---

### `packages/dataplat/src/dataplat/scd/hashing.py` (utility, transform)

**Analog:** `packages/dataplat/src/dataplat/load/staging.py` lines 653-672 (`_record_hash` computation) — this is the ONLY real hash-computation precedent in the codebase.

**Finding (correcting RESEARCH.md's Code Examples section):** RESEARCH.md's suggested `from dataplat.normalize.unicode import normalize_for_hash` does **not exist** — `packages/dataplat/src/dataplat/normalize/unicode.py` defines `UnicodeNormalizer`, a `StreamingStage` class (`apply(ctx, chunk) -> StageResult`) that NFC-normalizes every `str` field of a `RecordChunk` in place; it is not a standalone importable function. Verified by direct read of the full 104-line file — no `normalize_for_hash` symbol exists anywhere in the module. Two real options for `hashing.py`, in order of closeness to existing precedent:
1. **Preferred:** rely on the fact that by the time rows reach `staging.customers` (bronze), `UnicodeNormalizer` has ALREADY run as part of `_build_stages` (staging.py's own pipeline), so `name`/`country` values read back out of `staging.customers` for the recompute are already NFC-normalized — `hashing.py` then just needs `staging.py`'s exact hash-computation shape (pipe-joined SHA-256), not a second normalization pass:
```python
tracked_hash = hashlib.sha256(
    "|".join("" if v is None else str(v) for v in (name, country)).encode("utf-8"),
).digest()
```
2. If a defense-in-depth re-normalization is wanted at recompute time (values are being re-read from a different table than the original hash was computed against), call `unicodedata.normalize("NFC", field) if isinstance(field, str) else field` directly (the exact one-liner `UnicodeNormalizer.apply()` itself uses, `unicode.py` line 99) rather than inventing a new helper name.

**Do NOT reuse `_record_hash`/`_record_hash_version` values stored on old rows directly** — RESEARCH.md Open Question 3 flags this explicitly: `_record_hash_version` exists specifically so the hash recipe can change over time (META-02), so a naive "compare stored hashes" approach breaks the moment the recipe changes. Recompute the tracked-attribute hash fresh, under ONE consistent recipe, for the whole history at recompute time.

---

### `migrations/versions/0035_normalized_customers_scd2.py` (migration, DDL + backfill)

**Analog 1 (the constraint this migration replaces):** `migrations/versions/0006_normalized_customers_business_key_unique.py` (full file, 65 lines) — read in full above. Docstring convention to mirror:
```python
"""normalized.customers.customer_id — from a plain index to a real UNIQUE constraint.

Migration 0005's docstring reasoned that ... [explain WHY the prior constraint
existed and WHY this migration supersedes it, not just WHAT changes]

Revision ID: 0006
Revises: 0005
Create Date: 2026-08-13
"""
```
Apply the identical citation discipline: explain that `UNIQUE(customer_id)` (0006) is dropped because `ON CONFLICT (customer_id)` can no longer be this table's publish target once >1 row per key is legal (D-07), replaced by `EXCLUDE USING gist (customer_id WITH =, validity WITH &&)`.

**Analog 2 (original table DDL, for exact column-type consistency):** `migrations/versions/0005_normalized_customers.py` (full file, 103 lines) — `customer_id` is `sa.Integer()`, `event_ts` is `sa.DateTime(timezone=True)`, `name`/`country`/`birth_date` are `sa.Text()`/`sa.Date()`, all `nullable=True` except `customer_id`. New columns (`valid_to`, `is_current`, `validity`, and D-13's `signup_country`) must match this repo's existing `sa.Column(...)` style exactly, including `server_default=sa.text(...)` for backfill values.

**Full worked example already drafted in RESEARCH.md** (Pattern 1, ~lines 194-291) — treat as a strong starting draft, not verified-correct: uses `op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")`, `op.drop_constraint("uq_customers_customer_id", ...)`, `op.alter_column("event_ts", nullable=False, ...)`, generated `STORED` `tstzrange` column via raw `op.execute`, and `op.create_exclude_constraint(...)`. **Verify at plan/execution time** (RESEARCH.md's own flagged Assumption A1): the exact `using="gist"` kwarg spelling for Alembic `1.19.1`.

**New grant needed** (RESEARCH.md Security Domain, V4): migration 0005's original grant was `GRANT SELECT, INSERT, UPDATE ON normalized.customers TO etl_app` (line 97) — this phase's `DELETE`+`INSERT` recompute pattern requires adding `GRANT DELETE ON normalized.customers TO etl_app` in the new migration; verify via `\dp normalized.customers` that it is not already covered.

**D-13's `signup_country` Type-0 column** — a plain new `sa.Column("signup_country", sa.Text(), nullable=True)` alongside the others in this same migration (or a documented follow-up, per CONTEXT.md D-13's own text), following migration 0005's exact per-column style.

---

### Migration touching `meta.v_customers_lineage` (`is_current` filter)

**Analog:** `migrations/versions/0030_fix_v_customers_lineage_dedup_audit_model_name.py` (full file, 168 lines) — the MOST RECENT link in the view's edit chain (0012 → 0026 → 0030), and the clearest template for "drop + recreate the full view, verbatim column list except the one changed clause."

**Pattern to copy exactly** (0030 lines 154-167):
```python
def upgrade() -> None:
    """Drop + recreate the view with the corrected ... predicate."""
    op.execute("DROP VIEW meta.v_customers_lineage")
    op.execute(_FIXED_CREATE_VIEW)
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO etl_app")
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO grafana_reader")


def downgrade() -> None:
    """Drop + recreate migration 0026's (buggy) view verbatim -- a genuine reversal."""
    op.execute("DROP VIEW meta.v_customers_lineage")
    op.execute(_BUGGY_CREATE_VIEW)
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO etl_app")
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO grafana_reader")
```
**Discipline confirmed across all three predecessor migrations (0012/0026/0030):** Postgres has no `ALTER VIEW ... ADD COLUMN`/no predicate-only change either — always full `DROP VIEW` + `CREATE VIEW` with the ENTIRE column list copied verbatim except the one changed thing (here: add `AND c.is_current` to the base `FROM normalized.customers c` join, or filter it into a `WHERE c.is_current` clause). `downgrade()` must restore the PRIOR migration's view text verbatim (0030's own `_BUGGY_CREATE_VIEW` constant is literally migration 0026's output) — this is a strict, unbroken convention across all three predecessors; the new migration's `downgrade()` should restore 0030's `_FIXED_CREATE_VIEW` verbatim as its own "buggy" (pre-is_current-filter) constant.

**Grants unchanged:** always exactly `GRANT SELECT ... TO etl_app` + `GRANT SELECT ... TO grafana_reader`, never more (migration 0026's docstring explicitly notes `dbt_app` is deliberately NOT granted).

---

### `packages/dataplat/src/dataplat/load/publish/registry.py` (config/registry, MODIFIED)

**Analog:** itself — the existing 56-line file, full pattern already shown above (`PUBLISHER_REGISTRY` dict + `resolve_publisher`). Add exactly one entry:
```python
from dataplat.load.publish.scd import SCDPublisher
...
PUBLISHER_REGISTRY: dict[str, Publisher] = {
    "merge": MergePublisher(),
    "merge_orders": OrdersMergePublisher(),
    "scd": SCDPublisher(),
}
```
Update the module docstring's "Two entries today" line to "Three entries" and update `configs/datasets/customers.yaml`'s `load.strategy` from `merge` to `scd`.

---

### `packages/dataplat/src/dataplat/pipeline/run.py::_compute_silver_gold_reconciliation` (MODIFIED, ~line 293)

**Analog:** itself (full function read, lines 293-396) — currently does whole-table `count(*)`, `sum()`, `min/max`, `count(DISTINCT business_key_column)` against both `source_table` and `target_table` unconditionally.

**What must change (D-08 item 2):** `key_count_output` (line 375-379, `count(DISTINCT customer_id) FROM normalized.customers`) remains valid as-is (DISTINCT already handles multi-row-per-key correctly) but `output_count` (line 337, plain `count(*) FROM normalized.customers`) will now legitimately exceed `key_count_output` once SCD2 versions exist — the reconciliation math (`discrepancy = input_count - (output_count + dedup_count)`, per `test_reconciliation.py` line 440-442) needs to either compare `input_count` against `key_count_output` (current-versions-only) instead of raw `output_count`, or add a new `is_current`-scoped count. **Document the choice explicitly** — do not silently change the meaning of an existing named field (`output_count`) without updating its docstring (lines 316-330) to match.

**Existing test this logic must keep passing** (`tests/integration/test_reconciliation.py` lines 420-441) — this comment already documents that the "silver count == normalized count" invariant is NOT suite-wide, only a same-moment truth-reporting check:
```python
assert row["input_count"] == _table_row_count(env.migrated_dsn, "silver.customers")
assert row["output_count"] == _table_row_count(env.migrated_dsn, "normalized.customers")
assert row["discrepancy"] == row["input_count"] - (row["output_count"] + row["dedup_count"])
```

---

### `packages/dataplat/src/dataplat/metadata/repository.py::record_reconciliation` (MODIFIED, ~line 990)

**Analog:** itself — signature/column list must accept whatever new field(s) `_compute_silver_gold_reconciliation` above adds (e.g. a `key_count_output`-vs-`output_count` distinction), following the exact `# noqa: PLR0913 -- one keyword per meta.reconciliation_results column this writes` convention already on this method (line 990).

---

### `configs/datasets/customers.yaml` (MODIFIED)

**Analog:** itself — full file read above (119 lines). Additions needed, following the file's own commenting convention (every block has a `# block_name: is D-XX's ...` explanatory header comment):
1. `columns:` block — add `signup_country` (D-13, Type 0) as a new `ColumnContract` entry, `type: string`, following the exact style of the existing `country` entry (lines 105-108).
2. New config block for DELETE semantics (D-05: default `invalidate`) and the D-06 mass-delete circuit-breaker threshold — model after the existing `quality:` block's own comment-then-YAML style (lines 47-81); these become new fields on `DatasetConfig` (see `config/model.py` below), never hardcoded in Python.
3. `load.strategy: merge` → `load.strategy: scd` (line 32).

---

### `packages/dataplat/src/dataplat/config/model.py` (MODIFIED — DELETE-semantics + circuit-breaker config fields)

**Analog:** `ReconciliationConfig`/`QualityConfig` — the existing opt-in-nested-`BaseModel` pattern (lines 127-147 for `ReconciliationConfig` shown above):
```python
class ReconciliationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    sum_columns: list[str]
```
A new `ScdConfig`-shaped block (or fields folded onto `LoadConfig`) should follow this exact `ConfigDict(extra="forbid", frozen=True)` + plain-`str`-for-registry-resolved-values convention (module docstring lines 10-15: "Strategy/source fields ... are plain `str`, resolved through string-keyed registries elsewhere ... never a Python `Enum`"). DELETE semantics (`ignore | invalidate | new_record`) should validate as a closed enum-like string per RESEARCH.md's Security Domain V5 note ("matching `hop`'s app-validated-vocabulary convention") — check how `hop` is validated elsewhere in this file (search `Literal` usage, e.g. `_COLUMN_TYPES` at line 46) versus the plain-`str` registry pattern, and pick consistently.

---

### `tests/integration/test_publish_merge.py` → REPLACE with `tests/integration/test_publish_scd.py`

**Analog:** itself (obsolete, but its structural skeleton is the direct template for the new file) — 843 lines, testcontainers PostgreSQL against real migrations.

**Structural pattern to copy** (lines 1-70 shown above):
```python
"""Integration tests for ``dataplat.load.publish.merge.MergePublisher`` (LOAD-09, 04-04 Task 2).

Every positive-path test drives a real ``MergePublisher`` against a real
testcontainers PostgreSQL, migrated to head, publishing hand-built staging
tables (raw SQL, independent of ``dataplat.load.staging.StagingLoader``'s
own implementation -- keeping this task's tests self-contained) into
``normalized.customers``.
"""
...
_ADVISORY_LOCK_KEY = "normalized.customers"
_STAGING_COLUMNS_DDL = """
    customer_id text, name text, country text, birth_date text, event_ts text,
    _run_id bigint, _file_id bigint, _batch_id bigint,
    _source_row_number bigint, _record_hash bytea, _record_hash_version smallint
"""

def _make_context() -> PipelineContext:
    """A fully placeholder ``PipelineContext`` -- ``MergePublisher.publish()`` uses no field on it."""
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=None,  # type: ignore[arg-type]
        metadata=None,  # type: ignore[arg-type]
        objects=None,  # type: ignore[arg-type]
        db=None,  # type: ignore[arg-type]
        log=None,  # type: ignore[arg-type]
    )
```
**Critical divergence:** `SCDPublisher.publish()` almost certainly DOES need real `ctx.config`/columns (recompute logic needs to know which columns are Type-1/Type-2/Type-0), unlike `MergePublisher`'s fully-placeholder context — `_make_context()` cannot be reused verbatim; build a real minimal `DatasetConfig` fixture instead.

**The specific obsolete test to delete, not adapt:** `test_on_conflict_fails_without_the_unique_constraint_migration_0006_adds` (named explicitly in RESEARCH.md Finding F-4) — asserts the exact constraint D-07's migration drops.

---

### `tests/integration/test_publish_ingest.py`, `test_run_ingest.py`, `test_reconciliation.py` (MODIFIED — cardinality assumption fixes)

**Analog:** themselves. Concrete fix points, verified by direct read:
- `test_publish_ingest.py` line 327: `_normalized_customers_count(migrated_dsn: str) -> int` — a plain unfiltered `count(*)`. Used at lines 448/458 for before/after assertions across a `publish_ingest` call. Must become cardinality-aware (e.g. `count(DISTINCT customer_id)` for "how many customers" assertions, or accept a legitimately-larger post count when a version boundary was created).
- `test_run_ingest.py` lines 41-42: literal comment `"normalized.customers.customer_id" is unique across [runs]` — an explicit, now-false documented test-design assumption; update the comment and any assertion relying on it. Note the file also documents (lines 44-48) that `customer_id` ranges are deliberately partitioned across test files (`test_publish_merge.py` occupies 2000/3001/4001/5001) to avoid collisions in the shared session-scoped testcontainers Postgres — preserve this partitioning discipline for any new `customer_id` ranges the SCD tests introduce.
- `test_reconciliation.py` lines 420-441 (shown in full above): the exact assertion needing rework once `output_count` and `key_count_output` diverge for `customers`.

---

### `tests/integration/test_migrations.py` (MODIFIED — exists, verified 787 lines)

**Analog:** itself. Two specific existing tests need direct updates/replacement, both verified by direct read:
1. **`test_0006_customer_id_has_a_real_unique_constraint`** (lines 273-281) and its sibling **`test_0006_downgrade_restores_the_plain_index_and_reupgrade_restores_the_constraint`** (lines 283-317) — both assert `_customers_customer_id_constraint_types(migrated_dsn) == ("UNIQUE",)`. After migration 0035, this must become `("EXCLUDE",)` (or the exclusion-constraint's actual `pg_constraint.contype` code, `'x'` — verify via `\d normalized.customers` during Wave 0).
2. **`test_gold_indexes_exist_and_business_key_uniqueness_is_unchanged`** (lines 720-735+, shown above) — its docstring literally says "the pre-existing `uq_customers_customer_id`/`uq_orders_order_id` UNIQUE constraints ... still reject a duplicate business key". The `customers` half of this claim becomes false; the test must be split (orders half stays as-is; customers half either removed or replaced with an exclusion-constraint-rejects-overlap assertion).

**New test needed (SCD-12, RESEARCH.md Test Map)** — pattern-match against the SAME file's existing constraint-violation test style (search for how `test_dbt_app_can_insert_dedup_audit_but_not_update_or_delete`, line 631, structures a "attempt the forbidden thing, assert the specific psycopg error" test) for the new `test_exclusion_constraint_rejects_overlapping_validity` test: direct `INSERT` of two overlapping-validity rows for the same `customer_id`, expect `psycopg.errors.ExclusionViolation`.

---

### `tests/e2e/slice/test_backfill_2year_sweep.py` (MODIFIED, extends) + `tools/corpus/dated_series.py` (MODIFIED)

**Analog:** both files, together — `dated_series.py`'s full 330-line generator (read in full above) is the DIRECT template for D-11's corpus extension.

**Existing anomaly-injection pattern to extend** (`generate_dated_series`, lines 176-279): the function already threads through `gap_day_index` (missing file), `schema_change_day_index` (extra column), and `late_event_day_index`/`late_event_offset_days` (backdated row) as explicit keyword parameters, each independently placed via its own day-derived `Random` stream (R1: `stream_for(master_seed, filename)`). D-11's new anomalies (attribute-change event, late/out-of-order correction landing BETWEEN two published versions, a missing-customer snapshot, a deliberately-bad/truncated snapshot for D-06's circuit breaker) should follow the IDENTICAL parameter-threading convention — new `*_day_index`/`*_row_index` keyword-only parameters on `generate_dated_series`, each recorded on `BackfillCorpusManifest` (lines 144-173) so the live test asserts against known manifest values rather than re-deriving them.

**Determinism rules already enforced repo-wide** (module docstring lines 14-30, `tests/policy/test_generator_determinism_rules.py` scans this file by directory walk — no separate wiring needed for new code added here):
- R2: consume randomness only via `Random.random()`, never `choice`/`randint` — use the existing `_pick(rng, values)` helper (lines 316-319).
- R3/R4: build bytes as `str`, encode `utf-8`, explicit `\r\n` terminator — mirror lines 264-267 exactly.
- R6: no wall-clock/OS-entropy — every date derives from `start_date + timedelta(days=...)`.

**Existing customer_id partitioning convention** (dated_series.py lines 92-115) — `_CUSTOMER_ID_BASE = 2_100_100_000` is already carefully disjoint from every other e2e test's own ID range (documented ranges for `test_referential_orphan.py`, `test_backfill_reentry.py`, `test_pod_kill_retry.py`). Any new fixture IDs (e.g. for the D-10 concurrency test, or a separate small D-06 bad-snapshot fixture if kept outside the 2-year corpus per CONTEXT.md's discretion point) must pick yet another disjoint range and document it the same way.

**D-13's `signup_country` column** needs adding to `_DATASET_COLUMNS["customers"]` (line 54) and `_render_row`'s customers branch (lines 295-304) — as a Type-0 column, its value should be picked once per `customer_id` (not per row/day) and never vary across that key's repeated appearances in the corpus, which is itself the fixture-level proof of Type-0 semantics.

## Shared Patterns

### Publisher Protocol conformance
**Source:** `packages/dataplat/src/dataplat/load/publish/protocol.py` (full file, 93 lines)
**Apply to:** `scd.py`'s `SCDPublisher`
```python
class Publisher(Protocol):
    name: str
    def publish(
        self, ctx: PipelineContext, source_table: str, conn: Connection[Any],
    ) -> PublishResult: ...
```
`PublishResult.published_business_keys` (added per CR-01, phase-08 code review) must reflect ONLY business keys this specific publish call actually inserted/updated/invalidated — never a blind read of the staging table. For `SCDPublisher`, this means every `customer_id` whose version chain was recomputed THIS pass, plus every `customer_id` closed out by the DELETE-detection sweep.

### Advisory-lock single-writer discipline
**Source:** `pipeline/run.py` lines 1060-1066 (unchanged LOAD-09 mechanism, reused verbatim per RESEARCH.md Pattern 4)
```python
conn.execute(
    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
    (f"publish:{ctx.config.load.target}",),
)
```
**Apply to:** `SCDPublisher` — the lock is taken by the CALLER (`publish_ingest`), never inside `publish()` itself. Same key (`f"publish:{ctx.config.load.target}"`, i.e. `"publish:normalized.customers"`), unchanged even though the write logic behind it is entirely new (D-10's own rationale for still requiring a dedicated concurrency test on top of this unchanged primitive).

### BarrierStage shape (circuit breakers / referential checks)
**Source:** `packages/dataplat/src/dataplat/pipeline/protocol.py`'s `BarrierStage` Protocol, exercised by `circuit_breaker.py`/`referential.py`
```python
class SomeBarrier(BarrierStage):
    name = "..."
    def apply(self, ctx: PipelineContext) -> StageResult: ...
```
**Apply to:** `MassDeleteCircuitBreaker` in `delete_detection.py` — constructed with already-known totals (never re-derives counts from `ctx`), raises a domain exception (`QualityThresholdExceeded`, reused unchanged — no new exception type needed) on breach, returns a trivial-PASS `StageResult` otherwise.

### Wiring a new BarrierStage into `pipeline/run.py`
**Source:** `pipeline/run.py` lines 663-669 (`RejectionRateCircuitBreaker` construction site)
```python
if ctx.config.quality is not None and ctx.config.quality.rejection_rate_threshold is not None:
    circuit_breaker = RejectionRateCircuitBreaker(
        threshold=ctx.config.quality.rejection_rate_threshold,
        total_rows_read=staging_result.rows_read,
        total_rows_rejected=len(all_rejected),
    )
    all_findings.extend(circuit_breaker.apply(ctx).findings)
```
**Apply to:** wherever `publish_ingest` (not `stage_ingest` — the mass-delete breaker needs gold's `is_current` set and this run's silver snapshot, both only available at publish time, unlike the rejection-rate breaker which runs during staging) constructs and invokes `MassDeleteCircuitBreaker`, guarded by an opt-in config check (`ctx.config.scd is not None`, or wherever the new config field lands) matching this exact `if ... is not None` opt-in convention.

### Migration docstring/citation discipline
**Source:** every migration in `migrations/versions/` — universal convention, strongest examples: 0006, 0012, 0026, 0030 (all read in full above)
- Explain WHY the change is needed (what prior migration's reasoning no longer holds), not just WHAT changes.
- Cite the specific locked decision(s) driving it (`D-07`, `SCD-12`, etc.) and the specific prior migration being superseded, by number.
- `downgrade()` must be a genuine, verified reversal — restoring the PRIOR migration's exact artifact (view text, constraint) verbatim, not a best-effort approximation.
- Every `GRANT`/ownership change is justified explicitly (why this role, why this exact privilege, never broader).

### Config-not-code discipline
**Source:** `configs/datasets/customers.yaml` + `packages/dataplat/src/dataplat/config/model.py` module docstring (lines 10-15)
**Apply to:** DELETE semantics default, mass-delete circuit-breaker threshold, Type-0/1/2 column assignment — all must surface as YAML config validated by a `ConfigDict(extra="forbid", frozen=True)` Pydantic model, never hardcoded Python constants (this is the single most consistently enforced convention across every phase of this codebase).

## No Analog Found

None — every file in RESEARCH.md's/CONTEXT.md's file list has at least a role-match analog in the existing codebase. This is itself a notable finding: Phase 10 is a composition of entirely proven, in-repo primitives (RESEARCH.md's own "Key insight," line 395), not a domain requiring external pattern-borrowing.

## Metadata

**Analog search scope:** `packages/dataplat/src/dataplat/{load/publish,validate,scd,pipeline,metadata,config,normalize}/`, `migrations/versions/`, `configs/datasets/`, `tests/{integration,e2e/slice}/`, `tools/corpus/`
**Files scanned (read in full or targeted range):** `merge.py`, `merge_orders.py`, `protocol.py`, `registry.py`, `circuit_breaker.py`, `referential.py`, `staging.py` (targeted), `unicode.py`, `model.py` (targeted), migrations `0005`, `0006`, `0012`, `0023`, `0026`, `0030`, `customers.yaml`, `pipeline/run.py` (targeted, ~lines 260-400, 600-700, 985-1130), `metadata/repository.py` (targeted, ~lines 890-990), `dated_series.py` (full), `test_publish_merge.py` (header), `test_migrations.py` (targeted), `test_reconciliation.py` (targeted), `test_publish_ingest.py`/`test_run_ingest.py` (targeted)
**Pattern extraction date:** 2026-08-21
