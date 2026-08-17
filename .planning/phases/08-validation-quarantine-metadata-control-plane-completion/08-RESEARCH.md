# Phase 8: Validation, Quarantine & Metadata Control-Plane Completion - Research

**Researched:** 2026-08-17
**Domain:** Row-level data-quality validation engine, quarantine/backfill lifecycle, referential integrity across two live datasets, pre-pod-launch file-integrity gating in Airflow
**Confidence:** HIGH

## Summary

This phase does not need domain discovery — the shapes are already locked. `.planning/research/ARCHITECTURE.md` Q2.2/Q2.3 designed `meta.validation_results`/`meta.rejected_records` and the embedded-lineage-columns pattern before any code existed; `08-CONTEXT.md` D-01 through D-22 then closed every open design question research had left (backfill-only re-entry, 2-state resolution lifecycle, per-rule-type strategy, Airflow-side LOAD-10 gate placement). What planning actually needs from this document is narrower: (1) the locked shapes carried forward with citations, not re-derived; (2) how the new rule engine slots into the existing `StreamingStage`/`BarrierStage` seam without redesigning it; (3) concrete Alembic migration structure following this repo's own 13-migration precedent; (4) concrete Airflow patterns for the LOAD-10 pre-pod-launch gate and the `orders`→`customers` Asset dependency (D-15); (5) a Validation Architecture (Nyquist/Dimension-8) section covering how every one of these pieces gets tested, including the specific difficulty of proving a real Airflow backfill DagRun without a live cluster.

**Primary recommendation:** Build the rule engine as one new `StreamingStage` per structural/schema/type rule family (row-scoped, chunk-bounded) plus exactly one new `BarrierStage` for run-level aggregation (rejection-rate circuit breaker, D-10) and for referential-integrity checks that need the whole run's business keys before evaluating orphans (D-16). Persist `ValidationResult`/`RejectedRecord` findings inside the existing publication transaction (extends `Publisher.publish`'s transactional envelope — D-11's FAIL-means-rollback requirement is otherwise unenforceable). Model the LOAD-10 gate as a `PythonSensor`/`@task.sensor`-style Airflow-side gate performing two `S3Hook` HEAD calls (D-21), never as a pipeline `Source`/`Stage` — it must reject a file before any pod launches.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Structural/schema/type/quality row validation | API/Backend (`dataplat` pipeline, inside `ingest` pod) | — | Row-level errors must be attributable to a row inside the processor, where the parsed record still exists (CLAUDE.md "Logic placement": heavy processing in task pods, never the scheduler) |
| Referential-integrity check (`orders`→`customers`) | API/Backend (`dataplat` `BarrierStage`, inside `orders` `ingest` pod) | Database (query against `normalized.customers`) | Needs the whole run's parsed business keys plus a live read of the target table — a barrier, not a streaming stage |
| Quarantine persistence (`meta.rejected_records`, `meta.validation_results`) | Database/Storage (analytical PostgreSQL, `meta` schema) | — | Control-plane data, permanent, queried not drained (ARCHITECTURE.md §3.1) |
| Quarantine backfill trigger | Orchestrator (Airflow backfill CLI/API against the existing DAG) | — | D-01: no new entry point; the standard Airflow backfill mechanism IS the re-entry path |
| Resolution-state transition (`PENDING`→`RESOLVED`) | Database (side effect of the publication transaction on a backfill run) | — | D-04/D-05: never an API/UI action, only a whole-batch consequence of a run completing |
| LOAD-10/11 file-integrity gate | Orchestrator (Airflow-side `@task`/sensor, before pod launch) | — | D-18: must reject before a pod is even scheduled — Airflow already owns file discovery (`S3KeySensor`, frozen-manifest pattern) |
| `orders`↔`customers` DAG coupling | Orchestrator (Airflow Dataset/Asset scheduling) | — | D-15: `outlets=[...]` on `customers`' publish step, `schedule=[asset]` on `csv_ingest_orders` — pure Airflow-side wiring, no application code |
| Volume-anomaly statistical baseline (VALID-09) | Database (query over persisted `meta.validation_results`/`meta.quality_metrics` history) | API/Backend (barrier stage that queries the baseline and compares) | Statistical thresholds only, no ML (ROADMAP); the baseline itself is SQL over history, the comparison is a barrier stage |

## User Constraints (from CONTEXT.md)

### Locked Decisions

**Quarantine backfill path (VALID-08)**
- D-01: Backfill is the ONLY re-entry mechanism. "Backfill" is the locked term — never "redrive." A corrected file re-ingests by triggering the SAME ingestion DAG as an Airflow backfill run for the original logical date/batch. No separate redrive DAG, CLI, or endpoint.
- D-02: `load.strategy: merge` (upsert / `ON CONFLICT`) is what makes backfill safe. No new load-path branching for the backfill case.
- D-03: Granularity is whole-batch only — never an individual row or arbitrary subset.
- D-04: Resolution lifecycle on `meta.rejected_records` is exactly 2 states: `PENDING` and `RESOLVED`, reached via `REDRIVEN`-via-backfill or `DISCARDED`-via-explicit-batch-level-operator-action (model as `resolution_type`, not a third top-level state). Hard constraint: no per-row manual state editing, ever.
- D-05: A completed backfill run marks the rejected_records rows it supersedes resolved, linked (FK) to the new run_id.
- D-06: No new tooling for operators to find what to backfill — direct SQL against `meta.rejected_records`/`meta.files`.

**Bad-record strategy assignment**
- D-07: Strategy (`FAIL_FILE`/`REJECT_RECORD`/`QUARANTINE_FILE`/`QUARANTINE_RECORD`/`WARN_AND_CONTINUE`) is assigned per-rule-type, dataset-configurable.
- D-08: Structural failures (ragged rows) default to `REJECT_RECORD` — matches existing `RaggedRowGuard`, no behavior change, only a config surface added.
- D-09: `customers.yaml` gets a real `quality:` block (not fixture-only).
- D-10: A separate, configurable run-level rejection-rate threshold acts as a circuit breaker layered on top of row-level strategies.
- D-11: When a run escalates to FAIL, nothing publishes — the entire atomic publish transaction rolls back.
- D-12: `meta.rejected_records`/`meta.validation_results` are plain, unpartitioned tables in this phase.

**Referential integrity scope (VALID-07)**
- D-13: Proven with a real second dataset — `orders` referencing `customers` (`customer_id` FK).
- D-14: `orders` gets its own dedicated DAG, `csv_ingest_orders`, mirroring `csv_ingest_customers`'s shape.
- D-15: `orders` DAG is coupled to `customers` via an Airflow Dataset/Asset dependency (reduces, does not eliminate, orphan cases).
- D-16: Default orphan-order handling is `QUARANTINE_RECORD`: rows whose `customer_id` isn't found in `normalized.customers` go to `rejected_records` with `error_type=REFERENTIAL_ORPHAN`; rest of file loads normally.
- D-17: Minimal `orders` schema: `order_id`, `customer_id` (FK), `order_date`, `amount`. Same config shape as `customers.yaml`.

**File/manifest integrity gate placement (LOAD-10/11)**
- D-18: LOAD-10's checksum/size/extension/completeness checks run Airflow-side, before pod launch — a sensor/task does an S3 HEAD and gates the KPO.
- D-19: `_BATCH_COMPLETE` (LOAD-11) is built and corpus/fixture-tested but stays unexercised by both `customers.yaml` and `orders.yaml` (opt-in, unexercised — Phase 6 D-10 precedent).
- D-20: A gate failure is a file-level rejection recorded in `meta.files.status` — no `meta.ingestion_runs` row, no `run_id`, no `rejected_records` rows.
- D-21: "Transfer completion" is an object-stability check — two S3 HEAD calls a short interval apart; unchanged size/ETag means stable/complete.
- D-22: "Checksum" verification does not compare against an externally-supplied checksum file — the gate confirms readability and (re)computes `content_sha256` for `meta.files`, reusing the existing Phase-2/3 discovery-hash column.

### Claude's Discretion
- Exact column/index shape for `resolution_type` on `meta.rejected_records` (D-04) — single enum column or small lookup, as long as 2-state-lifecycle + no-per-row-edit hold.
- Exact naming/shape of the run-level rejection-rate threshold config key under `quality:` (D-10).
- Whether the Airflow-side integrity sensor (D-18) is a custom `@task` or a `PythonSensor`/deferrable sensor.

### Deferred Ideas (OUT OF SCOPE)
- Table partitioning of `meta.rejected_records`/`meta.validation_results` or any warehouse target — Phase 9's INCR-04.
- Anomaly detection over validation history (VALID-05/06) — Phase 9, depends on this phase's persisted results existing first.
- Retention/archival of `rejected_records`/`validation_results` — Phase 11 (Operations).

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| VALID-01 | Structural validation reports column-count/malformed-row/unclosed-quote/missing-delimiter with row number, column, error type, diagnostics | Extends existing `RaggedRowGuard` (already a `StreamingStage`, DoD-32-shaped `RejectedRecord`); add sibling `StreamingStage`s for the remaining structural cases, all writing into the same `meta.rejected_records` shape (ARCHITECTURE.md §2.2) |
| VALID-02 | Completeness/uniqueness/validity-range/pattern/referential-integrity quality checks with configurable thresholds → PASS/PASS_WITH_WARNING/FAIL/QUARANTINE | New `quality:` config block (D-09) driving a small registry of rule-type `StreamingStage`s (row-scoped rules) + one `BarrierStage` (run-aggregate threshold evaluation, D-10) |
| VALID-03 | Quarantine per configurable strategy, retaining source file/row/error/run/timestamp, never silently discarded | `RejectedRecord` → `meta.rejected_records` row per D-07's per-rule-type strategy dispatch; `errors.py`'s `QualityThresholdExceeded` is this phase's first raise site for the FAIL/circuit-breaker case |
| VALID-04 | Machine-readable validation reports persisted as PostgreSQL rows AND MinIO artifacts | `meta.validation_results` (ARCHITECTURE.md §2.2) + existing `report_uri` column on `meta.ingestion_runs` (migration 0004) pointing at the same JSON this phase already produces for `Receipt.report_uri` |
| VALID-07 | Referential integrity, configurable fail/quarantine/warn on orphans | `orders`→`customers` real second dataset (D-13..D-17); a `BarrierStage` querying `normalized.customers` for the run's collected `customer_id`s |
| VALID-08 | Documented re-drive (backfill) path back into the pipeline | D-01..D-06: Airflow backfill CLI/API against `csv_ingest_orders`/`csv_ingest_customers`, no new mechanism |
| VALID-09 | Volume/quality anomalies against persisted statistical baselines, no ML | Depends on VALID-04's persistence landing first (ROADMAP ordering); this phase only needs to persist the row-count/null-rate metrics VALID-09 (Phase 9) will read — do not build the anomaly comparison itself, only ensure `meta.validation_results`/an equivalent metrics surface exists to query later. Actually noted in ROADMAP as this phase's own criterion #5 (row count 10x baseline) — see Open Questions |
| LOAD-10 | File integrity verified before processing (checksum/size/extension/metadata/transfer-completion/optional control file) | Airflow-side `@task`/sensor (D-18), two-HEAD stability check (D-21), reuses `meta.files.content_sha256` (D-22) |
| LOAD-11 | Optional batch manifests/`_BATCH_COMPLETE`, manifest may be authoritative input | Built and corpus-tested, unexercised by both live datasets (D-19) |

**Note on VALID-09:** ROADMAP's phase-8 success criterion #5 explicitly requires "a file whose row count is 10x its historical baseline is flagged as a volume anomaly against persisted statistics" — this reads as VALID-09 behavior INSIDE phase 8's success criteria, even though the requirements table maps VALID-09 to Phase 9. Resolve this at planning time, not here (see Open Questions) — do not silently drop the success criterion, but do not contradict CONTEXT.md's `<domain>` section either, which explicitly lists "anomaly detection over time-series validation history (VALID-05/06 — Phase 9)" as out of scope while staying silent on whether the *simpler* single-file 10x-baseline check (arguably VALID-01/02 "quality" territory, not the historical-trend VALID-05/06) belongs here.
</phase_requirements>

## Locked Table Shapes (carried forward, not re-derived)

Source: `.planning/research/ARCHITECTURE.md` Q2.2 (lines 228-246) and Q2.3 (lines 247-265). CONTEXT.md D-12 overrides ARCHITECTURE.md's speculative partitioning language — treat both tables as plain, unpartitioned in this phase.

### `meta.validation_results`
> `run_id`, `rule_id`, `rule_type ∈ {FILE,STRUCTURAL,SCHEMA,TYPE,QUALITY,REFERENTIAL}`, `severity`, `outcome ∈ {PASS,PASS_WITH_WARNING,FAIL,QUARANTINE}`, `evaluated_count`, `failed_count`, `threshold jsonb`, `observed jsonb`

Enum-like columns (`rule_type`, `outcome`, `severity`) follow this repo's own established convention (migration 0009's `derived_from`/`compatibility`, migration 0002's `status`): plain `sa.Text()`, application-validated via Pydantic, never a native PostgreSQL `ENUM` and never a CHECK constraint. `[VERIFIED: migrations/versions/0009_meta_schema_versions.py, 0002_meta_files.py]`

### `meta.rejected_records`
> `run_id`, `file_id`, `source_row_number`, `source_byte_offset`, `raw_line text`, `error_type`, `error_column`, `error_message`, `rejected_at`. Overflow beyond `config.quarantine.max_inline_rows` spills to `s3://quarantine/…` with a pointer

Add (not in ARCHITECTURE.md's original sketch, required by D-04/D-05, Claude's discretion per CONTEXT.md): a `resolution_type` column (`PENDING` / `REDRIVEN` / `DISCARDED` — 2 *states*, `PENDING`/`RESOLVED`, but `resolution_type` disambiguates which of the two `RESOLVED` paths applied, matching D-04's "model this as a `resolution_type` value, not a third top-level state") and a `resolved_by_run_id` FK to `meta.ingestion_runs` (D-05's linkage). Follow migration 0004's deferred-FK pattern if `resolved_by_run_id` needs to reference a run that does not exist yet at insert time — it does not here (the resolving run always postdates the rejection), so a direct nullable FK, populated only on resolution, is sufficient; no deferred-constraint dance needed.

### Record-level lineage (embedded columns, `orders` reuses the pattern)
> Every table in `normalized.*`/`warehouse.*` carries: `_run_id bigint NOT NULL REFERENCES meta.ingestion_runs`, `_file_id bigint NOT NULL REFERENCES meta.files`, `_batch_id bigint NOT NULL REFERENCES meta.batches`, `_source_row_number bigint NOT NULL`, `_record_hash bytea NOT NULL`, `_ingested_at timestamptz NOT NULL DEFAULT now()`

`normalized.orders` must carry the identical six columns, verbatim, matching `normalized.customers`'s precedent (migration 0005) — including the `_record_hash_version` companion column migration 0005 added as a documented, confirmed extension of META-02 (`03-RESEARCH.md` Pitfall 3). Do not invent a `meta.record_lineage` row per order; the opt-in table stays out of scope (ARCHITECTURE.md §2.3's own "resist a table" framing, unchanged by this phase).

### Slice-vs-later table ownership (ARCHITECTURE.md §2.4, unchanged)
This phase owns exactly: `schema_versions` (already shipped, migration 0009 — not this phase's job, listed here only to avoid duplicate-migration confusion), `validation_results`, `rejected_records`. `run_stages`, `watermarks`, `dedup_audit`, `reconciliation_results` are explicitly Phase 9 ("Correctness phase") — do not create them here even though ROADMAP's Wave-E note lists `run_stages`/`dedup_audit` as tables "whose design already exists": their DDL exists in research, but their *migration* is Phase 9's job per this same table. Confirm this against ROADMAP's literal wave text before planning — if ROADMAP explicitly assigns `run_stages`/`dedup_audit` DDL to this phase's migration set, that instruction overrides this general table (ROADMAP is more specific than the general ARCHITECTURE.md phase-mapping).

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|--------------|-----|
| Quarantine re-entry mechanism | A custom "redrive" CLI/endpoint/table-driven retry queue | Airflow's own `airflow dags backfill` against the existing DAG (D-01) | Airflow already solves "re-run this DAG for this logical window," including retries/concurrency; a parallel mechanism would duplicate and could disagree with it |
| Row-level upsert-on-correction | A bespoke "has this row already been corrected" diff/compare step | `load.strategy: merge`'s existing `ON CONFLICT` (D-02) | The publish SQL is already idempotent per-key; a previously-rejected-now-valid row simply inserts on backfill, no new code path |
| File-completeness detection | A custom polling loop with sleep/retry hand-rolled around `boto3` | Airflow's own sensor/deferrable-operator machinery (`@task.sensor` or `PythonSensor`, matching the already-deployed `S3KeySensor` deferrable pattern) | Reinventing poke/defer/timeout semantics that Airflow's sensor framework already provides correctly is exactly the kind of infra-as-code effort CLAUDE.md says to avoid |
| Statistical anomaly detection | Any ML/forecasting library | Plain SQL comparison against `meta.validation_results`/a metrics table, using a configurable multiplier threshold (e.g. `row_count > 10 * avg(historical_row_count)`) | ROADMAP is explicit: "Statistical thresholds only. No ML anomaly detection." |
| Native Postgres enum types for `outcome`/`rule_type`/`resolution_type` | `CREATE TYPE ... AS ENUM` | `sa.Text()` + Pydantic `Literal` validation at the application layer | Zero native enums exist anywhere in this project's 13 prior migrations (verified) — adding the first one here breaks a real, load-bearing convention for no benefit, and native enum `ALTER TYPE ADD VALUE` inside a transaction is its own well-known Postgres footgun |

**Key insight:** Every "don't hand-roll" item above already has a working precedent living in this exact codebase (Airflow backfill, `ON CONFLICT` merge, deferrable sensors, text-not-enum columns) — this phase's job is applying those precedents to new tables/rules, not inventing new mechanisms.

## Architecture Patterns

### System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│ Airflow (orchestrator tier)                                          │
│                                                                        │
│  S3KeySensor (wait_for_files)                                        │
│         │                                                             │
│         ▼                                                             │
│  ┌──────────────────────┐   NEW: LOAD-10/11 gate (D-18)              │
│  │ integrity_gate (@task)│──reject──▶ meta.files.status='rejected'   │
│  │ 2x S3 HEAD (D-21)     │           (no run_id, no rejected_records,│
│  │ recompute sha256(D-22)│            D-20 — dead end, no pod launch)│
│  └──────────┬────────────┘                                           │
│             │ pass                                                    │
│             ▼                                                         │
│  discover (KPO) ──▶ frozen AssignmentDocument (unchanged pattern)    │
│             │                                                         │
│             ▼                                                         │
│  ingest (KPO, mapped) ─────────────────────────────────────────┐     │
│         (customers)                                              │     │
│                                                                    │     │
│  csv_ingest_customers  ──outlets=[customers_asset]──▶ Asset      │     │
│                                                          │  (D-15) │     │
│  csv_ingest_orders  ◀──schedule=[customers_asset]───────┘         │     │
│         (own wait_for_files/integrity_gate/discover/ingest,       │     │
│          mirrors customers' shape, D-14)                          │     │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼ (inside `ingest` pod, per-file, per-chunk)
┌─────────────────────────────────────────────────────────────────────┐
│ dataplat pipeline (API/Backend tier — packages/dataplat)              │
│                                                                        │
│  run_streaming(ctx, chunks, stages=[                                 │
│      RaggedRowGuard,            # existing, VALID-01 structural       │
│      NEW: CompletenessRule,     # StreamingStage, VALID-02            │
│      NEW: UniquenessRule,       # StreamingStage or BarrierStage      │
│      NEW: ValidityRangeRule,    # StreamingStage, VALID-02            │
│      NEW: PatternRule,          # StreamingStage, VALID-02            │
│  ])                                                                    │
│         │  (each StageResult.rejected/findings accumulate)            │
│         ▼                                                             │
│  NEW: ReferentialIntegrityBarrier(BarrierStage)  # VALID-07, orders   │
│         queries normalized.customers for the run's customer_ids       │
│         │                                                              │
│         ▼                                                              │
│  NEW: RejectionRateCircuitBreaker(BarrierStage)  # D-10, run-level    │
│         raises QualityThresholdExceeded if aggregate > threshold      │
│         │                                                              │
│         ▼                                                              │
│  Publisher.publish() — SINGLE transaction now also writes:            │
│      staging → normalized.* (existing)                                │
│      meta.validation_results (NEW, one row per rule per run)          │
│      meta.rejected_records (NEW, one row per rejected record)         │
│      — FAIL ⇒ entire transaction rolls back (D-11), nothing persists  │
│        except the run's own FAILED status row in meta.ingestion_runs  │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Database tier — analytical PostgreSQL, `meta` schema                  │
│  meta.validation_results, meta.rejected_records (queried, not drained)│
│  normalized.orders (+ 6 embedded lineage columns, same as customers)  │
└─────────────────────────────────────────────────────────────────────┘
```

### Recommended Project Structure

```
packages/dataplat/src/dataplat/
├── validate/                       # NEW package, sibling of normalize/, schema/
│   ├── __init__.py
│   ├── registry.py                 # rule_type -> Stage class, config-not-code (D-07)
│   ├── completeness.py             # StreamingStage: null/required-column checks
│   ├── uniqueness.py               # StreamingStage or BarrierStage per rule scope
│   ├── validity_range.py           # StreamingStage: min/max/pattern bounds
│   ├── pattern.py                  # StreamingStage: regex/format rules
│   ├── referential.py              # BarrierStage: orphan-key check (VALID-07)
│   └── circuit_breaker.py          # BarrierStage: run-level rejection-rate (D-10)
├── quarantine/                     # NEW package
│   ├── __init__.py
│   └── resolution.py               # resolution_type transition logic, called only
│                                    # from the publication-transaction path (D-04)
├── metadata/
│   └── repository.py               # extend MetadataRepository Protocol: methods for
│                                    # writing validation_results/rejected_records,
│                                    # resolving a batch's PENDING rows on backfill
migrations/versions/
├── 0014_meta_validation_results.py # NEW
├── 0015_meta_rejected_records.py   # NEW
├── 0016_normalized_orders.py       # NEW (D-17 schema + 6 lineage columns)
configs/datasets/
├── customers.yaml                  # add `quality:` block (D-09)
└── orders.yaml                     # NEW (D-13..D-17)
airflow/dags/
├── csv_ingest_customers.py         # add integrity_gate task before discover (D-18);
│                                    # add outlets=[customers_asset] to ingest/publish step
├── csv_ingest_orders.py            # NEW, mirrors csv_ingest_customers.py,
│                                    # schedule=[customers_asset] (D-15)
└── _common/
    └── integrity_gate.py           # NEW shared helper (mirrors kpo.py's precedent of
                                     # being the ONLY shared non-business-logic code)
```

### Pattern 1: Rule engine as a config-keyed strategy registry (extends existing Pattern 5)

**What:** Each `quality:` rule in a dataset's YAML names a `rule_type` (`STRUCTURAL`/`SCHEMA`/`TYPE`/`QUALITY`/`REFERENTIAL`) and a `strategy` (`FAIL_FILE`/`REJECT_RECORD`/`QUARANTINE_FILE`/`QUARANTINE_RECORD`/`WARN_AND_CONTINUE`, D-07). A registry (mirroring `SOURCE_REGISTRY`/`DEDUP_REGISTRY`/`PUBLISHER_REGISTRY`, `config/model.py`'s own documented convention) maps `rule_type` to a `StreamingStage`/`BarrierStage` class, never a hardcoded if/elif chain — `ColumnContract.type`'s docstring in `config/model.py` explicitly documents WHY the registry pattern is preferred over a closed `Literal` when there's a real extension point, and rule types are exactly that kind of extension point.

**When to use:** Every new rule type added after this phase (a real possibility — VALID-02 lists "completeness, uniqueness, validity ranges, patterns" as the DoD-mandated minimum set, not a closed set) should be a new registry entry, not a new `if` branch in a shared dispatcher.

**Example (registry shape, following `config/model.py`'s documented pattern):**
```python
# Source: packages/dataplat/src/dataplat/validate/registry.py (new, this phase)
# Mirrors dataplat.load.publish.registry.PUBLISHER_REGISTRY's existing shape.
from dataplat.pipeline.protocol import BarrierStage, StreamingStage

VALIDATION_RULE_REGISTRY: dict[str, type[StreamingStage] | type[BarrierStage]] = {
    "STRUCTURAL": RaggedRowGuard,          # already exists, VALID-01
    "QUALITY_COMPLETENESS": CompletenessRule,
    "QUALITY_UNIQUENESS": UniquenessRule,
    "QUALITY_VALIDITY_RANGE": ValidityRangeRule,
    "QUALITY_PATTERN": PatternRule,
    "REFERENTIAL": ReferentialIntegrityBarrier,
}
```

### Pattern 2: Barrier stage for anything needing "the whole run" (referential integrity, circuit breaker)

**What:** `BarrierStage.apply(ctx) -> StageResult` runs once per run, after every chunk is staged (`pipeline/protocol.py` docstring, verbatim: "Cross-batch deduplication, threshold evaluation, publication and reconciliation are barriers"). Referential integrity (VALID-07) and the rejection-rate circuit breaker (D-10) are both barriers by this exact definition — an orphan check needs every row's `customer_id` collected before it can query `normalized.customers` once (not once per chunk), and the circuit breaker needs the aggregate rejected/total count across the whole run.

**When to use:** Any rule whose evaluation depends on more than one chunk's data, or on a live query against another table.

**Example:**
```python
# Source: packages/dataplat/src/dataplat/pipeline/protocol.py (existing Protocol,
# this phase's referential.py implements it)
class ReferentialIntegrityBarrier(BarrierStage):
    name = "referential_integrity_customer_id"

    def apply(self, ctx: PipelineContext) -> StageResult:
        # Query staging.<dataset>__r<run_id> for distinct customer_id values,
        # anti-join against normalized.customers, mark orphans REFERENTIAL_ORPHAN
        # (D-16) with strategy QUARANTINE_RECORD from ctx.config.quality rules.
        ...
```

### Pattern 3: Validation persistence rides inside the existing publication transaction

**What:** `MergePublisher.publish()` today only writes to `normalized.customers`. This phase extends the transaction the caller (`run_ingest`, plan 04-05) already opens around `publish()` to also `INSERT` into `meta.validation_results`/`meta.rejected_records`, and to conditionally roll back the whole transaction when the circuit breaker (D-10) trips — never a separate, later, best-effort write.

**When to use:** Always, for this phase's persistence — this is what makes D-11 ("nothing publishes" on FAIL) true. A validation-results write that happened in a separate, already-committed transaction before the publish attempt would violate D-11: the report would exist even though the run "never happened."

**Example:**
```sql
-- Extends 04-RESEARCH.md's existing worked publication-transaction pattern.
-- Inside the same transaction Publisher.publish() runs in:
INSERT INTO meta.validation_results (run_id, rule_id, rule_type, outcome, evaluated_count, failed_count, threshold, observed)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);

INSERT INTO meta.rejected_records (run_id, file_id, source_row_number, error_type, error_column, error_message, raw_line, resolution_type)
SELECT %s, %s, source_row_number, error_type, error_column, error_message, raw_line, 'PENDING'
FROM unnest(...);  -- one row per RejectedRecord this run's stages accumulated

-- If the circuit breaker (D-10) trips: ROLLBACK the whole transaction. Nothing
-- above persists — the run's meta.ingestion_runs row still records status=FAILED,
-- written in a SEPARATE, always-committed statement outside this transaction
-- (matches the existing pattern: run-status bookkeeping already survives a
-- publish-transaction rollback in the vertical slice, 04-RESEARCH.md Q7).
```

### Pattern 4: Airflow-side LOAD-10 gate — two-HEAD object-stability check (D-21)

**What:** A `@task` (Claude's discretion: plain `@task`, not a `PythonSensor`, is simpler here since there is no "wait and retry" semantics needed beyond the sensor Airflow already uses upstream for file *arrival* — this gate runs once, after `wait_for_files` has already confirmed the object exists) calls `S3Hook(aws_conn_id="minio_default")`'s underlying `boto3` client's `head_object` twice, a short interval apart, and compares `ContentLength`/`ETag`. Verified via `apache-airflow-providers-amazon`'s `S3Hook.get_conn()` returning a real `boto3` S3 client — the same connection (`minio_default`) already resolved through Vault (SEC-05), so this gate needs no new credential wiring.

**When to use:** Before `discover` launches, for every file `wait_for_files` matched.

**Example:**
```python
# Source: airflow/dags/_common/integrity_gate.py (new, this phase)
# S3Hook.get_conn() -> boto3 S3 client, verified against
# apache-airflow-providers-amazon (pinned in Airflow's constraints file).
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import task

@task
def integrity_gate(bucket: str, key: str) -> dict[str, object]:
    hook = S3Hook(aws_conn_id="minio_default")
    client = hook.get_conn()
    first = client.head_object(Bucket=bucket, Key=key)
    time.sleep(STABILITY_CHECK_INTERVAL_SECONDS)  # short, e.g. 5s — config, not hardcoded
    second = client.head_object(Bucket=bucket, Key=key)
    if (first["ContentLength"], first["ETag"]) != (second["ContentLength"], second["ETag"]):
        # D-20: file-level rejection recorded directly on meta.files.status —
        # no run_id, no meta.ingestion_runs row. This write happens via a thin
        # dataplat CLI call (dataplat reject-file --reason ...) or, if the
        # Airflow image must never import dataplat (ADR-0004, unchanged),
        # a small dedicated psycopg call the DAG file makes directly — this
        # is the one place LOAD-10 forces a decision the plan must make
        # explicitly (see Open Questions).
        raise AirflowFailException(f"{key}: object not stable between HEAD checks")
    return {"content_length": first["ContentLength"], "etag": first["ETag"]}
```

**A real open question this pattern surfaces (see Open Questions below):** D-20 says a gate failure writes to `meta.files.status` directly, with no `run_id`/`ingestion_runs` row — but ADR-0004 (cited in `csv_ingest_customers.py`'s own docstring) establishes that the Airflow image never imports `dataplat`/`csv_processor`. The plan must decide whether `integrity_gate` (a) shells out to a tiny `dataplat`-image-free psycopg call embedded directly in the DAG file (a narrow, explicit exception, matching D-18's own "Airflow already owns file discovery" reasoning), or (b) launches a minimal KPO just to record the rejection (which reintroduces the "spin up a pod for a bad file" cost D-18 exists specifically to avoid). Precedent search found no existing direct-DB-write code path from an Airflow DAG file — this is a genuinely new architectural surface, not a reuse of an existing one.

### Pattern 5: Dataset/Asset coupling between `csv_ingest_customers` and `csv_ingest_orders` (D-15)

**What:** `airflow.sdk.Asset` (Airflow 3's renamed `Dataset` concept — confirmed current for 3.3.x via official docs) is declared once, referenced by URI, e.g. `Asset("s3://normalized/customers")`. The `customers` DAG's terminal publish-related task declares `outlets=[customers_asset]`; `csv_ingest_orders` is declared with `schedule=[customers_asset]` instead of a cron string. `[CITED: airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html]`

**When to use:** Exactly once, per D-15 — this is the only Dataset/Asset wiring this phase adds. It reduces but does not eliminate orphan orders (a customer from a later batch can legitimately still be missing) — this is explicitly acceptable per D-16's `QUARANTINE_RECORD` default, not a bug to fix.

**Example:**
```python
# Source: airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html
from airflow.sdk import Asset

customers_asset = Asset("s3://normalized/customers")

# In csv_ingest_customers.py — the ingest (or a new terminal) task:
ingest = TracingKubernetesPodOperator.partial(
    task_id="ingest",
    outlets=[customers_asset],
    ...
).expand(...)

# In csv_ingest_orders.py — the new DAG:
@dag(
    dag_id="csv_ingest_orders",
    schedule=[customers_asset],
    ...
)
def csv_ingest_orders() -> None:
    ...
```

**Caveat, cross-checked against ORCH-05's existing proof pattern:** `csv_ingest_customers.py`'s own `resolve_window()` task already proves `logical_date=None` never raises for an asset-triggered run — `csv_ingest_orders` inherits the identical risk (an Asset-scheduled run has no `logical_date` either) and must carry the same proof, not a new one.

### Anti-Patterns to Avoid
- **A native Postgres `ENUM` type for `outcome`/`rule_type`/`resolution_type`:** breaks this project's own zero-native-enum precedent across 13 migrations; use `sa.Text()` + Pydantic `Literal`.
- **A per-row "resolve" API/SQL convenience function:** D-04 is explicit and firm — no mechanism, however minor, may let an operator flip a single row's resolution status. Do not build one "just for debugging."
- **Writing `meta.validation_results`/`meta.rejected_records` outside the publication transaction:** breaks D-11's FAIL-means-rollback guarantee — a report that survives a rolled-back run is a lie about what happened.
- **A second redrive/reprocess entry point (CLI, endpoint, DAG):** D-01 is explicit — Airflow's own backfill IS the mechanism, full stop.
- **Importing `dataplat` into the Airflow image to implement the LOAD-10 gate:** violates ADR-0004 (unchanged by this phase, per canonical_refs); the gate must stay Airflow-native (`S3Hook`, plain `boto3`, or a narrow direct-DB call — see Pattern 4's open question).

## Common Pitfalls

### Pitfall 1: Treating VALID-09's phase-8 success criterion as fully in-scope when the requirement itself is mapped to Phase 9
**What goes wrong:** ROADMAP's phase-8 success criterion #5 ("row count 10x historical baseline flagged") reads like a demand to build volume-anomaly detection now, but REQUIREMENTS.md's traceability table maps VALID-09 to Phase 9, and CONTEXT.md's `<domain>` section lists anomaly detection as explicitly out of scope for this phase.
**Why it happens:** The success criterion was likely written before the requirement-to-phase mapping was finalized, or intends a narrower "does the metrics/baseline persistence exist" proof rather than the full anomaly-comparison logic.
**How to avoid:** Flag this explicitly for the planner/discuss-phase rather than silently picking a side — see Open Questions. Do not build full VALID-09 anomaly detection in this phase without a locked decision; do not silently drop the success criterion either.
**Warning signs:** A plan task that imports statistical/ML libraries, or one that skips writing any row-count metric at all.

### Pitfall 2: Writing validation results as a fire-and-forget report, decoupled from the publish transaction
**What goes wrong:** If `meta.validation_results`/`meta.rejected_records` are written in a step BEFORE or AFTER the publish transaction (rather than inside it), a FAIL that rolls back the publish still leaves a validation report behind — directly violating D-11 ("nothing publishes... FAIL is unambiguous: nothing from this run reaches the warehouse"). A report is not "nothing," and if it references a `run_id` whose data never landed, that's a lineage lie.
**Why it happens:** Natural instinct is "write findings as you go, chunk by chunk" (streaming), but the FAIL/circuit-breaker decision can only be known after all chunks are processed (a barrier).
**How to avoid:** Accumulate `RejectedRecord`/`ValidationResult` objects in memory across `run_streaming`'s existing chunk loop (the mechanism already exists — `StageResult.rejected`/`.findings` accumulate today, just aren't persisted yet), and persist them only inside the same transaction `Publisher.publish()` runs in, gated by the circuit breaker's own barrier evaluation.
**Warning signs:** A test that asserts "after a FAIL run, `meta.rejected_records` still has rows" would be asserting the WRONG behavior for a FAIL — but it IS the right behavior for `QUARANTINE_RECORD`/`REJECT_RECORD` strategies on a run that otherwise SUCCEEDS. Distinguish run-level FAIL (D-11, full rollback) from row-level QUARANTINE_RECORD (rows persist, run still succeeds) carefully in every test.

### Pitfall 3: Assuming `dag.test()` can prove the real backfill re-entry behavior end-to-end
**What goes wrong:** `dag.test()` executes a real DagRun in-process against a metadata DB, but the DAG's tasks are `KubernetesPodOperator`s — without a real cluster, `dag.test()` will either hang trying to schedule a real pod or must have `KubernetesPodOperator.execute` mocked (CLAUDE.md's own documented pattern: "Mock the KPO in unit tests... Real pod execution belongs in E2E only"). A `dag.test()`-based test can prove the DAG's *shape* correctly reacts to a backfill trigger (correct `logical_date`, correct task graph, correct `run_id`/`try_number` wiring) — it cannot prove that `meta.rejected_records` rows actually flip to `RESOLVED` after a real corrected file loads, because that logic lives inside the mocked-away pod.
**Why it happens:** `dag.test()` is genuinely new to this codebase (no existing usage found in `tests/`) — it is easy to reach for it expecting an all-in-one proof.
**How to avoid:** Split the proof into two tiers, matching this codebase's existing tier separation (`tests/unit`/`tests/integration`/`tests/e2e`): (1) a `dag.test()`-based behavioral test proving the DAG-level backfill mechanics (new tier — see Validation Architecture), with KPO execution mocked; (2) the real resolution-state-transition logic proven at the `dataplat`/`MetadataRepository` level with `tests/integration/`'s existing testcontainers PostgreSQL (no Airflow involved at all); (3) the full genuine end-to-end proof (real `airflow dags backfill` CLI against a real kind cluster, real corrected file, real `rejected_records` row flipping to `RESOLVED`) belongs in `tests/e2e/slice/`, `cluster`-marked, matching that directory's existing charter ("proves the PIPELINE's own ROADMAP success criteria... against the real deployed DAGs").
**Warning signs:** A single test file trying to do all three of the above in one `dag.test()` call.

### Pitfall 4: LOAD-10's "checksum verification" being (mis)implemented as external-checksum-file comparison
**What goes wrong:** DoD/Gap-4's original language ("checksum, size, extension, object metadata, transfer completion, optional control file") reads naturally as "compare against a `.sha256` sidecar file," which is what many real-world ingestion pipelines do. D-22 explicitly overrides this: there is no sidecar-file convention in this phase — "checksum" here means confirming the object is readable and (re)computing `content_sha256` into the existing `meta.files` column.
**Why it happens:** The DoD item's own wording is ambiguous without D-22's clarification.
**How to avoid:** Do not build a `.sha256`-file-reading code path. The gate's only checksum action is object-readability + hash computation feeding `meta.files.content_sha256` (already exists, migration 0002).
**Warning signs:** A plan task mentioning "read the companion checksum file" or "`*.sha256`."

### Pitfall 5: `orders.yaml`'s referential-integrity rule racing `customers`' own backfill
**What goes wrong:** D-15's Asset coupling reduces but does not eliminate the case where an `orders` file arrives referencing a `customer_id` that legitimately hasn't landed yet (a later `customers` batch). If the orphan-handling strategy (D-16, `QUARANTINE_RECORD`) is implemented as `FAIL_FILE` or blocks the whole run instead, a normal, expected race condition becomes a false alarm.
**Why it happens:** Referential integrity checks are often implemented as all-or-nothing gates in other systems; this phase's locked default is explicitly row-level (`QUARANTINE_RECORD`), not file-level.
**How to avoid:** Implement `ReferentialIntegrityBarrier` to quarantine only the orphaned rows (`error_type=REFERENTIAL_ORPHAN`), letting the rest of the file publish normally — exactly D-16's text. Verify with a test that intentionally races an `orders` file against a not-yet-loaded `customers` batch and asserts the non-orphan rows still land.
**Warning signs:** A test asserting the whole `orders` run fails when even one row has an unresolved FK.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 9.1.1 + hypothesis 6.165.3 + testcontainers 4.15.0 (all already pinned and in use) |
| Config file | `pyproject.toml` `[tool.pytest.ini_options]` (existing — `markers` list needs one addition: see Wave 0 Gaps) |
| Quick run command | `pytest tests/unit tests/property -q` (offline gate, no Docker/cluster needed — matches existing `slow`/`regression` marker conventions) |
| Full suite command | `pytest tests/unit tests/property tests/integration tests/e2e -q -m "not cluster"` for CI-safe full run; `pytest -m cluster` separately against a live kind cluster for the genuine E2E backfill/orphan proof |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| VALID-01 | Structural rule variants (unclosed quote, missing delimiter, column-count mismatch) each produce a `RejectedRecord` with row/column/error_type | unit | `pytest tests/unit/validate/test_structural_rules.py -x` | ❌ Wave 0 |
| VALID-02 | Completeness/uniqueness/validity-range/pattern rules each evaluate against a `RecordChunk` and produce `ValidationResult` with correct PASS/PASS_WITH_WARNING/FAIL/QUARANTINE outcome per configured threshold | unit + property (hypothesis: arbitrary chunk of nullable/duplicate/out-of-range values never crashes a rule, always classifies) | `pytest tests/unit/validate/test_quality_rules.py tests/property/test_quality_rules_never_raise.py -x` | ❌ Wave 0 |
| VALID-03 | Each of the 5 strategies (`FAIL_FILE`/`REJECT_RECORD`/`QUARANTINE_FILE`/`QUARANTINE_RECORD`/`WARN_AND_CONTINUE`) dispatches to the correct row/run-level action | unit | `pytest tests/unit/validate/test_strategy_dispatch.py -x` | ❌ Wave 0 |
| VALID-04 | `meta.validation_results`/`meta.rejected_records` rows exist after a run, AND a MinIO report artifact exists at `report_uri`, matching the same run | integration (testcontainers Postgres + MinIO — existing `tests/integration/conftest.py` fixtures) | `pytest tests/integration/test_validation_persistence.py -x -m integration` | ❌ Wave 0 |
| VALID-07 | An `orders` row with an unresolvable `customer_id` is quarantined (`REFERENTIAL_ORPHAN`), non-orphan rows in the same file still publish | integration (real `orders`+`customers` staging/normalized tables via testcontainers) | `pytest tests/integration/test_referential_integrity.py -x -m integration` | ❌ Wave 0 |
| VALID-07 (live) | A real orphan order, created by racing `orders` ingestion ahead of the referenced `customers` batch, is proven against the real deployed DAGs | e2e, cluster-marked | `pytest tests/e2e/slice/test_referential_orphan.py -x -m cluster` | ❌ Wave 0 |
| VALID-08 | A backfill DagRun for a corrected file's logical_date/batch resolves the batch's `PENDING` rejected_records rows to `RESOLVED`/`REDRIVEN`, linked to the new `run_id` | integration (dataplat-level: no Airflow needed — call the same resolution function a real backfill's publish path calls) | `pytest tests/integration/test_backfill_resolution.py -x -m integration` | ❌ Wave 0 |
| VALID-08 (DAG shape) | `csv_ingest_orders`/`csv_ingest_customers` correctly execute as a backfill DagRun (correct `logical_date`, task graph, `run_id`) with KPO execution mocked | NEW tier: `dag.test()`-based behavioral test | `pytest tests/dagtest/test_backfill_dagrun.py -x` | ❌ Wave 0, new tier |
| VALID-08 (live) | A real `airflow dags backfill` against the real deployed DAG, real corrected file, `meta.rejected_records` row genuinely flips to `RESOLVED` | e2e, cluster-marked | `pytest tests/e2e/slice/test_backfill_reentry.py -x -m cluster` | ❌ Wave 0 |
| VALID-09 (if in-scope, see Open Questions) | A file with 10x historical row-count average is flagged | unit + integration | `pytest tests/unit/validate/test_volume_anomaly.py -x` | ❌ Wave 0, conditional |
| LOAD-10 | `integrity_gate` rejects a file whose two HEAD calls disagree (unstable), a wrong-extension file, an empty file — before any pod launches | unit (mock `S3Hook`/`boto3` client, assert no downstream task ever runs) | `pytest tests/unit/test_integrity_gate.py -x` | ❌ Wave 0 |
| LOAD-10 (DAG shape) | `integrity_gate` task exists upstream of `discover` in both DAGs, `discover` never runs when the gate fails | `dag.test()`-based (KPO mocked) or plain `DagBag`-structural (dependency-order assertion, no execution needed) | `pytest tests/unit/test_dag_structure.py -x` (extend existing file) | ✅ extend existing |
| LOAD-11 | `_BATCH_COMPLETE` marker is honored when present in a fixture/corpus dataset (opt-in, unexercised by live configs per D-19) | unit + corpus fixture | `pytest tests/unit/validate/test_batch_complete_marker.py -x` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/unit tests/property -q` (fast, no Docker)
- **Per wave merge:** `pytest tests/unit tests/property tests/integration -q -m integration` (testcontainers-backed, requires Docker) plus `pytest tests/dagtest -q` (new tier, no Docker needed — `dag.test()` uses a testcontainers Postgres for the Airflow metadata DB, matching CLAUDE.md's documented pattern, so it DOES need Docker; place it alongside `tests/integration` in the per-wave-merge tier, not the per-commit tier)
- **Phase gate:** Full suite green (`tests/unit`, `tests/property`, `tests/integration`, `tests/dagtest`) before `/gsd:verify-work`; `tests/e2e/slice` (`cluster`-marked) run separately against a live kind cluster as this phase's genuine end-to-end proof of VALID-07/VALID-08's live-system success criteria (ROADMAP success criteria #3 and #5 explicitly require "real" proof, not fixtures-only)

### Wave 0 Gaps
- [ ] `tests/unit/validate/` — new directory, mirrors `tests/unit/detect/`/`tests/unit/normalize/`/`tests/unit/schema/`'s existing per-domain layout; covers VALID-01/02/03
- [ ] `tests/integration/test_validation_persistence.py`, `test_referential_integrity.py`, `test_backfill_resolution.py` — extend existing `tests/integration/conftest.py` fixtures (`migrated_dsn`, `s3_client`) rather than inventing new ones
- [ ] `tests/dagtest/` — NEW top-level test tier for `dag.test()`-based behavioral DAG tests. Needs its own `conftest.py` providing a session-scoped testcontainers PostgreSQL for the Airflow *metadata* DB (distinct from `tests/integration/conftest.py`'s *analytical* DB fixture — do not conflate the two databases, CLAUDE.md's own §4 constraint), `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` pointed at it, `AIRFLOW__CORE__EXECUTOR=LocalExecutor`, and a fixture patching `KubernetesPodOperator.execute` per CLAUDE.md's documented mocking pattern. Add a new pytest marker, e.g. `dagtest: needs a local Docker daemon (testcontainers PostgreSQL for Airflow metadata); excluded from the offline gate` to `pyproject.toml`'s `markers` list (mirrors the existing `integration`/`cluster` marker precedent)
- [ ] `tests/e2e/slice/test_referential_orphan.py`, `test_backfill_reentry.py` — extend existing `tests/e2e/slice/conftest.py` fixtures (`_require_cluster`, `s3_client`, `analytics_connection`/`etl_app` role connections), `cluster`-marked
- [ ] `configs/datasets/orders.yaml` fixture/corpus test data — new dataset needs its own tiny synthetic CSV fixture set (matching `customers`' precedent) under `tests/fixtures/` for both orphan and non-orphan rows
- [ ] Framework install: none — pytest/hypothesis/testcontainers all already pinned and used; `dag.test()` needs no new dependency (`apache-airflow` is already the Airflow image's own dependency, and `tests/dagtest/` only runs where the `apache-airflow` package is importable, i.e. the same environment `tests/unit/test_dag_structure.py` already imports `airflow.models.DagBag` from — confirm at planning time whether that's the repo's main venv or a separate one)

## Code Examples

### Extending `MetadataRepository` for validation persistence (follows existing Protocol convention)
```python
# Source: packages/dataplat/src/dataplat/metadata/repository.py (existing Protocol,
# this phase adds methods following the exact signature style already used by
# create_ingestion_run/finalize_publication -- explicit keyword-only args matching
# each table's real column set, not a generic dict-based write).
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

### `errors.py`'s first raise site this phase adds (per its own documented convention)
```python
# Source: packages/dataplat/src/dataplat/errors.py module docstring: "QualityThresholdExceeded
# and PublicationError are still deliberately absent: each is added by the phase that first
# raises it." This phase adds both.
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

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| `FEATURES.md` §3.2's "redrive" terminology and `PENDING → REDRIVEN \| DISCARDED \| ACCEPTED` 3-outcome lifecycle | D-01's "backfill" (never "redrive") and D-04's 2-state (`PENDING`/`RESOLVED`) lifecycle with `resolution_type` disambiguating | 08-CONTEXT.md discussion, 2026-08-17 | Any plan/code referencing "redrive" or a third `ACCEPTED` state is citing stale, superseded research language — treat `FEATURES.md` §3.2 as historical context only, not current design |
| ARCHITECTURE.md Q2.2's `meta.retention_policies`/partitioning framing for `rejected_records` | D-12: plain, unpartitioned tables this phase | 08-CONTEXT.md D-12 | Do not add partitioning DDL in this phase's migrations |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `integrity_gate` (LOAD-10) should be a plain `@task`, not a `PythonSensor`/deferrable sensor, since the "wait" semantics are already covered by the upstream `S3KeySensor` | Pattern 4 | Low — CONTEXT.md explicitly marks this as Claude's discretion; if the planner prefers a sensor (e.g. to retry the stability check with backoff rather than failing on first mismatch), that is a legitimate alternative within the same discretion |
| A2 | LOAD-10 gate failures write to `meta.files.status` via a narrow, DAG-file-local psycopg call rather than a KPO launch, to respect ADR-0004's "Airflow image never imports dataplat" boundary | Pattern 4 | Medium — this is a genuinely new architectural surface with no existing precedent in the codebase; if the planner instead decides a minimal KPO launch is acceptable for gate-failure bookkeeping, that changes the DAG's task graph and this phase's Dockerfile/image boundary discussion. Flagged explicitly as an Open Question below, not silently assumed |
| A3 | VALID-09's "10x baseline" ROADMAP success criterion is either narrowly in-scope for phase 8 (persistence + single-file comparison only, not full historical-trend anomaly detection) or should be re-scoped at planning time — not fully built here | Phase Requirements table, Pitfall 1 | Medium — building the wrong scope wastes a wave; not building any of it risks failing ROADMAP's literal success criterion #5. Flagged as Open Question |
| A4 | `resolution_type` values are exactly `PENDING`/`REDRIVEN`/`DISCARDED` (three text values on one column), satisfying D-04's "2 top-level states, disambiguated by `resolution_type`" framing | Locked Table Shapes | Low — D-04 explicitly delegates the exact column shape to Claude's discretion; an alternative (a boolean `is_resolved` + separate `resolution_type` nullable-until-resolved) would satisfy the same constraint equally well |

**If this table is empty:** N/A — see entries above.

## Open Questions

1. **Is VALID-09's "10x row-count baseline" check actually in-scope for Phase 8, given the requirement itself maps to Phase 9?**
   - What we know: ROADMAP's phase-8 success criterion #5 explicitly demands it; REQUIREMENTS.md's traceability table maps VALID-09 to Phase 9; CONTEXT.md's `<domain>` section lists "anomaly detection over time-series validation history (VALID-05/06)" as out of scope for Phase 8 but does not explicitly address VALID-09's simpler single-file-vs-baseline case.
   - What's unclear: Whether ROADMAP's success criterion describes a genuinely narrower capability (persist a row-count metric + one threshold comparison, no historical trend) that's meant to land THIS phase as a down payment on VALID-09, versus a drafting inconsistency that should be resolved by dropping the criterion or deferring it.
   - Recommendation: Surface this explicitly during `/gsd:plan-phase` or `/gsd:discuss-phase` follow-up, rather than the planner silently choosing. If built, keep it minimal: persist `row_count` per run (a `meta.quality_metrics`-shaped write, or a column on `meta.validation_results` with `rule_type='QUALITY'`/a dedicated `VOLUME` rule type) and one SQL comparison against `avg(historical row_count)`, no forecasting.
   - **(RESOLVED — see plan 08-09.)** Built minimally, exactly per this recommendation: `VolumeAnomalyBarrier` (a `BarrierStage`) persists `row_count`/comparison outcome as a `meta.validation_results` row with `rule_type="VOLUME"`, comparing against a persisted historical baseline — no forecasting, no ML, no historical-trend logic (that stays Phase 9's VALID-05/06).

2. **How does the LOAD-10 gate record a rejection without importing `dataplat` into the Airflow image?**
   - What we know: D-20 requires the rejection to land in `meta.files.status` with a reason, no `run_id`. ADR-0004 (cited, unchanged) forbids `dataplat` in the Airflow image. No existing DAG code writes directly to the analytical database today — `discover`/`ingest` are both KPO-launched precisely to keep the Airflow image free of `dataplat`.
   - What's unclear: Whether a narrow, explicit exception (a small inline `psycopg` call in the DAG file, using the same `DATAPLAT_DB_DSN`-equivalent credential resolved via Airflow's own Vault-backed connection, not `dataplat`'s `SecretsResolver`) is acceptable, or whether a minimal purpose-built KPO (accepting the "spin up a pod for a bad file" cost D-18 exists to avoid, but only for the gate-failure path, not the common case) is preferred.
   - Recommendation: Decide explicitly in planning — this is a real architectural fork, not a style choice. Lean toward the narrow inline-DB-call exception (matches D-18's stated rationale: "Airflow already owns file discovery," extending naturally to "and its own rejection bookkeeping").
   - **(RESOLVED — see plan 08-02.)** Decided exactly per this recommendation: a narrow inline `psycopg` call in `airflow/dags/_common/integrity_gate.py`'s `_reject_file`, never a KPO launch, never a `dataplat` import. Revision iteration 1 additionally resolved the `content_sha256 NOT NULL` conflict this pattern surfaces: a real SHA-256 of known-empty content for the empty-file case, a deterministic `REJECTED:<reason>`-derived sentinel hash for every case where the real bytes are unknown or ambiguous (wrong extension, unstable object, unreadable stream) — every rejection path now writes a real `meta.files` row, no exceptions.

3. **Does `tests/dagtest/`'s testcontainers-backed Airflow metadata DB conflict with `tests/integration/`'s existing testcontainers analytical DB in CI resource terms?**
   - What we know: CLAUDE.md's CI constraint caps GitHub-hosted runners at 4 CPU/16GB, already tight for the full local stack; `tests/integration/conftest.py` already runs one testcontainers PostgreSQL 18 + one testcontainers MinIO per session.
   - What's unclear: Whether adding a THIRD testcontainers PostgreSQL (Airflow metadata, matching CLAUDE.md's `dag.test()` guidance) for `tests/dagtest/` fits the same CI budget, or whether it needs to run in a separate CI job/stage.
   - Recommendation: Size this at planning time against actual CI runner behavior; consider whether `tests/dagtest/` needs its own CI job (parallel to, not combined with, `tests/integration/`) to avoid three concurrent containers plus the pytest process itself exceeding 16GB.
   - **(RESOLVED — see plan 08-13.)** `tests/dagtest/` stood up as its own top-level tier with its own session-scoped testcontainers Airflow-metadata PostgreSQL and its own `conftest.py`, sized to run as a separate CI job/stage from `tests/integration/`'s analytical-DB containers, per this recommendation (see `08-VALIDATION.md`'s Sampling Rate section).

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Docker daemon | `tests/integration/` (existing), `tests/dagtest/` (new, this phase) | Not probed this session — matches existing `tests/integration/conftest.py`'s own `_require_docker` skip-with-reason pattern | — | Both tiers already/will skip cleanly with a named reason, no fallback needed |
| Live kind cluster | `tests/e2e/slice/` (existing, extended this phase for VALID-07/VALID-08 live proofs) | Not probed this session — matches existing `_require_cluster` skip pattern | — | Skips cleanly; genuine live proof deferred to a session with a running cluster |
| `apache-airflow-providers-amazon`'s `S3Hook` | LOAD-10 gate (`integrity_gate.py`) | Already a transitive dependency of the pinned Airflow image (S3KeySensor already imports from this provider) | Matches Airflow 3.3.0's constraints-pinned provider version | — |

**Missing dependencies with no fallback:** None identified — every new capability builds on already-present, already-pinned dependencies (`apache-airflow-providers-amazon`, `psycopg`, `testcontainers`, `pytest`/`hypothesis`).

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|----------------|---------|-------------------|
| V2 Authentication | No | This phase adds no new authentication surface — reuses existing Vault-backed `minio_default`/`DATAPLAT_DB_DSN` credentials unchanged |
| V3 Session Management | No | Not applicable — no session concept in this phase |
| V4 Access Control | Yes | `meta.rejected_records`/`meta.validation_results` grants follow the exact `GRANT SELECT, INSERT, UPDATE ON meta.<table> TO etl_app` pattern every prior migration uses (migrations 0002, 0004, 0005, 0009) — no `DELETE` grant, matching D-04's no-per-row-edit constraint at the database-privilege level too, not just the application-logic level |
| V5 Input Validation | Yes | `quality:`/rule-type config blocks validated by Pydantic `extra="forbid", frozen=True` (existing `DatasetConfig` convention); raw `raw_line`/`error_message` values written to `meta.rejected_records` are untrusted CSV content and must never be interpolated into SQL as anything other than a bound parameter (the existing `_PUBLISH_SQL` pattern already demonstrates parameterized `%s` binding — the new INSERT statements must follow it identically, never string-formatting a rejected row's raw content into SQL text) |
| V6 Cryptography | No new surface | `content_sha256` recomputation (D-22, LOAD-10) reuses the existing hash recipe/`hash_version` column (META-02) — no new crypto primitive introduced |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|----------------------|
| SQL injection via a rejected row's raw content (`raw_line`, `error_message`) landing in `meta.rejected_records` | Tampering | Parameterized queries only (`%s` placeholders via psycopg) — never string-format a row's content into a SQL statement, matching this codebase's own `_PUBLISH_SQL` precedent (the ONLY dynamic fragment there is an identifier, never a value) |
| A malicious/malformed file crafted to pass the LOAD-10 stability check but change between the second HEAD and the actual `GET` in `discover`/`ingest` | Tampering | D-21's own stated scope limit: this guards against multi-PUT/overwrite-in-place, not a true TOCTOU race — S3/MinIO single-PUT GET semantics already prevent a genuine partial read (documented, not re-solved here); do not attempt to "fix" this with a third HEAD call, it is out of scope by design |
| An `orders` row with a `customer_id` crafted to collide with another dataset's business key across tenants (not applicable — single-tenant local platform) | N/A | Not applicable; this platform has no multi-tenancy concept (explicitly out of scope, REQUIREMENTS.md "Out of Scope" table) |

## Sources

### Primary (HIGH confidence)
- `.planning/research/ARCHITECTURE.md` Q2.2 (lines 228-246), Q2.3 (lines 247-265), §2.4 (lines 267-276), §3.1 (lines 282-287) — locked table shapes and lineage pattern, read directly this session
- `.planning/research/FEATURES.md` §3.2 (lines 193-202), §3.3 (lines 204-217) — quarantine/reporting design rationale (superseded terminology noted explicitly), read directly this session
- `.planning/phases/08-validation-quarantine-metadata-control-plane-completion/08-CONTEXT.md` — D-01 through D-22, read in full this session
- `.planning/REQUIREMENTS.md` — VALID-01..09, LOAD-10/11 full text and phase-mapping table, read in full this session
- Direct code reads this session: `packages/dataplat/src/dataplat/pipeline/protocol.py`, `engine.py`, `models/record.py`, `models/report.py`, `errors.py`, `models/receipt.py`, `models/assignment.py`, `config/model.py`, `config/registry.py`, `load/publish/merge.py`, `metadata/repository.py` (signatures), `storage/objectstore.py` (signatures)
- Direct migration reads this session: `migrations/versions/0002_meta_files.py`, `0004_meta_ingestion_runs.py`, `0005_normalized_customers.py`, `0009_meta_schema_versions.py`, `0010_meta_datasets_freshness.py` — confirmed zero-native-enum convention, deferred-FK pattern, grant pattern
- Direct DAG/test reads this session: `airflow/dags/csv_ingest_customers.py`, `airflow/dags/_common/kpo.py`, `tests/unit/test_dag_structure.py`, `tests/unit/conftest.py`, `tests/integration/conftest.py`, `tests/e2e/slice/conftest.py` (docstring) — confirmed existing DagBag-structural test pattern, testcontainers fixture pattern, no existing `dag.test()` usage
- `airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html` — Asset/outlets/schedule API confirmed current for Airflow 3.3.x via WebSearch this session `[CITED]`

### Secondary (MEDIUM confidence)
- CLAUDE.md's own "Testing Airflow 3 DAGs" section (project-committed, treated as authoritative for this repo's conventions) — `dag.test()` entry point, KPO-mocking pattern, `filterwarnings` guidance

### Tertiary (LOW confidence)
- None — every claim in this document traces to a locked CONTEXT.md decision, a direct code/migration read, or a cited official-docs search result this session.

## Metadata

**Confidence breakdown:**
- Standard stack / locked shapes: HIGH — carried forward verbatim from ARCHITECTURE.md/CONTEXT.md, cross-checked against 13 real migrations for convention consistency
- Architecture (rule engine placement, Airflow gate/Asset patterns): HIGH — every pattern extends an existing, working precedent in this exact codebase; the two genuinely novel surfaces (LOAD-10's DB-write-from-Airflow question, VALID-09's scope) are explicitly flagged as Open Questions rather than asserted
- Pitfalls: HIGH — each pitfall is derived from a specific, cited tension between two locked decisions (e.g. D-11 vs. streaming persistence) or a documented codebase convention (CLAUDE.md's KPO-mocking guidance) rather than generic ETL folklore

**Research date:** 2026-08-17
**Valid until:** 30 days (stable, locally-controlled codebase; no fast-moving external dependency drives this research's shelf life)
