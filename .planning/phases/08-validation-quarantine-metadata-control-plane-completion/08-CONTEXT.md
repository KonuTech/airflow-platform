# Phase 8: Validation, Quarantine & Metadata Control-Plane Completion - Context

**Gathered:** 2026-08-17
**Status:** Ready for planning

<domain>
## Phase Boundary

This phase delivers: (1) a real, persisted validation-rule engine (structural / schema / type / quality / referential rule types, each with its own configurable bad-record strategy) whose findings are written to new `meta.validation_results` and `meta.rejected_records` tables instead of being ephemeral report objects; (2) a first-class quarantine **backfill** path — not a bespoke redrive mechanism, but re-triggering the existing ingestion DAG as an Airflow backfill run against a corrected file, with rejected-records rows marked resolved/linked to the superseding run; (3) referential-integrity checking (VALID-07) proven against a real second dataset (`orders` → `customers`), not just fixtures; (4) an Airflow-side, pre-pod-launch file/manifest integrity gate (LOAD-10/11) that keeps partially-uploaded or corrupt files from ever reaching a pipeline run.

Out of scope for this phase: anomaly detection over time-series validation history (VALID-05/06 — Phase 9, depends on this phase's persisted results existing first), table partitioning of any kind (deferred to Phase 9's INCR-04), retention/archival policy for `rejected_records`/`validation_results` (Phase 11 Operations concern).

</domain>

<decisions>
## Implementation Decisions

### Quarantine backfill path (VALID-08)
- **D-01:** Backfill is the ONLY re-entry mechanism, and "backfill" is the locked term — never "redrive". A corrected file is re-ingested by triggering the SAME ingestion DAG as an Airflow backfill run for the original logical date/batch. No separate redrive DAG, CLI, or endpoint exists.
- **D-02:** `load.strategy: merge` (upsert / `ON CONFLICT`, Phase 4's atomic publish pattern) is what makes backfill safe: previously-rejected-now-valid rows insert naturally, already-loaded rows re-upsert harmlessly. No new load-path branching for the backfill case.
- **D-03:** Granularity is whole-batch only. A backfill acts on the entire rejection set for a run/file — never an individual row or an arbitrary subset.
- **D-04:** Resolution lifecycle on `meta.rejected_records` is exactly **2 states**: `PENDING` and `RESOLVED` (reached via either `REDRIVEN`-via-backfill or `DISCARDED`-via-explicit-batch-level-operator-action — model these as a `resolution_type` value, not a third top-level state). **Hard constraint: no per-row manual state editing, ever.** Resolution changes happen only as a whole-batch side effect of a backfill run completing, or an explicit batch-level discard operation. Do not build any UI, API, or SQL convenience that lets an operator flip a single row's status.
- **D-05:** When a backfill run completes, the rejected_records rows it supersedes are marked resolved and linked (FK) to the new run_id — this linkage is how lineage answers "was this ever fixed, and by what run."
- **D-06 [informational]:** No new tooling for operators to find what to backfill. They query `meta.rejected_records` / `meta.files` directly via SQL — matches the platform's existing SQL-queryable-lineage philosophy (no dashboard, no CLI helper for this in Phase 8).

### Bad-record strategy assignment
- **D-07:** Strategy (`FAIL_FILE` / `REJECT_RECORD` / `QUARANTINE_FILE` / `QUARANTINE_RECORD` / `WARN_AND_CONTINUE`) is assigned **per-rule-type**, dataset-configurable — each rule/rule_type (FILE / STRUCTURAL / SCHEMA / TYPE / QUALITY / REFERENTIAL) declares its own strategy in the dataset YAML. Not one blanket strategy per dataset.
- **D-08:** Structural failures (VALID-01, ragged rows) default to `REJECT_RECORD` — matches the existing `RaggedRowGuard` behavior from Phase 3/6; no behavior change to already-working code, only a config surface added on top.
- **D-09:** `customers.yaml` gets a **real** `quality:` block (not corpus/fixture-only) — proves the full VALID-01/02/03/04 chain live against the one real cycling dataset.
- **D-10:** A separate, configurable **run-level rejection-rate threshold** (e.g. FAIL if >10% of rows rejected) acts as a circuit breaker layered on top of row-level strategies — each rule keeps its own row-level strategy, but the aggregate can still escalate the whole run to FAIL.
- **D-11:** When a run escalates to FAIL, **nothing publishes** — the entire atomic publish transaction (Phase 4's staging → single-writer publish) rolls back. FAIL is unambiguous: nothing from this run reaches the warehouse. Good rows land only once the file is corrected and backfilled (ties directly to D-01).
- **D-12 [informational]:** `meta.rejected_records` and `meta.validation_results` are **plain tables**, not partitioned, in this phase. Partitioning (if ever needed) is a retention concern for Phase 11 and a well-trodden later migration, not something to build speculatively now.

### Referential integrity scope (VALID-07)
- **D-13:** VALID-07 is proven with a **real second dataset**, not fixtures only: a new `orders` dataset referencing `customers` (`customer_id` FK).
- **D-14:** `orders` gets its **own dedicated DAG**, `csv_ingest_orders`, mirroring `csv_ingest_customers`'s shape (same config-driven Source→Stage→Publisher pipeline, no new capability invented).
- **D-15:** `orders` DAG is coupled to the `customers` DAG via an **Airflow Dataset/Asset dependency** — reduces (does not eliminate) orphan cases, since a customer from a different/later batch can still be legitimately missing at ingest time.
- **D-16:** Default orphan-order handling is `QUARANTINE_RECORD`: rows whose `customer_id` isn't found in `normalized.customers` go to `rejected_records` with `error_type=REFERENTIAL_ORPHAN`; the rest of the file loads normally. This is a `QUARANTINE_RECORD`-strategy row (D-07), backfillable once the customer arrives (D-01).
- **D-17:** Minimal `orders` schema: `order_id`, `customer_id` (FK), `order_date`, `amount`. Same config shape as `customers.yaml` (`columns:` / `deduplication:` / `load:` / `freshness:` blocks) — no new dataset-config capability required.

### File/manifest integrity gate placement (LOAD-10/11)
- **D-18:** LOAD-10's checksum/size/extension/completeness checks run **Airflow-side, before pod launch** — a sensor/task does an S3 HEAD and gates the KubernetesPodOperator, rather than being the pipeline's first Stage. Fails fast, avoids spinning up a pod for a bad file, and matches Airflow already owning file discovery (frozen-manifest / `AssignmentDocument` pattern).
- **D-19:** `_BATCH_COMPLETE` (LOAD-11) is built and corpus/fixture-tested as a capability, but **stays unexercised** by both `customers.yaml` and `orders.yaml` — same "opt-in, unexercised" precedent as Phase 6's filename masks (06-CONTEXT.md D-10). Both live datasets remain single-file-per-drop; no dataset config turns the manifest marker on in this phase.
- **D-20:** A gate failure is a **file-level rejection recorded in `meta.files.status`** (e.g. `failed`/`rejected` + reason column) — no `meta.ingestion_runs` row, no `run_id`, no `rejected_records` rows are ever created for an integrity-gate failure. This is structurally distinct from a row-level validation-rule outcome inside a run: integrity failures never get far enough to become a run at all.
- **D-21:** "Transfer completion" verification is an **object-stability check**: the Airflow-side sensor does two S3 HEAD calls a short interval apart; unchanged size/ETag means the object is stable/complete. This guards against a producer doing multiple sequential PUTs (or overwrite-in-place), not against genuine partial reads — S3/MinIO single-PUT semantics already prevent those (a GET never returns a partial object).
- **D-22:** "Checksum" verification does **not** compare against an externally-supplied checksum file. The gate confirms the object is readable and (re)computes `content_sha256` for `meta.files`, reusing the existing Phase-2/3 discovery-hash column — there is no `*.sha256` sidecar convention in this phase.

### Claude's Discretion
- Exact column/index shape for the `resolution_type` field on `meta.rejected_records` (D-04) — whether it's a single enum column or a small lookup, as long as the 2-state-lifecycle + no-per-row-edit constraints hold.
- Exact naming/shape of the run-level rejection-rate threshold config key under `quality:` (D-10).
- Whether the Airflow-side integrity sensor (D-18) is a custom `@task` or a `PythonSensor`/deferrable sensor — implementation detail, not a locked architectural choice.

### Gap closure: VALID-08 backfill resolution scoping (2026-08-17, post-verification)

**Confirmed gap (08-VERIFICATION.md, live-proven):** `discover_files`'s `batch_key` is a
pure function of `content_sha256`, so a content-differing correction of a previously-rejected
row always discovers under a NEW `batch_id`. `resolve_rejected_records_for_batch` (D-05) scopes
strictly by `batch_id`, so it can never touch the ORIGINAL batch's `PENDING` row. Live-confirmed:
`test_backfill_reentry.py` ran discover → ingest → real backfill re-execution → corrected row
published to `normalized.customers`, then failed only at the final `REDRIVEN` assertion, exactly
as this gap predicts. Full trace: `.planning/debug/resolved/backfill-does-not-redrive-rejected-row.md`,
`08-VERIFICATION.md` `gaps[0]`.

- **D-23 (LOCKED — user decision, Rule 4):** Resolution scoping moves from strictly
  `batch_id`-scoped to **business-key-scoped**: `meta.rejected_records` gains a durable
  `business_key` value (the dataset's configured business/unique key column value for the
  rejected row, e.g. `customer_id`) captured at `record_rejected_records` insert time.
  Resolution matches on `(dataset, business_key)` with `resolution_type = 'PENDING'` —
  **not** on `batch_id` — so a backfill run completing resolves every PENDING reject sharing
  that business key, regardless of which batch originally rejected it or which batch the
  correction discovers under. This is the direction the codebase already leans (Phase 4's
  `ON CONFLICT`/`MERGE` publish pattern is itself business-key-driven, per §27/§55 in
  CLAUDE.md's stack notes) — extend that same identity concept to rejection resolution
  instead of inventing a second, batch-lineage-based identity scheme.
- **D-24 (LOCKED):** D-03 ("granularity is whole-batch only") is **preserved, reinterpreted**:
  resolution still only happens as a whole-batch side effect of a backfill run *completing*
  (D-04's no-per-row-manual-edit constraint is untouched) — what changes is the *matching
  predicate* used to decide which PENDING rows that side effect resolves (business-key match,
  not batch_id match). No new manual/per-row resolution API is introduced by this decision.
- **D-25 (LOCKED):** A row that fails validation before its business-key column can be
  reliably extracted (e.g. a structural/ragged-row failure where column positions are
  unreliable) stores `business_key = NULL`. A `NULL` business_key row is **never**
  auto-resolved by this mechanism — it remains `PENDING` until an explicit batch-level
  discard (D-04's other resolution path). This is a deliberate, narrower fallback, not a
  regression: today, with strict `batch_id` scoping, these rows *also* never auto-resolve
  for a content-differing correction, so no currently-working case is broken.
- **Claude's Discretion (added by this gap-closure decision):** Which dataset config field
  designates "the business key" for `business_key` extraction (a new explicit `quality:` or
  top-level dataset-config key, vs. reusing `deduplication:`'s existing key declaration if one
  exists) — read `configs/datasets/customers.yaml`/`orders.yaml` and the dataset-config Pydantic
  models before deciding; prefer reusing an existing key concept over inventing a new one.
  Whether `business_key` is stored as a single `text` column (requiring per-dataset
  single-column business keys) or a composite/JSON shape — default to the simplest option
  that covers `customers`/`orders`' actual (single-column) keys, note if a composite key
  need is discovered.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & roadmap
- `.planning/ROADMAP.md` Phase 8 section — goal, success criteria, Wave E plan guidance (validation ‖ metadata completion, coordinate only on `meta.validation_results` DDL; quarantine backfill is first-class, not documentation; statistical thresholds only, no ML)
- `.planning/REQUIREMENTS.md` VALID-01..04, VALID-07..09, LOAD-10, LOAD-11 — full requirement text and the requirement→phase mapping table

### Prior research (table shapes, lifecycle design)
- `.planning/research/ARCHITECTURE.md` Q2.2 — `meta.validation_results`, `meta.rejected_records` proposed column shapes (starting point; D-12 overrides its speculative partitioning)
- `.planning/research/ARCHITECTURE.md` Q2.3 — record-level lineage pattern (`_run_id`/`_file_id`/`_batch_id`/`_source_row_number`/`_record_hash`/`_ingested_at`) to reuse for `rejected_records` row provenance
- `.planning/research/FEATURES.md` §3.2 — quarantine/bad-record handling, the original "re-drive" GAP language (now superseded by D-01's "backfill" terminology)
- `.planning/research/FEATURES.md` §3.3 — validation reporting as persisted rows, Great Expectations lesson, anomaly detection's dependency on this phase

### Prior-phase precedent (do not re-litigate)
- `.planning/phases/06-universal-csv-engine-schema-contracts-normalization/06-CONTEXT.md` D-10 — the "opt-in, unexercised by customers.yaml" pattern D-19 explicitly reuses for `_BATCH_COMPLETE`
- `migrations/versions/0010_meta_datasets_freshness.py` — precedent that research-doc table proposals (e.g. `ARCHITECTURE.md`'s `dataset_sla`) get refined/superseded during actual implementation; expect the same for `validation_results`/`rejected_records` DDL
- `migrations/versions/0002_meta_files.py` — existing `meta.files` columns (`content_sha256`, `hash_version`, `status`, etc.) that D-20/D-22 build directly on top of, not alongside

### Architecture this phase must build within
- `docs/adr/0008-pipeline-composition-seam.md` — the `Source`→`RecordChunk`→`Stage`→`Publisher` composition seam; new validation logic is a `StreamingStage`/`BarrierStage`, not a redesign
- `packages/dataplat/src/dataplat/pipeline/protocol.py` — `PipelineContext`, `StreamingStage`/`BarrierStage` Protocols
- `packages/dataplat/src/dataplat/models/record.py` — `RecordChunk`, `RejectedRecord`, `StageResult` (existing shapes to extend, not replace)
- `packages/dataplat/src/dataplat/models/report.py` — `ValidationResult`, explicitly documented as "the minimal D-05 shape" awaiting this phase's richer enum-typed version and `meta.validation_results` DDL
- `packages/dataplat/src/dataplat/errors.py` — exception hierarchy; `QualityThresholdExceeded` and `PublicationError` are deliberately unadded, this phase's job per the "exception subclass added by the phase that first raises it" rule
- `packages/dataplat/src/dataplat/models/receipt.py` — `Receipt`'s docstring explicitly flags that no quarantine concept exists until this phase; will need a quarantine-aware field
- `packages/dataplat/src/dataplat/models/assignment.py` — `FileAssignment`/`BatchAssignment`/`AssignmentDocument`; docstring flags no schema-versioning/partitioning/quarantine-policy exists until Phase 6/8

### Config template for the new `orders` dataset
- `configs/datasets/customers.yaml` — the only real dataset config today; `orders.yaml` (D-13..D-17) must follow the same `source:`/`deduplication:`/`load:`/`batching:`/`freshness:`/`columns:` shape, with a `quality:` block added per D-09

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- Phase 4's atomic publish pattern (`UNLOGGED` staging → `pg_advisory_xact_lock` → `INSERT ... ON CONFLICT`) is exactly what makes D-01/D-11's all-or-nothing backfill and FAIL-rollback semantics work with zero new transactional machinery.
- `meta.files.content_sha256`/`status` columns (0002 migration) already exist — D-20/D-22 extend these rather than inventing a parallel integrity-tracking surface.
- The frozen-manifest `AssignmentDocument` discovery pattern is where D-18's Airflow-side gate naturally slots in — Airflow already owns "which files exist and are they ready" before any pod launches.

### Established Patterns
- Config-not-code: every new behavior surfaces as a `configs/datasets/<name>.yaml` block validated by Pydantic (`extra="forbid", frozen=True`) and synced into `meta.config_versions` — the `quality:` block (D-09) and per-rule-type strategy (D-07) follow this, not a code-level switch.
- "Opt-in, unexercised by customers.yaml" (Phase 6 D-10) — reused verbatim for `_BATCH_COMPLETE` (D-19).
- Exception subclasses are added by the phase that first raises them (`errors.py` convention) — this phase adds `QualityThresholdExceeded` and `PublicationError`.

### Integration Points
- New `meta.validation_results`/`meta.rejected_records` tables are the coordination point between the two Wave-E work streams (validation engine, metadata completion) per ROADMAP guidance — DDL must land before either stream's code depends on it.
- `csv_ingest_orders` DAG integration point: Airflow Dataset/Asset produced by `csv_ingest_customers`, consumed by `csv_ingest_orders` (D-15).
- Backfill re-entry (D-01) integrates through the standard Airflow backfill CLI/API against the existing DAG — no new entry point to build.

</code_context>

<specifics>
## Specific Ideas

- The user was explicit and firm that "backfill" (not "redrive") is the term to use everywhere — in code, config keys, table/column names, docs, and DAG-run semantics. Treat any occurrence of "redrive" in prior research docs (`FEATURES.md` §3.2) as superseded language.
- The user was explicit and firm that no mechanism may allow a human to flip an individual row's resolution status. Every resolution-state transition must be a side effect of a batch/run-level operation (a backfill run completing, or an explicit batch-level discard). This is a hard constraint, not a preference — do not build a convenience API/UI around it even if it seems minor.
- `orders` was deliberately chosen as a small, realistic second dataset (not a synthetic fixture-only construct) specifically so VALID-07 is proven end-to-end against real DAG execution, real Dataset/Asset scheduling, and a real orphan scenario — not just unit/property tests.

</specifics>

<deferred>
## Deferred Ideas

- **Table partitioning** (raised by the user mid-discussion) — PostgreSQL native partitioning of `meta.rejected_records`/`meta.validation_results`, or of normalized/warehouse target tables by `business_date`, belongs to Phase 9 (ROADMAP's own INCR-04 success criterion: "a record arriving three months late lands in its correct historical partition"). Phase 8's per-file/per-run staging+publish already isolates a FAIL rollback to just that file regardless of physical partitioning, so nothing in Phase 8 blocks on this. See D-12.
- **Anomaly detection over validation history** (VALID-05/06) — explicitly out of scope per ROADMAP; depends on this phase's persisted `meta.validation_results` existing first, and is Phase 9's job.
- **Retention/archival of rejected_records/validation_results** — a Phase 11 (Operations) concern; not designed in this phase beyond keeping the tables as plain (non-partitioned) tables now.

</deferred>

---

*Phase: 8-Validation, Quarantine & Metadata Control-Plane Completion*
*Context gathered: 2026-08-17*
