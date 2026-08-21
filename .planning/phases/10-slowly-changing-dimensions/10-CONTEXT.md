# Phase 10: Slowly Changing Dimensions - Context

**Gathered:** 2026-08-21
**Status:** Ready for planning

<domain>
## Phase Boundary

Historical truth for `normalized.customers` is maintained correctly — a changed tracked attribute
produces a new SCD2 version with correct `valid_from`/`valid_to`/`is_current`, the database rejects
overlapping validity intervals for a business key, and a late-arriving correction rebuilds a key's
history by recomputation, never in-place surgery.

**Scope note — CDC excluded (2026-08-21, same session as this discussion):** ROADMAP.md/
REQUIREMENTS.md were updated to drop `CDC-01/02/03` and rename this phase from "CDC & Slowly
Changing Dimensions" to "Slowly Changing Dimensions" before this discussion began. Reasoning: CDC's
design was already a CSV-delivered batch feed, not live monitoring of an external system, but
nothing in the project produces that feed format and SCD 0/1/2 build entirely from CSV batches
without it — see REQUIREMENTS.md's Out of Scope table for the full entry (DoD 44/45/46/87). `SCD-08`
(DELETE semantics) is re-scoped in this discussion to source from full-snapshot CSV batches instead
of a CDC feed.

**Locked by ROADMAP.md's Phase 10 plan guidance (prior to this discussion, not re-litigated here):**
- SCD2 is explicitly **Python-owned**, not dbt — Phase 08.1 rejected `dbt snapshot` for the same
  reason gold stays Python-owned (dbt's per-model transactions can't join META-03's
  single-transaction guarantee).
- SCD lands as a **`Publisher`** on the existing `Source`/`Publisher` seam (ADR-0008) — no parallel
  pipeline.
- Every SCD2 dimension carries a **`btree_gist` exclusion constraint** on `(business_key, validity
  range)` in its *creating* migration (PITFALLS #2) — cannot be retrofitted once overlaps exist.
- Corrections **recompute** history from the ordered batch record, never in-place interval surgery
  (PITFALLS #6).
- dbt vocabulary: surrogate key independent of the change hash, `hard_deletes = ignore | invalidate
  | new_record`, `valid_to_current` sentinel instead of NULL, both `timestamp`/`check`
  change-detection strategies as vocabulary.
- Change hashing uses **normalized** content with `hash_version` (Phase 3/6 precedent).

</domain>

<decisions>
## Implementation Decisions

### Dimension Scope

- **D-01:** SCD2 applies to **`customers` only**. `orders` stays out of SCD scope entirely —
  orders are immutable business events (a discrete transaction), not a "slowly changing" dimension;
  nothing about `orders.yaml`'s columns changes for this phase.
- **D-02:** Within `customers`, column treatment:
  - `customer_id` — the business key. Not itself tracked/versioned; identifies which version chain
    a row belongs to.
  - `name`, `country` — **Type 2** (full history). A change to either produces a new SCD2 version
    with a new `valid_from`/`valid_to`.
  - `birth_date` — **Type 1** (overwrite, no history). Reasoning: birth_date is not a genuinely
    "slowly changing" business fact for a real person — an incoming change is treated as a
    data-quality correction to the current row, not a new dimension version.
  - `event_ts` — not itself a change-tracked business attribute; it is the effective-date **source**
    (see D-03), excluded from the change hash the same way it's excluded from dedup's business
    columns today.

### Effective Dating (SCD-06)

- **D-03:** SCD2's `valid_from` reuses `customers.yaml`'s existing `event_ts` column — the same
  column already driving Phase 9's watermark (D-02, `09-CONTEXT.md`) and `MergePublisher`'s
  late-arrival guard. One date concept per dataset, not two competing ones; satisfies SCD-06's
  "never defaults to ingestion time" requirement structurally rather than by convention.

### Delete Detection (SCD-08)

- **D-04:** Phase 10 is the first phase to actually act on `source.change_semantics: "snapshot"` —
  declared in `customers.yaml` since Phase 4 but verified (via repo-wide grep) to have **zero code
  consumers today**. Each `customers.csv` file is now treated as a full point-in-time customer
  roster: the SCD Publisher compares the current run's set of `customer_id`s (post-dedup, from
  silver) against gold's currently-`is_current=true` set. A `customer_id` present in gold-current
  but absent from the new snapshot triggers the configured DELETE semantics (D-05).
- **D-05:** DELETE semantics default for `customers.yaml` is **`invalidate`** — a vanished
  customer's current SCD2 row gets closed out (`valid_to` set to the triggering run's effective
  date, `is_current=false`), preserving history rather than physically deleting the row or silently
  ignoring the absence (`ignore`) or inserting a tombstone version (`new_record`).
- **D-06 (LOCKED — user-added safety mechanism, direct precedent from Phase 8 D-10's rejection-rate
  circuit breaker, `08-CONTEXT.md`):** A configurable **mass-delete circuit breaker** guards
  `invalidate`: if the fraction of currently-current customers absent from a new snapshot exceeds a
  configurable threshold (Phase 8's own precedent value is 10% — treat as a starting point, not a
  locked number), the run **FAILs loudly** instead of auto-invalidating that fraction — treating an
  implausibly large disappearance as a probable truncated/bad source file, not a real mass-deletion
  event. Directly protects the Core Value ("no data is ever silently dropped, duplicated or
  corrupted"). Reuses `packages/dataplat/src/dataplat/validate/circuit_breaker.py`'s
  `RejectionRateCircuitBreaker` as the direct implementation template (same "count vs. threshold,
  fail the run" shape), not a new mechanism invented from scratch.
- Real fixtures required for the live proof: at least one snapshot genuinely missing a
  previously-present customer (exercises `invalidate`), plus a separate deliberately-bad snapshot
  missing an implausible fraction of customers (exercises the D-06 circuit breaker tripping) —
  whether this second fixture belongs inside or outside the D-11 2-year corpus is Claude's
  discretion (see below).

### Table Shape (LOCKED — user explicitly chose the higher-risk option, against the recommendation,
after a worked before/after example of both tradeoffs)

- **D-07:** `normalized.customers` **migrates in place** to SCD2 shape, rather than introducing a
  new, separate dimension table alongside the untouched original. `UNIQUE(customer_id)` (migration
  0006) is dropped and replaced with a `btree_gist` exclusion constraint on `(customer_id, validity
  range)`. Existing rows are backfilled as each customer's first SCD2 version (`valid_from` =
  earliest known `event_ts` or equivalent, `is_current=true`, `valid_to` = the `valid_to_current`
  sentinel).
- **D-08 (LOCKED, follows directly from D-07 — corrected mid-discussion, see below):** Every
  existing consumer of `normalized.customers`'s "one row per customer" assumption must be found,
  updated, and live-proven correct in **this phase**, not deferred. Known from context (research
  must still verify this list is exhaustive, not assumed complete):
  1. **`meta.v_customers_lineage`** (migrations 0012/0026/0030, `FROM normalized.customers c` —
     confirmed via grep) — needs an `is_current=true` filter or equivalent added.
  2. **Phase 9's silver→gold reconciliation** (`packages/dataplat/src/dataplat/pipeline/run.py`'s
     `_compute_silver_gold_reconciliation`, ~line 293, writing via
     `packages/dataplat/src/dataplat/metadata/repository.py`'s `record_reconciliation`) — the
     input/output count comparison must account for multiple gold rows now existing per
     `customer_id`.
  3. **`MergePublisher`** (`packages/dataplat/src/dataplat/load/publish/merge.py`) itself is not a
     "consumer to patch" — it IS the write path being redesigned into the SCD Publisher (its
     `INSERT ... ON CONFLICT (customer_id)` pattern cannot express SCD2 versioning and is replaced,
     not extended).
  - **Correction made mid-discussion:** Grafana dashboards were initially assumed to be a fourth
    consumer needing changes. Verified false by reading the actual dashboard provisioning
    (`helm/values/local/monitoring.yaml`): every panel queries `meta.ingestion_runs` for aggregate
    run metrics (`files_processed`, `rows_processed`, etc.) — **none** query `normalized.customers`
    directly. Grafana is explicitly NOT part of this phase's consumer-fix list.
- **Verified non-issue (a finding, not a decision):** `orders.customer_id` has **no
  database-level foreign key** to `normalized.customers.customer_id` — migration 0016's own
  docstring confirms this is deliberately app-level only (a plain non-unique index). Dropping
  `UNIQUE(customer_id)` for the exclusion constraint does not break any FK. Phase 8's
  `ReferentialIntegrityBarrier` (`packages/dataplat/src/dataplat/validate/referential.py`) uses an
  `EXISTS` check, which is cardinality-agnostic and needs no change for D-07.

### Surrogate Key

- **D-09:** The migrated table's surrogate key continues using **`BigInteger` +
  `Identity(always=True)`**, matching every existing table's convention (`normalized.customers`
  itself today, `meta.ingestion_runs`, `meta.run_stages`, `silver.customers`, etc.) — not
  `uuidv7()`. This deliberately supersedes STACK.md's original `uuidv7()` recommendation for
  surrogate keys: nothing in the codebase has ever adopted it, and codebase consistency wins over
  an unexercised aspirational recommendation.

### Concurrency

- **D-10 (LOCKED — user chose the more rigorous option, against the "inherit Phase 9's proof"
  recommendation):** Phase 10's live proof includes a **dedicated concurrent-SCD-publish test** — a
  live attribute change racing a backfill/correction for the same `customer_id` — rather than only
  inheriting Phase 9's `pg_advisory_xact_lock` concurrency proof (D-13, `09-CONTEXT.md`). Rationale
  given: SCD2's exclusion-constraint-plus-recompute-on-correction logic is genuinely new code, not a
  reuse of `MergePublisher`'s existing upsert path, so it deserves its own live concurrency proof.

### Late-Arriving Correction & Idempotent Replay Proof (SCD-07, SCD-09, SCD-10)

- **D-11 (LOCKED — user chose to extend the existing corpus over a smaller dedicated fixture set,
  consistent with the "prove at real scale" pattern already established in Phase 9's D-09/D-27):**
  The live proof for late-arriving corrections **extends Phase 9's existing 2-year backfill fixture
  corpus** (`tests/e2e/slice/test_backfill_2year_sweep.py` and its underlying fixture files) with
  genuine attribute-change events (at least one `name`/`country` change per D-02) and at least one
  deliberately out-of-order/late correction landing between two already-published SCD2 versions for
  the same `customer_id` — rather than building a small, separate SCD-only fixture set.
- **D-12:** The idempotent-replay proof (SCD-09/10) re-runs the **entire 2-year backfill a second
  time** (not just the single logical date containing the change) and asserts the SCD2 dimension's
  version count is unchanged — directly mirroring Phase 4's "re-run produces zero additional rows"
  pattern, applied to SCD2 version count instead of raw row count.

### Claude's Discretion

- Exact validity-range PostgreSQL type/representation for the exclusion constraint (e.g.
  `tstzrange`) and the literal `valid_to_current` sentinel value (e.g. a far-future timestamp
  constant vs. an application-level convention) — not discussed, implementation detail.
- Exact mass-delete circuit-breaker threshold value (D-06 named Phase 8's 10% as a starting
  reference, not a locked number for this dataset).
- Which dbt-vocabulary change-detection strategy (`timestamp` vs `check`) actually drives
  `customers.yaml`'s hash comparison — ROADMAP.md's plan guidance adopts both as vocabulary; SCD-05's
  "deterministic via normalized hash" requirement points toward `check`, but this wasn't asked
  directly.
- Whether the D-06 mass-delete-circuit-breaker fixture is folded into the same D-11 2-year corpus
  run or proven as a separate, smaller dedicated test — not specified. A deliberately-bad/truncated
  snapshot doesn't obviously belong inside the "realistic 2-year narrative," so a separate small test
  is likely cleaner, but this is left open.
- How SCD2 versioning behaves across the 2-year corpus's existing deliberate schema-version change
  (Phase 9 D-10) — not raised as a distinct question; research should verify correctness here,
  consistent with Phase 9's own QUAL-11 historical-schema-resolution proof, rather than assuming it
  needs no attention.
- Exact task/module shape for the SCD Publisher (e.g. whether it lives at
  `packages/dataplat/src/dataplat/load/publish/scd.py` alongside `merge.py`/`merge_orders.py`, or
  elsewhere) — follows the existing `Publisher` registry pattern
  (`packages/dataplat/src/dataplat/load/publish/registry.py`), naming left to planning.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & Roadmap
- `.planning/ROADMAP.md` Phase 10 section — goal, success criteria, plan guidance (Python-owned,
  `Publisher` seam, exclusion constraint, recompute-not-surgery, dbt vocabulary) — this session's own
  edit dropping CDC
- `.planning/REQUIREMENTS.md` SCD-01..12, QUAL-14 — full requirement text; Out of Scope table's CDC
  entry (DoD 44/45/46/87) explaining why CDC was dropped; traceability table

### Prior-phase precedent (do not re-litigate)
- `.planning/phases/08.1-dbt-silver-transformation-layer-dbt-postgres-adapter-owns-br/08.1-CONTEXT.md`
  — domain statement: "Phase 10's SCD2 stays entirely Python-owned; `dbt snapshot` is explicitly
  rejected there for the same structural reason gold stays Python-owned here"
- `.planning/phases/09-etl-correctness-dedup-incremental-backfill-recovery/09-CONTEXT.md` D-02
  (event_ts as the watermark/ordering column, reused here per D-03), D-09/D-27 (2-year corpus /
  "prove at real scale" pattern reused per D-11), D-13 (pg_advisory_xact_lock concurrency proof,
  explicitly NOT relied on alone per D-10)
- `.planning/phases/08-validation-quarantine-metadata-control-plane-completion/08-CONTEXT.md` D-10
  (rejection-rate circuit breaker — direct precedent for D-06's mass-delete circuit breaker)

### Code — Pipeline Mechanics This Phase Extends/Replaces
- `packages/dataplat/src/dataplat/load/publish/merge.py` — `MergePublisher`; the `INSERT ... ON
  CONFLICT (customer_id)` write path this phase's SCD Publisher replaces for `normalized.customers`
- `packages/dataplat/src/dataplat/load/publish/registry.py` — `Publisher` registry the new SCD
  Publisher registers into
- `packages/dataplat/src/dataplat/validate/circuit_breaker.py` — `RejectionRateCircuitBreaker`,
  direct implementation template for D-06's mass-delete circuit breaker
- `packages/dataplat/src/dataplat/validate/referential.py` — `ReferentialIntegrityBarrier`; verified
  unaffected by D-07 (EXISTS-based, cardinality-agnostic)
- `packages/dataplat/src/dataplat/pipeline/run.py` `_compute_silver_gold_reconciliation` (~line
  293) and `packages/dataplat/src/dataplat/metadata/repository.py` `record_reconciliation` (~line
  990) — D-08 item 2, the reconciliation logic that must account for multiple gold rows per
  `customer_id` post-migration
- `migrations/versions/0006_normalized_customers_business_key_unique.py` — the `UNIQUE(customer_id)`
  constraint D-07 replaces; explains why it exists (ON CONFLICT publish target) so its removal is a
  deliberate supersession, not a regression
- `migrations/versions/0012_meta_v_customers_lineage.py`,
  `migrations/versions/0026_v_customers_lineage_dbt_hop.py`,
  `migrations/versions/0030_fix_v_customers_lineage_dedup_audit_model_name.py` — `meta.v_
  customers_lineage`'s `FROM normalized.customers c` clauses, D-08 item 1
- `migrations/versions/0016_normalized_orders.py` — docstring confirming `orders.customer_id` has
  no DB-level FK to `normalized.customers` (the D-07 "verified non-issue")
- `packages/dataplat/src/dataplat/config/model.py` `SourceConfig.change_semantics` (~line 87) —
  the field D-04 is the first phase to actually interpret; docstring already says `"snapshot"` or
  `"cdc"` but the field has zero behavioral consumers today (grep-verified)
- `configs/datasets/customers.yaml` — `source.change_semantics: snapshot` (already declared,
  unexercised until now), `event_ts` column (D-03's effective-date source)
- `tests/e2e/slice/test_backfill_2year_sweep.py` and its underlying fixtures — the 2-year corpus
  D-11/D-12 extend rather than replace
- `helm/values/local/monitoring.yaml` / `helm/values/ci/monitoring.yaml` — Grafana dashboard
  provisioning, verified to query only `meta.ingestion_runs`, confirming Grafana is NOT a D-08
  consumer

### Memory
- `host_hardware_context` (project memory) — WSL2/kind resource constraints relevant to a
  concurrent-SCD-publish live test (D-10) and a second full 2-year backfill re-run (D-12)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `RejectionRateCircuitBreaker` (`validate/circuit_breaker.py`) — direct template for D-06's
  mass-delete circuit breaker; same "count vs. configurable threshold, fail the run" shape.
- `pg_advisory_xact_lock` single-writer publish lock (established in `merge.py`, proven under
  concurrency in Phase 9) — the SCD Publisher's write path reuses this same primitive even though
  D-10 still wants a dedicated live concurrency proof for the new SCD-specific logic layered on top.
- `meta.dedup_audit`'s post-hook/macro pattern (Phase 08.1 D-09) — a plausible template for however
  the SCD Publisher records what changed (which columns, old→new values) per version, though this
  wasn't explicitly discussed.
- Phase 9's 2-year fixture corpus generator and `test_backfill_2year_sweep.py` — direct base for
  D-11/D-12's extended proof.

### Established Patterns
- Layered defense (Phase 8 D-10's rejection-rate breaker, Phase 9's per-hop reconciliation) —
  directly informs D-06's mass-delete circuit breaker.
- "Config-not-code" — DELETE semantics default (D-05), column Type assignments (D-02), and the
  circuit-breaker threshold (D-06) should all surface as `customers.yaml` config, validated by
  Pydantic, not hardcoded — consistent with every prior phase's convention.
- Codebase-convention-over-aspirational-doc (newly reinforced by D-09) — when STACK.md's
  recommendation and the actual established codebase pattern diverge, this project has now twice
  chosen the actual codebase pattern (BigInteger+Identity here; `MERGE`-avoidance via `ON CONFLICT`
  in Phase 4 for the analogous PG-version-specific-feature reason).

### Integration Points
- `normalized.customers` DDL changes (D-07): drop `UNIQUE(customer_id)`, add exclusion constraint,
  add `valid_from`/`valid_to`/`is_current` (or equivalent) columns, backfill existing rows.
- New SCD Publisher registers into the existing `Publisher` registry
  (`load/publish/registry.py`), replacing `MergePublisher` as `customers.yaml`'s `load.strategy`
  target.
- `meta.v_customers_lineage` view redefinition (migration chain continues from 0030).
- `_compute_silver_gold_reconciliation` / `record_reconciliation` logic changes for
  multi-row-per-key gold cardinality.
- Fixture corpus additions: attribute-change events, a late/out-of-order correction, a missing
  (invalidate-triggering) customer, and a deliberately-bad/truncated snapshot (circuit-breaker
  trigger) — layered into or alongside the existing 2-year corpus per D-11.

</code_context>

<specifics>
## Specific Ideas

- The user's own framing for the CDC-exclusion decision (paraphrased): "if we have no other system
  to monitor for change, is CDC redundant?" — the actual answer (recorded in this session before
  this discussion) is more precise: CDC's design never required live monitoring, it's redundant
  because nothing produces the batch feed format it would consume, not because "there's nothing to
  watch."
- On the table-shape question (D-07), the user explicitly chose the riskier, more invasive option
  (in-place migration) after seeing a concrete worked before/after example of both choices and their
  consequences — this was a deliberate, informed choice against the recommendation, not an
  oversight. Treat D-07/D-08 as firmly locked, not something to quietly soften back toward the
  new-table option during planning.
- Similarly, D-10 (dedicated concurrency test) and D-11 (extend the real corpus rather than build a
  smaller dedicated one) are both cases where the user chose the more thorough/rigorous option over
  the lighter-weight recommendation — consistent with the same pattern already noted in Phase 9's
  own CONTEXT.md (D-12/D-27 "try the more ambitious option first").
- A genuine correction happened mid-discussion (D-08): an initial claim that Grafana dashboards
  needed updating for the table-shape migration was wrong and was caught by actually reading the
  dashboard provisioning file rather than assuming. The user then explicitly re-confirmed the
  corrected consumer list. This matters because CONTEXT.md's D-08 list should be trusted as
  code-verified, not just discussion-derived.

</specifics>

<deferred>
## Deferred Ideas

None raised that belong to a different phase. CDC's exclusion was handled as a roadmap/requirements
edit earlier in this session (before this discussion began), not as a deferred idea within Phase 10
— it's already reflected in ROADMAP.md/REQUIREMENTS.md.

### Reviewed Todos (not folded)
The one todo match for Phase 10 (`draft-adr-dbt-silver-layer-boundary.md`, score 0.6) was already
folded into Phase 08.1's scope during that phase's own discussion — nothing left to review here.

</deferred>

---

*Phase: 10-Slowly Changing Dimensions*
*Context gathered: 2026-08-21*
