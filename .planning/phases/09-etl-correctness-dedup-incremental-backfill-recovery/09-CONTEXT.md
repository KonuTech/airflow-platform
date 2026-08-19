# Phase 9: ETL Correctness — Dedup, Incremental, Backfill & Recovery - Context

**Gathered:** 2026-08-19
**Status:** Ready for planning

<domain>
## Phase Boundary

The platform processes only what is new, never loses late data, recovers from partial failure
without reading logs, and can prove target matches source.

**Scope note — significant since this discussion was originally paused (2026-08-18):**
DEDUP-01..04, INCR-03/04, and QUAL-10 have been remapped OUT of Phase 9 and into Phase 08.1
(dbt Silver Transformation Layer), which is now complete (13/13 plans, 15/15 must-haves verified
2026-08-19). Deduplication, late-arrival resolution, and dedup testing are entirely dbt-owned now
(`dbt/models/silver/`) — **not** part of this phase or this discussion.

Phase 9's actual remaining scope, confirmed against `.planning/REQUIREMENTS.md`'s traceability
table:
- **INCR-01, INCR-02** — incremental processing via watermarks (observational, not gating)
- **INCR-05, INCR-06** — backfills as a first-class capability, same pipeline, no bypass
- **LOAD-06** — partial-failure recovery determinable without reading logs
- **VALID-05, VALID-06** — source-to-target reconciliation and control-total validation
- **QUAL-11** — backfills tested for idempotency and historical schema resolution

The pipeline this phase builds on top of is `discover → stage → dbt_build → publish` (Phase 08.1's
3-task split), with `meta.run_stages` already tracking `STAGE_LOAD`/`PUBLISH` claims.

</domain>

<decisions>
## Implementation Decisions

### Watermarks (INCR-01, INCR-02)

- **D-01:** The watermark is **observational only** — `meta.watermarks` records the highest
  committed cursor value per dataset purely as an audit/freshness signal. It never filters which
  files/rows a run picks up; file selection stays owned entirely by the existing Phase 4
  idempotency-key mechanism. This satisfies INCR-01/02's literal wording without duplicating
  file-selection logic that already works, and avoids the AP4 anti-pattern (ARCHITECTURE.md
  line 1333) of a second, parallel "is this new" mechanism that could disagree with the first.
- **D-02:** Strategy is **EVENT_TIMESTAMP** — track `max(event_ts)` / `max(order_date)` ever
  committed, per dataset. Reuses the exact column already driving dedup ordering
  (`customers.yaml`/`orders.yaml` `order_by`) and `MergePublisher`'s late-arrival guard
  (`EXCLUDED.event_ts >= ...`). Naturally exercises INCR-02's "`>=`, never `>`" rule: a late file
  with an older max `event_ts` simply doesn't advance the watermark — correct and expected, not an
  error.
- **D-03:** Grain is a **single `'default'` `target_key`** per dataset — no per-source/country
  watermarks. `target_key` stays a forward-compat column, unexercised this phase (same "built but
  unexercised" pattern as `_BATCH_COMPLETE`, Phase 6 D-10 / Phase 8 D-19).
- **D-04:** `meta.watermark_history` (append-only audit of every watermark change) **is built this
  phase**, not deferred — small table (`dataset_id`, `target_key`, `old_value`, `new_value`,
  `run_id`, `changed_at`), written in the same publish transaction as the watermark advance.
- **Note:** dbt's own internal silver-model incremental cursor (08.1 D-05/D-06) is a completely
  separate mechanism — `_run_id`-based, never `event_ts`. Do not conflate the two watermarks;
  Phase 9's is dataset-level and observational, dbt's is model-level and filtering.

### Backfill Mechanics & "Historical Partition" (INCR-05, INCR-06, QUAL-11)

- **D-05:** ROADMAP success criterion #4's "correct historical partition" means **logical
  correctness, not physical partitioning**. Phase 08.1 (D-13) already rejected physical
  partitioning of gold (a `UNIQUE`-constraint conflict with a business-date partition key), going
  index-only instead. A late record "landing in its correct historical partition" means it's
  correctly attributed by business timestamp and correctly ordered relative to other events for
  its business key — which dbt silver (08.1 D-06) and `MergePublisher`'s guard (08.1 D-10) already
  deliver structurally. Phase 9's job is to **prove this end-to-end live** (a 3-month-late-arrival
  test), not build new partitioning.
- **D-06:** A missing file in a backfill window gets an **explicit gap record, and the run
  continues** — the backfill DagRun for that logical date completes with an explicit "no file
  found" outcome recorded in `meta` (distinct from a failure); other dates in the window keep
  processing. Matches the Core Value: nothing silently dropped, everything explainable via SQL.
- **D-07:** Historical schema-version resolution is **proof/test only**, not new capability. Phase
  6 already resolves a file's historical schema version at parse time via `config_versions`.
  QUAL-11's job is to prove it live: backfill an old file whose schema differs from current and
  confirm it parses under ITS historical version.
- **D-08:** Phase 9 **closes the `silver.orders` 0-rows gap** flagged in
  `08.1-VERIFICATION.md` Operational Observations #1 (08.1's live backfill proof only covered
  `csv_ingest_customers`) — running the orders backfill to bring `silver.orders`/
  `normalized.orders` to parity is part of Phase 9's own live backfill proof (ROADMAP success
  criterion #3), using `orders` (which already has the orphan-order/referential-integrity story
  from Phase 8).
- **D-09 (LOCKED — user decision, chosen against the recommended shorter-window option):** The
  2-year backfill window (ROADMAP success criterion #3) uses **genuinely 2 years of synthetic
  fixture files**, not a shorter representative window.
- **D-10:** The 2-year fixture span deliberately contains, combined in the same window (not
  tested as isolated smaller fixtures): a regular file-drop cadence, **at least one deliberate
  schema-version change** partway through, **at least one deliberate missing file** (gap,
  exercises D-06), and **at least one file with an out-of-order/late event** relative to its
  neighbors.
- **D-11 (HARD REQUIREMENT — user-stated, confirmed twice, do not water down):** No new
  operator-facing backfill-trigger tooling is built — **the native `airflow backfill create`
  command is the only trigger mechanism** (matching Phase 8's D-06 "no new tooling" precedent).
  **Critical correction:** `airflow dags backfill` does **not exist** in this cluster's installed
  Airflow 3.3.0 — the real, live-confirmed command is:
  ```
  airflow backfill create --dag-id <dag> --from-date <date> --to-date <date> --reprocess-behavior completed
  ```
  (source: `08.1-13-SUMMARY.md`, already used successfully for 08.1's own D-16/D-18 live proof).
  But: `csv_ingest_customers` and `csv_ingest_orders` **must genuinely, provably support backfill
  end-to-end for the full 2-year window** — covering schema evolution, late/out-of-order events,
  incremental watermarks, and dedup correctness **together, simultaneously**, not as isolated
  unit-tested mechanics. **If the current DAG structure doesn't already do this correctly, Phase 9
  refactors the DAGs — not just adds tests around the existing structure — until it does.** The
  DAGs must process **past batches (backfill), the current/triggering batch, and future scheduled
  batches all through the same structure**, with no special-casing by temporal mode.
  **Research-flagged, not yet verified:** `08.1-13-SUMMARY.md`'s own tech-pattern notes record
  that `discover_files` already re-scans the whole bucket regardless of the triggering DagRun's
  window/`logical_date` — i.e. discovery is already structurally backfill-agnostic (idempotency
  key/content-hash decide eligibility, not a date-scoped listing). **Research must verify the
  actual current DAG code against this requirement before assuming a full rewrite is needed** —
  the real gap, if any, may be narrower (e.g. confined to how watermarks/reconciliation interact
  across a genuine multi-year run) rather than a ground-up restructure.

### Concurrency (backfill parallelism, live+backfill overlap)

- **D-12 (LOCKED — resource-dependent, degrade gracefully):** Target **bounded/limited
  parallelism** (e.g. Airflow pools capping concurrent logical-date runs) as the **default** for
  the 2-year backfill's per-logical-date DagRuns. **Fall back to `max_active_runs=1` (sequential)
  if the local kind cluster's real resource limits make parallel infeasible** — this project's own
  memory notes flag laptop/WSL2 constraints as a real, recurring concern. Research/planning
  determines the actual concurrency bound **empirically** against real cluster capacity, not by
  assumption.
- **D-13 (LOCKED — in scope, not deferred to an operational practice):** Live (current-date)
  ingestion running **concurrently** with a historical backfill for the **same dataset** is **in
  scope** for Phase 9, not treated as an out-of-band operational combination (e.g. "run backfills
  in a maintenance window"). Rely on Airflow's own concurrency controls (pools/`max_active_runs`)
  plus the existing single-writer publish advisory lock (`pg_advisory_xact_lock`) to make this
  safe, and **prove it live** — not just assert it by architecture.

### Recovery & Checkpoint Scope (LOAD-06)

- **D-14:** `dbt_build` gets its **own `meta.run_stages` entry** (a 3rd `stage_name` value,
  alongside the existing `STAGE_LOAD`/`PUBLISH`), recorded by the DAG task itself — not a
  claim/lease (dbt's own idempotency stays fully decoupled from the Python claim mechanism, per
  08.1 D-02). This closes the one blind spot in the current recovery-visibility design: a single
  SQL query should answer "what succeeded, what remains" for the **whole** pipeline, not just its
  two Python-claimed ends.
- **D-15:** Recovery is **retry-only — rollback never applies**. Because every stage
  (stage/dbt_build/publish) commits atomically or not at all, a `FAILED` or lease-expired stage
  has never partially committed anything. The correct recovery action is always "retry that
  stage," safe by construction (idempotency keys, dbt's own idempotent re-run, `ON CONFLICT`
  publish). Recovery reporting should **explicitly state** "retry stage X" — proving rollback's
  structural absence, rather than building unused rollback machinery for a state that cannot
  occur.
- **D-16:** "What succeeded, what remains" is surfaced via **a SQL view**
  (e.g. `meta.v_run_recovery`) joining `meta.ingestion_runs` + `meta.run_stages`, giving one query
  with an unambiguous "next action: retry stage X" / "complete" verdict — same "SQL-queryable
  lineage" philosophy as `meta.v_customers_lineage`. No dashboard, no CLI, matching Phase 8's D-06
  precedent.
- **D-17:** Stuck-lease reclaim stays **fully automatic for all 3 stages** — same self-healing
  `ON CONFLICT DO UPDATE` reclaim pattern Phase 4 established for the single `ingestion_runs`
  lease, now applied independently per-stage. No operator confirmation gate for any stage,
  including `publish`.
- **D-18:** Phase 9's live-cluster proof **extends pod-kill testing to `dbt_build`** — Phase 4/8
  already proved live pod-kill recovery for stage and publish; `dbt_build` (new in 08.1) has never
  been proven against a mid-run kill. Real pod-kill, not simulated, matching the existing
  precedent.
- **D-19:** A run/stage that exhausts its retries (Airflow's own `max_retries`) fires through
  **Phase 7's existing Grafana Alerting path** — add a rule querying the new `meta.v_run_recovery`
  view. No new/second alerting mechanism; "one alerting engine" stays true (Phase 7 D-07).

### Reconciliation (VALID-05, VALID-06)

- **D-20 (LOCKED — user chose the more granular option, against the recommendation):**
  Reconciliation is checked **at each hop separately** — raw→bronze, bronze→silver, silver→gold —
  not just raw-vs-gold. Each hop proves its own fidelity.
- **D-21:** Each hop's check runs **inline with the owning stage**: raw→bronze reconciles
  inside/right after `stage` (Python, same transaction as the COPY); bronze→silver reconciles
  inside `dbt_build`; silver→gold reconciles inside `publish` (Python, same META-03 transaction).
  Matches this project's recurring layered-defense philosophy — each stage proves its own fidelity
  before handing off.
- **D-22 (LOCKED — user-added critical accounting rule, do not implement as a naive count
  comparison):** A discrepancy is **recorded and the run continues — never blocks** (matches
  VALID-09's anomaly-detection precedent: flag, don't block). But the comparison itself must be
  **quarantine-aware**: at each hop, reconciliation compares **input count against (output count +
  rows routed to `meta.rejected_records` for that hop)** — never a naive raw input-vs-output
  comparison. `meta.reconciliation_results` should join/reference `meta.rejected_records` and
  `meta.dedup_audit` so the expected-vs-actual comparison already nets out every known, legitimate
  reduction (quarantine, dedup) before flagging a genuine, unexplained discrepancy.
- **D-23:** The source-provided control total (VALID-06) is carried by **extending the
  `_BATCH_COMPLETE` manifest** (LOAD-11, built-but-unexercised per Phase 8 D-19) with
  `expected_row_count`/`expected_checksum` fields. Phase 9 is the first thing to actually exercise
  this manifest — giving it real purpose instead of staying dormant, reusing infrastructure rather
  than inventing a second sidecar-file convention.
- **D-24:** `meta.reconciliation_results` grain is **per file, per hop** — one row per
  `(file_id, hop)`. Matches the manifest's own per-file control total and the platform's existing
  file/batch/record identity model (LOAD-04/LOAD-08).
- **D-25:** The sum check (one of VALID-05's five checks: counts, sums, checksums, min/max, key
  counts) is **dataset-conditional, config-declared** — a new optional `reconciliation:` block in
  dataset YAML declares which column(s) get summed (e.g. `orders.yaml: sum_columns: [amount]`);
  `customers.yaml` simply omits it (no natural numeric column to sum — `event_ts`/`birth_date` are
  dates). Matches the project's "config-not-code, opt-in unexercised where it doesn't apply"
  pattern (Phase 6 D-10, Phase 8 D-19).
- **D-26 (LOCKED — user explicitly requested "both," confirmed):** The bronze→silver
  reconciliation check inside `dbt_build` uses **both** mechanisms, not either/or: a **custom
  macro writes the durable `meta.reconciliation_results` row** in the same transaction as the
  model write (matching 08.1 D-09's `meta.dedup_audit` post-hook pattern exactly), **AND** a
  **native dbt test with `severity: warn`** provides visible, non-blocking signal in dbt's own run
  output/CI. Low marginal cost — the macro is needed regardless for durability; the dbt test is a
  cheap idiomatic add-on. Consistent with this project's recurring layered-defense pattern
  (business-key + hash dedup, gold guard kept even after silver is correct).

### Test-Tier Placement

- **D-27 (LOCKED — live-first, testcontainers-fallback, same pattern as D-12):** Target running
  the **FULL 2-year sweep on the real live kind cluster** as the primary goal, including the
  QUAL-11 idempotency re-run (re-running the full backfill must produce zero additional rows). If
  that proves impractical on local hardware/time budget, **fall back to full-scale in
  testcontainers with only a representative subset proven live** — the same split 08.1 already
  used successfully (655-test testcontainers suite for full mechanics, a handful of files for the
  live proof).

### Cleanup

- **D-28:** Remove the **vestigial `deduplication:` block** in `customers.yaml`/`orders.yaml`
  (references a `DEDUP_REGISTRY` that was never built — dedup is entirely dbt-owned now, per 08.1
  D-07). Phase 9 touches these YAML files anyway (adding the new `reconciliation:`/`sum_columns`
  block) — a natural place to also delete the dead config, avoiding confusion for a future reader.

### Claude's Discretion

None — every question in this discussion was answered with a specific choice; no "you decide"
deferrals remain open for research/planning to resolve independently. (Contrast with the
pre-Phase-08.1 checkpoint, which had left both watermark questions to Claude's discretion — those
are now superseded by D-01/D-02 above.)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & Roadmap
- `.planning/ROADMAP.md` Phase 9 section (lines 555-582) — goal, success criteria, Wave F plan
  guidance (10a ‖ 10d → 10b → 10c ordering), PITFALLS #7/#14, checkpoint mechanics guidance
- `.planning/REQUIREMENTS.md` INCR-01/02/05/06, LOAD-06, VALID-05/06, QUAL-11 — full requirement
  text; traceability table confirms DEDUP-01..04/INCR-03/04/QUAL-10 are Phase 08.1/Complete, not
  Phase 9

### Architecture Design (existing, not yet migrated except run_stages)
- `.planning/research/ARCHITECTURE.md` lines 233-236 — `meta.run_stages`, `meta.watermarks`,
  `meta.watermark_history` column shapes (this phase's D-01..D-04, D-14 target)
- `.planning/research/ARCHITECTURE.md` line 273 — "Correctness phase" table:
  `run_stages`/`watermarks`/`watermark_history`/`reconciliation_results` (run_stages already
  migrated in 08.1; the rest are this phase's job)
- `.planning/research/ARCHITECTURE.md` line 578 (ROADMAP Phase 9 plan guidance, PITFALLS #7) —
  watermark advances only from observed committed cursor values, lagged, inside publish
  transaction, using `>=` never `>`
- `.planning/research/ARCHITECTURE.md` lines 1333-1335 (AP4) — advancing the watermark outside
  the publication transaction is the anti-pattern D-01 avoids by design
- `.planning/research/ARCHITECTURE.md` line 817 — lease-reclaim pattern ("row returned → this pod
  owns the run") that D-17 extends to all 3 stages
- `.planning/research/ARCHITECTURE.md` line 854 — intra-file byte-offset checkpointing is v2,
  "build it only if a fixture demands it" — carried forward unchanged from ROADMAP's own guidance,
  not reopened by this discussion

### Phase 08.1 — dbt Silver Layer (the architecture Phase 9 builds on top of)
- `.planning/phases/08.1-dbt-silver-transformation-layer-dbt-postgres-adapter-owns-br/08.1-CONTEXT.md`
  D-02 (dbt_build decoupled from Python claim), D-05/D-06 (dbt's own `_run_id`-based internal
  incremental cursor, distinct from Phase 9's watermark), D-07 (dbt owns dedup entirely), D-09
  (`meta.dedup_audit` macro/post-hook pattern D-26 reuses), D-13 (physical partitioning rejected —
  the basis for D-05), D-17 (two-phase claim precedent D-14 extends)
- `.planning/phases/08.1-dbt-silver-transformation-layer-dbt-postgres-adapter-owns-br/08.1-VERIFICATION.md`
  Operational Observations #1 — `silver.orders` 0-rows gap D-08 closes
- `.planning/phases/08.1-dbt-silver-transformation-layer-dbt-postgres-adapter-owns-br/08.1-13-SUMMARY.md`
  — **CRITICAL**: `airflow dags backfill` does NOT exist in this cluster's installed Airflow
  3.3.0. Real command: `airflow backfill create --dag-id ... --from-date ... --to-date ...
  --reprocess-behavior completed`. Also documents `discover_files`'s whole-bucket-rescan behavior
  (research flag for D-11) and the live-proof/testcontainers test-tier split D-27 explicitly
  follows.
- `migrations/versions/0025_meta_run_stages.py` — existing `run_stages` table
  (`STAGE_LOAD`/`PUBLISH` only, `etl_app`-only grants); this phase adds a `DBT_BUILD` `stage_name`
  value (D-14) and the recovery view (D-16)

### Phase 8 — Validation, Quarantine (precedents this phase reuses)
- `.planning/phases/08-validation-quarantine-metadata-control-plane-completion/08-CONTEXT.md` D-06
  ("no new tooling, query meta.* directly via SQL" — precedent for D-11/D-16), D-19
  (`_BATCH_COMPLETE` "opt-in, unexercised" precedent D-23 reuses), D-11 ("FAIL means nothing
  publishes" — the precedent D-22 deliberately does NOT follow for reconciliation, which is
  record-and-continue instead)
- `migrations/versions/0015_meta_rejected_records.py`,
  `migrations/versions/0024_meta_dedup_audit_decisions.py` — tables D-22's quarantine-aware
  reconciliation accounting rule must join against

### Phase 7 — Observability (alerting reuse)
- `.planning/phases/07-observability-metrics-tracing-lineage/07-CONTEXT.md` D-05/D-07 (one
  alerting engine — Grafana Alerting, Postgres-datasource rules) — the path D-19 reuses for
  exhausted-retry alerts

### Code — Pipeline Mechanics This Phase Extends
- `packages/dataplat/src/dataplat/load/publish/merge.py` — `MergePublisher`; `EXCLUDED.event_ts
  >= normalized.customers.event_ts` guard (precedent for D-02's watermark strategy);
  `pg_advisory_xact_lock` single-writer lock (precedent for D-13's concurrency safety claim)
- `packages/dataplat/src/dataplat/load/publish/merge_orders.py` — `OrdersMergePublisher`, same
  treatment
- `packages/dataplat/src/dataplat/metadata/repository.py` —
  `claim_run_stage`/`heartbeat_run_stage`/`complete_run_stage`, the pattern D-14's `dbt_build`
  tracking and D-17's per-stage lease reclaim extend
- `airflow/dags/csv_ingest_customers.py` — discover→stage→dbt_build→publish task graph; the
  `resolve_window`/`build_stage_args` tasks and `S3KeySensor(bucket_key="customers/*.csv")`
  trigger this phase's D-11 requirement applies to; must be verified (not assumed) against the
  past/current/future uniformity requirement
- `airflow/dags/csv_ingest_orders.py` — same DAG-shape requirement applies; also the target of
  D-08's silver-parity backfill
- `configs/datasets/customers.yaml` (event_ts, `order_by`, vestigial `deduplication:` block) /
  `configs/datasets/orders.yaml` (order_date, amount, vestigial `deduplication:` block) — targets
  of D-25 (new `reconciliation:` block) and D-28 (cleanup)

### Memory
- `host_hardware_context` (project memory) — WSL2/kind resource constraints; informs both D-12's
  backfill-parallelism bound and D-27's live-vs-testcontainers fallback decision. A host restart
  can silently break kind's DAGs mount and freeze Airflow scheduling cluster-wide — relevant if a
  long-running 2-year live backfill spans a host restart.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `meta.run_stages` (migration 0025) — two-phase claim state machine already exists for
  `STAGE_LOAD`/`PUBLISH`; D-14 extends it with a `DBT_BUILD` value rather than building a new
  mechanism.
- `packages/dataplat/src/dataplat/metadata/repository.py`'s claim/heartbeat/complete functions —
  direct template for whatever `dbt_build` status-recording function D-14 needs (though dbt_build
  is explicitly NOT claimed/leased the same way — it just records status).
- `meta.v_customers_lineage` (migration 0012, extended 0026/0030) — direct precedent/template for
  D-16's `meta.v_run_recovery` view.
- `dbt/macros/dedup_audit_post_hook.sql` (08.1) — direct template for D-26's reconciliation macro
  (same "post-hook writes an audit table in the model's own transaction" pattern).
- Grafana Alerting infrastructure (Phase 7) — D-19 adds a rule, doesn't build new alerting
  plumbing.
- `_BATCH_COMPLETE` manifest schema (Phase 8, unexercised) — D-23 extends it rather than inventing
  a new sidecar convention.

### Established Patterns
- **Layered defense, not redundancy** — a recurring project theme (dedup: business-key primary +
  hash secondary; gold's guard kept even once silver is correct; DB constraints backing
  app-level logic) — directly informs D-26's "both macro and dbt test" decision.
- **"Opt-in, unexercised until a dataset needs it"** (Phase 6 D-10, Phase 8 D-19) — the pattern
  D-03 (target_key), D-25 (reconciliation:/sum_columns), and D-23 (_BATCH_COMPLETE) all follow.
- **"No new tooling, query meta.* via SQL directly"** (Phase 8 D-06) — the pattern D-11 (backfill
  trigger) and D-16 (recovery surface) both follow.
- **Config-not-code** — every new behavior (watermark strategy per dataset, reconciliation sum
  columns) surfaces as dataset YAML, validated by Pydantic, synced via `ConfigRegistry`.
- **Live-first, testcontainers-fallback under real resource constraints** — a pattern established
  fresh in THIS discussion (D-12, D-27) — try the more thorough/parallel/live approach first,
  degrade gracefully to the safer/faster alternative if the local kind cluster can't sustain it.
  Not yet an established codebase pattern, but now a locked discussion decision for planning to
  follow.

### Integration Points
- New `meta.watermarks`/`meta.watermark_history`/`meta.reconciliation_results` tables — new
  migrations, following the existing `meta.*` grant pattern (etl_app/dbt_app least-privilege,
  migrations 0008/0019/0021).
- `meta.run_stages` gets a new `DBT_BUILD` `stage_name` value written by the `dbt_build` DAG task
  (no migration needed — `stage_name` is app-validated `Text`, not an ENUM, per migration 0025's
  own design note).
- New `meta.v_run_recovery` SQL view joining `ingestion_runs` + `run_stages`.
- New Grafana Alert rule querying `meta.v_run_recovery` (Phase 7's existing alerting engine).
- `dbt/macros/` gets a new reconciliation macro (bronze→silver hop); `dbt/models/silver/schema.yml`
  gets new `severity: warn` tests.
- `packages/dataplat/src/dataplat/load/staging.py`/`publish/merge.py` get new inline
  reconciliation checks (raw→bronze, silver→gold hops).
- `configs/datasets/customers.yaml`/`orders.yaml` get a new `reconciliation:` block and lose the
  vestigial `deduplication:` block.
- Fixture generator (wherever Phase 1's seed-generated CSV corpus lives) needs a genuine 2-year,
  cadence + schema-change + gap + late-event fixture set (D-09/D-10).

</code_context>

<specifics>
## Specific Ideas

- The user was explicit and firm (confirmed twice, verbatim: *"the current dags need to process
  past batches, current batch, and future batches once scheduled"*) that the DAGs must handle all
  three temporal modes through one uniform structure — this is D-11's hard requirement, not a
  preference to be traded off against effort.
- The user pushed back on the recommended shorter-window fixture approach and explicitly chose a
  genuine 2-year fixture span (D-09) — this reflects a preference for proving correctness against
  realistic scale over minimizing fixture-generation/CI cost, echoed again in D-27's live-first
  test-tier choice.
- The user's own framing for the reconciliation accounting rule (paraphrased): *"remember that we
  are putting invalid data rows into invalid postgresql dedicated table"* — a reminder that
  `meta.rejected_records` already exists and reconciliation must account for it, not treat every
  quarantined row as an unexplained loss (D-22).
- The user's own question — *"Would it make sense to have both?"* — for the dbt-test-vs-macro
  choice led directly to D-26; when in doubt about whether two mechanisms are redundant or
  complementary, the user leans toward asking rather than assuming, and toward layered
  belt-and-suspenders designs when the marginal cost is low.
- Twice in this discussion (D-12 backfill parallelism, D-27 test-tier placement) the user gave a
  **preference ordering** rather than a single fixed choice: try the more ambitious/thorough
  option first, degrade to the safer option only if local hardware can't sustain it. Apply this
  same "try, then degrade" framing if research/planning surfaces similar resource-bound choices
  not explicitly covered here.

</specifics>

<deferred>
## Deferred Ideas

None raised that belong to a different phase — every topic that came up during this discussion
(including the concurrency and cleanup follow-ups) was within Phase 9's actual scope. The
DEDUP-related work that WAS deferred out of this phase already happened in Phase 08.1 before this
discussion began (see `<domain>` above) — that's a completed phase, not a deferred idea.

### Reviewed Todos (not folded)
The one todo match for Phase 9 (`draft-adr-dbt-silver-layer-boundary.md`, score 0.2) was already
folded into Phase 08.1's scope during that phase's own discussion — nothing left to review here.

</deferred>

---

*Phase: 9-ETL Correctness — Dedup, Incremental, Backfill & Recovery*
*Context gathered: 2026-08-19*
