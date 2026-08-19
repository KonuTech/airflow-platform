# Phase 9: ETL Correctness — Dedup, Incremental, Backfill & Recovery - Research

**Researched:** 2026-08-19
**Domain:** Airflow 3.3.0 backfill/concurrency mechanics, PostgreSQL watermark/reconciliation design, dbt post-hook macros, Airflow metadata-recovery views — all on top of an already-built `discover → stage → dbt_build → publish` pipeline.
**Confidence:** HIGH — every claim below was verified directly against this repository's own installed Airflow 3.3.0 source, this repository's own code, or this repository's own migrations. No web search was needed; every open question CONTEXT.md flagged was resolvable by reading code that already exists in this tree.

## Summary

Phase 9 is not a build-from-scratch phase. Every mechanism it needs a foundation for already exists: `meta.run_stages` (two-phase claim/lease), `MergePublisher`'s advisory-lock + late-arrival guard, `meta.dedup_audit`'s post-hook pattern, `meta.v_customers_lineage`'s multi-hop SQL view, and a genuinely content-hash-driven, date-agnostic `discover_files`. The phase's real work is: (1) add four new `meta.*` tables/columns (`watermarks`, `watermark_history`, `reconciliation_results`, a `DBT_BUILD` `run_stages` value) via Alembic, each following an existing grant pattern; (2) add one SQL view (`meta.v_run_recovery`) and one Grafana alert rule, both templated directly on Phase 7/8 precedent; (3) add one dbt post-hook macro templated directly on `dedup_audit_post_hook.sql`; (4) prove live, on the real cluster, that backfill/recovery/reconciliation genuinely work — which surfaces one load-bearing, previously-unverified fact this research resolves below.

**The single most important finding:** `discover_files` (verified by direct read, `packages/dataplat/src/dataplat/discovery.py`) lists the dataset's *entire* bucket/prefix on every call and selects work purely by content-hash idempotency — it reads no `logical_date`, no `data_interval`, no wall-clock time, and `meta.files.business_date` is **never populated anywhere in this codebase**. This confirms CONTEXT.md's D-11 hypothesis: discovery is already structurally backfill-agnostic, and **no DAG task-graph rewrite is needed**. But it also surfaces the *real* gap D-11 didn't anticipate: Airflow's `backfill create --from-date/--to-date` enumerates one DagRun **per tick of the DAG's own schedule** within that window, and both `csv_ingest_customers`/`csv_ingest_orders` are scheduled `*/1 * * * *` (every minute). A literal 2-year `--from-date`/`--to-date` span at that cadence would attempt to materialize **over one million DagRuns** — computationally absurd on any hardware, let alone this project's own documented WSL2/kind CPU constraints. Since file selection is content-hash-driven, not logical-date-driven, the fix is not to change the pipeline's logic but to choose a **small, schedule-appropriate `--from-date`/`--to-date` window** (enough ticks to drain the discovery batching cap across the whole 2-year *fixture corpus*, which can be uploaded to MinIO ahead of time in full) — see Common Pitfall 1 and Open Question 1 below.

**Primary recommendation:** Build the four new tables/view/macro exactly on the cited precedents (they are proven-safe, live-tested patterns in this exact codebase), keep `csv_ingest_customers`/`csv_ingest_orders` structurally unchanged (no rewrite), and scope the live 2-year backfill proof around a short `--from-date`/`--to-date` window against a fully-pre-uploaded 2-year fixture corpus, using `airflow backfill create`'s own `--max-active-runs` flag (not a new Airflow Pool) for D-12's bounded parallelism.

## User Constraints (from CONTEXT.md)

<user_constraints>

### Locked Decisions

**Watermarks (INCR-01, INCR-02)**
- **D-01:** The watermark is **observational only** — `meta.watermarks` records the highest committed cursor value per dataset purely as an audit/freshness signal. It never filters which files/rows a run picks up; file selection stays owned entirely by the existing Phase 4 idempotency-key mechanism. This satisfies INCR-01/02's literal wording without duplicating file-selection logic that already works, and avoids the AP4 anti-pattern (ARCHITECTURE.md line 1333) of a second, parallel "is this new" mechanism that could disagree with the first.
- **D-02:** Strategy is **EVENT_TIMESTAMP** — track `max(event_ts)` / `max(order_date)` ever committed, per dataset. Reuses the exact column already driving dedup ordering (`customers.yaml`/`orders.yaml` `order_by`) and `MergePublisher`'s late-arrival guard (`EXCLUDED.event_ts >= ...`). Naturally exercises INCR-02's "`>=`, never `>`" rule: a late file with an older max `event_ts` simply doesn't advance the watermark — correct and expected, not an error.
- **D-03:** Grain is a **single `'default'` `target_key`** per dataset — no per-source/country watermarks. `target_key` stays a forward-compat column, unexercised this phase (same "built but unexercised" pattern as `_BATCH_COMPLETE`, Phase 6 D-10 / Phase 8 D-19).
- **D-04:** `meta.watermark_history` (append-only audit of every watermark change) **is built this phase**, not deferred — small table (`dataset_id`, `target_key`, `old_value`, `new_value`, `run_id`, `changed_at`), written in the same publish transaction as the watermark advance.
- **Note:** dbt's own internal silver-model incremental cursor (08.1 D-05/D-06) is a completely separate mechanism — `_run_id`-based, never `event_ts`. Do not conflate the two watermarks; Phase 9's is dataset-level and observational, dbt's is model-level and filtering.

**Backfill Mechanics & "Historical Partition" (INCR-05, INCR-06, QUAL-11)**
- **D-05:** ROADMAP success criterion #4's "correct historical partition" means **logical correctness, not physical partitioning**. Phase 08.1 (D-13) already rejected physical partitioning of gold (a `UNIQUE`-constraint conflict with a business-date partition key), going index-only instead. A late record "landing in its correct historical partition" means it's correctly attributed by business timestamp and correctly ordered relative to other events for its business key — which dbt silver (08.1 D-06) and `MergePublisher`'s guard (08.1 D-10) already deliver structurally. Phase 9's job is to **prove this end-to-end live** (a 3-month-late-arrival test), not build new partitioning.
- **D-06:** A missing file in a backfill window gets an **explicit gap record, and the run continues** — the backfill DagRun for that logical date completes with an explicit "no file found" outcome recorded in `meta` (distinct from a failure); other dates in the window keep processing. Matches the Core Value: nothing silently dropped, everything explainable via SQL.
- **D-07:** Historical schema-version resolution is **proof/test only**, not new capability. Phase 6 already resolves a file's historical schema version at parse time via `config_versions`. QUAL-11's job is to prove it live: backfill an old file whose schema differs from current and confirm it parses under ITS historical version.
- **D-08:** Phase 9 **closes the `silver.orders` 0-rows gap** flagged in `08.1-VERIFICATION.md` Operational Observations #1 (08.1's live backfill proof only covered `csv_ingest_customers`) — running the orders backfill to bring `silver.orders`/`normalized.orders` to parity is part of Phase 9's own live backfill proof (ROADMAP success criterion #3), using `orders` (which already has the orphan-order/referential-integrity story from Phase 8).
- **D-09 (LOCKED — user decision, chosen against the recommended shorter-window option):** The 2-year backfill window (ROADMAP success criterion #3) uses **genuinely 2 years of synthetic fixture files**, not a shorter representative window.
- **D-10:** The 2-year fixture span deliberately contains, combined in the same window (not tested as isolated smaller fixtures): a regular file-drop cadence, **at least one deliberate schema-version change** partway through, **at least one deliberate missing file** (gap, exercises D-06), and **at least one file with an out-of-order/late event** relative to its neighbors.
- **D-11 (HARD REQUIREMENT — user-stated, confirmed twice, do not water down):** No new operator-facing backfill-trigger tooling is built — **the native `airflow backfill create` command is the only trigger mechanism** (matching Phase 8's D-06 "no new tooling" precedent). **Critical correction:** `airflow dags backfill` does **not exist** in this cluster's installed Airflow 3.3.0 — the real, live-confirmed command is: `airflow backfill create --dag-id <dag> --from-date <date> --to-date <date> --reprocess-behavior completed` (source: `08.1-13-SUMMARY.md`, already used successfully for 08.1's own D-16/D-18 live proof). But: `csv_ingest_customers` and `csv_ingest_orders` **must genuinely, provably support backfill end-to-end for the full 2-year window** — covering schema evolution, late/out-of-order events, incremental watermarks, and dedup correctness **together, simultaneously**, not as isolated unit-tested mechanics. **If the current DAG structure doesn't already do this correctly, Phase 9 refactors the DAGs — not just adds tests around the existing structure — until it does.** The DAGs must process **past batches (backfill), the current/triggering batch, and future scheduled batches all through the same structure**, with no special-casing by temporal mode. **Research-flagged, not yet verified:** `08.1-13-SUMMARY.md`'s own tech-pattern notes record that `discover_files` already re-scans the whole bucket regardless of the triggering DagRun's window/`logical_date` — i.e. discovery is already structurally backfill-agnostic (idempotency key/content-hash decide eligibility, not a date-scoped listing). **Research must verify the actual current DAG code against this requirement before assuming a full rewrite is needed** — the real gap, if any, may be narrower (e.g. confined to how watermarks/reconciliation interact across a genuine multi-year run) rather than a ground-up restructure.
  - **RESOLVED BY THIS RESEARCH:** confirmed true (see Summary and Common Pitfall 1) — no DAG rewrite is needed for discovery logic itself. The real, previously-unstated gap is the schedule-interval-vs-backfill-window mismatch, not the task graph.

**Concurrency (backfill parallelism, live+backfill overlap)**
- **D-12 (LOCKED — resource-dependent, degrade gracefully):** Target **bounded/limited parallelism** (e.g. Airflow pools capping concurrent logical-date runs) as the **default** for the 2-year backfill's per-logical-date DagRuns. **Fall back to `max_active_runs=1` (sequential) if the local kind cluster's real resource limits make parallel infeasible** — this project's own memory notes flag laptop/WSL2 constraints as a real, recurring concern. Research/planning determines the actual concurrency bound **empirically** against real cluster capacity, not by assumption.
  - **RESOLVED BY THIS RESEARCH:** the native lever is `airflow backfill create --max-active-runs N` (a real, documented CLI flag, default 10) — not a new Airflow Pool. See Architecture Pattern 2.
- **D-13 (LOCKED — in scope, not deferred to an operational practice):** Live (current-date) ingestion running **concurrently** with a historical backfill for the **same dataset** is **in scope** for Phase 9, not treated as an out-of-band operational combination (e.g. "run backfills in a maintenance window"). Rely on Airflow's own concurrency controls (pools/`max_active_runs`) plus the existing single-writer publish advisory lock (`pg_advisory_xact_lock`) to make this safe, and **prove it live** — not just assert it by architecture.
  - **RESOLVED BY THIS RESEARCH:** Airflow 3.3.0's scheduler already tracks active-run concurrency **separately per `(dag_id, backfill_id)`** — a live scheduled DagRun (`backfill_id IS NULL`, bound by the DAG's own `max_active_runs=1`) and a backfill's DagRuns (`backfill_id = X`, bound by `Backfill.max_active_runs`) are counted independently. This is native, out-of-the-box support for D-13 with zero DAG code changes. See Architecture Pattern 2.

**Recovery & Checkpoint Scope (LOAD-06)**
- **D-14:** `dbt_build` gets its **own `meta.run_stages` entry** (a 3rd `stage_name` value, alongside the existing `STAGE_LOAD`/`PUBLISH`), recorded by the DAG task itself — not a claim/lease (dbt's own idempotency stays fully decoupled from the Python claim/lease/heartbeat mechanism, per 08.1 D-02). This closes the one blind spot in the current recovery-visibility design: a single SQL query should answer "what succeeded, what remains" for the **whole** pipeline, not just its two Python-claimed ends.
- **D-15:** Recovery is **retry-only — rollback never applies**. Because every stage (stage/dbt_build/publish) commits atomically or not at all, a `FAILED` or lease-expired stage has never partially committed anything. The correct recovery action is always "retry that stage," safe by construction (idempotency keys, dbt's own idempotent re-run, `ON CONFLICT` publish). Recovery reporting should **explicitly state** "retry stage X" — proving rollback's structural absence, rather than building unused rollback machinery for a state that cannot occur.
- **D-16:** "What succeeded, what remains" is surfaced via **a SQL view** (e.g. `meta.v_run_recovery`) joining `meta.ingestion_runs` + `meta.run_stages`, giving one query with an unambiguous "next action: retry stage X" / "complete" verdict — same "SQL-queryable lineage" philosophy as `meta.v_customers_lineage`. No dashboard, no CLI, matching Phase 8's D-06 precedent.
- **D-17:** Stuck-lease reclaim stays **fully automatic for all 3 stages** — same self-healing `ON CONFLICT DO UPDATE` reclaim pattern Phase 4 established for the single `ingestion_runs` lease, now applied independently per-stage. No operator confirmation gate for any stage, including `publish`.
- **D-18:** Phase 9's live-cluster proof **extends pod-kill testing to `dbt_build`** — Phase 4/8 already proved live pod-kill recovery for stage and publish; `dbt_build` (new in 08.1) has never been proven against a mid-run kill. Real pod-kill, not simulated, matching the existing precedent.
- **D-19:** A run/stage that exhausts its retries (Airflow's own `max_retries`) fires through **Phase 7's existing Grafana Alerting path** — add a rule querying the new `meta.v_run_recovery` view. No new/second alerting mechanism; "one alerting engine" stays true (Phase 7 D-07).

**Reconciliation (VALID-05, VALID-06)**
- **D-20 (LOCKED — user chose the more granular option, against the recommendation):** Reconciliation is checked **at each hop separately** — raw→bronze, bronze→silver, silver→gold — not just raw-vs-gold. Each hop proves its own fidelity.
- **D-21:** Each hop's check runs **inline with the owning stage**: raw→bronze reconciles inside/right after `stage` (Python, same transaction as the COPY); bronze→silver reconciles inside `dbt_build`; silver→gold reconciles inside `publish` (Python, same META-03 transaction). Matches this project's recurring layered-defense philosophy — each stage proves its own fidelity before handing off.
- **D-22 (LOCKED — user-added critical accounting rule, do not implement as a naive count comparison):** A discrepancy is **recorded and the run continues — never blocks** (matches VALID-09's anomaly-detection precedent: flag, don't block). But the comparison itself must be **quarantine-aware**: at each hop, reconciliation compares **input count against (output count + rows routed to `meta.rejected_records` for that hop)** — never a naive raw input-vs-output comparison. `meta.reconciliation_results` should join/reference `meta.rejected_records` and `meta.dedup_audit` so the expected-vs-actual comparison already nets out every known, legitimate reduction (quarantine, dedup) before flagging a genuine, unexplained discrepancy.
- **D-23:** The source-provided control total (VALID-06) is carried by **extending the `_BATCH_COMPLETE` manifest** (LOAD-11, built-but-unexercised per Phase 8 D-19) with `expected_row_count`/`expected_checksum` fields. Phase 9 is the first thing to actually exercise this manifest — giving it real purpose instead of staying dormant, reusing infrastructure rather than inventing a second sidecar-file convention.
- **D-24:** `meta.reconciliation_results` grain is **per file, per hop** — one row per `(file_id, hop)`. Matches the manifest's own per-file control total and the platform's existing file/batch/record identity model (LOAD-04/LOAD-08).
- **D-25:** The sum check (one of VALID-05's five checks: counts, sums, checksums, min/max, key counts) is **dataset-conditional, config-declared** — a new optional `reconciliation:` block in dataset YAML declares which column(s) get summed (e.g. `orders.yaml: sum_columns: [amount]`); `customers.yaml` simply omits it (no natural numeric column to sum — `event_ts`/`birth_date` are dates). Matches the project's "config-not-code, opt-in unexercised where it doesn't apply" pattern (Phase 6 D-10, Phase 8 D-19).
- **D-26 (LOCKED — user explicitly requested "both," confirmed):** The bronze→silver reconciliation check inside `dbt_build` uses **both** mechanisms, not either/or: a **custom macro writes the durable `meta.reconciliation_results` row** in the same transaction as the model write (matching 08.1 D-09's `meta.dedup_audit` post-hook pattern exactly), **AND** a **native dbt test with `severity: warn`** provides visible, non-blocking signal in dbt's own run output/CI. Low marginal cost — the macro is needed regardless for durability; the dbt test is a cheap idiomatic add-on. Consistent with this project's recurring layered-defense pattern (business-key + hash dedup, gold guard kept even after silver is correct).

**Test-Tier Placement**
- **D-27 (LOCKED — live-first, testcontainers-fallback, same pattern as D-12):** Target running the **FULL 2-year sweep on the real live kind cluster** as the primary goal, including the QUAL-11 idempotency re-run (re-running the full backfill must produce zero additional rows). If that proves impractical on local hardware/time budget, **fall back to full-scale in testcontainers with only a representative subset proven live** — the same split 08.1 already used successfully (655-test testcontainers suite for full mechanics, a handful of files for the live proof).

**Cleanup**
- **D-28:** Remove the **vestigial `deduplication:` block** in `customers.yaml`/`orders.yaml` (references a `DEDUP_REGISTRY` that was never built — dedup is entirely dbt-owned now, per 08.1 D-07). Phase 9 touches these YAML files anyway (adding the new `reconciliation:`/`sum_columns` block) — a natural place to also delete the dead config, avoiding confusion for a future reader.
  - **RESEARCH WARNING — see Common Pitfall 4:** this is not a pure YAML edit. `dataplat.config.model.DatasetConfig.deduplication` is a **required** (non-Optional) Pydantic field with no default in `configs/defaults.yaml`. Deleting the YAML block without also touching the model will make config validation fail hard for both datasets.

### Claude's Discretion

None — every question in this discussion was answered with a specific choice; no "you decide" deferrals remain open for research/planning to resolve independently. (Contrast with the pre-Phase-08.1 checkpoint, which had left both watermark questions to Claude's discretion — those are now superseded by D-01/D-02 above.)

### Deferred Ideas (OUT OF SCOPE)

None raised that belong to a different phase — every topic that came up during this discussion (including the concurrency and cleanup follow-ups) was within Phase 9's actual scope. The DEDUP-related work that WAS deferred out of this phase already happened in Phase 08.1 before this discussion began — that's a completed phase, not a deferred idea.

</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| INCR-01 | Incremental processing works without full dataset reloads, via timestamp/watermark, monotonic ID, batch ID or file-based strategies | D-01/D-02: `meta.watermarks` EVENT_TIMESTAMP strategy, observational only; file selection remains the existing idempotency-key mechanism (Architecture Pattern 1) |
| INCR-02 | Watermarks advance only from observed committed cursor values, lagged, inside the publication transaction, using `>=` never `>` | Architecture Pattern 1 — write `meta.watermarks`/`meta.watermark_history` inside `publish_ingest`'s existing transaction (`packages/dataplat/src/dataplat/pipeline/run.py` lines 827-923), after `publisher.publish()` returns, using `GREATEST(existing, new)` semantics |
| INCR-05 | Backfills run through the same pipeline as normal ingestion, no simplified bypass path | Summary finding: `discover_files` is already date-agnostic; no bypass exists or is needed — verified live in code |
| INCR-06 | Backfills are idempotent, use correct historical files, respect historical schema versions, handle missing files explicitly | D-06 (gap record), D-07 (schema version proof-only, already works per Phase 6), Common Pitfall 1 (window-sizing) |
| LOAD-06 | After a partial failure the platform determines what succeeded/remains/retry-or-rollback without manual log inspection | Architecture Pattern 3 (`meta.v_run_recovery`, D-14/D-15/D-16), templated on `meta.v_customers_lineage` |
| VALID-05 | Source-to-target reconciliation: counts, sums, checksums, min/max, key counts, discrepancies reported explicitly | Architecture Pattern 4 (`meta.reconciliation_results`, D-20..D-26), templated on `dedup_audit_post_hook.sql` |
| VALID-06 | Source-provided control totals validated against loaded target | D-23: extend `_BATCH_COMPLETE` manifest — Common Pitfall 5 (marker is currently existence-only, never content-read) |
| QUAL-11 | Backfills tested for idempotency and historical schema resolution | D-27 test-tier plan; `tests/e2e/slice/test_backfill_reentry.py` and `test_pod_kill_retry.py` are direct live-test precedents (Code Examples section) |

</phase_requirements>

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Watermark advance (INCR-01/02) | API/Backend (`dataplat.pipeline.run.publish_ingest`) | Database (`meta.watermarks`/`meta.watermark_history`) | Must live inside the same PostgreSQL transaction as the publish `INSERT ... ON CONFLICT` (META-03) — cannot be a separate Airflow-side write |
| Backfill trigger (INCR-05/06) | Orchestrator (Airflow CLI, `airflow backfill create`) | — | D-11 hard requirement: native CLI only, no new tooling layer |
| Backfill window sizing / logical-date enumeration | Orchestrator (Airflow scheduler's `Backfill`/`DagRun` model) | — | Airflow's own timetable determines how many DagRuns a `--from-date`/`--to-date` window creates — this is scheduler-owned, not pipeline-owned (see Common Pitfall 1) |
| File/business-date scoping within a backfill | API/Backend (`dataplat.discovery.discover_files`) | — | Already content-hash-driven, not date-driven — confirmed no change needed |
| dbt_build stage-status recording (D-14) | Orchestrator (Airflow DAG task, plain `psycopg`) | Database (`meta.run_stages`) | `dbt_app` has **zero** grant on `meta.run_stages` (migration 0025) — the write must come from an Airflow-side task using `etl_app`-equivalent credentials, mirroring `integrity_gate.py`'s own sanctioned ADR-0004 exception, NOT from inside the dbt pod itself |
| Recovery visibility (LOAD-06) | Database (SQL view `meta.v_run_recovery`) | Observability (Grafana Postgres datasource, D-19 alert) | "SQL-queryable, no dashboard" is the established Phase 8 pattern; Grafana is the alerting consumer, not the source of truth |
| Reconciliation raw→bronze (D-21) | API/Backend (`StagingLoader`/`stage_ingest`) | Database (`meta.reconciliation_results`) | Same Python transaction as the COPY into durable bronze (`promote_to_durable_bronze`) |
| Reconciliation bronze→silver (D-21/D-26) | Transformation (dbt post-hook macro) | Database (`meta.reconciliation_results`, dbt native test) | Templated exactly on `dedup_audit_post_hook.sql`'s "same transaction as model write" pattern |
| Reconciliation silver→gold (D-21) | API/Backend (`publish_ingest`) | Database (`meta.reconciliation_results`) | Same transaction as the `MergePublisher`/`OrdersMergePublisher` upsert |
| Control-total ingestion (D-23) | Orchestrator (discovery-time, `_apply_batch_complete_marker_gate`) | API/Backend (reconciliation comparison) | The marker object's *content* (not just its presence) must be read — a new capability, not a reuse of the existing presence-only check |

## Standard Stack

No new external libraries are required by this phase. Every mechanism (Alembic migrations, psycopg COPY/transactions, dbt Jinja macros, Grafana Alerting-as-code, Airflow CLI) is already pinned and in use elsewhere in this codebase (see project CLAUDE.md's Technology Stack section for exact versions — Airflow `3.3.0`, PostgreSQL 17/18, `psycopg[binary,pool] 3.3.4`, Alembic `1.19.1`). This phase is pure application/config/SQL work on top of the existing stack.

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Native `airflow backfill create --max-active-runs N` | A hand-rolled Airflow Pool + `pool=` on every task | D-12/D-11 both favor reusing native Airflow mechanisms over new tooling; a Pool requires a bootstrap step (`airflow pools set`, no committed pools-as-code convention exists in this repo) that a CLI flag doesn't. Rejected — see Architecture Pattern 2. |
| A dbt-side write to `meta.run_stages` for `DBT_BUILD` | An Airflow-side plain-`psycopg` write (mirrors `integrity_gate.py`) | `dbt_app` has zero grant on `meta.run_stages` (migration 0025, deliberate D-02 decoupling). Granting it would re-couple dbt to the Python claim mechanism, contradicting 08.1 D-02. Airflow-side write is correct — see Architectural Responsibility Map. |
| Extending `_BATCH_COMPLETE` to carry control totals (D-23) | A brand-new sidecar manifest file | D-23 explicitly rejects this — reuse over invention, matching Phase 8 D-19's "opt-in, unexercised" precedent now being exercised. |

## Package Legitimacy Audit

Not applicable — this phase installs no new external packages (Python, dbt, or otherwise). All work uses already-vetted, already-pinned dependencies documented in the project's Technology Stack (CLAUDE.md). No `pip install`/`npm install`/Alembic-external-plugin additions are introduced.

## Architecture Patterns

### System Architecture Diagram

```
                         ┌─────────────────────────────────────────────┐
                         │   Airflow Scheduler (Airflow 3.3.0)          │
                         │                                               │
  Live traffic  ───────▶ │  DagRun (backfill_id=NULL)                   │
  (schedule=*/1min)      │  bound by @dag(max_active_runs=1)            │
                         │                                               │
  `airflow backfill      │  DagRun(s) (backfill_id=X)                   │
   create --from-date    │  bound by Backfill.max_active_runs (D-12)    │
   --to-date --max-      │  ── counted SEPARATELY from live runs (D-13) │
   active-runs N`  ─────▶│                                               │
                         └───────────────┬───────────────────────────────┘
                                         │  (same task graph, either run type)
                                         ▼
        wait_for_files(S3KeySensor) → list_matched_keys → integrity_gate (LOAD-10, ×3 cap)
                                         │
                                         ▼
                        discover (discover_files: whole-bucket, content-hash scoped —
                                   NEVER logical_date-scoped; D-06 gap record on
                                   missing file happens HERE if a listed manifest
                                   entry never resolves)
                                         │
                                         ▼
              stage (StagingLoader.load → promote_to_durable_bronze)
                    │  D-21 raw→bronze reconciliation: INSERT meta.reconciliation_results
                    │  (same txn as COPY) comparing rows_read vs (bronze rows + this
                    │  hop's meta.rejected_records) — D-22 quarantine-aware
                    ▼
              dbt_build (own ServiceAccount/Vault role, own meta.run_stages
                         DBT_BUILD entry written by the DAG task, D-14)
                    │  D-21/D-26 bronze→silver reconciliation: dedup_audit_post_hook.sql
                    │  -style macro writes meta.reconciliation_results in same txn as
                    │  silver model write, PLUS a native dbt test (severity: warn)
                    ▼
              publish (publish_ingest: pg_advisory_xact_lock, single-writer,
                       reads ENTIRE silver.<dataset> cumulatively)
                    │  D-01/D-02/D-04: meta.watermarks/meta.watermark_history advance
                    │  HERE, same transaction as the MergePublisher upsert, using
                    │  GREATEST(existing, EXCLUDED.event_ts) — never a bare `>`
                    │  D-21 silver→gold reconciliation: meta.reconciliation_results
                    ▼
        normalized.customers / normalized.orders (gold) ── outlets=[customers_asset]

  Recovery: meta.v_run_recovery (D-16) = LEFT JOIN of meta.ingestion_runs +
  meta.run_stages (3 stage_name values) → "next action: retry stage X" / "complete"
  → Grafana Alert rule (D-19, Phase 7's existing engine) fires on exhausted retries.
```

### Recommended Project Structure

No new top-level packages — extends existing modules:
```
migrations/versions/
├── 00XX_meta_watermarks.py              # D-01..D-04
├── 00XX_meta_reconciliation_results.py  # D-20..D-25
└── 00XX_meta_v_run_recovery.py          # D-16 (view + grants, "drop + recreate" pattern)

packages/dataplat/src/dataplat/
├── metadata/repository.py               # + record_watermark / get_current_watermark
├── metadata/postgres.py                 # + record_dbt_build_stage-equivalent SQL (called from Airflow side, see below)
├── load/staging.py                      # + raw->bronze reconciliation write (D-21)
└── pipeline/run.py                      # publish_ingest: + watermark advance + silver->gold reconciliation, inside existing transaction

airflow/dags/_common/
└── run_stage_recorder.py (new, small)   # D-14: plain-psycopg DBT_BUILD run_stages write,
                                          # mirrors _common/integrity_gate.py's ADR-0004 exception

dbt/macros/
└── reconciliation_post_hook.sql         # D-26, templated on dedup_audit_post_hook.sql

helm/values/local/monitoring.yaml
helm/values/ci/monitoring.yaml
└── grafana.alerting.rules.yaml          # + D-19 rule querying meta.v_run_recovery

configs/datasets/customers.yaml
configs/datasets/orders.yaml
└── + reconciliation: {sum_columns: [...]}  (orders only, D-25)
    - deduplication: {...}                  (removed, D-28 — see Pitfall 4 for the model-side prerequisite)

tests/fixtures/ (or tools/corpus/generators.py)
└── + 2-year backfill corpus generator (D-09/D-10) — see Open Question 2
```

### Pattern 1: Observational watermark advance inside the existing publish transaction

**What:** `meta.watermarks`/`meta.watermark_history` are written by `publish_ingest` (`packages/dataplat/src/dataplat/pipeline/run.py`), inside the SAME `with ctx.db.connection() as conn, conn.transaction():` block that already holds `pg_advisory_xact_lock` and calls `publisher.publish()` — never a separate Airflow-side write, never a separate transaction (INCR-02, META-03).

**When to use:** Every `publish_ingest` invocation that finalizes at least one `STAGED` run (the existing `if not staged: ... no_op` early-return already exists; skip watermark logic on that path too).

**Example** — verified real code this pattern extends, `packages/dataplat/src/dataplat/pipeline/run.py` (lines 824-923):
```python
# Source: packages/dataplat/src/dataplat/pipeline/run.py, live-read this session
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
    # --- Phase 9 addition, same transaction, after publish, before the
    #     per-run finalize loop: advance the watermark using GREATEST(),
    #     which structurally enforces ">=, never >" (INCR-02) without a
    #     conditional branch, and unconditionally logs to watermark_history
    #     even when the value doesn't move (append-only audit, D-04).
    conn.execute(
        """
        INSERT INTO meta.watermarks (dataset_id, target_key, cursor_value)
        VALUES (%s, 'default', (SELECT max(event_ts) FROM {source_table}))
        ON CONFLICT (dataset_id, target_key) DO UPDATE
           SET cursor_value = GREATEST(meta.watermarks.cursor_value, EXCLUDED.cursor_value)
        RETURNING cursor_value
        """.format(source_table=source_table),
        (dataset_id,),
    )
    # ... existing finalize loop (claim_run_stage PUBLISH, finalize_publication,
    #     complete_run_stage) unchanged below this point.
```
Note: `event_ts`/`order_date` are the exact `order_by` columns already declared in `customers.yaml`/`orders.yaml` (D-02) — the `SELECT max(...)` target column must be resolved per-dataset (a small config-driven lookup, not a hardcoded column name, since `orders` uses `order_date` not `event_ts`).

### Pattern 2: Native Airflow backfill concurrency — no new Pool needed

**What:** Airflow 3.3.0's scheduler (`.venv/lib/python3.12/site-packages/airflow/jobs/scheduler_job_runner.py`, lines 2680-2764, verified by direct read of the installed package) counts "active runs" **grouped by `(dag_id, backfill_id)`**:
```python
# Source: installed airflow 3.3.0, jobs/scheduler_job_runner.py (verified this session)
query = (
    select(DagRun.dag_id, DagRun.backfill_id, func.count(DagRun.id).label("num_running"))
    .where(DagRun.state == DagRunState.RUNNING)
    .group_by(DagRun.dag_id, DagRun.backfill_id)
)
...
if backfill_id is not None:
    backfill = dag_run.backfill
    if active_runs >= backfill.max_active_runs:      # <-- Backfill's OWN bound
        ... skip this run for now
elif dag_run.max_active_runs:
    if active_runs >= dag_run.max_active_runs:        # <-- DAG's own (@dag(max_active_runs=1))
        ... skip this run for now
```
A live scheduled DagRun has `backfill_id IS NULL` and is bound by the `@dag(max_active_runs=1)` decorator already on both DAGs. A backfill's DagRuns all share one `backfill_id` and are bound **independently** by `airflow backfill create`'s own `--max-active-runs` flag (default `10`, confirmed via `airflow backfill create --help` against the installed CLI):
```
--max-active-runs MAX_ACTIVE_RUNS   Max active runs for this backfill.
--reprocess-behavior {none,completed,failed}
```
This means **D-13 (live + backfill concurrently, same dataset) is natively supported with zero DAG code changes** — the existing `max_active_runs=1` on both DAGs only ever throttles the *live* run type. D-12's "bounded parallelism, degrade to sequential" is satisfied purely by choosing `--max-active-runs` on the `airflow backfill create` invocation itself (e.g. `--max-active-runs 3`, falling back to `--max-active-runs 1` if the cluster's known CPU headroom problems recur — see the project's own `host_hardware_context` memory notes and the repeated `FailedScheduling: Insufficient cpu` incidents logged in STATE.md).

**Important caveat, verified from the DAG files themselves:** `stage`, `dbt_build`, and `publish` are each already capped at `max_active_tis_per_dag=1` (`airflow/dags/csv_ingest_customers.py`/`csv_ingest_orders.py`, both DAGs, all three tasks). This is a **per-task-id, cross-DagRun** cap — it already serializes stage/dbt_build/publish pod execution across *every* concurrently-active DagRun, live or backfill. Raising `--max-active-runs` therefore increases how many DagRuns can be simultaneously *discovering* (the `integrity_gate`/`discover` fan-out, currently capped at 3 via `.override(max_active_tis_per_dag=3)`), but does **not** by itself parallelize the expensive stage/dbt_build/publish work — that stays sequential platform-wide regardless of `--max-active-runs`, unless the plan deliberately raises those caps too (a real resource-vs-throughput tradeoff to make explicitly, not assume).

**When to use:** the live 2-year backfill proof (D-27), and any future multi-tenant-dataset backfill.

### Pattern 3: `meta.v_run_recovery` — templated on `meta.v_customers_lineage`

**What:** A `LEFT JOIN`-based view over `meta.ingestion_runs` + `meta.run_stages` (3 `stage_name` rows per run once D-14 lands: `STAGE_LOAD`, `DBT_BUILD`, `PUBLISH`), computing a `next_action` column server-side. This is the exact "drop + recreate the view" migration pattern this repo already uses three times (migrations 0012, 0026, 0030 — verified by direct read; 0030's own docstring explains *why*: "Postgres has no `ALTER VIEW ... ADD COLUMN`, and no equivalent for changing a JOIN predicate either").

**When to use:** LOAD-06's single-query "what succeeded, what remains, retry-or-rollback" requirement.

**Example** — the exact join/grant shape to follow, verified real code (`migrations/versions/0030_fix_v_customers_lineage_dedup_audit_model_name.py`):
```sql
-- Source: migrations/versions/0030_..., adapted shape for meta.v_run_recovery
CREATE VIEW meta.v_run_recovery AS
SELECT
    r.run_id, r.dataset_id, r.status AS run_status,
    r.logical_date, r.dag_id, r.dag_run_id,
    sl.status AS stage_load_status, sl.lease_expires_at AS stage_load_lease,
    db.status AS dbt_build_status,
    pb.status AS publish_status,
    CASE
        WHEN r.status = 'SUCCEEDED' AND pb.status = 'SUCCEEDED' THEN 'complete'
        WHEN sl.status IN ('FAILED', 'PENDING') OR sl.status IS NULL THEN 'retry stage STAGE_LOAD'
        WHEN db.status IN ('FAILED', 'PENDING') OR db.status IS NULL THEN 'retry stage DBT_BUILD'
        WHEN pb.status IN ('FAILED', 'PENDING') OR pb.status IS NULL THEN 'retry stage PUBLISH'
        ELSE 'in progress'
    END AS next_action  -- D-15: always "retry", never "rollback"
FROM meta.ingestion_runs r
LEFT JOIN meta.run_stages sl ON sl.run_id = r.run_id AND sl.stage_name = 'STAGE_LOAD'
LEFT JOIN meta.run_stages db ON db.run_id = r.run_id AND db.stage_name = 'DBT_BUILD'
LEFT JOIN meta.run_stages pb ON pb.run_id = r.run_id AND pb.stage_name = 'PUBLISH';
```
Grants: `GRANT SELECT ON meta.v_run_recovery TO etl_app, grafana_reader;` — exact precedent from migration 0012/0026/0030's own `upgrade()`.

### Pattern 4: dbt reconciliation post-hook — templated on `dedup_audit_post_hook.sql`

**What:** `dbt/macros/dedup_audit_post_hook.sql` (verified by full read this session) is the direct, load-bearing template for D-26's bronze→silver reconciliation macro. Its three hard-won, empirically-verified lessons (documented in its own header, all confirmed present in the file) apply identically to the new macro:
1. Accept `dataset_name` as a plain string, resolve via `meta.dataset_id_for_name(text)` (a `SECURITY DEFINER` function, migration 0028) — never a direct `SELECT` against `meta.datasets` (fails `dbt_app`'s least-privilege grant test).
2. Accept `source_schema`/`source_identifier`/`target_schema`/`target_identifier` as **plain strings**, never `{{ source(...) }}`/`{{ this }}` objects passed as macro arguments — verified unreliable across `post_hook`'s two-pass (config-time vs. compile-time) Jinja evaluation.
3. Derive any "floor"/watermark value **from the audit table's own history** (`coalesce(max(...), 0)`), never from a `run_query()` value captured into a `post_hook` config string — the same config-vs-compile-pass timing bug documented in point 3 of that file's header.

**Grant precedent** (migration 0024, verified): `dbt_app` gets `GRANT SELECT, INSERT ON meta.reconciliation_results TO dbt_app` (INSERT-only — a post-hook only appends, never revises history), `etl_app`/`grafana_reader` get `SELECT`. `dbt_app` needs `GRANT USAGE ON SCHEMA meta TO dbt_app` too, but migration 0021/0024 already granted this — **do not re-grant** (idempotent `GRANT` is harmless but a duplicate migration statement is dead weight).

**The dbt-test half of D-26** ("both," locked): add a `tests:` entry with `config: {severity: warn}` under the relevant model in `dbt/models/silver/schema.yml`, asserting (e.g.) `bronze_row_count = silver_kept_count + silver_dropped_count` via a singular test or a generic `dbt_utils.expression_is_true`-style check — this is additive to the macro, not a replacement.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Bounding backfill DagRun parallelism (D-12) | A new Airflow Pool + `pool=` kwarg on every task | `airflow backfill create --max-active-runs N` | Native CLI flag, already verified present in this Airflow 3.3.0 install; zero new infrastructure, consistent with D-11's "no new tooling" |
| Detecting live+backfill collision risk (D-13) | A custom "is a backfill currently running for this dataset" guard/lock | Nothing extra — Airflow's own `(dag_id, backfill_id)`-scoped concurrency accounting plus the existing `pg_advisory_xact_lock` in `MergePublisher`/`OrdersMergePublisher` already make this safe | Verified in Architecture Pattern 2; building a second guard would duplicate a control that already exists and could disagree with it |
| "Which files fall in this backfill's date range" | A new date-scoped listing/filter inside `discover_files` | Nothing — `discover_files` already lists the whole bucket and dedups by content-hash; scope the fixture upload and the `--from-date`/`--to-date` window size instead (Common Pitfall 1) | Rewriting `discover_files` to be date-aware would reintroduce exactly the "second, parallel is-this-new mechanism" anti-pattern D-01 explicitly avoids for watermarks |
| Reconciliation discrepancy accounting (D-22) | A raw input-count vs. final output-count diff | A three-way comparison against `meta.rejected_records` (existing, migration 0015) and `meta.dedup_audit` (existing, migration 0024) | Both tables already carry per-file/per-run/per-batch linkage (`run_id`, `file_id`, `batch_id` FKs, verified by direct read) — reconciliation should join them, not duplicate their bookkeeping |
| Control-total delivery (D-23) | A new sidecar/manifest file convention | Extend `_BATCH_COMPLETE`'s existing (currently presence-only) check to read its body | `config.source.batch_complete_marker` and its discovery-time gate already exist (`dataplat/discovery.py::_apply_batch_complete_marker_gate`) — see Common Pitfall 5 for the real extension needed |

**Key insight:** Every "don't hand-roll" item above exists because this codebase already solved a structurally identical problem in Phase 4/6/8/08.1 — the discipline this phase needs is *finding and reusing* those solutions, not inventing new ones. The one genuinely new mechanism is the reconciliation SQL/macro shape itself (D-20..D-26), and even that has a byte-for-byte structural template (`dedup_audit_post_hook.sql`).

## Common Pitfalls

### Pitfall 1: A literal 2-year `--from-date`/`--to-date` window at the DAGs' current schedule is computationally infeasible
**What goes wrong:** Both `csv_ingest_customers` and `csv_ingest_orders` are scheduled `schedule="*/1 * * * *"` (every minute — verified by direct read of both DAG files). `airflow backfill create --from-date/--to-date` enumerates one DagRun per tick of the DAG's *own* timetable within that window (verified: `airflow/models/backfill.py`'s `_create_backfill`/`_create_backfill_dag_run_non_partitioned` iterate the DAG's schedule, not a fixed daily/weekly grain). A genuine 2-calendar-year window at 1-minute granularity is roughly **1,051,200 DagRuns** — infeasible on any hardware, and especially so given this project's own documented, repeated CPU-starvation incidents (STATE.md: `FailedScheduling: Insufficient cpu` events, `max_active_tis_per_dag` caps added specifically to survive fan-out).
**Why it happens:** `meta.files.business_date` is never populated anywhere in this codebase (confirmed: `discovery.py`'s own module docstring states this explicitly, and `MetadataRepository.create_file` accepts no such column) — so there is no mechanism connecting a historical file's real business date to a specific backfill `logical_date`/DagRun at all. The backfill CLI's date range is a *scheduler* concept (which ticks of the DAG's timetable get a DagRun), completely decoupled from *which content-hash-distinct files get discovered* by whichever DagRun happens to run.
**How to avoid:** Treat "the 2-year fixture corpus" (D-09/D-10) and "the `airflow backfill create --from-date/--to-date` window" as two **separate** things. Upload the full 2-year, content-varied fixture corpus to MinIO's `raw/customers/`/`raw/orders/` prefixes ahead of time (a setup step, not something the backfill trigger itself needs to "cover" chronologically). Then invoke `airflow backfill create` with a **small** window sized to the DAG's real 1-minute cadence — just enough ticks (DagRuns) to drain `discover_files`'s `max_units_per_run: 100` batching cap across the full fixture set at least twice (once to discover everything, once for the QUAL-11 idempotency re-run to prove zero new rows). For ~730 files/dataset/year × 2 years ≈ 1,460 files/dataset, that's `⌈1460/100⌉ = 15` discover calls minimum per dataset — a `--from-date`/`--to-date` window of roughly 20-30 minutes (with `--max-active-runs` bounding how many run concurrently) is enough, not two years of ticks. **This is the actual scope decision the plan must make explicitly** — see Open Question 1.
**Warning signs:** `airflow backfill create` hanging or the Airflow metadata Postgres growing unexpectedly large during dry-run testing; `airflow backfill create --dry-run` first to see the enumerated DagRun count before ever running for real.

### Pitfall 2: `publish_ingest` reads the ENTIRE `silver.<dataset>` table on every call, not a per-run slice
**What goes wrong:** Assuming the watermark's `SELECT max(event_ts)` (Pattern 1) or a reconciliation count (D-21's silver→gold hop) can be scoped to "this run's own rows" via a `WHERE _run_id = ...` filter.
**Why it happens:** Verified directly in `packages/dataplat/src/dataplat/pipeline/run.py` (lines 866-886, code comment is explicit): `publisher.publish()` "ran ONCE per `publish_ingest` invocation as a single upsert pass over the ENTIRE cumulative `silver.<dataset>` table (never scoped to one run's own `_run_id` range)." The existing code already documents this as a deliberate, accepted simplification for `rows_loaded` attribution across a multi-run finalize pass.
**How to avoid:** Design the watermark's `SELECT max(event_ts)` and the silver→gold reconciliation comparison to match this reality — they should compare against the **whole target table's current state** (or the specific set of business keys `result.published_business_keys` names, which the publish SQL's `RETURNING` clause already surfaces), not attempt a per-run-scoped query that the existing publish SQL structurally cannot support without also changing `merge.py`/`merge_orders.py`'s `_PUBLISH_SQL` (out of scope — the module docstring explicitly says that SQL "requires ZERO change").
**Warning signs:** A reconciliation or watermark query that silently returns 0/NULL for every run after the first because it filtered on a `_run_id` that no longer represents "new" rows in a cumulative table.

### Pitfall 3: Airflow's `--reprocess-behavior completed` re-triggers succeeded DagRuns, but `discover_files`'s own idempotency will no-op almost everything
**What goes wrong:** Expecting every re-triggered DagRun in a `--reprocess-behavior completed` backfill to do real work.
**Why it happens:** `discover_files` returns `None` (no `DiscoveredUnit`) for any file whose `get_or_create_ingestion_run` call returns `status == "SUCCEEDED"` under the *current* idempotency-key formula (verified: `_process_ungrouped_object`/`_process_multipart_group`, both check this and log `decision="ALREADY_SUCCEEDED"`). A re-triggered DagRun for an already-fully-processed logical date will typically find zero new work — this is **correct**, not a bug, and is exactly what QUAL-11's "re-running produces zero additional rows" needs to prove.
**How to avoid:** Design the QUAL-11 live idempotency proof around row-count assertions (before/after counts identical) rather than assuming every re-triggered DagRun does visible work — a fast "no-op" DagRun on the second sweep is the expected, desired outcome.
**Warning signs:** A test that asserts "N discover units returned" on a re-run and fails when it gets 0 — that 0 is success, not failure, for the idempotency proof.

### Pitfall 4: Removing `deduplication:` from the dataset YAMLs (D-28) breaks Pydantic validation unless the model changes too
**What goes wrong:** Deleting the `deduplication:` block from `customers.yaml`/`orders.yaml` alone.
**Why it happens:** Verified by direct read of `packages/dataplat/src/dataplat/config/model.py`: `DatasetConfig.deduplication: DeduplicationConfig` is a **required** field (no `| None`, no default), and `configs/defaults.yaml` supplies no fallback value for it (`grep` confirmed zero `deduplication:` occurrences there). `load_config`/Pydantic validation will raise `ValidationError: deduplication field required` the moment either YAML file no longer supplies it.
**How to avoid:** This is a package-level change, not a config-only edit. The minimal, low-blast-radius fix (recommended, not yet decided by any locked decision): make `deduplication: DeduplicationConfig | None = None` in `dataplat/config/model.py`, and guard `_check_deduplication_keys_are_business_key_columns` for `self.deduplication is not None`. This keeps `DeduplicationConfig` itself, and the **13 other test files** that construct `DeduplicationConfig`/`deduplication:` blocks (verified via `grep -rl`: `tests/unit/test_csv_processor_cli.py`, `tests/unit/validate/test_batch_complete_marker.py`, `tests/integration/test_staging_quality_rules.py`, `tests/unit/test_discovery.py`, `tests/unit/test_run_ingest_trace.py`, `tests/integration/test_run_ingest.py`, `tests/unit/test_csv_source_multipart.py`, `tests/unit/test_csv_source_inspect.py`, `tests/integration/test_schema_resolution.py`, `tests/integration/test_discover_files.py`, `tests/integration/test_backfill_idempotency.py`, `tests/integration/test_staging_durability.py`, `tests/integration/test_stage_ingest.py`, `tests/integration/test_publish_transaction_wiring.py`, `tests/integration/test_staging_loader.py`, `tests/integration/test_publish_ingest.py`) fully intact and passing — an Optional field with an explicit value supplied is unaffected. Fully purging `DeduplicationConfig` (the alternative, more literal reading of "remove") would require touching all ~13 of those files for zero behavior change, which is disproportionate to a "cleanup" decision.
**Warning signs:** Any test in the list above failing with a Pydantic `ValidationError` after this phase's YAML edits land — confirms the model wasn't updated in step with the YAML.

### Pitfall 5: `_BATCH_COMPLETE`'s existing gate only checks the marker object's *presence*, never reads its *content*
**What goes wrong:** Assuming D-23's `expected_row_count`/`expected_checksum` fields can be "added" to the manifest with no code change, since `_BATCH_COMPLETE` "already exists."
**Why it happens:** Verified by direct read of `_apply_batch_complete_marker_gate` (`packages/dataplat/src/dataplat/discovery.py`): the entire check is `if not any(obj.key == marker_key for obj in listed): ... withhold`. The marker object's *bytes* are never fetched (no `get_object` call anywhere in this function) — it is purely a key-existence check today. `SourceConfig.batch_complete_marker: str | None = None` is just a filename suffix, not a schema for file content.
**How to avoid:** D-23 requires new code: (1) a documented JSON (or similar) shape for the marker file's body (`expected_row_count`, `expected_checksum`, presumably per-dataset-batch), (2) a real `objects.get_object(...)` read of that body once presence is confirmed, (3) parsing/validating it, (4) threading the parsed values through to wherever the silver→gold (or a dedicated raw-level) reconciliation check compares against them. Scope this explicitly as new work in the plan, not as "wire up an existing field."
**Warning signs:** A plan task that describes this as "read `expected_row_count` from the manifest" with no corresponding task to define/parse the manifest's actual JSON body — the current code has no such body-read path to reuse.

### Pitfall 6: A `FAILED` `run_stages` row does not by itself mean Airflow's retries are exhausted
**What goes wrong:** D-19's alert firing (or not firing) at the wrong time — either alerting on every ordinary retry-in-progress `FAILED` stage, or never firing because a stuck lease looks identical to a genuinely-exhausted one.
**Why it happens:** `meta.run_stages.status = 'FAILED'` (set by `complete_run_stage`) records that *one attempt* of a stage ended badly — it says nothing about whether Airflow's own `retries=N` budget (2 or 3, per task, verified in both DAG files) has been exhausted. A `FAILED` row can be immediately followed by a fresh Airflow retry that re-claims the stage via `claim_run_stage`'s existing `ON CONFLICT ... WHERE status IN ('PENDING','FAILED')` clause (verified) within seconds.
**How to avoid:** Design the D-19 alert condition around a **time-based** proxy — e.g. a `run_stages` row that has been `FAILED` (or a `RUNNING` row whose `lease_expires_at` has passed) for longer than a threshold with no newer row for the same `(run_id, stage_name)` — mirroring the existing freshness-alert pattern's own `for: 5m` Grafana Alerting field (verified in `helm/values/local/monitoring.yaml`'s `rules.yaml`), rather than firing on the raw `FAILED` status alone.
**Warning signs:** Alert noise on every ordinary Airflow retry cycle (threshold too tight), or no alert ever firing for a genuinely stuck pipeline (threshold missing entirely).

## Code Examples

### Airflow backfill CLI — verified live against the installed 3.3.0 binary
```
$ python3 -m airflow backfill create --help
Usage: airflow backfill create [-h] --dag-id DAG_ID
                               [--dag-run-conf DAG_RUN_CONF] [--dry-run]
                               --from-date FROM_DATE
                               [--max-active-runs MAX_ACTIVE_RUNS]
                               [--reprocess-behavior {none,completed,failed}]
                               [--run-backwards]
                               [--run-on-latest-version | --no-run-on-latest-version]
                               --to-date TO_DATE
```
Source: this session's own `python3 -m airflow backfill create --help` invocation against the repo's `.venv`. `--dry-run` is the recommended first step for sizing the window (Pitfall 1).

### Existing live pod-kill test — direct template for D-18's dbt_build extension
`tests/e2e/slice/test_pod_kill_retry.py::test_pod_kill_mid_load_produces_no_duplicates` (verified, full read this session) is the exact pattern: poll `meta.ingestion_runs` for a mid-flight signal (`status='RUNNING' AND rows_read > 0`), `kubectl -n etl delete pod <pod_name> --wait=false`, then poll for the run reaching `SUCCEEDED` via a fresh retried pod, and assert exact row counts (no duplicates, nothing missing). **Caveat for D-18:** `dbt_build` has no equivalent per-row `rows_read` heartbeat signal (dbt's own execution is opaque to `meta.ingestion_runs`) — the mid-flight detection needs a different signal, most naturally `meta.run_stages` reaching `DBT_BUILD`/`RUNNING` (once D-14 lands) rather than a row-count threshold. Plan this test's polling condition around the new D-14 status write, not a `rows_read`-style metric that dbt_build will never populate.

### Existing live backfill-reentry test — direct template for QUAL-11
`tests/e2e/slice/test_backfill_reentry.py` (grepped this session) already invokes the real `airflow backfill create --dag-id ... --from-date ... --to-date ...` CLI via `kubectl exec` and polls `backfill_dag_run`/`meta.rejected_records` for re-resolution — this is 08.1's own proven live-proof pattern (VALID-08/D-01/D-23) and should be extended, not replaced, for Phase 9's own backfill/idempotency proof.

## State of the Art

| Old Approach (this repo's own history) | Current Approach (this phase) | When Changed | Impact |
|--------------------------|------------------|---------------|--------|
| `meta.run_stages` tracks only `STAGE_LOAD`/`PUBLISH` (migration 0025) | Adds a 3rd `DBT_BUILD` value, written Airflow-side (D-14) | This phase | `meta.v_run_recovery` becomes a whole-pipeline view, closing 08.1's documented blind spot |
| `_BATCH_COMPLETE` marker: presence-only check | Marker body is read and parsed for control totals (D-23) | This phase | First real exercise of LOAD-11's "may be the authoritative input" clause |
| `deduplication:` YAML block: vestigial, dbt-owned dedup made it dead (08.1) | Removed from dataset YAML; `DatasetConfig.deduplication` becomes Optional | This phase | Cleanup — see Pitfall 4 for the required model change |

**Deprecated/outdated:** The `deduplication:` block's own `DEDUP_REGISTRY` concept it referenced was never built (confirmed by 08.1's own context) — nothing to migrate away from at runtime, purely a config/model cleanup.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | The recommended minimal fix for D-28 (make `deduplication` Optional rather than fully remove `DeduplicationConfig`) is the right scope choice | Common Pitfall 4 | If the user actually wants the class fully purged, ~13 test files need coordinated updates instead of 2 YAML files + 1 model field — a materially larger task than D-28's "cleanup" framing suggests. This is a scope judgment call, not a verified fact, and should be confirmed during planning. |
| A2 | A `--from-date`/`--to-date` window of roughly 20-30 minutes (at the DAGs' 1-minute schedule) is sufficient to drain the discovery batching cap across a ~1,460-file/dataset fixture corpus | Common Pitfall 1, Open Question 1 | The exact number depends on `max_active_tis_per_dag` caps and real cluster throughput, not yet measured live — this is a reasoned estimate from `max_units_per_run: 100`, not an empirical measurement. Actual window sizing must be validated with `--dry-run` and a first small-scale live attempt before committing to the full 2-year corpus run. |

## Open Questions

1. **What exact `--from-date`/`--to-date` window (and how many minutes/DagRuns) should the live 2-year backfill proof use?**
   - What we know: the schedule is `*/1 * * * *`; the fixture corpus needs to be ~1,460 files/dataset (2 years × ~365 days × 2 datasets, assuming daily cadence per D-10's "regular file-drop cadence"); `max_units_per_run: 100` caps each discover call.
   - What's unclear: the exact minimum window size that reliably drains the corpus without hitting this cluster's known CPU-starvation failure mode (STATE.md's repeated `FailedScheduling` incidents), and whether `--max-active-runs` should be raised above 1 for this specific test given `max_active_tis_per_dag=1` already serializes the expensive stages.
   - Recommendation: plan a `--dry-run` sizing step first (cheap, no real execution), then a small pilot window (e.g. 5-10 DagRuns) before committing to whatever window the pilot suggests is needed to clear the full corpus.

2. **Does the 2-year fixture corpus generator already exist, or is it new work?**
   - What we know: `tools/corpus/` (verified: `generators.py`, `manifest.py`, `digests.py`, `__main__.py`) is the existing, seed-driven fixture generation tool (`docs/adr/0005-fixture-corpus-generated-from-a-seed.md`), invoked via `python -m tools.corpus generate --manifest <path> --out <dir>`; `tests/fixtures/slice-corpus.yaml` is the existing manifest (small-scale, `master_seed: "airflow-platform/slice-corpus/v1"`).
   - What's unclear: whether a NEW manifest (e.g. `tests/fixtures/backfill-corpus.yaml`) needs to be authored for the 2-year, schema-change/gap/late-event combination D-10 requires, or whether `tools/corpus/generators.py` needs new generator functions to express a "daily cadence over N days with an injected schema-version boundary and an injected gap" shape it may not currently support.
   - Recommendation: read `tools/corpus/generators.py`'s available generator functions during planning (not fully audited this research pass) before assuming the existing tool can express D-10's combined requirements without new generator code.

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | No new authentication surface — reuses existing Vault-backed Airflow Connections and Kubernetes ServiceAccounts |
| V3 Session Management | No | N/A — no session concept in this phase's scope |
| V4 Access Control | Yes | Least-privilege PostgreSQL role grants per new table (`etl_app`/`dbt_app`/`grafana_reader`), following the exact pattern of migrations 0024/0025/0028 — `dbt_app` gets `INSERT`-only where it writes, `SELECT`-only elsewhere; the `DBT_BUILD` `run_stages` write must come from the Airflow side (etl_app-equivalent), never a new `dbt_app` grant on `meta.run_stages` (would violate 08.1 D-02's deliberate decoupling) |
| V5 Input Validation | Yes | The `_BATCH_COMPLETE` marker body (D-23, Pitfall 5) is attacker-influenced-adjacent (arrives via the same `raw` bucket as untrusted CSV content) — its parsed `expected_row_count`/`expected_checksum` must be validated (correct types, non-negative, bounded length) before use, matching this codebase's existing "never trust file content" discipline (CSV validation barriers) |
| V6 Cryptography | No | Checksums here are integrity comparisons (reconciliation), not a cryptographic/authentication control — reuse the existing `hashlib.sha256` convention already used throughout (`discovery.py`, `integrity_gate.py`) rather than inventing a new algorithm choice |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| A malicious/malformed `_BATCH_COMPLETE` marker body claiming a false `expected_row_count`/`expected_checksum` to mask a genuine data-loss discrepancy | Tampering | D-22's design already treats a discrepancy as record-and-continue (never trusted blindly to suppress an alert) — ensure the reconciliation comparison direction is "flag when actual doesn't match expected," never "silently trust expected over actual" |
| A `dbt_app`-scoped SQL injection via string-interpolated `source_schema`/`source_identifier` in the new reconciliation macro | Tampering | These are dbt-config-controlled strings (from `dbt_project.yml`/model `config()`, not row data) — exactly the same trust boundary `dedup_audit_post_hook.sql` already accepts; do not widen this to accept any row-derived value as a schema/table-name fragment |
| Backfill CLI invoked with an unbounded `--from-date`/`--to-date` window (accidental or malicious) causing DagRun-table exhaustion / DoS on the Airflow metadata Postgres | Denial of Service | `--dry-run` sizing check before any real invocation (Pitfall 1); this is an operational discipline, not a code-level control, since `airflow backfill create` is intentionally a trusted-operator CLI command, not an exposed API in this project's threat model |

## Sources

### Primary (HIGH confidence — verified this session by direct file read or command execution against this repository/its installed dependencies)
- `packages/dataplat/src/dataplat/discovery.py` — full read; confirms whole-bucket, content-hash-driven, `logical_date`-agnostic discovery; `meta.files.business_date` never populated
- `airflow/dags/csv_ingest_customers.py`, `airflow/dags/csv_ingest_orders.py` — full read; confirms `schedule="*/1 * * * *"`, `max_active_runs=1`, `max_active_tis_per_dag` caps on `integrity_gate`(3)/`stage`/`dbt_build`/`publish`(1 each)
- `airflow/dags/_common/integrity_gate.py` — full read; confirms the ADR-0004 "Airflow writes directly to `meta` via plain psycopg" precedent D-14's `DBT_BUILD` write should follow
- `packages/dataplat/src/dataplat/pipeline/run.py` (lines 780-948) — full read; confirms the exact publish transaction shape, the advisory-lock call, and that `publisher.publish()` operates on the whole cumulative `silver.<dataset>` table, not a per-run slice
- `packages/dataplat/src/dataplat/metadata/repository.py` (lines 460-620) — full read; confirms `claim_run_stage`/`heartbeat_run_stage`/`complete_run_stage`/`list_staged_run_ids` exact SQL shapes and the `run_id`-FK-based `run_stages` design
- `migrations/versions/0025_meta_run_stages.py` — full read; confirms `stage_name` is plain `Text` (not ENUM), `UNIQUE(run_id, stage_name)`, `etl_app`-only grants (zero grant to `dbt_app`)
- `migrations/versions/0012_meta_v_customers_lineage.py`, `0026_v_customers_lineage_dbt_hop.py`, `0030_fix_v_customers_lineage_dedup_audit_model_name.py` — full read; confirms the "drop + recreate whole view" migration pattern and its stated rationale
- `dbt/macros/dedup_audit_post_hook.sql` — full read; the direct template for D-26, including its documented gotchas
- `migrations/versions/0024_meta_dedup_audit_decisions.py`, `0028_dbt_app_meta_datasets_select_grant.py` — full read; confirms exact `dbt_app`/`etl_app`/`grafana_reader` grant pattern and the `SECURITY DEFINER` function precedent for narrow cross-schema reads
- `packages/dataplat/src/dataplat/load/publish/merge.py`, `merge_orders.py` — full read; confirms `pg_advisory_xact_lock` ownership split, `EXCLUDED.event_ts >= ...`/`order_date IS NULL OR ...` late-arrival guards
- `packages/dataplat/src/dataplat/config/model.py` (grepped `DeduplicationConfig`/`DatasetConfig`/`FreshnessConfig` definitions) — confirms `deduplication` is a required field with no default
- `configs/datasets/customers.yaml`, `configs/datasets/orders.yaml` — full read; confirms exact `order_by`/`deduplication`/`amount` shapes D-02/D-25/D-28 reference
- `migrations/versions/0015_meta_rejected_records.py`, `0004_meta_ingestion_runs.py` (grepped columns) — confirms `run_id`/`file_id`/`batch_id` FK linkage and `meta.ingestion_runs`' full column set (`logical_date`, `rows_read`, `error_detail` JSONB, etc.)
- `helm/values/local/monitoring.yaml` (grepped `alerting:`/`rules.yaml`) — confirms the exact Grafana-Alerting-as-code `rules.yaml` shape D-19's new rule should follow, including the `for: 5m` time-based pattern
- `tests/e2e/slice/test_pod_kill_retry.py` — full read; direct template for D-18
- `tests/e2e/slice/test_backfill_reentry.py` (grepped) — confirms the live `airflow backfill create` invocation pattern already proven in this repo
- `.venv/lib/python3.12/site-packages/airflow/jobs/scheduler_job_runner.py` (lines 2680-2764, 4020-4035) — full read of the installed Airflow 3.3.0 package; confirms `(dag_id, backfill_id)`-scoped active-run accounting
- `.venv/lib/python3.12/site-packages/airflow/models/backfill.py` (grepped) — confirms `Backfill.max_active_runs` column, default 10
- `python3 -m airflow backfill create --help` — executed live against this repo's `.venv`; confirms `--max-active-runs`, `--reprocess-behavior {none,completed,failed}`, `--dry-run`, `--from-date`/`--to-date` flags, and the absence of any `--pool` flag
- `tools/corpus/` (`__main__.py`, `manifest.py` grepped) and `tests/fixtures/slice-corpus.yaml` — confirms the existing seed-driven fixture generation tool and its invocation convention
- `pyproject.toml` (`[tool.pytest.ini_options]`) — confirms existing pytest markers (`cluster`, `integration`, `dbt`, `dagtest`) this phase's tests should use
- `.planning/REQUIREMENTS.md`, `.planning/STATE.md`, `.planning/phases/09-.../09-CONTEXT.md`, `.planning/phases/07-.../07-CONTEXT.md` — full read

### Secondary / Tertiary
None — every claim in this document was resolved against a primary source available directly in this repository or its installed dependencies. No web search was performed or needed.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — no new dependencies; every mechanism reuses already-pinned, already-verified tooling.
- Architecture: HIGH — every pattern is a direct, verified template from existing code in this exact repository, not an inferred design.
- Pitfalls: HIGH for Pitfalls 1-5 (each traced to a specific verified code fact); MEDIUM for Pitfall 6 (the alerting-threshold recommendation is a reasoned design, not something this codebase has built before).
- Open Questions: genuinely open — window sizing (OQ1) needs an empirical `--dry-run`/pilot step during planning or execution, and the fixture-generator capability gap (OQ2) needs a direct read of `tools/corpus/generators.py`'s current generator function set before planning can commit to "reuse as-is" vs. "extend."

**Research date:** 2026-08-19
**Valid until:** 30 days (stable, in-repo mechanics; no external ecosystem drift risk since no new dependencies were introduced)
