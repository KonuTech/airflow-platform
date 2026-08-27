---
status: accepted
date: 2026-08-28
---

# ADR-0012: Quarantined runs' bronze/silver artifacts are retained, excluded from gold, and identifiable — full silver disposition deferred

## Context and Problem Statement

ROUND 14 of the `ci-pipeline-ingestion-timeout` debug session made `QUARANTINED` a terminal
`meta.ingestion_runs` status: a publish pass that trips a deterministic quality breaker
(`QualityThresholdExceeded` — the mass-delete circuit breaker, or a rejection-rate breach)
quarantines every run of that pass, and discovery/stage/publish never re-offer those runs.
That decision correctly stopped the ROUND 11 "self-sustaining poison" shape — but it only
blocked the **pass**, not the **data**. By the time publish runs, a quarantined run's rows
have already been (a) durably staged into bronze (`staging.<dataset>`) by `stage_ingest`, and
(b) usually already materialized into silver (`silver.<dataset>`) by the `dbt_build` task that
sits between stage and publish. ROUND 15's live CI run (33103279876) measured the consequence
at scale: **3,000,000+ silver rows attributed to quarantined runs**, and the end-of-session
silver census showed corpus business keys whose lineage attribution had migrated to
quarantined runs (byte-identical replays win the deterministic `_run_id desc` tie-break).

Worse than retention: two live **gold leak paths** existed. `SCDPublisher`'s per-key recompute
reads a key's *entire* bronze history (deliberately unscoped — Finding F-1's late-correction
requirement), so any later pass touching a key folded quarantined bronze rows into the
recomputed gold chain. And `MergePublisher`/`OrdersMergePublisher` publish the *whole*
cumulative silver table, so quarantined silver rows leaked into gold on the next successful
pass. Quarantine that does not actually withhold the quarantined delivery from consumers
violates the platform's Core Value ("no data is ever silently dropped, duplicated or
corrupted" — and equally, no *withheld* data may be silently delivered).

## Decision

**Implemented now (ROUND 16, the minimal correct piece):**

1. **Gold exclusion at every publisher.** `scd.py::_BRONZE_HISTORY_SQL`,
   `merge.py::_PUBLISH_SQL` and `merge_orders.py::_PUBLISH_SQL` all gained
   `_run_id NOT IN (SELECT run_id FROM meta.ingestion_runs WHERE status = 'QUARANTINED')`.
   The `NOT IN` shape (never an inner join) means rows whose `_run_id` has no metadata row at
   all — test harnesses, per-run scratch tables — stay included by default, and an operator
   **re-opening** a quarantined run (the recorded status flip ROUND 14 named as the recovery
   path) automatically re-includes its rows with zero further mechanism.
2. **Retention, not deletion, of bronze.** Bronze rows from quarantined runs stay in
   `staging.<dataset>`. Raw is immutable (§63/ADR-0011) and bronze is the platform's durable,
   cumulative, traceable record of what was actually delivered — deleting it would erase the
   very evidence an operator needs to investigate the quarantine. Exclusion happens at the
   consumers, keyed on run status.
3. **Identifiability as a first-class object.** `meta.v_quarantined_artifacts` (migration
   0041) lists every quarantined run with its retained bronze/silver row counts, readable by
   `etl_app`/`analytics_owner`/`grafana_reader`. "Identifiable and explainable, never silently
   resident" is now a queryable fact, not a code-reading exercise.

**Deferred (recorded here, not rushed):** the **silver-layer disposition**. Silver still
retains quarantined runs' rows and the dbt dedup can still attribute a business key's silver
row to a quarantined run. A clean fix requires design work that is out of proportion to this
round:

- *Read-time exclusion in the silver models* needs `dbt_app` visibility of run status —
  today `dbt_app` deliberately has **zero** grant on `meta.ingestion_runs` (D-08's boundary).
  A narrow `SECURITY DEFINER` function or a single-purpose status view could thread that
  needle, but it must be designed against D-08's threat model, not bolted on.
- *Retro-cleanup on quarantine* (delete the run's silver rows when publish quarantines it)
  is **not** safe alone: if the quarantined row *won* the business-key dedup, deleting it
  leaves the key with no silver row while valid older bronze exists — and the
  `meta.dbt_processed_runs` claim ledger (finding 21, same round) correctly refuses to
  reprocess already-claimed older runs. Cleanup therefore requires re-materializing every
  displaced key from its remaining non-quarantined bronze — publisher-style recompute logic
  living in dbt's domain.
- The interim risk is bounded: silver's only *gold-feeding* consumers are the publishers,
  which now exclude quarantined rows; remaining exposure is silver-direct reads (lineage
  views, dashboards, `meta.dedup_audit` attribution), which is a visibility concern — exactly
  what `meta.v_quarantined_artifacts` makes explicit.

## Consequences

- Gold can no longer receive quarantined data through any publisher path; an operator status
  flip is the single lever that re-admits a delivery, matching ROUND 14's recovery design.
- Silver/gold reconciliation figures (`_compute_silver_gold_reconciliation`, dbt
  reconciliation rows) will show a persistent, *explainable* input-vs-output difference while
  quarantined silver rows are retained — `meta.v_quarantined_artifacts` is the explanation.
- The silver disposition work above should be scheduled as its own plan item; until then,
  any consumer needing "silver minus quarantined" can join against
  `meta.v_quarantined_artifacts` (or `meta.ingestion_runs.status`) explicitly.
