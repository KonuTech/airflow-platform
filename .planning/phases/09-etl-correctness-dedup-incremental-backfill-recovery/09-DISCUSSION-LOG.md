# Phase 9: ETL Correctness — Dedup, Incremental, Backfill & Recovery - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-08-19
**Phase:** 9-ETL Correctness — Dedup, Incremental, Backfill & Recovery
**Areas discussed:** Watermark purpose & strategy, "Historical partition" reconciliation
(incl. backfill mechanics), Recovery & checkpoint scope across 3 stages, Reconciliation shape
(VALID-05/06), Concurrency, Cleanup & dbt implementation nuance, Test-tier placement

**Note on session history:** This phase had a prior discussion checkpoint from 2026-08-18 that was
explicitly discarded at the start of this session (user chose "Start fresh") because Phase 08.1
(dbt Silver Transformation Layer) shipped in the interim and remapped DEDUP-01..04/INCR-03/04/
QUAL-10 entirely out of Phase 9's scope. See `09-CONTEXT.md`'s `<domain>` section for the full
scope-change explanation.

---

## Watermark purpose & strategy

| Question | Options | Selected |
|---|---|---|
| What should the INCR-01/02 watermark actually DO, given file-level idempotency already prevents reprocessing? | Observational only (recommended) / Active gate / You decide | ✓ Observational only |
| Which watermark strategy? | EVENT_TIMESTAMP (recommended) / MONOTONIC_ID / You decide | ✓ EVENT_TIMESTAMP |
| target_key granularity? | Single 'default' per dataset (recommended) / Per-source/country / You decide | ✓ Single 'default' |
| Build meta.watermark_history now? | Build it now (recommended) / Defer / You decide | ✓ Build it now |

**Notes:** Both datasets already have a business-time column used elsewhere for ordering
(customers.event_ts, orders.order_date) — EVENT_TIMESTAMP reuses this rather than inventing a
second notion of "business time." dbt's own internal silver-model incremental cursor (08.1
D-05/D-06) is `_run_id`-based and is a separate, unrelated mechanism from this dataset-level
watermark.

---

## "Historical partition" reconciliation (backfill mechanics)

| Question | Options | Selected |
|---|---|---|
| What does "correct historical partition" mean given 08.1 rejected physical partitioning? | Logical correctness, not physical partitioning (recommended) / Revisit physical partitioning / You decide | ✓ Logical correctness |
| Missing file in backfill window? | Explicit gap record, run continues (recommended) / Fail the whole backfill / You decide | ✓ Explicit gap record |
| Historical schema versioning — new work or proof only? | Proof/test only (recommended) / New capability needed / You decide | ✓ Proof/test only |
| Close silver.orders 0-rows gap (08.1-VERIFICATION.md)? | Yes, fold it in (recommended) / No, separate task / You decide | ✓ Yes, fold it in |
| 2-year backfill window — real span or shorter? | Shorter representative window (recommended) / Genuinely 2 years / You decide | ✓ **Genuinely 2 years** (against recommendation) |
| What should the fixture span contain? | Cadence + schema change + gap (recommended) / Just volume/cadence / You decide | ✓ Cadence + schema change + gap + late event |
| Backfill trigger tooling vs. native command? | Native command only (recommended) / Thin wrapper / You decide | ✓ Native command, PLUS a hard DAG-refactor requirement (see below) |

**Notes:** The last question's answer carried a major user-added clarification, confirmed twice
across two follow-up turns: the current `csv_ingest_customers`/`csv_ingest_orders` DAGs must
genuinely support backfill across schema evolution, late events, incremental watermarks, and
dedup TOGETHER — refactor if the current structure doesn't already do this. DAGs must handle past
(backfill), current, and future batches through the SAME structure. Also surfaced during this
area: `airflow dags backfill` does not exist in this cluster's Airflow 3.3.0 — the real command is
`airflow backfill create --dag-id ... --from-date ... --to-date ... --reprocess-behavior
completed` (from `08.1-13-SUMMARY.md`). Also surfaced: `discover_files` already re-scans the whole
bucket regardless of triggering window — logged as a research flag, not assumed to mean no
refactor is needed.

---

## Recovery & checkpoint scope across 3 stages

| Question | Options | Selected |
|---|---|---|
| Should dbt_build's status be queryable via meta.run_stages too? | Yes — 3rd entry (recommended) / No — dbt's own history / You decide | ✓ Yes |
| Genuine ROLLBACK case, or retry only? | Retry only (recommended) / Real rollback path needed / You decide | ✓ Retry only |
| How to surface "what succeeded, what remains"? | SQL view (recommended) / Raw query only / You decide | ✓ SQL view |
| Lease reclaim — automatic for all 3, or operator confirmation for any? | Fully automatic (recommended) / Publish requires confirmation / You decide | ✓ Fully automatic |
| Extend pod-kill testing to dbt_build? | Yes (recommended) / No, dbt's guarantees sufficient / You decide | ✓ Yes |
| Exhausted-retry alerting? | Reuse Phase 7's alert path (recommended) / SQL view only / You decide | ✓ Reuse Phase 7's path |

---

## Reconciliation shape (VALID-05/06)

| Question | Options | Selected |
|---|---|---|
| Source/target definition given raw→bronze→silver→gold? | Raw vs. gold, accounting for drops (recommended) / Each hop separately / You decide | ✓ **Each hop separately** (against recommendation) |
| Where should each hop's check run? | Inline with owning stage (recommended) / Separate RECONCILE stage / You decide | ✓ Inline with owning stage |
| Discrepancy handling? | Block, record in meta (recommended) / Record and continue, never block / You decide | ✓ Record and continue |
| Source-provided control total carrier? | Extend _BATCH_COMPLETE manifest (recommended) / CSV trailer/footer row / You decide | ✓ Extend manifest |
| reconciliation_results grain? | Per file, per hop (recommended) / Per run, aggregated / You decide | ✓ Per file, per hop |
| Sum check dataset-conditional? | Dataset-conditional, config-declared (recommended) / Require every dataset / You decide | ✓ Dataset-conditional |

**Notes:** The discrepancy-handling answer carried a critical user-added accounting rule
(paraphrased: "remember we put invalid rows into a dedicated invalid table"): input count must be
compared against (output count + rows routed to meta.rejected_records for that hop), not a naive
raw comparison — netting out known/legitimate reductions (quarantine, dedup) before flagging a
genuine discrepancy. Confirmed explicitly in a follow-up turn.

---

## Concurrency (backfill parallelism, live+backfill overlap)

| Question | Options | Selected |
|---|---|---|
| Backfill DagRuns — parallel or sequential? | Sequential (recommended) / Parallel / You decide | ✓ **Bounded/limited parallelism as default, sequential fallback** (user's own framing, not a listed option verbatim) |
| Live ingestion concurrent with historical backfill, same dataset — in scope? | Out of scope (recommended) / In scope, must be provably safe / You decide | ✓ **In scope** (against recommendation) |

**Notes:** Both answers were given in the user's own words rather than picking a listed option
verbatim ("Limited Parallel should be default if my laptop survives it... if we wont managed to
run parallel, lets do sequential"; "max runs of Airflow should be able to deal with it, if I am
not mistaken") — both were reflected back and explicitly confirmed in a follow-up turn before
being locked into CONTEXT.md as D-12/D-13.

---

## Cleanup & dbt implementation nuance

| Question | Options | Selected |
|---|---|---|
| Remove vestigial deduplication: block in customers.yaml/orders.yaml? | Yes, remove (recommended) / Leave it / You decide | ✓ Yes, remove |
| Bronze→silver reconciliation check: native dbt test or custom macro? | Custom macro (recommended) / Native dbt test (severity: warn) / — | ✓ **Both** — user asked "would it make sense to have both?", Claude assessed yes (low marginal cost, consistent with the project's layered-defense pattern), user confirmed |

---

## Test-tier placement

| Question | Options | Selected |
|---|---|---|
| Full 2-year sweep on live cluster, or testcontainers + subset live? | Testcontainers + subset live (recommended) | Live cluster too | You decide | ✓ **Live-first, testcontainers-fallback** — user's own framing ("As first Option 2. if that wont work Option 1"), confirmed explicitly |

---

## Claude's Discretion

None. Every question in this discussion was answered with a specific choice — no "you decide"
deferrals remain open. This supersedes the discarded 2026-08-18 checkpoint, which had left both
of its watermark questions to Claude's discretion.

## Deferred Ideas

None raised that belong to a different phase. The DEDUP-related work that was originally paused
mid-discussion on 2026-08-18 was resolved by inserting and completing Phase 08.1 before this
session began — not deferred by this session.
