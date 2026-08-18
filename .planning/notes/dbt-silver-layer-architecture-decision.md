---
title: dbt Silver Transformation Layer — architecture decision
date: 2026-08-18
context: /gsd-explore session, launched from a pause mid-way through /gsd:discuss-phase 9
---

# dbt Silver Transformation Layer — architecture decision

## What triggered this

Discussing Phase 9 (ETL Correctness — Dedup, Incremental, Backfill & Recovery), the user raised
a medallion architecture (bronze/silver/gold) proposal while answering a question about late/
out-of-order data — motivated by positive prior experience using dbt for deduplication and
late-arriving-event handling. This directly collided with a decision already locked at the
project level: `PROJECT.md`'s Key Decisions table and `REQUIREMENTS.md`'s Out of Scope table
both excluded dbt ("adding dbt would fork the transformation story into two paradigms"). Rather
than decide that reversal as a side effect of Phase 9 discussion, the Phase 9 session was paused
(`.planning/phases/09-etl-correctness-dedup-incremental-backfill-recovery/09-DISCUSS-CHECKPOINT.json`)
and this dedicated exploration ran instead.

## Conclusion

**dbt is back in scope, narrowly:** it owns bronze → silver only. Bronze (raw CSVs on MinIO,
already exists, unchanged) and gold (`normalized.*` in the analytical Postgres, published by the
existing Python `MergePublisher`) are untouched. dbt's job is the space between: cleaning,
deduplication, and late-arriving-event resolution, landing in a new, persisted **silver schema in
the same analytical Postgres** — not on MinIO, not via DuckDB.

This reverses `PROJECT.md`'s blanket "dbt excluded" Key Decision, but only for this narrow slice.

## Reasoning, in the order it was established

1. **dbt Core is genuinely open source in 2026.** dbt Core v2.0 (alpha June 2026) is built on the
   Fusion engine, fully Apache 2.0. The user's assumption was correct — this wasn't a licensing
   concern to begin with, but worth confirming since it's foundational to using it at all.

2. **A true bronze/silver/gold-on-MinIO design was considered and rejected.** `dbt-duckdb` can
   genuinely read/write CSV/Parquet on S3-compatible storage (MinIO qualifies) and can `ATTACH` a
   live PostgreSQL database to write gold directly — the three-layer-on-object-storage pattern is
   technically buildable. But best-practice sources are consistent that production-grade silver
   layers doing dedup/merge need an open table format (Delta Lake / Iceberg / Hudi) underneath for
   ACID guarantees and safe concurrent writes — plain CSV/Parquet on object storage doesn't give
   you that. That's a new storage format, likely a new catalog, and DuckDB as a third compute
   engine (alongside the existing Python `dataplat` library and PostgreSQL) — a large infrastructure
   addition for a platform whose core value proposition is a *simple, traceable, provably correct*
   pipeline, not an object-storage lakehouse. The user agreed: silver goes into Postgres, "just as
   for gold."

3. **dbt-postgres's `merge` incremental strategy compiles to real PostgreSQL `MERGE`.** This is the
   load-bearing finding. This project's own `MergePublisher` (Phase 4,
   `packages/dataplat/src/dataplat/load/publish/merge.py`) deliberately avoids SQL `MERGE` and uses
   `INSERT ... ON CONFLICT` instead, because of a **documented, verified PostgreSQL bug (BUG #18279)**:
   two concurrent `MERGE` transactions can each decide independently — against their own snapshot —
   that no matching row exists, and both attempt an insert, so the loser raises a unique-violation
   instead of falling through to its update branch. dbt's `merge` strategy walks straight into the
   same bug if it's ever used on a concurrently-written table. `delete+insert` (dbt's other Postgres
   incremental strategy) is not equivalent — it deletes matching keys then inserts, which is not the
   same atomic single-statement conflict resolution `ON CONFLICT` gives.

4. **META-03 (already built, live-proven at 10M rows) requires one transaction.** Data rows,
   watermark advance, and run-status update commit together or not at all — this is what makes
   idempotency, watermark correctness, and log-free recovery a single mechanism instead of three
   independent, racy ones. dbt manages its own connections/transactions per model; there is no
   supported way to make dbt's writes execute inside a transaction that Airflow's Python code also
   holds open for the watermark/status write. Letting dbt own the *gold* publish would force a choice
   between (a) a weaker two-step commit (dbt commits data, a separate step commits watermark+status —
   reintroducing exactly the crash-window race Phase 4 eliminated) or (b) hacking dbt internals to
   share a connection, unsupported and fragile. Keeping dbt scoped to silver sidesteps this entirely:
   the existing, already-proven Python publish path is untouched.

5. **The boundary is architecturally idiomatic for dbt, not a workaround.** dbt's own
   staging → intermediate → marts convention already *is* a bronze/silver/gold shape under different
   names. "Staging" models are explicitly meant to be clean, standardized, testable, *reusable*
   output — dbt has no convention against a non-dbt consumer reading that output. Stopping dbt at
   silver is a legitimate, common pattern.

6. **Least-privilege fits directly.** The standard dbt-on-Postgres pattern is a dedicated `dbt` role
   with read access to source/bronze-landing schemas and write access only to its own (silver)
   schema — this slots into the existing role-separation discipline from Phases 2/5/7
   (`etl_app`, `analytics_owner`, `grafana_reader`) as one more role, not a new privilege model.

7. **Orchestration is a solved problem either way.** Astronomer Cosmos renders each dbt model as its
   own Airflow task (per-model retries, observability — a nice conceptual fit with this project's
   per-file KubernetesPodOperator granularity). A plain `KubernetesPodOperator` running `dbt build`
   is the lower-ceremony alternative, closer to the existing pattern. Not decided here — planning's
   job.

8. **One honest gap: lineage stops at dbt's boundary.** dbt's own docs/lineage graph won't show
   "this gold row came from this silver row from this bronze file" once a non-dbt step (the Python
   publisher) takes over. This is fine *only* because the project already treats its own
   SQL-queryable `meta.*` lineage (OBS-07, `meta.v_customers_lineage`) as the source of truth rather
   than dbt's docs — but the lineage view needs to explicitly bridge the dbt-owned/Python-owned
   boundary, not assume dbt gives that for free.

9. **The same boundary logic was tested against Phase 10's SCD2 and held.** `dbt snapshot` looked
   like an appealing shortcut for SCD Type 2 (it natively tracks `valid_from`/`valid_to`/`is_current`).
   But dbt Labs' own documentation states snapshots are **not a replacement for CDC or event
   streaming** — they work by periodically re-diffing a source table's current state, not by
   consuming an ordered CDC event stream with explicit operation/sequence/transaction-ID semantics,
   which is exactly what CDC-01/CDC-02/SCD-08 require. Snapshots also don't protect against partial
   ingestion states on their own — orchestration has to guarantee that, the same transactional
   problem as gold. SCD2's historized dimension tables (surrogate keys, the database-enforced
   non-overlap `EXCLUDE` constraint, late-arriving corrections by recomputing history from an
   ordered event log per SCD-07) are gold-layer, transactionally significant work — the same
   reasoning that kept Phase 9's gold Python-owned applies symmetrically. **Conclusion: Phase 10's
   SCD2 stays Python-owned; dbt is not used there either.**

## What this changes going forward

- A new phase, **08.1: dbt Silver Transformation Layer**, is inserted between Phase 8 and Phase 9
  in `ROADMAP.md` (decimal/urgent-insertion numbering, per this project's convention). It is not
  yet planned — `/gsd-plan-phase 08.1` (after a `/gsd:discuss-phase 08.1` if more implementation
  gray areas need surfacing) breaks it down.
- **Phase 9's paused dedup-strategy decision needs revisiting.** The Phase 9 discussion (before it
  was paused) had already locked "a new pre-publish Python `DeduplicationStage`" as the dedup
  mechanism (see `09-DISCUSS-CHECKPOINT.json`, area "Deduplication strategy & audit"). Once 08.1's
  real silver-schema shape exists, that decision needs to be reconciled: dedup may move fully into
  dbt models (silver *is* the deduplicated layer), or the Python stage may survive as a secondary
  safety net operating on dbt's already-deduplicated silver output (matching the layered
  business-key-primary/hash-secondary defense-in-depth the user asked for during Phase 9
  discussion — that reasoning doesn't disappear, it just gets a new home).
- A follow-up ADR should formalize this, superseding `PROJECT.md`'s original "dbt excluded" Key
  Decision and `REQUIREMENTS.md`'s "dbt or an external transformation framework" Out-of-Scope entry
  — tracked as a todo.
- The concurrency safety of whichever dbt-postgres incremental strategy actually gets used for
  silver-layer dedup (given `merge`'s `MERGE`-bug exposure) needs live proof before being trusted —
  tracked as a research question.

## What did NOT change

- Bronze ingestion (Phases 1–8: CSV parsing, encoding/dialect detection, schema contracts, file
  integrity, quarantine) is entirely untouched — dbt never touches raw file ingestion.
- The existing Python `MergePublisher`/`OrdersMergePublisher` gold-publish path, the
  `pg_advisory_xact_lock` + `INSERT ... ON CONFLICT` mechanism, and META-03's single-transaction
  guarantee are all unchanged.
- Phase 10's SCD2 design stays Python-owned; `dbt snapshot` is explicitly rejected there too, for
  the same structural reason as gold in Phase 9.
