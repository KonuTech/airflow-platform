---
status: accepted
date: 2026-08-18
---

# ADR-0010: dbt owns bronze-to-silver transformation only — gold publish and SCD2 stay Python-owned

## Context and Problem Statement

`PROJECT.md`'s Key Decisions table and `REQUIREMENTS.md`'s Out of Scope table both currently
record a blanket exclusion of dbt: "README §36/§54–61 model transformation and SCD logic in the
Python layer; adding dbt would fork the transformation story across two paradigms." That decision
was reopened mid-way through discussing Phase 9 (ETL Correctness), when the user raised a
medallion (bronze/silver/gold) architecture proposal motivated by positive prior experience using
dbt for deduplication and late-arriving-event handling. Rather than decide the reversal as a side
effect of Phase 9 planning, a dedicated exploration session ran instead
(`.planning/notes/dbt-silver-layer-architecture-decision.md`), and Phase 08.1 (this phase) exists
to implement its conclusion. This record formalizes that conclusion as a permanent architecture
decision so every later plan in this phase — and any future phase touching transformation — can
cite it instead of re-deriving the reasoning.

The exploration's chain of reasoning, condensed: dbt Core is genuinely open source in 2026 (v2.0,
Apache 2.0), so licensing was never the blocker. A true bronze/silver/gold-on-MinIO design (via
`dbt-duckdb`) was considered and rejected — production-grade dedup/merge on object storage needs
an open table format (Delta/Iceberg/Hudi) for ACID guarantees that plain CSV/Parquet don't provide,
which would add a new storage format, a new catalog, and DuckDB as a third compute engine, for a
platform whose value proposition is a simple, traceable, provably correct pipeline. Silver
therefore lands in the same analytical PostgreSQL gold already lives in.

The load-bearing finding is that dbt-postgres's `merge` incremental strategy compiles to literal
PostgreSQL `MERGE` — and this project's own `MergePublisher` (Phase 4) deliberately avoids SQL
`MERGE` in favor of `INSERT ... ON CONFLICT` because of a documented, verified PostgreSQL
concurrency bug (**BUG #18279**): two concurrent `MERGE` transactions can each decide, against
their own snapshot, that no matching row exists, and both attempt an insert — the loser then
raises a unique-violation instead of falling through to its update branch. Letting dbt own gold
would walk straight into the same bug on a concurrently-written table. Separately, **META-03**
(already built and live-proven at 10M-row scale) requires that data rows, watermark advancement
and run-status update commit inside a single transaction — dbt manages its own per-model
connections/transactions with no supported way to share that transaction with Airflow's Python
code, so letting dbt own gold would force either a weaker two-step commit (reintroducing the
crash-window race Phase 4 eliminated) or an unsupported hack against dbt internals.

## Considered Options

* **A — Keep dbt fully excluded (status quo).** No change; transformation and SCD stay entirely
  Python-owned, as `PROJECT.md`'s original Key Decision states.
* **B — dbt owns bronze→silver→gold end-to-end.** dbt reads raw bronze data and writes all the way
  through to the published `normalized.*` gold tables, replacing the existing Python publish path.
* **C — dbt owns bronze→silver only; gold publish stays the existing Python `MergePublisher`
  (chosen).** dbt cleans, deduplicates and resolves late-arriving events into a new, persisted
  silver schema in the same analytical PostgreSQL; the already-proven `INSERT ... ON CONFLICT`
  gold-publish path and META-03's single-transaction guarantee are untouched.

## Decision Outcome

Chosen option: **C**, because dbt's `merge` incremental strategy compiles to literal PostgreSQL
`MERGE` — the same concurrency hazard (PG BUG #18279) `MergePublisher` was already built to avoid
— and because dbt's own per-model transactions cannot participate in META-03's single-transaction
guarantee. Option A leaves genuine value (dbt's purpose-built dedup/late-arrival handling) off the
table for no remaining reason once the licensing and storage-format concerns are resolved. Option
B would either reintroduce BUG #18279's exposure or force gold onto `delete+insert` (not
equivalent to `ON CONFLICT`'s atomic single-statement resolution) and would still break META-03's
transactional guarantee. Option C is also architecturally idiomatic for dbt, not a workaround:
dbt's own staging → intermediate → marts convention already is a bronze/silver/gold shape under
different names, and stopping a non-dbt consumer at dbt's staging/silver output is a legitimate,
common pattern.

### Consequences

* Good, because deduplication and late-arrival-resolution logic moves into a purpose-built tool
  (dbt's incremental models, tests, and `merge`/`delete+insert` strategies operating only on the
  non-concurrently-written silver schema) instead of a hand-rolled Python stage having to
  re-implement the same logic from scratch.
* Good, because least-privilege role separation slots directly into the existing
  `etl_app`/`analytics_owner`/`grafana_reader` pattern (Phases 2/5/7) — a dedicated `dbt` role with
  read access to bronze-landing schemas and write access only to its own silver schema is one more
  role in an already-established discipline, not a new privilege model.
* Bad, because this decision adds a new image, a new role, and a new credential-delivery path
  (dbt's Postgres connection) to the platform's operational surface — costs this record should
  name honestly rather than presenting the decision as free.
* Bad, because lineage now has an explicit hop to bridge: dbt's own docs/lineage graph stops at
  its boundary and won't show "this gold row came from this silver row from this bronze file" once
  the non-dbt Python publisher takes over. This is acceptable only because the project already
  treats its own SQL-queryable `meta.*` lineage (`meta.v_customers_lineage`, OBS-07) as the source
  of truth rather than dbt's docs — but the lineage view must explicitly bridge the
  dbt-owned/Python-owned boundary, not assume dbt provides that for free.
* Neutral, because Phase 10's SCD2 design was tested against this exact boundary logic and it
  held: `dbt snapshot` looked like a shortcut for SCD Type 2, but dbt Labs' own documentation
  states snapshots are not a replacement for CDC or event streaming — they re-diff a source
  table's current state rather than consuming an ordered CDC event stream with explicit
  operation/sequence/transaction-ID semantics (CDC-01/CDC-02/SCD-08), and they don't independently
  protect against partial-ingestion states. SCD2 stays Python-owned for the same structural reason
  gold does, which means this decision's boundary logic already generalizes rather than needing a
  second, unrelated decision at Phase 10.

## Migration trigger

Not "none" — either of the following is a concrete, observable reason to revisit this boundary:

* **dbt-postgres ships an incremental strategy proven safe under PostgreSQL's snapshot semantics**
  for concurrently-written tables — i.e., a strategy that does not exhibit PG BUG #18279's failure
  mode and can participate in (or be made durably equivalent to) a single-transaction commit
  alongside a watermark/status write. If that becomes true, extending dbt's ownership from silver
  into gold becomes worth re-evaluating.
* **A future phase needs dbt to own gold too** — for example, if the platform's gold layer grows
  requirements (complex multi-source joins, wide test coverage across many marts) that make the
  hand-rolled Python `MergePublisher` genuinely harder to maintain than a dbt model would be, and
  the concurrency/transaction concerns above have been independently resolved or accepted.

## References

* README §4.2, §25–27, §32–36, §60–61, §68, §75, §81.6, §83
* `.planning/research/SUMMARY.md`
* `.planning/phases/08.1-dbt-silver-transformation-layer-dbt-postgres-adapter-owns-br/08.1-RESEARCH.md`
* `.planning/notes/dbt-silver-layer-architecture-decision.md` — the exploration session this
  record formalizes
* `packages/dataplat/src/dataplat/load/publish/merge.py` — the existing `MergePublisher`,
  deliberately avoiding SQL `MERGE` for PG BUG #18279
* `docs/adr/0008-pipeline-composition-seam.md` — the `Source`/`Publisher` protocol seam this
  decision's Python-owned gold path continues to implement against
* PostgreSQL BUG #18279 — concurrent `MERGE` unique-violation-instead-of-update race
* META-03 — single-transaction publication guarantee (`.planning/REQUIREMENTS.md`)
