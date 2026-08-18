# Open Research Questions

Questions surfaced during exploration/discussion that need investigation before or during
planning. Append new entries; do not delete answered ones — mark them resolved instead.

---

## Q1: Which dbt-postgres incremental strategy is actually concurrency-safe for silver-layer dedup?

**Raised:** 2026-08-18, `/gsd-explore` session on the dbt Silver Transformation Layer
(`.planning/notes/dbt-silver-layer-architecture-decision.md`).

**Status:** Open.

**Question:** dbt-postgres's `merge` incremental strategy compiles to a real PostgreSQL `MERGE`
statement, which this project already knows is not concurrency-safe under snapshot semantics
(documented PostgreSQL BUG #18279 — two concurrent `MERGE`s can both miss each other and both
attempt an insert-branch, so the loser raises a unique-violation instead of falling through to
its update-branch; this is exactly why `packages/dataplat/src/dataplat/load/publish/merge.py`
uses `INSERT ... ON CONFLICT` instead of `MERGE`). The alternative dbt offers on Postgres,
`delete+insert`, is not the same atomic single-statement conflict resolution either — it deletes
matching keys then inserts, which has its own race exposure under concurrent writers.

Before Phase 08.1 (dbt Silver Transformation Layer) is planned/executed, this needs a real
answer, matching this project's "proof over prose" standard (Phase 4/5/7 precedent — a live,
automated test that actually exercises concurrent writes, not just a read of dbt's docs):

- Does the silver layer actually need concurrent writers in practice (e.g. does
  `max_active_tis_per_dag`/dataset-level Airflow pooling already serialize writes to a given
  dataset's silver schema, making the question moot)? Check the DAG-level concurrency model
  before assuming multi-writer exposure exists at all.
- If concurrent writes to silver ARE possible: is `merge` provably unsafe under this project's
  actual DAG concurrency shape, or does the PG BUG #18279 window require conditions
  (`isolation level, timing) that don't occur under this project's `pg_advisory_xact_lock`-adjacent
  patterns?
- Is `delete+insert` safe under this project's concurrency model, or does it need its own
  advisory-lock wrapper (mirroring the existing gold-publish pattern) to be trustworthy?
- Is there a custom dbt incremental materialization that can emit `INSERT ... ON CONFLICT`
  directly for Postgres, sidestepping both built-in strategies' issues?

**Resolve via:** a dedicated spike (`/gsd:spike`) or research pass during Phase 08.1 planning,
with a live concurrent-write test as the pass criterion — not a documentation read alone.
