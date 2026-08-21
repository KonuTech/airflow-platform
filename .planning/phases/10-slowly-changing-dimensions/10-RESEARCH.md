# Phase 10: Slowly Changing Dimensions - Research

**Researched:** 2026-08-21
**Domain:** PostgreSQL SCD Type 2 (exclusion constraints, temporal recomputation), dbt/Python read-path boundary
**Confidence:** MEDIUM-HIGH — the DDL/constraint mechanics are HIGH confidence (verified against PostgreSQL docs, Alembic docs, this repo's own migrations); the **read-path for late-arriving corrections** is a genuinely open design question this research resolves with a concrete, code-grounded recommendation, not a verified fact — flagged accordingly throughout.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** SCD2 applies to `customers` only. `orders` stays out of SCD scope entirely.
- **D-02:** Column treatment — `customer_id` business key (untracked); `name`/`country` **Type 2**; `birth_date` **Type 1** (overwrite); `event_ts` not itself tracked, it's the effective-date source (D-03), excluded from the change hash exactly as it's excluded from dedup's business columns today.
- **D-03:** `valid_from` **reuses `customers.yaml`'s existing `event_ts` column** — the same column driving Phase 9's watermark and `MergePublisher`'s late-arrival guard. One date concept, not two.
- **D-04:** `source.change_semantics: "snapshot"` (declared since Phase 4, zero code consumers today — grep-verified) becomes meaningful for the first time. Each `customers.csv` is a full point-in-time roster; the SCD Publisher compares **this run's** set of `customer_id`s (post-dedup, from silver) against gold's currently-`is_current=true` set. Absent = DELETE trigger.
- **D-05:** DELETE semantics default for `customers.yaml` is **`invalidate`** (close out `valid_to`/`is_current=false`, preserve history).
- **D-06 (LOCKED):** A configurable **mass-delete circuit breaker** guards `invalidate` — if the fraction of currently-current customers absent from a new snapshot exceeds a configurable threshold (Phase 8's 10% is a starting reference, not locked), the run **FAILs loudly**. Direct precedent: `RejectionRateCircuitBreaker` (`validate/circuit_breaker.py`) — same "count vs. threshold, fail the run" shape, reused as the implementation template.
  - Real fixtures required: (1) a snapshot genuinely missing a previously-present customer (`invalidate` path), (2) a separate deliberately-bad snapshot missing an implausible fraction (circuit-breaker trip). Whether fixture (2) folds into the D-11 corpus or stays a separate small test is Claude's discretion.
- **D-07 (LOCKED, user chose the higher-risk option against recommendation):** `normalized.customers` **migrates in place** to SCD2 shape. `UNIQUE(customer_id)` (migration 0006) is dropped, replaced by a `btree_gist` exclusion constraint on `(customer_id, validity range)`. Existing rows backfill as each customer's first SCD2 version (`valid_from` = earliest known `event_ts`, `is_current=true`, `valid_to` = the sentinel).
- **D-08 (LOCKED, follows from D-07):** Every existing consumer of `normalized.customers`'s "one row per customer" assumption must be found, updated, and live-proven **in this phase**. Known-from-context list (research must verify it's exhaustive, not assumed complete — **research found it is NOT exhaustive, see Finding F-1 below**):
  1. `meta.v_customers_lineage` (migrations 0012/0026/0030) — needs `is_current=true` filter or equivalent.
  2. Phase 9's silver→gold reconciliation (`pipeline/run.py::_compute_silver_gold_reconciliation`, `metadata/repository.py::record_reconciliation`) — must account for multi-row-per-key gold cardinality.
  3. `MergePublisher` is **replaced**, not a "consumer to patch."
  - Grafana dashboards verified NOT a consumer (query only `meta.ingestion_runs`).
- **Verified non-issue:** `orders.customer_id` has no DB-level FK to `normalized.customers` (migration 0016 docstring) — dropping `UNIQUE(customer_id)` breaks no FK. `ReferentialIntegrityBarrier` is `EXISTS`-based, cardinality-agnostic, needs no change.
- **D-09:** Surrogate key stays **`BigInteger` + `Identity(always=True)`**, matching every existing table — NOT `uuidv7()`. Supersedes STACK.md's original recommendation for codebase consistency.
- **D-10 (LOCKED):** A **dedicated concurrent-SCD-publish test** (live attribute change racing a backfill/correction for the same `customer_id`) is required, not just inherited from Phase 9's `pg_advisory_xact_lock` proof — because SCD2's exclusion-constraint-plus-recompute logic is genuinely new code, not a reuse of `MergePublisher`'s upsert path.
- **D-11 (LOCKED):** The late-correction/idempotent-replay proof **extends Phase 9's existing 2-year backfill corpus** (`tests/e2e/slice/test_backfill_2year_sweep.py`) with genuine attribute-change events and at least one late/out-of-order correction landing between two already-published SCD2 versions — not a new dedicated fixture set.
- **D-12:** The idempotent-replay proof re-runs the **entire 2-year backfill a second time** and asserts SCD2 version count is unchanged.

### Claude's Discretion

- Exact validity-range PostgreSQL type/representation and the literal `valid_to_current` sentinel value.
- Exact mass-delete circuit-breaker threshold value (10% is a reference, not locked).
- Which dbt-vocabulary change-detection strategy (`timestamp` vs `check`) drives `customers.yaml`'s hash comparison.
- Whether the D-06 mass-delete fixture folds into the D-11 corpus or stays separate.
- How SCD2 versioning behaves across the 2-year corpus's existing deliberate schema-version change (Phase 9 D-10) — research must verify correctness here.
- Exact task/module shape for the SCD Publisher (naming left to planning; follows the `Publisher` registry pattern).

### Deferred Ideas (OUT OF SCOPE)

None specific to Phase 10 — CDC's exclusion was handled as a prior roadmap/requirements edit, not a deferred idea within this phase's discussion.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| SCD-01 | SCD Type 0 retains original values | `birth_date`... wait, SCD-01 is Type 0, not tracked at all in D-02's column list — see Open Question 1. Pattern 1/Code Examples show the column-treatment dispatch table that must include a genuine Type-0 example, not just Type-1/Type-2. |
| SCD-02 | SCD Type 1 overwrites without history | `birth_date` (D-02). Pattern 1 (recompute SQL) shows Type-1 columns are overwritten on the CURRENT version row in place, never versioned. |
| SCD-03 | SCD Type 2 maintains `valid_from`/`valid_to`/`is_current` | Pattern 1 (DDL) + Pattern 2 (recompute). `event_ts` doubles as `valid_from` per D-03 — see Finding F-3. |
| SCD-04 | Business/surrogate keys distinct; surrogate independent of change hash | D-09 (`BigInteger`+`Identity`) already satisfies this structurally — the identity sequence has zero relationship to `_record_hash`. |
| SCD-05 | Deterministic change detection via normalized hash | Reuses `dataplat.normalize.unicode` (NFC) + `_record_hash`/`_record_hash_version` (META-02 precedent, migration 0005). See Open Question 2 (`timestamp` vs `check` strategy). |
| SCD-06 | Effective dating never defaults to ingestion time | D-03 structurally satisfies this — `event_ts` is the sole date concept. |
| SCD-07 | Late-arriving changes correct historical intervals via recompute, not surgery | **Finding F-1/F-2** (this document's central finding) — resolves where the "ordered batch history" actually lives. |
| SCD-08 | Configurable DELETE semantics (`ignore \| invalidate \| new_record`) | D-04/D-05/D-06 + Finding F-2 (run-scoping the snapshot read). |
| SCD-09 | Repeated/replayed identical events → exactly one logical version | Pattern 2's recompute is naturally idempotent at the result level (DELETE+INSERT of a deterministically-derived chain). |
| SCD-10 | SCD processing idempotent under re-application | Same as SCD-09 — D-12's full-corpus-rerun proof. |
| SCD-11 | Backfills supported without blindly overwriting current state | Pattern 2's per-key recompute is backfill-safe by construction (it reads the full ordered history, not "whatever arrived most recently"). |
| SCD-12 | `btree_gist` exclusion constraint in the creating migration | Pattern 1 (Code Examples) — exact DDL. |
| QUAL-14 | SCD tested incl. late corrections + idempotent re-application | Validation Architecture section; D-11/D-12's extension of `test_backfill_2year_sweep.py`. |
</phase_requirements>

## Summary

Phase 10 replaces `MergePublisher` with a new, Python-owned **SCD Publisher** that turns `normalized.customers` into a true SCD2 dimension, migrated in place per D-07. The DDL mechanics (PostgreSQL `btree_gist` exclusion constraint, a generated `tstzrange` validity column, a far-future `valid_to_current` sentinel) are well-documented, low-risk, and directly precedented by this repo's own Alembic conventions — Pattern 1 below is a hand-written migration in the exact style of migrations 0005/0006/0023.

The genuinely hard problem — and the one ROADMAP.md flagged as still-open — is **where the SCD Publisher reads its "ordered batch history" from** for SCD-07's late-arriving-correction recompute. Direct code inspection (`dbt/models/silver/silver_customers.sql`) proves `silver.customers` is **not** an append-only history: dbt's own `delete+insert`/`unique_key=customer_id` incremental strategy collapses it to exactly one row per business key — the current highest-`event_ts` winner — and a late-arriving row with an *older* `event_ts` than what's already resident is **deduplicated away by dbt before gold ever sees it** (classified `SUPERSEDED_BY_NEWER` in `meta.dedup_decisions`). This means a naive SCD Publisher wired the same way `MergePublisher` is (reading `silver.<dataset>`) can never observe the late correction SCD-07 requires it to handle. The durable, cumulative, never-truncated bronze table (`staging.customers`, migration 0022) is the actual "ordered batch history" and must be the recompute source. This is documented in Finding F-1 with a concrete, prescriptive recommendation.

A second, closely related finding (F-2) is that D-04's DELETE-detection snapshot diff must scope its `silver.customers` read to **only this pass's newly-staged `run_id`s** (`WHERE _run_id = ANY(staged_run_ids)`, mirroring `record_watermark`'s own established scoping) — a whole-table read (the pattern `MergePublisher`'s `_PUBLISH_SQL` and `_compute_silver_gold_reconciliation` both currently use) would make DELETE-detection permanently vacuous, because `silver.customers` retains every customer ever seen forever (dbt's incremental model never deletes a business key it stops seeing).

A third finding (F-3) is that this repo already has TWO precedent patterns for "current/historical" data (`meta.config_versions`/`meta.schema_versions`'s `valid_from`/`valid_to IS NULL` + partial unique index), and that pattern is **explicitly insufficient** for true SCD2 with late corrections (it has no overlap protection and no recompute mechanism) — the plan must not accidentally reuse it verbatim.

A fourth finding (F-4) is that the "must find every consumer" mandate (D-08) undercounts the real blast radius: a grep across `tests/` shows at least 9 test files directly querying/asserting against `normalized.customers`'s one-row-per-key shape, including one file (`test_publish_merge.py`) that is now **entirely obsolete** (it tests `MergePublisher` directly, including an assertion that the dropped `UNIQUE` constraint must exist) and several (`test_publish_ingest.py`, `test_run_ingest.py`, `test_reconciliation.py`, `test_backfill_2year_sweep.py`) with row-count/dedup assertions that will silently produce false failures — or worse, false passes — once `normalized.customers` legitimately holds multiple rows per `customer_id`.

**Primary recommendation:** Build the SCD Publisher as a new module (e.g. `load/publish/scd.py`) registered as `"scd"` in `PUBLISHER_REGISTRY`, targeting `normalized.customers`. On each `publish_ingest` pass: (1) run D-04's DELETE-detection sweep against `silver.customers` scoped to this pass's `staged_run_ids`, applying `invalidate` + the mass-delete circuit breaker; (2) for every `customer_id` touched by this pass (new bronze rows in `staging.customers` since the SCD Publisher's own self-derived watermark, mirroring `dedup_audit_post_hook.sql`'s independent-watermark trick), recompute that key's ENTIRE version chain by reading **all** of that key's rows from `staging.customers` ordered by `event_ts`, and replace it with a single-transaction `DELETE ... WHERE customer_id = %s` + `INSERT` of the freshly recomputed chain, guarded by the same `pg_advisory_xact_lock` LOAD-09 already uses and backstopped by the exclusion constraint.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| SCD2 version-chain recomputation | API/Backend (`dataplat` Python library) | Database (PostgreSQL, exclusion constraint as backstop) | Explicitly Python-owned per Phase 08.1's precedent (dbt transactions can't join META-03's single-transaction guarantee); DB enforces the invariant Python must never violate, never the other way around. |
| Overlapping-interval prevention | Database (PostgreSQL `EXCLUDE USING gist`) | — | Must be enforced at the DB layer per SCD-12/PITFALLS #2 — a constraint that lives only in application code is not a constraint. |
| Bronze deduplication (exact-row, business-key-latest) | API/Backend (dbt, via `dbt_app`) | — | Unchanged — Phase 08.1's boundary. The SCD Publisher does NOT re-implement this; it reads bronze's raw history for a *different* purpose (full chronological reconstruction, not "pick one winner"). |
| DELETE-semantics snapshot diff | API/Backend (`dataplat`, SCD Publisher) | Database (read against `silver.customers`) | Business logic (circuit breaker, semantics dispatch) belongs in Python per this project's "config-not-code, logic-not-DDL" convention; the diff itself is a SQL anti-join, same shape as `ReferentialIntegrityBarrier`. |
| Mass-delete circuit breaker | API/Backend (`dataplat`) | — | Direct extension of `RejectionRateCircuitBreaker`'s existing `BarrierStage` shape. |
| Concurrency serialization | Database (`pg_advisory_xact_lock`) | API/Backend (lock acquisition call site) | Unchanged LOAD-09 mechanism — same lock key pattern (`hashtextextended('publish:normalized.customers', 0)`), reused not reinvented. |

## Standard Stack

No new external packages are introduced by this phase. Every library involved is already pinned and in use elsewhere in this codebase.

### Core (already in use, no version change)

| Library | Version (pinned, STACK.md) | Purpose in this phase | Why Standard |
|---------|---------|---------|--------------|
| `psycopg[binary,pool]` | `3.3.4` | Executes the recompute `DELETE`/`INSERT`, the DDL migration, the DELETE-detection anti-join | Native `Decimal`/`datetime` adaptation, already the sole DB driver in `dataplat` |
| Alembic (hand-written revisions) | `1.19.1` | New migration adding `btree_gist` extension, generated `tstzrange` column, exclusion constraint | Same hand-written-revision discipline as migrations 0001-0034; `--autogenerate` cannot emit exclusion constraints (STACK.md, confirmed again below) |
| SQLAlchemy | `2.0.51` | `op.create_exclude_constraint()` — a first-class Alembic op, PostgreSQL-specific | `[VERIFIED: alembic.sqlalchemy.org/en/latest/ops.html]` — confirmed signature below |
| PostgreSQL | **18** (analytical instance, STACK.md §C) | `EXCLUDE USING gist`, `btree_gist` extension, `tstzrange`, generated `STORED` columns | Already the pinned analytical-DB major; `btree_gist` ships in PostgreSQL's own `contrib` and needs no separate install |

### btree_gist extension — a genuine first for this repo

`grep -rn "CREATE EXTENSION" migrations/ helm/` returns **zero matches** — no migration in this repository has ever installed a PostgreSQL extension before. This phase's migration is the first `CREATE EXTENSION IF NOT EXISTS btree_gist;`. Migrations in this repo run via `make migrate-analytics` against the CNPG **superuser** credential (`Makefile` lines 185-237: `secret_name="analytics-db-superuser"`), so `CREATE EXTENSION` will succeed without any additional grant — `[VERIFIED: repo grep + Makefile read]`.

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `EXCLUDE USING gist (customer_id WITH =, validity WITH &&)` | A hand-rolled `BEFORE INSERT/UPDATE` trigger checking for overlaps in application logic or a trigger function | Rejected outright by SCD-12/PITFALLS #2 — a trigger is still "application logic living in the database," not a real constraint, and is not concurrency-safe against two simultaneous transactions the way a real index-backed exclusion constraint is. |
| Generated `STORED` `tstzrange` column | An expression-only exclusion constraint element (`tstzrange(event_ts, valid_to)` inline, no stored column) | Either works technically (Alembic's `create_exclude_constraint` accepts arbitrary expressions as elements). A stored generated column is recommended for this codebase: it is queryable directly (`WHERE validity @> now()`), debuggable in `psql`, and matches this repo's stated preference for explicit, inspectable columns over hidden expression logic (e.g. every other lineage/audit column here is a real column, never a view-only computed value). |
| `valid_to_current` sentinel (`TIMESTAMPTZ '9999-12-31 00:00:00+00'`) | `NULL` for the current row's `valid_to` | ROADMAP.md's plan guidance already locks in "a `valid_to_current` sentinel rather than NULL" (dbt vocabulary). `NULL` would technically still work for the exclusion constraint (an unbounded-upper range is valid GiST input), but querying "give me the row valid at time T" requires `COALESCE(valid_to, 'infinity')` everywhere NULL is used — the sentinel avoids that entirely, and the value `9999-12-31` is dbt's own documented example for this exact config (`dbt_valid_to_current`) — `[CITED: docs.getdbt.com/reference/resource-configs/dbt_valid_to_current]`. |
| A separate `valid_from` column | Reusing `event_ts` as `valid_from` (D-03, locked) | D-03 is locked — do not add a redundant `valid_from` column that merely mirrors `event_ts`'s value. See Finding F-3 for the naming/discoverability tradeoff this creates and how to mitigate it. |

**Installation:** None — no new packages. The only new infrastructure primitive is `CREATE EXTENSION IF NOT EXISTS btree_gist;` inside the phase's first migration.

## Package Legitimacy Audit

Not applicable — this phase installs no new external packages (Python or otherwise). `btree_gist` is a PostgreSQL contrib extension bundled with the PostgreSQL server image already deployed (CloudNativePG's standard images ship `contrib` extensions; this should be confirmed live against the actual CNPG image during Wave 0 execution, but no separate package install is required).

## Architecture Patterns

### System Architecture Diagram

```
                       silver.customers (dbt, delete+insert,          staging.customers (dbt source,
                       unique_key=customer_id — CURRENT WINNER        durable/LOGGED, cumulative,
                       ONLY, one row per key, never deleted)          append-only — FULL ORDERED HISTORY)
                              |                                              |
                              |  (1) DELETE-detection sweep                 |  (2) per-key recompute read
                              |      scoped to staged_run_ids               |      ALL rows for touched keys,
                              |      -> snapshot diff vs. gold              |      ORDER BY event_ts
                              v                                              v
                    +-------------------------------------------------------------+
                    |                    SCD Publisher (new, Python)               |
                    |  load/publish/scd.py — registered "scd" in PUBLISHER_REGISTRY|
                    |                                                              |
                    |  Step A: pg_advisory_xact_lock('publish:normalized.customers')|
                    |  Step B: DELETE-detection (silver, run-scoped) -> invalidate  |
                    |          candidates, guarded by mass-delete circuit breaker   |
                    |  Step C: touched-key discovery (self-derived watermark over   |
                    |          staging.customers._run_id, mirrors dbt macro's own   |
                    |          independent-watermark trick)                         |
                    |  Step D: per touched key -> read staging.customers history,   |
                    |          normalize+hash, split Type-1 (birth_date, overwrite  |
                    |          current row's field) vs Type-2 (name/country, new    |
                    |          version boundary), recompute full chain              |
                    |  Step E: DELETE existing rows for key + INSERT recomputed     |
                    |          chain, same transaction                              |
                    +-------------------------------------------------------------+
                              |
                              v
                    normalized.customers (SCD2 shape: EXCLUDE USING gist
                    (customer_id WITH =, validity WITH &&), backstop only —
                    should never actually fire if Step D's math is correct)
                              |
                              v
                    meta.v_customers_lineage (is_current=true filter added)
                    Phase 9 silver->gold reconciliation (multi-row-per-key aware)
```

### Recommended Project Structure

```
packages/dataplat/src/dataplat/load/publish/
├── merge.py               # unchanged — orders/other future single-row datasets
├── merge_orders.py        # unchanged
├── scd.py                 # NEW — SCDPublisher, this phase's core module
├── registry.py            # +1 entry: "scd": SCDPublisher()
└── protocol.py            # unchanged Publisher Protocol (see Finding F-2 for why
                            # NOT to change its signature)

packages/dataplat/src/dataplat/scd/               # NEW subpackage (or fold into load/publish/
├── recompute.py           # pure function: ordered staging rows -> recomputed version chain
├── delete_detection.py    # snapshot-diff + mass-delete circuit breaker (extends
│                           # validate/circuit_breaker.py's RejectionRateCircuitBreaker shape)
└── hashing.py             # normalized-content hash for name/country (SCD-05), reuses
                            # dataplat.normalize.unicode's existing NFC normalization

migrations/versions/
└── 0035_normalized_customers_scd2.py   # btree_gist extension, drop UNIQUE, add
                                          # valid_to/is_current/validity columns,
                                          # EXCLUDE USING gist, backfill existing rows
```

### Pattern 1: The SCD2 exclusion-constraint migration (SCD-12)

**What:** A single hand-written Alembic migration that (a) installs `btree_gist`, (b) drops migration 0006's `UNIQUE(customer_id)`, (c) adds `valid_to`/`is_current`/a generated `validity` column, (d) backfills existing rows as first-versions, (e) adds the exclusion constraint.

**When to use:** Once, in the migration that first makes `normalized.customers` capable of holding >1 row per `customer_id` — never retrofitted later (PITFALLS #2: an exclusion constraint cannot be added once overlapping intervals already exist).

**Example:**
```python
# Source: PostgreSQL docs (rangetypes.html, btree_gist extension) +
# Alembic ops.html (create_exclude_constraint) — [VERIFIED via WebSearch +
# alembic.sqlalchemy.org fetch, cross-checked against this repo's own
# migration-writing conventions, e.g. migrations 0005/0006/0023]
"""normalized.customers -- migrate in place to SCD2 shape (D-07, SCD-01..12).

... (docstring following this repo's own dense, decision-citing convention) ...
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0035"
down_revision = "0034"
branch_labels = None
depends_on = None

_SENTINEL = "9999-12-31 00:00:00+00"  # dbt's own dbt_valid_to_current example value


def upgrade() -> None:
    # (a) first extension this repo has ever installed -- runs as the CNPG
    # superuser via `make migrate-analytics`, so no additional GRANT needed.
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")

    # (b) undo migration 0006 -- ON CONFLICT (customer_id) can no longer be
    # this table's publication mechanism once >1 row per key is legal.
    op.drop_constraint("uq_customers_customer_id", "customers", schema="normalized", type_="unique")

    # event_ts is DB-nullable today (migration 0005) even though the app
    # contract (customers.yaml) declares it required -- tighten it here,
    # since a NULL event_ts would silently produce an unbounded-lower
    # tstzrange and defeat the exclusion constraint's overlap detection
    # for any customer whose FIRST row ever had a null event_ts.
    op.alter_column("customers", "event_ts", nullable=False, schema="normalized")

    op.add_column(
        "customers",
        sa.Column(
            "valid_to",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text(f"'{_SENTINEL}'::timestamptz"),
        ),
        schema="normalized",
    )
    op.add_column(
        "customers",
        sa.Column("is_current", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        schema="normalized",
    )
    # Existing rows: D-07's "backfilled as each customer's first SCD2
    # version" -- server_default already gave them valid_to=sentinel,
    # is_current=true, matching that requirement with zero extra UPDATE.

    # Generated STORED column -- queryable, debuggable, matches this repo's
    # "explicit columns over hidden expressions" convention. Any generated
    # column addition rewrites the table -- acceptable here (dev-scale data).
    op.execute(
        "ALTER TABLE normalized.customers "
        "ADD COLUMN validity tstzrange "
        "GENERATED ALWAYS AS (tstzrange(event_ts, valid_to, '[)')) STORED"
    )

    # (e) the SCD-12 constraint itself.
    op.create_exclude_constraint(
        "excl_customers_business_key_validity",
        "customers",
        ("customer_id", "="),
        ("validity", "&&"),
        schema="normalized",
        using="gist",
    )
    op.create_index(
        "ix_customers_is_current",
        "customers",
        ["customer_id"],
        unique=False,
        schema="normalized",
        postgresql_where=sa.text("is_current"),
    )


def downgrade() -> None:
    op.drop_index("ix_customers_is_current", table_name="customers", schema="normalized")
    op.drop_constraint(
        "excl_customers_business_key_validity", "customers", schema="normalized", type_="exclude"
    )
    op.execute("ALTER TABLE normalized.customers DROP COLUMN validity")
    op.drop_column("customers", "is_current", schema="normalized")
    op.drop_column("customers", "valid_to", schema="normalized")
    op.alter_column("customers", "event_ts", nullable=True, schema="normalized")
    op.create_unique_constraint(
        "uq_customers_customer_id", "customers", ["customer_id"], schema="normalized"
    )
```

**Verification needed before locking this migration in a plan:** whether `op.create_exclude_constraint`'s `using="gist"` kwarg is the correct spelling in this repo's pinned Alembic `1.19.1` (the fetched docs example did not show the `using=` kwarg explicitly — `EXCLUDE` defaults to GiST when omitted per PostgreSQL itself, so `using="gist"` may be redundant but should not be harmful; confirm against a real `alembic upgrade head` run in Wave 0, not assumed).

### Pattern 2: Recompute-not-surgery correction (SCD-07, SCD-09, SCD-10, SCD-11)

**What:** For every `customer_id` touched by a publish pass, discard that key's currently-published version chain entirely and rebuild it from that key's full ordered history in `staging.customers` — never patch a single row's `valid_to` in place.

**When to use:** On every publish pass, for every business key with new bronze activity since the SCD Publisher's own last-processed watermark — not only when a correction is suspected. Treating "normal forward-only append" and "late correction" as the SAME code path (both are just "recompute this key's chain from its full history") is what makes SCD-09/SCD-10's idempotency and SCD-11's backfill-safety automatic rather than two separately-tested branches.

**Example (illustrative SQL shape, not literal migration text):**
```sql
-- Source: this repo's own DELETE+INSERT precedent style (merge.py's
-- DISTINCT ON / ORDER BY tie-break, silver_customers.sql's row_number()
-- ranking) -- adapted here for a full chronological rebuild, not a
-- single-winner pick. [ASSUMED novel composition -- no external SCD2
-- "recompute" reference implementation was found verbatim; this is a
-- reasoned synthesis of PostgreSQL's own documented primitives plus this
-- repo's established tie-break conventions, not a copied pattern.]

-- Step D: read the FULL ordered history for one touched customer_id from
-- the durable bronze table (never silver -- see Finding F-1).
WITH history AS (
    SELECT customer_id, name, country, birth_date, event_ts,
           _run_id, _file_id, _batch_id, _source_row_number,
           _record_hash, _record_hash_version
    FROM   staging.customers
    WHERE  customer_id = %(customer_id)s
    ORDER  BY event_ts::timestamptz ASC, _source_row_number ASC
),
-- Type-2 change points: a new version starts whenever the NORMALIZED
-- HASH of the tracked (name, country) columns differs from the
-- immediately preceding row's hash (SCD-05). birth_date (Type-1) never
-- creates a boundary -- it always takes the LATEST value across the
-- whole history and is applied only to the CURRENT (is_current=true) row.
change_points AS (
    SELECT *,
           (normalized_hash(name, country) IS DISTINCT FROM
            LAG(normalized_hash(name, country)) OVER (ORDER BY event_ts)) AS starts_new_version
    FROM history
)
-- Step E, inside the SAME transaction as the advisory lock:
-- DELETE FROM normalized.customers WHERE customer_id = %(customer_id)s;
-- INSERT INTO normalized.customers (..., valid_to, is_current, validity)
--   SELECT ..., LEAD(event_ts, 1, '9999-12-31'::timestamptz)
--                 OVER (ORDER BY event_ts) AS valid_to,
--          (row_number() OVER (ORDER BY event_ts DESC) = 1) AS is_current
--   FROM (SELECT *, SUM(starts_new_version::int) OVER (ORDER BY event_ts)
--         AS version_group FROM change_points) versioned
--   WHERE starts_new_version OR event_ts = (SELECT MIN(event_ts) FROM history)
--   -- last row's birth_date is overwritten with the history's OWN latest
--   -- birth_date value (Type-1), not the version-boundary row's own value.
```

**Why DELETE+INSERT, not UPDATE-in-place:** the recomputed chain's row *count* can change (a correction can merge two previously-distinct versions back into one if the "new" data proves an intermediate change never actually happened, or split one version into two). An `UPDATE`-only approach cannot add/remove rows; a full per-key `DELETE`+`INSERT` inside one transaction, protected by the advisory lock, handles both directions uniformly and is what makes the operation genuinely idempotent (same inputs → same recomputed set → same final state on every replay, satisfying SCD-09/SCD-10 by construction rather than by a separate "already applied" check).

**Do NOT use SQL `MERGE` for this step**, even though STACK.md and the PostgreSQL 18 pin make `MERGE ... RETURNING OLD/NEW` tempting for "SCD2 change capture": `MergePublisher`'s own module docstring documents **PostgreSQL BUG #18279** — two concurrent `MERGE` transactions can each decide independently (against their own snapshot) that no matching row exists, and both attempt an insert, so the loser raises a unique-violation instead of falling through to its update branch. This bug affects `MERGE` regardless of the `RETURNING OLD/NEW` enhancement (a `RETURNING`-clause change does not touch `MERGE`'s underlying snapshot-decision logic) — `[VERIFIED: this repo's own merge.py docstring, cross-checked against the PG18 RETURNING OLD/NEW web research above, which only documents a RETURNING-clause enhancement, not a concurrency-model change]`. The per-key `DELETE`+`INSERT` under the SAME `pg_advisory_xact_lock` this codebase already uses for `MergePublisher`/`OrdersMergePublisher` sidesteps this identically to how those two Publishers already do.

### Pattern 3: DELETE-semantics snapshot diff, run-scoped (SCD-08, D-04/D-05/D-06)

**What:** Compare `silver.customers` rows belonging to **this publish pass's `staged_run_ids` only** against gold's current `is_current=true` set — never the whole cumulative `silver.customers` table.

**When to use:** Once per publish pass, before the per-key recompute step, since a customer absent from THIS run's file (but present historically) is the DELETE signal — and `silver.customers` retains every customer ever seen (dbt's `delete+insert` incremental strategy never removes a business key that stops appearing in new bronze), so an unscoped read can never detect an absence.

**Example:**
```sql
-- Source: mirrors record_watermark's own established _run_id = ANY(%s)
-- scoping (metadata/repository.py) -- [VERIFIED: this repo's own code,
-- read directly this session]
WITH this_run_snapshot AS (
    SELECT DISTINCT customer_id
    FROM   silver.customers
    WHERE  _run_id = ANY(%(staged_run_ids)s)
),
vanished AS (
    SELECT c.customer_id
    FROM   normalized.customers c
    WHERE  c.is_current
      AND  c.customer_id NOT IN (SELECT customer_id FROM this_run_snapshot)
)
SELECT count(*) FROM vanished;  -- circuit-breaker input, same shape as
                                  -- RejectionRateCircuitBreaker's ratio check
```

**Critical caveat — this diff is only correct if the CSV that produced `this_run_snapshot` is genuinely the customer roster's full extent.** `source.change_semantics: "snapshot"` is a *dataset-level* declaration (D-04) — if `customers.csv` were ever delivered as a partial/incremental file for a "snapshot" dataset, every currently-current customer NOT in that partial file would look "vanished" and trip either `invalidate` or the circuit breaker. This is precisely what D-06's circuit breaker exists to catch (a truncated/bad file looks identical to a real mass deletion) — but it means the circuit-breaker threshold tuning (Claude's discretion) needs to account for legitimate day-to-day roster size variance, not just catastrophic truncation.

### Pattern 4: Advisory lock reuse + the dedicated concurrency test (D-10)

**What:** The SCD Publisher takes the SAME `pg_advisory_xact_lock(hashtextextended('publish:normalized.customers', 0))` LOAD-09 already establishes (unchanged lock key — `ctx.config.load.target` is still `"normalized.customers"`), acquired by the SAME caller (`publish_ingest`'s orchestration in `pipeline/run.py`) before calling `publisher.publish()`, exactly as today.

**What D-10's dedicated test must prove, concretely:** two genuinely concurrent writers to `normalized.customers` — one applying a live attribute change (new `event_ts`, newer than the current version) and one applying a backfill/correction (older `event_ts`, landing between two existing versions) for the **same `customer_id`** — must serialize through the advisory lock such that (a) neither transaction ever observes the other's half-applied `DELETE`+`INSERT`, (b) the exclusion constraint never actually fires (it is a backstop, not the primary mechanism — if it fires during this test, that is a bug in Pattern 2's math, not proof the constraint "worked"), and (c) the final state is deterministic regardless of which of the two writers' transaction commits first (LOAD-09's single-writer-per-target-table guarantee applied to a NEW code path, not the `INSERT...ON CONFLICT` path Phase 9's own concurrency proof already covers).

**Precedent for the live-cluster test shape:** `tests/e2e/slice/test_backfill_2year_sweep.py::test_live_run_concurrent_with_backfill_same_dataset` (D-13, Phase 9) is the closest existing pattern — poll for two concurrently-`running` DagRuns (one live-scheduled, one from a triggered backfill), then assert no corruption. **This exact test's own corruption assertion will break post-migration** (`SELECT customer_id, count(*) ... HAVING count(*) > 1` — see Finding F-4) and must be rewritten to check for overlapping `validity` ranges or duplicate `is_current=true` rows per key, not "any duplicate `customer_id` row at all" (which is now the *expected*, correct SCD2 shape).

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Overlapping validity-interval prevention | A Python-side "check before insert" guard, or a `BEFORE INSERT` trigger doing a manual overlap `SELECT` | PostgreSQL `EXCLUDE USING gist (customer_id WITH =, validity WITH &&)` | Race-condition-proof at the index level; a Python/trigger check-then-act is a classic TOCTOU bug under concurrent writers — exactly what D-10's test exists to catch if this rule is violated. |
| "Is this timestamp inside this version's window" queries | Manual `valid_from <= X AND (valid_to IS NULL OR valid_to > X)` boilerplate scattered across query sites | `validity @> X::timestamptz` (range containment operator) against the generated `tstzrange` column | One operator, index-usable (GiST), and eliminates the NULL-handling bug class entirely once the sentinel replaces NULL. |
| Change detection | Comparing raw `name`/`country` string equality across rows | The SAME normalized-hash machinery (`dataplat.normalize.unicode` NFC + `_record_hash`) already used for dedup (CSV-12, DEDUP-03) | CSV-12's own stated reason applies verbatim here: NFC/NFD variants of the same value would otherwise produce phantom SCD2 versions — this is a documented, tested risk this codebase already solved once; solve it once, reuse it. |
| Mass-delete detection | A bespoke threshold-check function for this one dataset | `RejectionRateCircuitBreaker`'s exact shape (`validate/circuit_breaker.py`), parameterized differently | Direct precedent named in CONTEXT.md D-06 — same "count vs. configurable threshold, raise a domain exception, fail the run" contract, just fed a "vanished fraction" instead of a "rejection rate." |
| Watermark/self-derived floor tracking for "which keys changed since I last looked" | A new bespoke run-tracking table | Either `meta.watermarks` with a distinct `target_key` (e.g. `"scd_customers"`) reusing `record_watermark`'s existing `GREATEST()`/history machinery, OR the self-derived-floor trick `dedup_audit_post_hook.sql` already uses (`coalesce(max(...), 0)` over the SCD Publisher's own prior output) | Both are proven, existing patterns in this exact codebase; a third bespoke mechanism adds a third thing to reason about for no benefit. |

**Key insight:** every mechanism this phase needs (overlap prevention, hash-based change detection, threshold circuit breakers, self-derived watermarks) already has a proven, in-repo precedent from Phases 3-9. The genuinely novel work is the *composition* — chaining "read full bronze history for a touched key" → "recompute the version chain" → "atomically replace it" — not any individual primitive.

## Runtime State Inventory

> This phase is a schema/data migration (D-07: in-place migration of `normalized.customers`), not a rename/rebrand. The standard 5-category rename inventory mostly does not apply, but the underlying question — "what runtime state assumes the old shape and will silently misbehave after the migration?" — absolutely does. See **Finding F-4** below for the concrete answer; this table gives the abbreviated categorical pass.

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| Stored data | `normalized.customers` itself — every existing row must backfill as a first SCD2 version (`valid_to`=sentinel, `is_current`=true). No separate migration script needed — `server_default` on `ADD COLUMN` backfills existing rows automatically within the same DDL statement. | DDL-only, no separate data-migration script (see Pattern 1). |
| Live service config | None — no external service (Vault, Airflow connections, Grafana) stores `normalized.customers`'s row shape. | None. |
| OS-registered state | None. | None. |
| Secrets/env vars | None — no secret references this table's shape. | None. |
| Build artifacts / installed packages | None — no compiled artifact embeds this schema. | None. |
| **Code + test consumers of "one row per `customer_id`"** (not a standard category, but the REAL risk here — see Finding F-4) | 3 production-code consumers (D-08's own list, verified accurate) **plus** at least 9 test files with row-count/uniqueness assertions that assume single-row-per-key. | Both categories must be fixed and re-proven in this phase's own plan — see Finding F-4 for the full list. |

## Common Pitfalls

### Pitfall 1 (Finding F-1): `silver.customers` cannot supply late-arriving corrections

**What goes wrong:** A late-arriving row for `customer_id=X` with an `event_ts` older than what's currently in `silver.customers` never reaches the SCD Publisher if the Publisher reads `silver.customers` the same way `MergePublisher` does today.

**Why it happens:** `dbt/models/silver/silver_customers.sql` is `materialized='incremental', incremental_strategy='delete+insert', unique_key='customer_id'`, ranking `new_bronze UNION ALL existing_silver_contenders` by `row_number() over (partition by customer_id order by event_ts desc, ...)` and keeping only `rn=1`. A late, older-`event_ts` row loses this ranking and is classified `SUPERSEDED_BY_NEWER` in `meta.dedup_decisions` — it never becomes a `silver.customers` row. `[VERIFIED: direct read of dbt/models/silver/silver_customers.sql and dbt/macros/dedup_audit_post_hook.sql this session]`.

**How to avoid:** The SCD Publisher's recompute step (Pattern 2) must read from `staging.customers` (the durable, cumulative, never-deduplicated bronze table, migration 0022) — never from `silver.customers` — whenever it needs a business key's full ordered history for version-chain recomputation.

**Warning signs during planning:** any plan task that has the SCD Publisher's correction logic read exclusively from `source_table = f"silver.{ctx.config.dataset}"` (the parameter name `publish_ingest` already passes to every `Publisher.publish()`) without also reading `staging.customers` is very likely to fail SCD-07's live proof — a late correction landing between two published versions will be silently dropped by dbt before the SCD Publisher ever runs.

### Pitfall 2 (Finding F-2): unscoped snapshot reads make DELETE-detection permanently vacuous

**What goes wrong:** If the DELETE-detection diff (Pattern 3) reads the WHOLE `silver.customers` table (matching `MergePublisher`'s own `_PUBLISH_SQL` and `_compute_silver_gold_reconciliation`'s established "read the entire cumulative table" convention), it will NEVER detect a deletion, because `silver.customers` retains every customer ever seen, forever.

**Why it happens:** dbt's `delete+insert`/`unique_key=customer_id` strategy only ever adds/updates rows for keys present in the current incremental batch; it has no delete-detection of its own (that's precisely why SCD-08/D-04 exists as new Phase-10 work) — so a customer who "vanished" from today's CSV is still sitting in `silver.customers` from whenever they were last seen.

**How to avoid:** Scope the DELETE-detection query to `WHERE _run_id = ANY(%(staged_run_ids)s)` — the same `staged_run_ids` list `publish_ingest` already computes and passes to `record_watermark`.

**Warning signs:** a live test where a genuinely-vanished customer's row is NOT closed out (`is_current` stays `true` forever) despite the fixture correctly omitting them from the new snapshot file.

### Pitfall 3 (Finding F-3): the `meta.config_versions`/`meta.schema_versions` "SCD1-ish" pattern is NOT a template for this phase

**What goes wrong:** This repo already has a `valid_from`/`valid_to IS NULL` + partial-unique-index pattern (migrations 0001, 0009, reused by `config/registry.py` and `schema/repository.py`) that superficially looks like "the SCD2 pattern this codebase already uses." Copying it for `normalized.customers` would be a real regression.

**Why it happens:** That pattern's `UPDATE ... SET valid_to = now() WHERE valid_to IS NULL` + `INSERT ... version = max + 1` sequence (1) has NO overlap protection at all — it relies entirely on the two-statement sequence never racing, protected only by whatever transaction/locking discipline the CALLER happens to apply, not a database constraint; and (2) has no recompute mechanism — a "correction" there always means "the current version was wrong, supersede it," never "insert a version chronologically between two already-closed ones," because config/schema versions are strictly append-forward (nothing about "config as of last Tuesday" needs revising after the fact). SCD-07 explicitly requires exactly what that pattern cannot do.

**How to avoid:** Use Pattern 1/2 (exclusion constraint + full recompute) for `normalized.customers`. Do not model this migration on migrations 0001/0009 despite their superficial `valid_from`/`valid_to` column-naming similarity.

**Warning signs:** a plan task that describes the SCD Publisher's write path as "update the current row's `valid_to`, then insert a new row" (a two-statement sequence, matching `config/registry.py`'s pattern) rather than "recompute and replace the full chain" — this is the in-place-surgery approach D-07/SCD-07 explicitly reject.

### Pitfall 4 (Finding F-4): the test blast radius is much larger than D-08's named list

**What goes wrong:** D-08 names 3 production-code consumers to fix. A repo-wide grep for `normalized.customers` across `tests/` returns **26 files**; a closer read of a sample shows the following are DIRECTLY affected by the shape change, beyond D-08's named list:

- `tests/integration/test_publish_merge.py` — tests `MergePublisher` directly against `normalized.customers`, including `test_on_conflict_fails_without_the_unique_constraint_migration_0006_adds`, which asserts the EXACT constraint this phase's migration drops. **This entire file is obsolete** once the SCD Publisher replaces `MergePublisher` for this dataset — it should be deleted (or repurposed to test `MergePublisher` against a different, still-single-row-per-key target if one exists) and a new `test_publish_scd.py` written in its place.
- `tests/integration/test_publish_ingest.py` — `_normalized_customers_count()` helper does `SELECT COUNT(*) FROM normalized.customers` with no filter and asserts exact before/after counts across a publish call. Will over-count once one publish can legitimately add multiple SCD2 versions for one file's worth of changes.
- `tests/integration/test_run_ingest.py` — comment at line 41 states "`normalized.customers.customer_id` is unique across [runs]" as a documented test design assumption.
- `tests/integration/test_reconciliation.py` — line 439 asserts `row["output_count"] == _table_row_count(env.migrated_dsn, "normalized.customers")`, directly dependent on 1-row-per-key cardinality for its correctness math (D-08 item 2 already flags the PRODUCTION reconciliation code; this is the TEST that exercises it, needs the same rework).
- `tests/e2e/slice/test_backfill_2year_sweep.py::test_live_run_concurrent_with_backfill_same_dataset` — the exact "no duplicates" assertion described in Pattern 4 above.
- `tests/e2e/slice/test_smoke_and_idempotency.py`, `test_pod_kill_retry.py`, `test_referential_orphan.py`, `test_concurrent_select.py`, `test_backfill_reentry.py`, `test_dbt_silver_pipeline.py` — all reference `normalized.customers`; each needs individual review for a cardinality assumption, not assumed clean by omission from this list.

**Why it happens:** `normalized.customers`'s "one row per business key" invariant was true and heavily tested from Phase 4 through Phase 9 — nine phases' worth of tests were written against a real, then-correct assumption that this phase deliberately breaks.

**How to avoid:** Treat "audit and fix every test file touching `normalized.customers`" as its own explicit, budgeted task/wave in the plan — not an afterthought folded into the D-08 production-code consumer-fix task. Budget time for `test_publish_merge.py`'s full replacement, not just a search-and-patch.

**Warning signs:** a plan that scopes D-08's consumer-fix work to exactly 3 files (matching CONTEXT.md's literal list) will very likely leave several of the above tests silently red (or, worse, silently green-but-vacuous) after Wave-level execution.

### Pitfall 5: three-valued NULL logic in the recompute's tie-break, same class as `merge_orders.py`'s known fix

**What goes wrong:** `OrdersMergePublisher`'s own module docstring documents a real, previously-shipped bug: a plain `EXCLUDED.order_date >= normalized.orders.order_date` comparison is NULL (not TRUE) whenever the existing row's date is NULL, silently preventing an update forever. The SCD Publisher's recompute logic (Pattern 2) does comparable ordering/comparison work (`event_ts` ordering, hash equality with `LAG()`) and must apply the same discipline this repo already learned from that bug.

**Why it happens:** SQL's three-valued logic makes `NULL >= x` and `NULL = x` both evaluate to `NULL`, not `FALSE` — any `WHERE`/`CASE`/window-function comparison touching a nullable column silently excludes NULL-valued rows from matching, without raising an error.

**How to avoid:** After Pattern 1's migration tightens `event_ts` to `NOT NULL`, the primary ordering key is safe. `name`/`country` (Type-2 tracked columns) are `nullable=True` in `customers.yaml`'s config today but declared `required: true` — verify DB-level nullability matches before relying on `IS DISTINCT FROM` (which correctly treats `NULL IS DISTINCT FROM NULL` as `FALSE`, unlike plain `=`/`<>` — use `IS DISTINCT FROM` for the hash-change comparison specifically because it IS NULL-safe, not despite it).

**Warning signs:** a `birth_date`/`name`/`country` value that is legitimately `NULL` in the fixture corpus stops correctly triggering (or stops correctly NOT triggering) a new SCD2 version.

## Code Examples

### Reusing `RejectionRateCircuitBreaker`'s shape for the mass-delete breaker

```python
# Source: dataplat/validate/circuit_breaker.py, read directly this session.
# The mass-delete breaker mirrors this exact constructor-parameterized,
# apply(ctx)-raises-a-domain-exception shape -- fed "vanished / current"
# instead of "rejected / read".
class MassDeleteCircuitBreaker(BarrierStage):
    name = "mass_delete_circuit_breaker"

    def __init__(self, *, threshold: float, current_count: int, vanished_count: int) -> None:
        self._threshold = threshold
        self._current_count = current_count
        self._vanished_count = vanished_count

    def apply(self, ctx: PipelineContext) -> StageResult:
        if self._current_count == 0:
            return _trivial_pass(...)  # empty dimension can never mass-delete
        ratio = self._vanished_count / self._current_count
        if ratio > self._threshold:
            raise QualityThresholdExceeded(
                f"{ratio:.2%} of current customers vanished from this snapshot, "
                f"exceeds configured threshold {self._threshold:.2%}",
                context={...},
            )
        return _pass_result(...)
```

### Normalized-hash change detection, reusing existing NFC normalization

```python
# Source: dataplat/normalize/unicode.py (already normalizes before hashing
# for dedup, CSV-12) -- reuse the SAME function for SCD-05's "normalization
# strictly precedes hashing" requirement, never a second independent
# normalization implementation.
from dataplat.normalize.unicode import normalize_for_hash  # exact name TBD, verify at plan time

def tracked_attribute_hash(name: str, country: str, *, hash_version: int) -> bytes:
    canonical = "\x1f".join(normalize_for_hash(v) for v in (name, country))
    return hashlib.sha256(canonical.encode("utf-8")).digest()
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| `normalized.customers` as a `MergePublisher`-owned, one-row-per-key upsert target | `normalized.customers` as an SCD2 dimension with a `btree_gist` exclusion constraint | This phase (Phase 10) | `MergePublisher` is retired for this one dataset (remains in use for `orders` via `OrdersMergePublisher`, unaffected). |
| dbt `MERGE`/literal SQL `MERGE` considered for gold writes | Rejected repo-wide (PostgreSQL BUG #18279), `INSERT...ON CONFLICT` for single-row targets, `DELETE`+`INSERT` per key for SCD2 targets | Established Phase 4, reaffirmed by this research for Phase 10 | PG18's `MERGE...RETURNING OLD/NEW` enhancement does NOT change this recommendation — the concurrency bug is orthogonal to the `RETURNING` clause. |
| dbt `snapshot` materialization considered for SCD2 | Explicitly rejected (Phase 08.1, ADR-0010) — SCD2 stays Python-owned | Phase 08.1 (prior to this phase) | This phase's SCD Publisher is NOT a `dbt snapshot`; do not reach for dbt's own `hard_deletes`/`dbt_valid_to_current` CONFIG mechanism — only its VOCABULARY is being adopted (per ROADMAP guidance), the implementation is bespoke Python. |

**Deprecated/outdated:** Nothing in this domain is deprecated by an upstream vendor — this section is empty beyond the internal architectural shifts above.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `op.create_exclude_constraint(..., using="gist")`'s exact kwarg spelling is correct for Alembic `1.19.1` | Pattern 1 | Low — migration fails fast at `alembic upgrade head` (caught in Wave 0, not a silent runtime bug); Alembic docs fetch did not show the `using=` kwarg in the one example retrieved, only inferred from PostgreSQL's own default-to-GiST behavior. |
| A2 | No existing reference implementation for "recompute an SCD2 chain from durable bronze history in Python + psycopg" was found; Pattern 2's SQL shape is a reasoned synthesis, not a verified/copied pattern | Pattern 2, Architecture Patterns | Medium — the LEAD()/change-point SQL sketch needs to be built and tested against real fixture data during planning/execution; treat it as a starting design, not verified-correct SQL. |
| A3 | The CNPG-provisioned PostgreSQL image includes the `btree_gist` contrib extension available for `CREATE EXTENSION` without a separate image/Helm-values change | Standard Stack, Package Legitimacy Audit | Medium — if the deployed CNPG Postgres image lacks contrib extensions, the phase's first migration fails immediately; standard CNPG images DO ship contrib extensions, but this was not directly verified against the live cluster's actual image in this research session. |
| A4 | `staging.customers`'s durability (LOGGED, never truncated) is sufficient to serve as the full "ordered batch history" for every customer_id indefinitely (i.e., no retention/archival policy purges old bronze rows) | Finding F-1, Pattern 2 | High if wrong — if bronze retention is ever added later (INFRA-11, Phase 11, currently pending/not yet built), the SCD Publisher's recompute source would silently lose history older than the retention window. Not a risk for Phase 10 itself (INFRA-11 is Phase 11, not yet built), but worth an explicit note/ADR for future-proofing. |

**Assumptions requiring explicit user/planner confirmation before locking:** A2 (the recompute SQL shape) is the one genuinely load-bearing assumption — it should be validated with a small spike/prototype early in Wave 0, before committing to the full plan's task breakdown, since if the LEAD()-based version-chain math is wrong in some edge case (e.g., two consecutive bronze rows with identical `event_ts`), the whole correction mechanism needs rethinking.

## Open Questions

1. **SCD-01 (Type 0) has no assigned column in D-02's locked column-treatment table.**
   - What we know: D-02 assigns `customer_id` (business key, untracked), `name`/`country` (Type 2), `birth_date` (Type 1). No column is designated Type 0 ("retains original values," i.e., never overwritten even by a correction).
   - What's unclear: whether SCD-01 is satisfied structurally by *some other* mechanism (e.g., `_ingested_at`/`_run_id`/`_file_id` lineage columns already never change once written, which could argue SCD-01 is satisfied by the existing embedded-lineage-column convention rather than needing a dedicated *business* column) or whether the plan needs a genuine Type-0 business column example to prove SCD-01 concretely, the way SCD-02/SCD-03 get real proofs via `birth_date`/`name`+`country`.
   - Recommendation: raise this explicitly during planning — either designate `customer_id` itself (immutable once a version chain exists, matching Type 0's definition literally) as the Type-0 proof, with a test asserting a correction can never change which `customer_id` a version chain belongs to, or confirm with the user this requirement is satisfied by a different existing mechanism. Do not silently skip SCD-01's live proof.

2. **`timestamp` vs `check` change-detection strategy (dbt vocabulary, Claude's discretion per CONTEXT.md) — which one actually governs `customers.yaml`'s hash comparison?**
   - What we know: SCD-05 requires "deterministic via normalized hash" — this points toward dbt's `check` strategy vocabulary (compare specific tracked columns via hash) rather than `timestamp` (compare a single updated-at column to detect ANY change without inspecting which columns changed).
   - What's unclear: whether the `event_ts`-driven ordering (used for version boundaries) and the hash-driven "did the TRACKED columns actually change" check (used to decide WHETHER to create a new version) need to be named/documented as two distinct concerns in the plan, or whether conflating them (treating `event_ts` monotonic advance as sufficient signal, dbt's `timestamp` strategy) would produce phantom versions whenever `event_ts` advances but `name`/`country` do not.
   - Recommendation: `check` strategy is correct — a new SCD2 version must be gated on the NORMALIZED HASH changing (SCD-05's literal text), never merely on a newer `event_ts` arriving. `event_ts` decides ordering and boundary placement; the hash decides whether a boundary is drawn at all. Document both roles explicitly and distinctly in the plan.

3. **Does the 2-year corpus's existing deliberate schema-version change (Phase 9 D-10) interact correctly with SCD2 versioning?**
   - What we know: `tools/corpus/dated_series.py` injects a schema-version boundary (`loyalty_tier` column appended at `schema_change_day_index`) as one of its three deliberate anomalies. Phase 9 proved historical files process under their historical schema version (QUAL-11).
   - What's unclear: whether a schema change mid-corpus could be misread as a business-attribute change by the SCD Publisher's hash comparison (e.g., if `hash_version` bumps at the schema-change boundary the way `_record_hash_version` is designed to for META-02, does that ALSO need to avoid spuriously triggering a new SCD2 version purely because the hash RECIPE changed, not because `name`/`country` actually changed?).
   - Recommendation: verify directly during planning/execution — the recomputation logic must compare NORMALIZED VALUES (or hashes computed under a SINGLE, consistent recipe applied uniformly across the whole history at recompute time), never raw stored hashes computed under potentially-different `hash_version`s at different points in time. This is a real risk given `_record_hash_version` exists specifically to let the hash recipe change over the project's life (META-02) — a naive "compare stored `_record_hash` values directly" implementation would break the moment the recipe ever changes, even though this phase's OWN column set (`name`, `country`) is unaffected by the loyalty_tier schema addition. Since the schema-change column (`loyalty_tier`) is not one of the SCD-tracked columns (`name`/`country`), this may be a non-issue in practice — but must be verified against the live fixture, not assumed.

## Environment Availability

Skipped — this phase has no external dependencies beyond the already-deployed analytical PostgreSQL 18 instance and its `btree_gist` contrib extension (see Assumption A3), which is not a separately-installed tool/service/runtime in the sense this section covers.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest `9.1.1` (`[tool.pytest.ini_options]`, `pyproject.toml`) |
| Config file | `pyproject.toml` (`minversion = "9.0"`, `testpaths = ["tests"]`, `--strict-markers --strict-config`) |
| Quick run command | `pytest tests/unit -q` (offline gate, no Docker/cluster needed) |
| Full suite command | `pytest tests/unit tests/integration -q -m "not cluster and not manifests and not dbt"` locally with Docker; `pytest tests/e2e/slice -q -m cluster` against the live kind cluster for the phase gate |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| SCD-01 | Type 0 column retains original value under correction | unit | `pytest tests/unit/test_scd_recompute.py -k type_zero -x` | ❌ Wave 0 |
| SCD-02 | `birth_date` overwritten in place, no history | unit | `pytest tests/unit/test_scd_recompute.py -k type_one -x` | ❌ Wave 0 |
| SCD-03 | `valid_from`/`valid_to`/`is_current` correct on a real change | integration | `pytest tests/integration/test_publish_scd.py -k version_boundary -x -m integration` | ❌ Wave 0 |
| SCD-04 | Surrogate key independent of hash, distinct from business key | unit | `pytest tests/unit/test_scd_recompute.py -k surrogate_independence -x` | ❌ Wave 0 |
| SCD-05 | Deterministic normalized-hash change detection | unit + property | `pytest tests/unit/test_scd_hashing.py -x` (+ hypothesis property, mirrors QUAL-16's determinism precedent) | ❌ Wave 0 |
| SCD-06 | Effective dating never defaults to ingestion time | integration | `pytest tests/integration/test_publish_scd.py -k effective_dating -x -m integration` | ❌ Wave 0 |
| SCD-07 | Late correction recomputes chain from ordered history | integration + e2e | `pytest tests/integration/test_publish_scd.py -k late_correction -x -m integration`; live proof in `test_backfill_2year_sweep.py` extension (D-11) | ❌ Wave 0 (integration); extends existing file (e2e) |
| SCD-08 | Configurable DELETE semantics + circuit breaker | integration | `pytest tests/integration/test_scd_delete_detection.py -x -m integration` | ❌ Wave 0 |
| SCD-09 | Replayed identical event → exactly one version | integration | `pytest tests/integration/test_publish_scd.py -k idempotent_replay -x -m integration` | ❌ Wave 0 |
| SCD-10 | Idempotent under re-application | e2e | `test_backfill_2year_sweep.py`'s existing `test_idempotent_rerun_produces_zero_additional_rows` pattern, extended per D-12 | Extends existing file |
| SCD-11 | Backfill-safe, never blindly overwrites current state | e2e | Same D-11/D-12 corpus extension | Extends existing file |
| SCD-12 | `btree_gist` exclusion constraint rejects overlaps | integration | `pytest tests/integration/test_migrations.py -k exclusion_constraint -x -m integration` (direct `INSERT` of an overlapping row, expect `psycopg.errors.ExclusionViolation`) | ❌ Wave 0 (extends existing file) |
| QUAL-14 | SCD tested incl. late corrections + idempotent re-application | e2e | Covered by SCD-07/SCD-09/SCD-10's own commands above | — |

### Sampling Rate

- **Per task commit:** `pytest tests/unit -q` (fast, no Docker)
- **Per wave merge:** `pytest tests/unit tests/integration -q -m "not cluster and not manifests and not dbt"` (testcontainers PostgreSQL)
- **Phase gate:** `pytest tests/e2e/slice -q -m cluster` (extended `test_backfill_2year_sweep.py`) green against the live kind cluster before `/gsd:verify-work`

### Wave 0 Gaps

- [ ] `tests/unit/test_scd_recompute.py` — pure-function unit tests for Pattern 2's recompute logic (Type-0/1/2 dispatch, LAG-based change-point detection), no DB needed if the function is written to accept plain ordered records rather than a live cursor.
- [ ] `tests/unit/test_scd_hashing.py` — normalized-hash determinism, reusing `dataplat.normalize.unicode`.
- [ ] `tests/integration/test_publish_scd.py` — real `SCDPublisher` against real PostgreSQL (testcontainers), covering version-boundary creation, effective-dating, late-correction recompute, idempotent replay.
- [ ] `tests/integration/test_scd_delete_detection.py` — snapshot-diff + mass-delete circuit breaker, real PostgreSQL.
- [ ] Extend `tests/integration/test_migrations.py` (exists — verify) with a direct exclusion-constraint-rejects-overlap assertion.
- [ ] Extend `tests/e2e/slice/test_backfill_2year_sweep.py` per D-11/D-12: attribute-change events, one late/out-of-order correction, one missing-customer fixture, one deliberately-bad-snapshot fixture (D-06 discretion point), a rewritten Pattern-4 corruption assertion, and the D-10 dedicated concurrency test.
- [ ] Fix or replace: `tests/integration/test_publish_merge.py` (replace), `tests/integration/test_publish_ingest.py`, `tests/integration/test_run_ingest.py`, `tests/integration/test_reconciliation.py`, plus a full review pass over the remaining ~20 files a grep for `normalized.customers` surfaces (Finding F-4).

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | Unaffected — no new auth surface. |
| V3 Session Management | No | Unaffected. |
| V4 Access Control | Yes | `normalized.customers`'s existing `GRANT SELECT, INSERT, UPDATE ON normalized.customers TO etl_app` (migration 0005) — this phase ADDS `DELETE` usage (Pattern 2's per-key `DELETE`+`INSERT`), so the migration must also `GRANT DELETE ON normalized.customers TO etl_app` (currently NOT granted — `etl_app` has never needed `DELETE` on this table before; verify via `\dp normalized.customers` during Wave 0, do not assume the existing grant already covers it). |
| V5 Input Validation | Yes | Unchanged — `customers.yaml`'s existing Pydantic-validated contract, `extra="forbid"`. No new user-facing input surface this phase introduces (the DELETE-semantics config value `ignore \| invalidate \| new_record` should be validated as a closed enum-like string in the config model, matching `hop`'s app-validated-vocabulary convention). |
| V6 Cryptography | No | The normalized-hash (SHA-256) is a change-detection fingerprint, not a security cryptographic control — same classification as the existing `_record_hash` usage elsewhere in this codebase. |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| SQL injection via dynamically-interpolated table/column identifiers in the recompute SQL | Tampering | Same discipline as `merge.py`/`referential.py`'s own documented threat models (T-04-01, T-08-11): every interpolated fragment in the SCD Publisher's SQL must be a config/run-derived IDENTIFIER (dataset name, numeric run id, fixed column name), never row content — document this explicitly in the new module's own docstring, matching the established convention. |
| Privilege escalation via the new `DELETE` grant | Elevation of Privilege | `etl_app`'s new `DELETE` grant is scoped to exactly `normalized.customers` (not schema-wide `DELETE`), matching this repo's existing narrow-grant convention (e.g. `dbt_app`'s `dedup_audit`/`dedup_decisions` INSERT-only scoping, migration 0024). |
| Race between concurrent writers producing a corrupted/partial version chain | Tampering, Denial of Service | `pg_advisory_xact_lock` (Pattern 4) + the exclusion constraint as backstop — already this repo's established mitigation for LOAD-09, reused unchanged in mechanism, newly tested for this specific code path (D-10). |

## Sources

### Primary (HIGH confidence)

- This repository, read directly this session: `packages/dataplat/src/dataplat/load/publish/{merge.py,merge_orders.py,registry.py,protocol.py}`, `packages/dataplat/src/dataplat/validate/{circuit_breaker.py,referential.py}`, `packages/dataplat/src/dataplat/pipeline/run.py` (lines 216-420, 990-1180), `packages/dataplat/src/dataplat/metadata/repository.py` (lines 895-1070), `packages/dataplat/src/dataplat/config/model.py`, `migrations/versions/{0001,0005,0006,0009,0012,0016,0023,0024,0026,0030,0032}_*.py`, `dbt/models/silver/silver_customers.sql`, `dbt/macros/dedup_audit_post_hook.sql`, `configs/datasets/customers.yaml`, `tests/e2e/slice/test_backfill_2year_sweep.py`, `tests/integration/test_publish_merge.py`, `tests/integration/{test_publish_ingest,test_run_ingest,test_reconciliation,test_watermarks}.py`, `.planning/phases/10-slowly-changing-dimensions/10-CONTEXT.md`, `.planning/REQUIREMENTS.md`, `.planning/STATE.md`, `Makefile` (migrate-analytics target), `pyproject.toml` (pytest config), `.planning/config.json`.
- `[VERIFIED: alembic.sqlalchemy.org/en/latest/ops.html]` — `op.create_exclude_constraint()` signature and usage example.
- `[VERIFIED: postgresql.org/docs/current/rangetypes.html and related PG18 docs]` — `EXCLUDE USING gist`, `btree_gist`, `tstzrange` half-open-bound default semantics, MERGE RETURNING OLD/NEW (PG18 release notes).

### Secondary (MEDIUM confidence)

- `[CITED: docs.getdbt.com/reference/resource-configs/hard-deletes]` — dbt's `hard_deletes: ignore | invalidate | new_record` vocabulary, confirmed genuine (not invented) and matches ROADMAP.md's cited vocabulary exactly.
- `[CITED: docs.getdbt.com/reference/resource-configs/dbt_valid_to_current]` — dbt's own documented example sentinel value `'9999-12-31'` for `dbt_valid_to_current`.
- WebSearch results on PostgreSQL `btree_gist`/exclusion-constraint SCD2 usage patterns (Neon docs, Simple Talk/Red Gate PostgreSQL range-overlap article) — cross-checked against the official PostgreSQL `rangetypes.html` documentation, consistent.

### Tertiary (LOW confidence)

- Pattern 2's exact recompute SQL shape (the `LEAD()`/change-point CTE sketch) is a novel synthesis, not sourced from any external reference implementation — flagged explicitly as Assumption A2, needs a spike/prototype before being locked into a plan's task text.

## Metadata

**Confidence breakdown:**

- Standard stack / DDL mechanics: HIGH — every primitive (`btree_gist`, `EXCLUDE USING gist`, generated `STORED` columns, `op.create_exclude_constraint`) is directly documented by PostgreSQL/Alembic and cross-checked against this repo's own working migration conventions.
- Architecture / read-path for corrections (Finding F-1/F-2): MEDIUM-HIGH — the PROBLEM (silver collapses history, unscoped reads defeat DELETE-detection) is HIGH confidence, directly proven by reading `silver_customers.sql` and `record_watermark`'s own code. The RECOMMENDED RESOLUTION (read `staging.customers` for recompute; scope DELETE-detection to `staged_run_ids`) is a reasoned design recommendation for the planner to ratify, not a locked architectural fact — no prior phase or CONTEXT.md decision explicitly names this resolution.
- Pitfalls / test blast radius (Finding F-4): HIGH — directly grep- and read-verified against the actual test files in this repository this session.
- Recompute SQL correctness (Pattern 2's literal SQL): LOW-MEDIUM — illustrative, not verified by execution; flagged as Assumption A2, needs a Wave 0 spike.

**Research date:** 2026-08-21
**Valid until:** No external dependency in this research is fast-moving (PostgreSQL 18 and Alembic 1.19.1 are already pinned and stable); the internal architectural findings (F-1 through F-4) remain valid for the life of this phase's plan and do not expire on a calendar basis — they should simply be re-verified against the actual codebase state if execution starts more than a few weeks after this research (in case an intervening quick-task or phase touches `silver_customers.sql`, the `Publisher` protocol, or the test files named in Finding F-4).
