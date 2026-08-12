# Phase 3: `dataplat` Core Library & Metadata Control Plane - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-08-12
**Phase:** 3-dataplat-core-library-metadata-control-plane
**Areas discussed:** CSV reading scope, Vertical-slice demo dataset, Observability seam depth, Test suite gating, Metadata schema migration granularity, Exception hierarchy scope

---

## CSV reading capability landed in Phase 3

| Option | Description | Selected |
|--------|-------------|----------|
| Minimal working Source (recommended) | Real `csv_processor.Source` over the `RecordStream`/`Source` protocol — hardcoded UTF-8, comma, header row 0, no detection. Proven against a fixture; Phase 4 plugs it in directly. | ✓ (via "Let Claude decide") |
| Chunking primitive only | Just the `csv.reader`/`newline=""` pattern inside `dataplat`, proven with an in-memory stream — no real `csv_processor.Source`. | |
| Let Claude decide | | (selected) |

**User's choice:** Let Claude decide — resolved to "Minimal working Source."
**Notes:** Phase 4 has zero CSV-* requirements of its own and is the protected critical path; it needs a real reader to plug into, not a reason to absorb CSV-reading scope itself.

---

## Vertical-slice demo dataset

| Option | Description | Selected |
|--------|-------------|----------|
| Keep research's shape (recommended) | `customer_id, name, country, birth_date, event_ts` — the shape ARCHITECTURE.md uses throughout its examples. | ✓ |
| Switch to transactions | `amount`/`transaction_id`-shaped dataset, also used in ARCHITECTURE.md's config-not-code example. | |
| Something else — I'll describe it | User-specified domain/columns. | |

**User's choice:** Keep research's shape — `normalized.customers` with `customer_id, name, country, birth_date, event_ts`.
**Notes:** Direct selection, no follow-up requested.

---

## Observability seam depth

| Option | Description | Selected |
|--------|-------------|----------|
| Thread no-op calls now (recommended) | `metrics.py`/`tracing.py` exist with real call sites already placed in pipeline stages, no-op until Phase 7 swaps backends. | ✓ (via "Let Claude decide") |
| Seams only, no instrumentation yet | Empty modules/protocols, no pipeline call sites yet; Phase 7 does both wiring and instrumentation. | |
| Let Claude decide | | (selected) |

**User's choice:** Let Claude decide — resolved to "Thread no-op calls now."
**Notes:** Matches PROJECT.md's already-committed "most complete observability tier" stance. Logging itself (structlog) is real now regardless — that part was never in question, only metrics/tracing.

---

## Test suite gating for testcontainers

| Option | Description | Selected |
|--------|-------------|----------|
| Separate target (recommended) | `make check` stays Docker-free; a new target (e.g. `make test-integration`) mirrors Phase 2's `cluster-verify` precedent. | ✓ (via "Let Claude decide") |
| Fold into `make check` | One command runs everything, including testcontainers-backed tests; `make check` starts requiring Docker. | |
| Let Claude decide | | (selected) |

**User's choice:** Let Claude decide — resolved to "Separate target."
**Notes:** Discovered after the initial answer: `Makefile` (the `test:` target, lines 94–97) already contains a Phase-1-authored comment stating integration/property/e2e tests are deliberately excluded and "Phase 3 must add them to a target that can provide those, and must not assume `make check` already collects them." This upgrades the recommendation from a preference to a standing repository instruction.

---

## Metadata schema migration granularity

| Option | Description | Selected |
|--------|-------------|----------|
| Slice tables only (recommended) | 5 slice tables + `batch_files` + `normalized.customers`. Matches the roadmap's plan-guidance bullet literally. | ✓ (via "Let Claude decide") |
| Whole schema now | All ~19 `meta.*` tables created empty in this phase's migrations. | |
| Let Claude decide | | (selected) |

**User's choice:** Let Claude decide — resolved to "Slice tables only."
**Notes:** Raised because success criterion 1's "creates the whole meta schema" phrasing is in tension with the plan-guidance bullet's literal "land the five tables... plus normalized.customers." Resolved by reading "whole" as referring to the design being coherent/complete (already true, in ARCHITECTURE.md §2), not to every table being materialized as DDL in this phase.

---

## Exception hierarchy scope

| Option | Description | Selected |
|--------|-------------|----------|
| Only Phase 3's branches (recommended) | `DataPlatformError` base + `ConfigurationError`, `StorageError`, `SecretResolutionError` — what this phase's code can actually raise. | ✓ (via "Let Claude decide") |
| Full hierarchy now | All ~10 branches from ARCHITECTURE.md §4.5 exist now, most unraised until their owning phase. | |
| Let Claude decide | | (selected) |

**User's choice:** Let Claude decide — resolved to "Only Phase 3's branches."
**Notes:** Consistent with the project-wide instruction against building for hypothetical future requirements — an exception subclass with no raise site and no test is dead code.

---

## Claude's Discretion

- CSV scope, observability depth, test gating, migration granularity, and exception hierarchy
  scope were all explicitly delegated ("Let Claude decide") and resolved to the option marked
  "(recommended)" at the time it was presented, for the reasons stated above and recorded in
  CONTEXT.md.
- Exact fixture CSV content for proving the minimal `Source`.
- Whether an in-memory fake `MetadataRepository` exists alongside the Postgres-backed one.
- Whether a hypothesis property test covers chunking-boundary correctness now or waits.
- Exact `normalized.customers` column types/constraints beyond the five named business columns.
- The §68-departure ADR's number and wording (next free number is 0008).

## Deferred Ideas

- Whole-`meta`-schema-now migrations — rejected for this phase; revisit only if a later
  migration hits an FK inconsistency the up-front design didn't anticipate.
- Full exception hierarchy now — rejected for this phase; branches land with their owning phase.
- `vault://` `SecretsResolver` implementation — Phase 5.
- Concrete `Publisher` strategies (`merge`, SCD/CDC, `partition_replace`, `full_swap`) — `merge`
  in Phase 4, SCD/CDC in Phase 10.
- Real metrics/tracing backends — Phase 7.
- Airflow-side `config-sync` DAG — Phase 4 or later, once a DAG exists to host it.
