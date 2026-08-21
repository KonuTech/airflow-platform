# Phase 10: Slowly Changing Dimensions - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-08-21
**Phase:** 10-Slowly Changing Dimensions
**Areas discussed:** Dimension scope, Delete detection, Effective dating, Late-correction/replay
proof, Surrogate key, Concurrency, Table shape

---

## Dimension Scope (which dataset/columns get SCD2)

| Option | Description | Selected |
|--------|-------------|----------|
| customers.yaml is the SCD2 dimension | name/country/birth_date can genuinely change; orders stays as-is (immutable events) | ✓ |
| Both customers and orders get SCD2 | Treat order attributes (e.g. amount corrections) as versioned history too | |
| Column-by-column Type assignment for customers | Decide per-column Type rather than uniform treatment | ✓ |

**User's choice:** Both "customers.yaml is the SCD2 dimension" and "column-by-column Type assignment" selected together (multiSelect).
**Notes:** Led to a follow-up round assigning explicit Types per column.

### Type-2 columns
| Option | Description | Selected |
|--------|-------------|----------|
| name + country | Realistic changes (legal name, relocation), avoids birth_date's ambiguity | ✓ |
| name + country + birth_date | Track all three as full history | |

**User's choice:** name + country.

### birth_date treatment
| Option | Description | Selected |
|--------|-------------|----------|
| Type 1 — overwrite, no history | Incoming change treated as a data-quality correction | ✓ |
| Type 0 — immutable, first value wins | Later values ignored entirely | |
| Type 2 — full history | Only relevant if included in Type-2 set above | |

**User's choice:** Type 1 — overwrite, no history.
**Notes:** → CONTEXT.md D-01, D-02.

---

## Delete Detection (SCD-08)

| Option | Description | Selected |
|--------|-------------|----------|
| Yes — treat each customers.csv as a full snapshot | First phase to act on `change_semantics: "snapshot"` (grep-confirmed zero consumers today) | ✓ |
| Build the mechanism, leave it unexercised | Same "opt-in, unexercised" pattern as `_BATCH_COMPLETE` | |

**User's choice:** Yes — full snapshot interpretation.

### DELETE semantics default
| Option | Description | Selected |
|--------|-------------|----------|
| invalidate | Close out the current SCD2 row (valid_to set, is_current=false) | ✓ |
| new_record | Insert a tombstone version | |
| ignore | No signal from absence | |

**User's choice:** invalidate.

### Mass-delete circuit breaker (raised by Claude mid-discussion after finding the delete-safety risk)
| Option | Description | Selected |
|--------|-------------|----------|
| Yes — add a mass-delete circuit breaker | Mirrors Phase 8 D-10's rejection-rate breaker; FAIL if too many customers vanish in one snapshot | ✓ |
| No — invalidate whatever the snapshot says | No threshold, trust every snapshot at face value | |

**User's choice:** Yes — add the circuit breaker.
**Notes:** → CONTEXT.md D-04, D-05, D-06.

---

## Effective Dating (SCD-06)

| Option | Description | Selected |
|--------|-------------|----------|
| Reuse event_ts as the SCD2 effective-date source | Same column already driving Phase 9's watermark/dedup ordering | ✓ |
| Introduce a separate effective-date column/config | Decoupled from event_ts | |

**User's choice:** Reuse event_ts.
**Notes:** → CONTEXT.md D-03.

---

## Late-Correction / Replay Proof (SCD-07, SCD-09, SCD-10)

| Option | Description | Selected |
|--------|-------------|----------|
| Small dedicated SCD fixture set | Purpose-built, easier to reason about precisely | |
| Extend Phase 9's 2-year backfill corpus | One continuous multi-phase story, matches established "prove at real scale" pattern | ✓ |

**User's choice:** Extend Phase 9's 2-year corpus (against the initial recommendation).

### Replay-idempotency proof shape
Initial question was unclear to the user ("I dont fully understand the question") — reframed with a
plain-language explanation before re-asking.

| Option | Description | Selected |
|--------|-------------|----------|
| Re-run the whole 2-year backfill a second time, assert zero new SCD2 versions | Mirrors Phase 4's "re-run → zero additional rows" pattern | ✓ |
| Re-run only the single logical date containing the change | Cheaper/faster, narrower proof | |

**User's choice:** Re-run the whole 2-year backfill a second time.
**Notes:** → CONTEXT.md D-11, D-12.

---

## Surrogate Key

| Option | Description | Selected |
|--------|-------------|----------|
| BigInteger + Identity, matching every existing table | Consistency with the codebase's one actual convention over STACK.md's unused uuidv7() recommendation | ✓ |
| uuidv7() per STACK.md | First table to actually adopt STACK.md's original recommendation | |

**User's choice:** BigInteger + Identity.
**Notes:** → CONTEXT.md D-09.

---

## Concurrency

| Option | Description | Selected |
|--------|-------------|----------|
| Inherit Phase 9's proof, no dedicated SCD concurrency test | Same pg_advisory_xact_lock primitive already proven safe | |
| Add a dedicated live concurrent-SCD test | SCD2's exclusion-constraint + recompute logic is new code, deserves its own proof | ✓ |

**User's choice:** Add a dedicated live concurrent-SCD test (against the recommendation).
**Notes:** → CONTEXT.md D-10.

---

## Table Shape

Presented first as an abstract choice, then re-presented with a full worked before/after example
(two-row-per-customer table contents for both options) after the user asked for elaboration.

| Option | Description | Selected |
|--------|-------------|----------|
| New table, e.g. normalized.dim_customers | Additive, zero risk to existing consumers (10M+ live rows, Phase 7 lineage view, Grafana, Phase 9 reconciliation) | |
| Migrate normalized.customers in place to SCD2 shape | Textbook single-table SCD2 design; existing consumers must be updated and re-proven | ✓ |

**User's choice:** Migrate in place (against the recommendation), after requesting and receiving a
concrete worked example of both options' row-level consequences.

### Consumer-fix scope confirmation
| Option | Description | Selected |
|--------|-------------|----------|
| Yes — all three consumers get updated and proven in this phase | meta.v_customers_lineage, Grafana dashboards, Phase 9 reconciliation all in scope | ✓ (later corrected) |
| No — only fix what's proven broken, defer the rest | | |

**User's choice:** Yes, all three — but Claude subsequently found (via reading
`helm/values/local/monitoring.yaml`) that Grafana dashboards query only `meta.ingestion_runs`, never
`normalized.customers`, so "Grafana" was factually wrong as a consumer. Corrected with the user and
re-confirmed:

| Option | Description | Selected |
|--------|-------------|----------|
| Yes — drop Grafana, keep v_customers_lineage + reconciliation as the two consumers to fix | Matches what the code actually shows | ✓ |
| Still verify Grafana explicitly in this phase, just in case | Add a verification step rather than trusting today's grep | |

**User's choice:** Yes — drop Grafana, keep the two verified consumers.
**Notes:** → CONTEXT.md D-07, D-08 (final, corrected version).

---

## Claude's Discretion

- Exact validity-range PostgreSQL type/representation and the literal `valid_to_current` sentinel value.
- Exact mass-delete circuit-breaker threshold value (Phase 8's 10% cited as a reference, not locked).
- Which dbt-vocabulary change-detection strategy (`timestamp` vs `check`) drives the hash comparison.
- Whether the D-06 mass-delete-circuit-breaker fixture is folded into the D-11 2-year corpus run or proven separately.
- How SCD2 versioning behaves across the 2-year corpus's existing deliberate schema-version change.
- Exact module/task shape for the SCD Publisher.

## Deferred Ideas

None — CDC's exclusion was handled as a ROADMAP.md/REQUIREMENTS.md edit earlier in this session,
before this discussion began, not raised as a mid-discussion deferral.
