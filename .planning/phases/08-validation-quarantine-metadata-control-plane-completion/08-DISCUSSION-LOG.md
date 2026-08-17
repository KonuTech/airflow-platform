# Phase 8: Validation, Quarantine & Metadata Control-Plane Completion - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-08-17
**Phase:** 8-Validation, Quarantine & Metadata Control-Plane Completion
**Areas discussed:** Quarantine re-drive path (VALID-08), Bad-record strategy assignment, Referential integrity scope (VALID-07), File/manifest integrity gate placement (LOAD-10/11)

---

## Quarantine re-drive path (VALID-08)

| Option | Description | Selected |
|--------|-------------|----------|
| Dedicated redrive mechanism on `rejected_records` | A precise, auditable mechanism operating directly on rejected-record rows | ✓ (refined below) |
| Re-upload as new arrival | Treat a correction as an ordinary new file drop | |
| Both, dataset's choice | Let each dataset config pick | |

**User's choice:** Initially picked the dedicated-mechanism option, then corrected via free text: **not a separate redrive mechanism at all** — reuses the *same* ingestion DAG, triggered as an Airflow backfill run against a corrected file for that batch. `load.strategy: merge` (upsert) means previously-rejected-now-valid rows insert naturally while already-loaded rows just re-upsert harmlessly.

**Notes:** User was explicit: use the term **"backfill"** throughout, not "redrive" or similar. This overrides the ROADMAP/REQUIREMENTS wording (VALID-08 literally says "re-drive path").

| Option | Description | Selected |
|--------|-------------|----------|
| Whole rejection batch for a run/file | Backfill operates at run/file granularity | ✓ |
| Individual rows or arbitrary subset | Fine-grained per-row re-entry | |

**User's choice:** Whole batch for a run/file.

Follow-up: should `rejected_records` be marked resolved/linked when backfilled?
**User's choice:** "Yes, mark rejected_records resolved and linked to the new run" (plain-text confirmation).

Follow-up: does Phase 8 need a 3-state resolution lifecycle (PENDING/REDRIVEN/DISCARDED/ACCEPTED, including a per-row human-override "ACCEPTED" state)?

**User's choice (sharp correction via free text):** "Only batches can flag data rows. You are asking questions if a user could change a status of row manually. That should not happen." — `ACCEPTED` as a per-row manual override was rejected outright.

**Final:** 2 states only — `PENDING` and resolved (via backfill or an explicit batch-level discard action). Both resolution paths are whole-batch operations; no per-row manual state editing is ever permitted.

Follow-up: how does an operator find which file/run to backfill?
**User's choice:** Query `meta.rejected_records`/`meta.files` directly via SQL — no new tooling or view needed.

---

## Bad-record strategy assignment

| Option | Description | Selected |
|--------|-------------|----------|
| Per-rule-type, dataset-configurable | Each rule/rule_type declares its own strategy | ✓ |
| One strategy per dataset | A single blanket strategy | |

**User's choice:** Per-rule-type, dataset-configurable.

| Option | Description | Selected |
|--------|-------------|----------|
| REJECT_RECORD by default (structural failures) | Matches existing RaggedRowGuard behavior | ✓ |
| FAIL_FILE by default | Stricter default | |

**User's choice:** REJECT_RECORD by default — no change to already-working Phase 3/6 code.

| Option | Description | Selected |
|--------|-------------|----------|
| Add a real `quality:` block to customers.yaml | Proves the VALID-01/02/03/04 chain live | ✓ |
| Corpus/fixture-tested only | Keep it unexercised, like other opt-in features | |

**User's choice:** Add a real `quality:` block to customers.yaml.

Follow-up ("More questions") led to a threshold-escalation question:

| Option | Description | Selected |
|--------|-------------|----------|
| Yes, a rejection-rate threshold can escalate the run to FAIL | Circuit breaker on top of row-level strategies | ✓ |
| No, row-level strategy is final | No aggregate escalation | |

**User's choice:** Yes — a run-level rejection-rate threshold can escalate to FAIL even when individual rules are REJECT_RECORD.

| Option | Description | Selected |
|--------|-------------|----------|
| Nothing publishes, whole run rolled back | FAIL means an unambiguous all-or-nothing rollback | ✓ |
| Good rows still publish, FAIL is just an alert | Partial publish on FAIL | |

**User's choice:** Nothing publishes — whole run rolled back via Phase 4's atomic publish pattern.

**Notes (free text mid-discussion):** User raised PostgreSQL table partitioning ("files as partitions on a particular date or datetime"). After exploring further ("More questions" twice), this was resolved as a sub-decision scoped to whether the *new* Phase 8 tables should be partitioned now:

| Option | Description | Selected |
|--------|-------------|----------|
| Plain table now, partition later if needed | Defer partitioning; migration path is well-trodden | ✓ |
| Partition by month from day one | Partition immediately | |

**User's choice:** Plain table now, partition later if needed. The broader partitioning idea (normalized/warehouse target tables by `business_date`) was deferred to Phase 9 (see Deferred Ideas).

---

## Referential integrity scope (VALID-07)

| Option | Description | Selected |
|--------|-------------|----------|
| Add a small second real dataset (orders → customers) | Prove VALID-07 against real DAG execution | ✓ |
| Corpus/fixture-tested only | Claude's original recommendation | |

**User's choice:** Add a small second dataset — overrode Claude's fixture-only recommendation.

| Option | Description | Selected |
|--------|-------------|----------|
| Own separate DAG | `csv_ingest_orders`, mirroring `csv_ingest_customers` | ✓ |
| Extra task inside customers DAG | Bolt onto the existing DAG | |

**User's choice:** Own separate DAG.

| Option | Description | Selected |
|--------|-------------|----------|
| Quarantine the orphan row, load the rest | QUARANTINE_RECORD for orphans | ✓ |
| Warn only, load everything | No quarantine on orphan | |

**User's choice:** Quarantine the orphan row, load the rest.

Follow-up ("More questions") on DAG coupling:

| Option | Description | Selected |
|--------|-------------|----------|
| Fully independent, check current state | Claude's original recommendation | |
| Depend via Airflow Dataset/Asset | Explicit scheduling coupling | ✓ |

**User's choice:** orders DAG depends on customers via an Airflow Dataset/Asset — overrode Claude's "fully independent" recommendation.

Follow-up on schema:

| Option | Description | Selected |
|--------|-------------|----------|
| order_id, customer_id (FK), order_date, amount | Minimal realistic schema | ✓ |
| Claude's discretion | Open-ended | |

**User's choice:** order_id, customer_id (FK), order_date, amount.

---

## File/manifest integrity gate placement (LOAD-10/11)

| Option | Description | Selected |
|--------|-------------|----------|
| Airflow-side, before pod launch | S3 HEAD gate before KubernetesPodOperator | ✓ |
| Pod-side, first pipeline stage | Integrity check inside the dataplat Stage pipeline | |

**User's choice:** Airflow-side, before pod launch.

| Option | Description | Selected |
|--------|-------------|----------|
| Stay unexercised | `_BATCH_COMPLETE` built and tested but not turned on by any live dataset | ✓ |
| Adopt it now on both datasets | Require the marker on customers.yaml and orders.yaml | |

**User's choice:** Stay unexercised — same "opt-in, unexercised" pattern as Phase 6's filename masks.

| Option | Description | Selected |
|--------|-------------|----------|
| File-level rejection, recorded in meta.files | No run/run_id created on gate failure | ✓ |
| Still creates a run, which immediately FAILs | Integrity failures modeled as a run outcome | |

**User's choice:** File-level rejection, recorded in meta.files.

| Option | Description | Selected |
|--------|-------------|----------|
| Object stability check | Two S3 HEAD calls, unchanged size/etag = complete | ✓ |
| Trust S3 atomicity, skip a stability check | No settling check | |

**User's choice:** Object stability check.

| Option | Description | Selected |
|--------|-------------|----------|
| S3 ETag vs re-computed content_sha256 | No externally-supplied checksum expected | ✓ |
| Optional sidecar checksum file | *.sha256 convention | |

**User's choice:** S3 ETag vs re-computed content_sha256, reusing the existing discovery hash column.

---

## Claude's Discretion

- Exact column/index shape for the `resolution_type` field on `meta.rejected_records`.
- Exact naming/shape of the run-level rejection-rate threshold config key under `quality:`.
- Whether the Airflow-side integrity sensor is a custom `@task` or a `PythonSensor`/deferrable sensor.

## Deferred Ideas

- **Table partitioning** (files-as-partitions by date/datetime, and normalized/warehouse target tables by `business_date`) — raised by the user, belongs to Phase 9 (ROADMAP's INCR-04 success criterion). Phase 8's own new tables (`rejected_records`, `validation_results`) stay plain tables for now (see Bad-record strategy assignment section above).
- **Anomaly detection over validation history** (VALID-05/06) — depends on this phase's persisted results existing first; Phase 9's job.
- **Retention/archival of rejected_records/validation_results** — Phase 11 (Operations) concern.
