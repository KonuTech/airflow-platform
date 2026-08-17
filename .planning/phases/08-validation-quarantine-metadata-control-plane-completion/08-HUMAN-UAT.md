---
status: partial
phase: 08-validation-quarantine-metadata-control-plane-completion
source: [08-VERIFICATION.md]
started: 2026-08-17T13:40:00Z
updated: 2026-08-17T13:40:00Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. Deploy this phase's artifacts to the live kind cluster and run the E2E slice tests
expected: Deploy this phase's artifacts to the live kind cluster (apply migrations 0014-0017 to analytics-db, rebuild/redeploy the csv-processor image with configs/datasets/orders.yaml + customers.yaml's quality block baked in, sync the Airflow DAG bundle so csv_ingest_orders.py is picked up, and unseal Vault), then run `pytest tests/e2e/slice/test_referential_orphan.py tests/e2e/slice/test_backfill_reentry.py -m cluster`. test_referential_orphan.py should pass, proving VALID-07 (referential-orphan quarantine + non-orphan publish) live end-to-end. test_backfill_reentry.py's outcome is the key open question — see test 2 below.
result: [pending]

### 2. Investigate whether a content-differing "corrected" file re-upload actually flips its predecessor's meta.rejected_records row from PENDING to REDRIVEN
expected: Either the assertion in test_backfill_resolves_previously_rejected_row holds (VALID-08's documented re-drive path is genuinely proven end-to-end), or it fails because meta.batches.batch_key is a pure function of content_sha256 (dataplat/discovery.py) while resolve_rejected_records_for_batch resolves PENDING rows strictly by batch_id — a corrected (content-different) file discovers under a brand-new batch_id, so the resolve call scoped to the new batch never touches the original PENDING row's batch. This is a Rule-4-territory architecture question (08-14-SUMMARY.md explicitly declines to fix it as a one-line change) that can only be empirically settled once the live cluster is deployed and the e2e test actually executes. The published-data half of VALID-08 (corrected data lands in normalized.customers/orders via ON CONFLICT upsert, independent of batch_id) is already proven by other integration tests and not in question — only the resolution_type/audit-trail bookkeeping for a genuinely content-different correction is uncertain.
result: [pending]

## Summary

total: 2
passed: 0
issues: 0
pending: 2
skipped: 0
blocked: 0

## Gaps
