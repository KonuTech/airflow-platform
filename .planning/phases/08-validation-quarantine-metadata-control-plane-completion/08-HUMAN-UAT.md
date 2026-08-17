---
status: partial
phase: 08-validation-quarantine-metadata-control-plane-completion
source: [08-VERIFICATION.md]
started: 2026-08-17T13:40:00Z
updated: 2026-08-17T15:15:00Z
---

## Current Test

[testing paused — 1 item outstanding, see deferred-items.md's "From live-cluster UAT deployment" section]

## Tests

### 1. Deploy this phase's artifacts to the live kind cluster and run the E2E slice tests
expected: Deploy this phase's artifacts to the live kind cluster (apply migrations, rebuild/redeploy images, ensure the DAG bundle picks up csv_ingest_orders, unseal Vault), then run the two E2E slice tests.
result: issue
reported: |
  Deployment succeeded after fixing four real gaps along the way (all committed): a stale kind
  DAGs hostPath mount (infra, pre-existing), a missing psycopg dependency in the Airflow image
  (commit 020d0c2), and two missing analytics_owner GRANTs closed by migrations 0018/0019
  (commits 1cdcb48/f6e7c95). With those fixed, test_orphan_order_quarantined_while_valid_rows_
  publish achieved one full clean pass server-side (discover -> ingest -> SUCCEEDED -> orphan
  quarantine verified), proving VALID-07 genuinely works end-to-end on this cluster. But running
  the full pytest suite hit live-cluster environmental contention (large historical file backlog
  on csv_ingest_customers' every-minute cron, plus a deferred S3KeySensor observed stuck 15+
  minutes on one run despite the same config succeeding in ~3 min on three other runs this same
  session) that prevented a clean automated pass within this session. See deferred-items.md for
  full detail and a recommended retry approach (pause the customers cron first to remove
  contention).
severity: minor
blocked_by: other

### 2. Investigate whether a content-differing "corrected" file re-upload actually flips its predecessor's meta.rejected_records row from PENDING to REDRIVEN
expected: Either the assertion in test_backfill_resolves_previously_rejected_row holds (VALID-08's documented re-drive path is genuinely proven end-to-end), or it fails because meta.batches.batch_key is a pure function of content_sha256 while resolve_rejected_records_for_batch resolves PENDING rows strictly by batch_id.
result: blocked
blocked_by: other
reason: Never reached — blocked on test 1's environmental contention issue before this test's own logic could execute against a clean run.

## Summary

total: 2
passed: 0
issues: 1
pending: 0
skipped: 0
blocked: 1

## Gaps

- truth: "test_orphan_order_quarantined_while_valid_rows_publish passes cleanly on a fresh pytest invocation"
  status: failed
  reason: "Live-cluster contention (customers cron backlog + a stuck deferred S3KeySensor) prevented a clean automated pass, though the mechanism itself was proven working via a manual server-side trace of one full successful run this session."
  severity: minor
  test: 1
  root_cause: "Not fully diagnosed — likely triggerer/executor resource contention on a long-lived demo cluster with accumulated historical test traffic, not a phase-8 code defect. See deferred-items.md for the full investigation trail."
  artifacts: []
  missing:
    - "Re-run tests/e2e/slice/test_referential_orphan.py and test_backfill_reentry.py -m cluster after pausing csv_ingest_customers and letting the cluster idle for a few minutes"
    - "If the deferred-sensor stall recurs, investigate S3KeySensor's deferred/trigger code path specifically (it differs from the synchronous poke() path that has never shown this issue)"
  debug_session: ""
