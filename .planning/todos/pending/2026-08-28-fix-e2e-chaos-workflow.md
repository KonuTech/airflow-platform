---
created: 2026-08-28T14:56:20Z
title: Fix e2e-chaos.yml (QUAL-15 chaos suite) failing on every push
area: testing
files:
  - .github/workflows/e2e-chaos.yml
  - tests/e2e/chaos/test_database_unavailable.py:260
  - tests/e2e/chaos/test_duplicate_batch.py:105
  - tests/e2e/chaos/test_invalid_encoding.py:188
  - tests/e2e/chaos/test_malformed_csv.py:135
  - tests/e2e/chaos/test_minio_unavailable.py:318
  - tests/e2e/chaos/test_oom.py:219
  - tests/e2e/chaos/test_pod_crash.py:198
  - tests/e2e/chaos/test_vault_unavailable.py:278
---

## Problem

`e2e-chaos.yml`'s `chaos-verify` target (QUAL-15's dedicated-cluster chaos
suite, covering the 9 `tests/e2e/chaos/*` files plus 2 `tests/e2e/vault`
scenarios) has failed on **every single push since before
`debug/ci-pipeline-ingestion-timeout` began** — confirmed back to run
`624cf4f2` on 2026-08-24, i.e. it long predates that debug session and is not
a regression from any of its rounds. It is a third, separate, pre-existing
broken workflow — distinct from both `cluster-slice-verify` (`e2e-full.yml`,
the target of the `ci-pipeline-ingestion-timeout` debug session) and the
offline testcontainers "CI" workflow already flagged out of scope at the
start of that session.

Observed failure sample (run `33181630925`, headSha `3db1fde`, 8 of the
suite's tests):

**Dominant pattern — 6 of 8 failures, one shared root cause:**
`test_database_unavailable`, `test_duplicate_batch`, `test_malformed_csv`,
`test_minio_unavailable`, `test_pod_crash`, `test_vault_unavailable` all fail
at their own setup assertion with variants of:

```
AssertionError: normalized.customers has fewer than 20 rows on this live
cluster -- this test needs prior customers ingestion to have already happened
assert 0 == 20
```

or

```
AssertionError: no CURRENT (valid_to IS NULL) meta.config_versions row for
dataset='customers' -- this test needs the dataset already synced at least once
assert None is not None
```

This dedicated chaos cluster is never getting its `customers` dataset seeded
(0 rows, no current config_version) before these individual chaos scenarios
run. Something that used to seed/sync `customers` on this cluster — a setup
job, a prerequisite `make` target, or an assumption about workflow ordering
— is broken or missing.

**Two distinct, unrelated symptoms also present in the same run (not part of
the seeding pattern):**

- `test_invalid_encoding.py::test_windows1250_customers_file_is_detected_and_correctly_decoded`
  — the real production `detect_encoding()` call returns
  `source='undetermined', confidence=0.0` instead of `source='detected'` for
  a cp1250 fixture with no BOM/contract override. A genuine detection-logic
  question, independent of the seeding issue.
- `test_oom.py::test_oom_pod_dies_cleanly_and_leaves_no_partial_published_rows`
  — the OOM probe pod terminates with reason `'Completed'` instead of
  `'OOMKilled'`. The test's own assertion message already suggests the
  hypothesis: the 256Mi memory ceiling may no longer be undersized for this
  cluster's current staged-data volume.

## Solution

TBD. Start a **new, separate `/gsd:debug` session** scoped specifically to
`e2e-chaos.yml` once the current `debug/ci-pipeline-ingestion-timeout`
session closes out — do not fold this into that session; it's a different
workflow with (at least) two structurally different root causes:

1. Root-cause and fix the customers-seeding gap for the dedicated chaos
   cluster (likely the highest-leverage fix — clears 6 of 8 observed
   failures at once).
2. Investigate the encoding-detection `'undetermined'` result independently
   — check whether it's a genuine detector regression or a fixture/sample
   issue.
3. Investigate whether the OOM probe's 256Mi ceiling needs raising to match
   current staged-data volume, or whether something changed to make the
   publish step lighter-weight than the probe assumes.

Re-run `gh run list --workflow=e2e-chaos.yml` for the current failure
signature before starting, since more pushes will have landed by the time
this is picked up.
