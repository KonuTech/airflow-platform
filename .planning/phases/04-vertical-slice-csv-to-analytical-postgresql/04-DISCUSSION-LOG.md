# Phase 4: Vertical Slice — CSV to Analytical PostgreSQL - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-08-13
**Phase:** 4-Vertical Slice — CSV to Analytical PostgreSQL
**Areas discussed:** File-arrival trigger, Slice CSV content & volume, Pod-kill / retry demonstration, Local dev/demo workflow

---

## File-arrival trigger

| Option | Description | Selected |
|--------|-------------|----------|
| Scheduled polling | DAG on a short schedule interval; `discover_files` lists the bucket each run | |
| Event-driven (MinIO webhook → Airflow API) | MinIO bucket notification calls Airflow's REST API on arrival | |
| Manual/API trigger only | No automatic sensing this phase | |
| Deferrable S3KeySensor | Waits via the triggerer; zero worker-slot cost; no new infra/credentials | ✓ |

**User's choice:** Initially leaned toward the webhook option, but asked "What do you think?" — flagging concern that scheduled polling would "reserve real resources." Claude gave a trade-off analysis (polling isn't actually resource-heavy since `discover_files` is a lightweight task, not a pod; but the webhook option requires a receiver + an Airflow API credential with nowhere real to live before Vault in Phase 5, which is exactly the scope-creep the ROADMAP calls out for this phase) and proposed the deferrable `S3KeySensor` as a third path. User selected it.

**Notes:** Follow-up questions settled: poke interval = 30s; `max_active_runs=1`; sensor watches `customers/*.csv` only (wildcard_match); one DAG run processes all files visible in the same poke window via a frozen manifest (each file → one Dynamic Task Mapping unit), not one-file-one-run.

---

## Slice CSV content & volume

| Option | Description | Selected |
|--------|-------------|----------|
| Synthetic Faker-style data | Generated from a seed, consistent with QUAL-08's corpus policy | ✓ |
| Small hand-authored fixture | Easy to eyeball, but no help for the U3 throughput/RSS spike | |

**User's choice:** Synthetic Faker-style data.

**Notes:** Volume for the U3 spike settled at ~1M rows (recommended — large enough to force multiple staging chunks past the default 500k `checkpoint_threshold_rows`) over ~100k rows (stays under the threshold, never exercises the checkpoint path) or "you decide." A separate small (~50–200 row) fixture was chosen for fast E2E/idempotency tests rather than reusing the 1M-row file everywhere, to keep CI fast. Fixture generation extends the existing `tools/corpus/` seeded generator rather than introducing a new one.

---

## Pod-kill / retry demonstration

| Option | Description | Selected |
|--------|-------------|----------|
| `kubectl delete pod` mid-run | Real crash path against the 1M-row file | ✓ |
| Processor self-kill via test-only env var | Deterministic but less faithful to a genuine external kill | |

**User's choice:** `kubectl delete pod` mid-run.

**Notes:** Made a permanent automated E2E test (not a one-off manual proof) — chosen over the manual-proof alternative because QUAL-06/QUAL-09 and the project's QUAL-07 policy ("every important discovered bug/behavior gains a permanent test") apply directly. A second, dedicated concurrent-SELECT-during-publish test was added to directly prove success criterion #3's "never observes a half-loaded table" clause, rather than leaving it as an inference from the retry test. Mid-load detection uses polling `meta.ingestion_runs.rows_read` with a timeout (never `sleep`, per PITFALLS.md's explicit flakiness warning) rather than scraping pod logs for a marker line. Also settled here: `configs/datasets/customers.yaml` gets an explicit duplicate-file-content policy of `skip` (over `reprocess`), which is what makes success criterion #2's "re-upload under a different name → zero additional rows" true by early-exit.

---

## Local dev/demo workflow

| Option | Description | Selected |
|--------|-------------|----------|
| Makefile target | `make ingest-demo FILE=...`, consistent with existing `make cluster-up`/`make doctor`/`make fixtures` | ✓ |
| Manual `mc cp` + Airflow UI | No new tooling, more repetitive | |
| Scripted demo (Python/bash) | More full-featured, more to maintain | |

**User's choice:** Makefile target.

**Notes:** User explicitly pushed back on a proposed CLI-trigger shortcut ("Do not take shortcuts for demo, quick tests... let sensor do its job") — the target uploads via `mc cp` and lets the real `S3KeySensor` notice the file; it does not also call `airflow dags trigger` to skip the wait. While waiting, the target polls `meta.ingestion_runs` (timeout, not sleep) and prints the receipt (run_id, status, rows_loaded, duration) rather than just printing the Airflow UI URL and leaving the user to watch by hand.

---

## Claude's Discretion

- Exact Makefile target implementation details (how it resolves the run row for a given uploaded file, receipt formatting).
- Whether `tools/corpus/` needs structural changes to support realistic (non-edge-case) Faker-style data, or a new generation path within the same package.

## Deferred Ideas

None — discussion stayed within phase scope. MinIO-webhook-based triggering was evaluated and explicitly rejected for this phase (see File-arrival trigger above), not deferred as a future idea.
