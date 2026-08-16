---
phase: 07-observability-metrics-tracing-lineage
plan: 05
subsystem: observability
tags: [opentelemetry, tracing, metrics, w3c-traceparent, otlp, postgresql, dataplat]

# Dependency graph
requires:
  - phase: 07-observability-metrics-tracing-lineage
    plan: 02
    provides: "dataplat.observability.tracing/.metrics real OTel SDK backends (configure()/start_span()/increment()/flush())"
provides:
  - "dataplat.cli.main() extracts an incoming W3C TRACEPARENT into the active OTel context and configures both tracing/metrics backends once, before any subcommand dispatches"
  - "run_ingest() opens its own pipeline.run_ingest span (a genuine child of any extracted parent), captures trace_id/span_id, and persists both onto the claimed meta.ingestion_runs row"
  - "runs_started/runs_finished counters (dataset+stage+status labels) emitted around every claimed run, including on a run-fatal exception, never on a refused claim"
  - "the publish transaction (advisory lock + Publisher.publish() + finalize_publication()) wrapped in its own nested pipeline.publish child span"
affects: [07-04, 07-07, 07-08]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "TRACEPARENT extraction via opentelemetry.propagate.extract() + context.attach(), once near process start, deliberately never detached (one-shot CLI process)"
    - "An outer try/finally (metric-emission-on-every-exit-path) wrapping a pre-existing inner try/finally (heartbeat cleanup) without conflating the two"
    - "In-memory-exporter-backed real TracerProvider (tracing._provider poked directly) for asserting genuine parent/child span relationships in tests, bypassing the public configure(otlp_endpoint=...) API which always builds a network-bound OTLPSpanExporter"

key-files:
  created:
    - tests/unit/test_cli_trace_extraction.py
    - tests/unit/test_run_ingest_trace.py
  modified:
    - packages/dataplat/src/dataplat/cli.py
    - packages/dataplat/src/dataplat/pipeline/run.py
    - packages/dataplat/src/dataplat/metadata/repository.py
    - packages/dataplat/src/dataplat/metadata/postgres.py
    - tests/integration/test_run_ingest.py

key-decisions:
  - "pipeline.publish's span + ctx.db.connection() + conn.transaction() combined into one parenthesized with-statement (span as the outermost context manager) to satisfy ruff's SIM117 rule while keeping the span's duration inclusive of connection acquisition"
  - "runs_started/runs_finished labeled dataset+stage+status only (D-04's bounded label set) -- never run_id/file_id/batch_id"
  - "The new outer try/finally (run_status tracking for runs_finished) is kept strictly separate from the pre-existing inner try/finally (heartbeat cleanup) -- never merged, per the plan's own explicit instruction"

patterns-established:
  - "Pattern: a one-shot CLI process extracts incoming trace context once via context.attach() and deliberately never detaches -- the extracted parent must outlive the whole remaining process lifetime"
  - "Pattern: a function that must emit an unconditional completion metric on every exit path (including exceptions) uses `status = \"failed\"` before an outer try, `status = \"succeeded\"` as the last statement before the normal return, and increments from a `finally` block -- never `except`, so the original exception always propagates unmodified"

requirements-completed: [OBS-08, OBS-10]

duration: 40min
completed: 2026-08-16
---

# Phase 7 Plan 5: run_ingest Tracing & Metrics Summary

**`dataplat.cli.main()` extracts incoming W3C `TRACEPARENT` into the OTel context and configures both backends once; `run_ingest()` opens its own child span, persists `trace_id`/`span_id` onto the claimed `meta.ingestion_runs` row, emits `runs_started`/`runs_finished` on every claimed-run exit path, and wraps its publish transaction in a nested `pipeline.publish` span — closing OBS-10's pod-side trace continuity and D-03's two remaining live gauges.**

## Performance

- **Duration:** ~40 min
- **Completed:** 2026-08-16
- **Tasks:** 3 (all `type="auto"`, `tdd="true"` on Tasks 1-2)
- **Files modified:** 6 (2 created, 4 modified) + 1 diligence-note doc (`deferred-items.md`)

## Accomplishments

- `dataplat.cli.main()` now extracts an incoming `TRACEPARENT` env var into the active OTel context via `opentelemetry.propagate.extract()` (degrading to "no parent" on a malformed value, never raising — T-07-14) and configures both `tracing`/`metrics` OTLP backends exactly once, before `entry_points(group="dataplat.plugins")` ever loads
- `run_ingest()` wraps its entire claim-through-return body in its own `pipeline.run_ingest` span, reads its own `trace_id`/`span_id` via `format_trace_id`/`format_span_id` (never a garbage all-zero-hex string when unconfigured — `None`/`None` instead), and passes both into `claim_ingestion_run`
- `claim_ingestion_run` (Protocol + `PostgresMetadataRepository` implementation) widened with optional `trace_id`/`span_id` keyword args, persisted into the same `UPDATE meta.ingestion_runs` statement that already sets `k8s_pod_name`
- `runs_started`/`runs_finished` counters (D-04's bounded `dataset`+`stage`+`status` label set) emitted around every genuinely claimed run: `runs_started` once at claim time, `runs_finished` unconditionally from a `finally` block (never `except`) so a run-fatal exception still propagates unmodified — neither metric fires on a refused/skipped claim
- The atomic publish transaction (advisory lock + `Publisher.publish()` + `finalize_publication()`) now runs inside its own nested `pipeline.publish` span — OBS-10's "→ PostgreSQL" segment, a genuine child of `pipeline.run_ingest`
- A new integration test proves the whole chain live against testcontainers PostgreSQL: an injected `traceparent`'s trace id survives into `meta.ingestion_runs.trace_id` unmodified, while `span_id` is a genuinely new, well-formed 16-hex-character value

## Task Commits

Each task was committed atomically:

1. **Task 1: cli.py — extract TRACEPARENT, configure both observability backends once** - `3bcf2c5` (feat)
2. **Task 2: run_ingest()'s own span/trace capture, plus runs_started/runs_finished** - `e076c1d` (feat)
3. **Task 3: Integration proof — TRACEPARENT round-trips into meta.ingestion_runs correctly** - `2b56ada` (test)

## Files Created/Modified

- `packages/dataplat/src/dataplat/cli.py` - `_extract_incoming_trace_context()` + `tracing.configure()`/`metrics.configure()` calls in `main()`, before the plugin-loading loop
- `packages/dataplat/src/dataplat/pipeline/run.py` - `run_ingest()` wrapped in `pipeline.run_ingest`/nested `pipeline.publish` spans; `runs_started`/`runs_finished` emission via a new outer try/finally
- `packages/dataplat/src/dataplat/metadata/repository.py` - `claim_ingestion_run` Protocol widened with optional `trace_id`/`span_id`
- `packages/dataplat/src/dataplat/metadata/postgres.py` - `claim_ingestion_run` implementation's `UPDATE` statement widened to set `trace_id`/`span_id`
- `tests/unit/test_cli_trace_extraction.py` - new: proves all 3 TRACEPARENT extraction behaviors (unset, well-formed, malformed)
- `tests/unit/test_run_ingest_trace.py` - new: proves all 6 span/metrics behaviors against fakes (no real DB)
- `tests/integration/test_run_ingest.py` - new test: proves the full TRACEPARENT round-trip live against testcontainers PostgreSQL
- `.planning/phases/07-observability-metrics-tracing-lineage/deferred-items.md` - logs 2 pre-existing, out-of-scope mypy findings discovered while diligence-checking this file

## Decisions Made

- **`pipeline.publish`'s span combined with its connection/transaction into one parenthesized `with` statement.** The plan's action text says "wrap the existing publish-transaction block in a new NESTED `with tracing.start_span(...)` block," which two separately-nested `with` blocks would satisfy literally — but ruff's `SIM117` (already a project-wide lint gate) flags exactly that shape. Combining them into `with (tracing.start_span("pipeline.publish"), ctx.db.connection() as conn, conn.transaction()):` is semantically identical (Python enters left-to-right, exits right-to-left — the span still opens first and closes last, covering the full transaction including connection acquisition) and is lint-clean. Verified via the unit test's `test_publish_span_is_a_genuine_child_of_the_run_ingest_span`, which asserts the real, recorded parent/child relationship directly.
- **`metrics.increment` patched via a direct `from dataplat.observability import metrics` import in tests, not `run_module.metrics`.** `mypy --strict`'s `no_implicit_reexport` doesn't consider `metrics`/`tracing` explicitly re-exported attributes of `dataplat.pipeline.run` (it only imports them, doesn't re-export via `__all__`), so `run_module.metrics.increment` fails a strict type check even though it's the identical module object at runtime. Importing `metrics` directly in the test file sidesteps this cleanly.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] `pipeline.publish`'s nested `with` combined into one parenthesized statement**
- **Found during:** Task 2
- **Issue:** Two separately-nested `with tracing.start_span(...):` / `with ctx.db.connection() as conn, conn.transaction():` blocks (the plan's literal phrasing) trip ruff's `SIM117` ("Use a single `with` statement with multiple contexts instead of nested `with` statements") — a real, enforced lint gate (`make check`) that would otherwise fail the build.
- **Fix:** Combined into `with (tracing.start_span("pipeline.publish"), ctx.db.connection() as conn, conn.transaction()):` — semantically identical entry/exit order, confirmed via a passing parent-child span assertion test.
- **Files modified:** `packages/dataplat/src/dataplat/pipeline/run.py`
- **Verification:** `uv run ruff check` clean; `test_publish_span_is_a_genuine_child_of_the_run_ingest_span` passes
- **Committed in:** `e076c1d` (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking/lint-gate)
**Impact on plan:** Cosmetic syntax difference only — the resulting span nesting, timing and parent/child relationship are unchanged and independently proven by tests. No scope creep.

## Issues Encountered

- **The plan's own literal verification command (`pytest tests/integration/test_run_ingest.py -m integration -q` / `-m integration -x -q -k trace`) selects zero tests in this file.** `test_run_ingest.py`'s existing 8 tests (plan 04-05) — and this plan's new 9th test — carry no `@pytest.mark.integration` marker; only `tests/integration/test_metrics_otlp.py` (plan 07-02) and `tests/property/test_determinism.py` use that marker repo-wide (confirmed via a full-repo grep). This is a pre-existing marker-application inconsistency, not something this plan's changes caused or should silently paper over by adding a module-level marker to 8 files outside this plan's declared scope. The real `make test-integration` Makefile target itself never passes `-m integration` (`pytest tests/integration -q`, no marker filter) — that is the command this plan's own verification was actually run with, and it exits 0 with all 9 tests (8 pre-existing + 1 new) passing. Confirmed via `git stash`/`stash pop` that this marker gap predates this plan.
- **2 pre-existing `mypy --strict` findings in `test_run_ingest.py`'s untouched crash-simulation tests** (`no_implicit_reexport` on `run_module.StagingLoader`/`run_module.resolve_publisher`, from plan 04-05) — found while diligence-checking the file beyond this plan's own declared mypy gate (`packages/dataplat/src/dataplat` only, never `tests/`). Verified pre-existing via `git stash` back to this plan's own prior commit. Logged in `deferred-items.md`, not auto-fixed (outside this task's file scope; same repo-wide bug class already logged for plans 06-02 and 07-02).

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- OBS-10's pod-side half is complete and proven: `meta.ingestion_runs.trace_id`/`.span_id` are no longer always `NULL`, and a real cross-process trace-continuity round-trip is proven against a live testcontainers PostgreSQL. Plan 07-04 (the Airflow KPO pod-spec `TRACEPARENT` injection, the other half of OBS-10) and plan 07-08 (proving the trace live against a real cluster) can build directly on this.
- D-03's three live-gauge signals are now all real: `rows_rejected`/`rows_kept` (plan 07-02, already done), plus this plan's `runs_started`/`runs_finished` — Grafana (plan 07-07) can derive "runs currently in-flight" (`sum(runs_started) - sum(runs_finished)`) and "recent failure rate" (`runs_finished{status="failed"}` over the total) directly from these two monotonic counters.
- No blockers. The pre-existing `mypy`/marker inconsistencies noted above are cosmetic, already logged, and do not block any downstream plan.

---
*Phase: 07-observability-metrics-tracing-lineage*
*Completed: 2026-08-16*

## Self-Check: PASSED

- FOUND: packages/dataplat/src/dataplat/cli.py
- FOUND: packages/dataplat/src/dataplat/pipeline/run.py
- FOUND: packages/dataplat/src/dataplat/metadata/repository.py
- FOUND: packages/dataplat/src/dataplat/metadata/postgres.py
- FOUND: tests/unit/test_cli_trace_extraction.py
- FOUND: tests/unit/test_run_ingest_trace.py
- FOUND: tests/integration/test_run_ingest.py
- FOUND: .planning/phases/07-observability-metrics-tracing-lineage/deferred-items.md
- FOUND commit: 3bcf2c5 (Task 1)
- FOUND commit: e076c1d (Task 2)
- FOUND commit: 2b56ada (Task 3)
