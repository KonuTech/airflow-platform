---
phase: 07-observability-metrics-tracing-lineage
plan: 02
subsystem: observability
tags: [opentelemetry, otlp, metrics, tracing, dataplat, otel-sdk]

# Dependency graph
requires:
  - phase: 03-dataplat-core-library-metadata-control-plane
    provides: the no-op `dataplat.observability.metrics`/`tracing` seams and the two real call sites already threaded through `pipeline/engine.py` (D-03) that this plan gives real backends
provides:
  - a real OTLP-backed `dataplat.observability.metrics.{configure,increment,flush}`, genuine no-op when unconfigured
  - a real OTel-SDK-backed `dataplat.observability.tracing.{configure,start_span,flush}`, genuine non-recording no-op when unconfigured
  - D-04's bounded metric label set (`dataset`+`stage`+`status`) on `RaggedRowGuard.apply()`'s two counters
  - a genuine, wire-level proof (real `ThreadingHTTPServer` decoding real protobuf) that `increment()` reaches an OTLP/HTTP receiver with exactly the D-04 label set
affects: [07-03, 07-04, 07-05, 07-06, 07-07, 07-08]

# Tech tracking
tech-stack:
  added: [opentelemetry-sdk==1.44.0, opentelemetry-exporter-otlp-proto-http==1.44.0]
  patterns:
    - "Module-owned provider singleton (not opentelemetry's own set_tracer_provider()/set_meter_provider() global registry) for a freely re-callable configure(), mirroring logging.py's structlog.configure() precedent"
    - "Monkeypatch the OTLP exporter class's export() method (not the whole class/ABC) in unit tests, to prove real wiring without opening a real network socket"
    - "Explicit provider.shutdown() inside a test body (before returning) to unregister the SDK's own atexit shutdown-on-exit hook, avoiding a doomed real-network flush attempt at interpreter exit once monkeypatch has reverted"

key-files:
  created:
    - tests/unit/observability/__init__.py
    - tests/unit/observability/test_metrics.py
    - tests/unit/observability/test_tracing.py
    - tests/integration/test_metrics_otlp.py
    - .planning/phases/07-observability-metrics-tracing-lineage/deferred-items.md
  modified:
    - packages/dataplat/pyproject.toml
    - packages/dataplat/src/dataplat/observability/metrics.py
    - packages/dataplat/src/dataplat/observability/tracing.py
    - packages/dataplat/src/dataplat/pipeline/engine.py
    - tests/unit/test_pipeline_errors.py
    - uv.lock

key-decisions:
  - "metrics.py/tracing.py own a private module-level TracerProvider/MeterProvider singleton instead of calling opentelemetry.{trace,metrics}.set_{tracer,meter}_provider() -- those functions are documented 'can only be done once' (verified against the installed 1.44.0 source) and would silently strand a later configure() call in the same process"
  - "RaggedRowGuard.apply()'s two metrics.increment() calls carry dataset=/stage=/status= as inline keyword arguments, not a shared **labels dict -- required for the plan's must_haves.key_links structural pattern (metrics\\.increment\\(.*dataset=) to actually find the literal substring 'dataset=' in the file"
  - "tests/integration/test_metrics_otlp.py tolerates 'at least one' identical captured export (not exactly one), per the plan's own acceptance-criteria wording -- explicit provider.shutdown() triggers one additional, harmless duplicate flush beyond the explicit flush() call"

requirements-completed: [OBS-08, OBS-10]

# Metrics
duration: 40min
completed: 2026-08-15
---

# Phase 07 Plan 02: OTel SDK Backends for dataplat.observability.{metrics,tracing} Summary

**Wired `dataplat.observability.metrics`/`tracing`'s Phase-3 no-op seams to real OpenTelemetry SDK backends (OTLP/HTTP), added D-04's bounded `dataset`/`stage`/`status` metric labels to `RaggedRowGuard.apply()`, and proved genuine OTLP/HTTP wire delivery with a real protobuf-decoding receiver.**

## Performance

- **Duration:** ~40 min
- **Started:** 2026-08-15T22:00Z (approx.)
- **Completed:** 2026-08-15T22:39Z
- **Tasks:** 3 (plus one same-plan correction commit)
- **Files modified:** 11 (5 created, 6 modified)

## Accomplishments

- `metrics.py`/`tracing.py` are real, OTLP-backed OTel SDK implementations behind their exact Phase-3 no-op call signatures -- every existing caller (`pipeline/engine.py`'s two `metrics.increment(...)` calls, `tracing.start_span(...)`) keeps working unmodified in shape.
- Genuine no-op parity proven, not merely "configured to drop": unconfigured `start_span()` produces a span with `is_recording() is False` and an invalid span context; unconfigured `increment()` never even instantiates `OTLPMetricExporter`.
- `RaggedRowGuard.apply()`'s two counters (`rows_rejected`/`rows_kept`) now carry D-04's bounded label set (`dataset`+`stage`+`status`) -- never an unbounded identity like `run_id`/`file_id`/`batch_id`.
- A real, local OTLP/HTTP receiver (`http.server.ThreadingHTTPServer`, decoding actual `ExportMetricsServiceRequest` protobuf bytes) observes exactly the labeled metric `increment()` sends, with a permanent regression assertion that no fourth attribute key ever reaches the wire.

## Task Commits

Each task was committed atomically:

1. **Task 1: Real OTel SDK backends for metrics.py and tracing.py** - `e594719` (feat)
2. **Task 2: Bounded labels on the two threaded call sites in pipeline/engine.py** - `dd4efe8` (feat)
3. **Task 3: Integration test -- increment() genuinely reaches an OTLP/HTTP receiver** - `d26d381` (test)
4. **Correction (Task 2 follow-up): revert to inline label kwargs** - `80be5de` (fix)

**Plan metadata:** committed separately below (docs: complete plan)

## Files Created/Modified

- `packages/dataplat/pyproject.toml` - Added `opentelemetry-sdk>=1.44,<2` and `opentelemetry-exporter-otlp-proto-http>=1.44,<2`
- `packages/dataplat/src/dataplat/observability/metrics.py` - Real `configure()`/`increment()`/`flush()`, module-owned `MeterProvider` singleton
- `packages/dataplat/src/dataplat/observability/tracing.py` - Real `configure()`/`start_span()`/`flush()`, module-owned `TracerProvider` singleton
- `packages/dataplat/src/dataplat/pipeline/engine.py` - `RaggedRowGuard.apply()`'s two `metrics.increment()` calls carry `dataset`/`stage`/`status`; `ARG002` suppression removed
- `tests/unit/observability/__init__.py` - New test package (mirrors `tests/unit/detect/`'s trivial shape)
- `tests/unit/observability/test_metrics.py` - 4 tests: no-op-is-genuine, configured-reaches-exporter, counter-cached-by-name, flush-is-safe-unconfigured
- `tests/unit/observability/test_tracing.py` - 5 tests: no-op-is-genuine, configured-is-real, yields-None, safely-re-callable (regression guard for the set-once deviation), flush-is-safe-unconfigured
- `tests/integration/test_metrics_otlp.py` - Real `ThreadingHTTPServer` OTLP/HTTP receiver, decodes real protobuf, asserts the exact D-04 label set reached the wire
- `tests/unit/test_pipeline_errors.py` - `_make_context()`'s `config=None` placeholder became `SimpleNamespace(dataset="test_dataset")` (Rule 1 fix, see Deviations)
- `.planning/phases/07-observability-metrics-tracing-lineage/deferred-items.md` - Logs two pre-existing, out-of-scope malformed `# type: ignore` comments found while diligence-checking mypy on files outside `make typecheck`'s scope
- `uv.lock` - Regenerated for the two new dependencies (`uv lock`)

## Decisions Made

- **Module-owned provider singleton, not OTel's global registry.** `opentelemetry.trace.set_tracer_provider()`/`opentelemetry.metrics.set_meter_provider()` are both documented "can only be done once, a warning will be logged if any further attempt is made" -- verified directly against the installed `opentelemetry-api==1.44.0` source (`_TRACER_PROVIDER_SET_ONCE`/`_METER_PROVIDER_SET_ONCE`, both `Once()`-guarded). Routing `configure()` through them would make it non-reconfigurable within one process -- breaking both this plan's own two-scenario test suite (no-op then configured, both exercised as separate test functions in one pytest session) and any future legitimate reconfiguration. `metrics.py`/`tracing.py` instead hold a private module-level provider, updated unconditionally on every `configure()` call and read directly by `increment()`/`start_span()` -- mirroring `logging.py`'s `structlog.configure()` precedent (freely re-callable, immediate effect) and `secrets/resolver.py`'s existing module-level-singleton idiom (`global _client  # noqa: PLW0603`). `start_as_current_span()`'s "current span" propagation is `contextvars`-based and entirely independent of which `TracerProvider` instance produced the `Tracer`, so a downstream `opentelemetry.trace.get_current_span()` read (a later plan's KPO-pod entrypoint) is unaffected. Proven live: `tests/unit/observability/test_tracing.py::test_configure_is_safely_re_callable_within_one_process` configures a real endpoint then reconfigures back to `None` within one test and observes both transitions actually take effect -- which would fail under the literal `set_tracer_provider()` approach.
- **Inline label kwargs, not a shared `**labels` dict, on `RaggedRowGuard.apply()`'s two calls.** An intermediate refactor (committed, then reverted) factored `dataset`/`stage` into one shared dict, unpacked via `**labels` at each call site -- this satisfied Task 2's own prose grep acceptance criteria but silently broke the plan's `must_haves.key_links` structural pattern (`metrics\.increment\(.*dataset=`), since a dict-literal (`"dataset": ctx.config.dataset`) never writes the literal substring `dataset=` anywhere in the file. Reverted to inline `dataset=ctx.config.dataset, stage=self.name, status="..."` kwargs on both calls -- verified the `key_links` pattern now matches under `re.DOTALL` (the realistic mode for a multi-line-call-aware structural checker). See Deviations for the resulting tension with Task 2's own literal grep.
- **Integration test tolerates "at least one" identical export, matching the plan's own wording.** `test_metrics_otlp.py` calls `metrics.flush()` (per Task 3's action text) and then `metrics._provider.shutdown()` (this plan's own addition, to unregister the SDK's atexit hook before the fake receiver's socket closes) -- `shutdown()` performs one more internal flush, so the receiver legitimately captures 2 identical `ExportMetricsServiceRequest`s from one `increment()` call. The plan's own acceptance-criteria text explicitly anticipates this ("captured exactly one (or at least one)"); the test asserts every captured data point agrees rather than requiring exactly one, which is a *stronger* proof than picking just the first.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `opentelemetry.trace/metrics`'s "set-once" global registry would silently break re-configuration**
- **Found during:** Task 1, while implementing `configure()` per the plan's literal action text (which names `opentelemetry.trace.set_tracer_provider(...)` and `trace.get_tracer(__name__)` directly)
- **Issue:** Verified live against the installed `opentelemetry-api==1.44.0` source that `set_tracer_provider()`/`set_meter_provider()` are `Once()`-guarded -- a second call in the same process is silently ignored (warning-logged only). This plan's own Task 1 `<behavior>` block describes two `configure()` scenarios (unconfigured, then configured) that pytest would run as separate test functions in one process, which would have made the second scenario's `configure()` call a no-op against the OTel global registry, leaving `start_span()`/`increment()` permanently bound to whichever endpoint configured first.
- **Fix:** `metrics.py`/`tracing.py` hold their own private module-level provider singleton instead of ever calling `set_tracer_provider()`/`set_meter_provider()`. `increment()`/`start_span()` read that singleton directly.
- **Files modified:** `packages/dataplat/src/dataplat/observability/metrics.py`, `packages/dataplat/src/dataplat/observability/tracing.py`
- **Verification:** `tests/unit/observability/test_tracing.py::test_configure_is_safely_re_callable_within_one_process` proves both directions of reconfiguration take effect within one test; full suite (`uv run pytest tests/unit/observability -q`) 9/9 passing.
- **Committed in:** `e594719` (Task 1 commit)

**2. [Rule 1 - Bug] `RaggedRowGuard.apply()`'s new `ctx.config.dataset` read broke 5 existing pipeline tests**
- **Found during:** Task 2, running the task's own required regression command (`uv run pytest tests/unit -k pipeline -q`) immediately after adding the D-04 labels
- **Issue:** `tests/unit/test_pipeline_errors.py::_make_context()` built `PipelineContext(config=None, ...)` (a deliberate placeholder, since no code previously read `ctx.config`). Task 2's new `dataset=ctx.config.dataset` argument raised `AttributeError: 'NoneType' object has no attribute 'dataset'` for every test exercising `RaggedRowGuard.apply()`.
- **Fix:** `_make_context()`'s `config=None` placeholder became `config=SimpleNamespace(dataset="test_dataset")` -- a minimal stand-in exposing only the one attribute now genuinely read, rather than constructing a full `DatasetConfig` (which would need `source`/`deduplication`/`load`/`batching`/`columns`, all still irrelevant to this file's tests). Module and function docstrings updated to reflect `config.dataset` is no longer unused.
- **Files modified:** `tests/unit/test_pipeline_errors.py`
- **Verification:** Failure reproduced first (`AttributeError` on 5/6 tests), then `uv run pytest tests/unit -k pipeline -q` -- 6/6 passing after the fix; full `uv run pytest tests/unit tests/regression -q --no-cov` -- 397/397 passing (up from 388, the 9 new observability tests).
- **Committed in:** `dd4efe8` (Task 2 commit)

**3. [Rule 1 - Bug, self-correction] `**labels` dict refactor broke the plan's `must_haves.key_links` structural pattern**
- **Found during:** A post-Task-3 diligence re-read of the plan's own `must_haves` frontmatter block
- **Issue:** Task 2's literal acceptance-criteria grep (`metrics.increment("rows_rejected"` matching on one line, with `dataset=ctx.config.dataset, stage=self.name, status="rejected"` visible) cannot be satisfied by any formatting that also passes this project's enforced `ruff format --check` gate at 100 chars/line -- the unwrapped single line is 121/109 characters for the two calls, and `ruff format`'s own wrapping algorithm always separates the callee from its arguments once wrapping is engaged. The first fix (a shared `labels = {"dataset": ..., "stage": ...}` dict, unpacked via `**labels`) made both calls fit on one line each, satisfying the grep's anchor -- but it also removed the literal substring `dataset=` from the file entirely (dict-literal syntax uses `:`, not `=`), which broke the plan's separately-declared, more consequential `must_haves.key_links` pattern (`metrics\.increment\(.*dataset=`), verified failing under both `re.search` and `re.search(..., re.DOTALL)`.
- **Fix:** Reverted to inline `dataset=`/`stage=`/`status=` keyword arguments on both calls, letting `ruff format` fully explode each call across 7 lines. Verified the `key_links` pattern now matches under `re.DOTALL` (the realistic mode for a checker designed to tolerate wrapped multi-line calls); Task 2's own single-line grep still does not match under any `ruff format`-compliant formatting at these argument lengths -- documented as an accepted, unresolvable tension between two verification methods within the same plan, with the more structurally-binding one (`must_haves.key_links`) prioritized.
- **Files modified:** `packages/dataplat/src/dataplat/pipeline/engine.py`
- **Verification:** `python3 -c "import re; ... re.search(pattern, content, re.DOTALL)"` -- `True`; `uv run pytest tests/unit tests/regression -q --no-cov` -- 397/397 passing, unchanged from before the correction.
- **Committed in:** `80be5de` (correction commit)

**4. [Rule 1 - Bug] `metrics._provider`/`tracing._provider`'s atexit shutdown hook would attempt a real network call after `monkeypatch` reverts**
- **Found during:** Task 1/3, while writing the "configured" test scenarios
- **Issue:** `opentelemetry.sdk.{trace.TracerProvider, metrics.MeterProvider}` both default `shutdown_on_exit=True`, registering an `atexit` hook on every instance. `pytest`'s `monkeypatch` fixture auto-reverts a patched `export()` method at the end of each test -- meaning a provider left configured with a monkeypatched (fake, no-network) exporter would, at true interpreter exit (after `monkeypatch` has already reverted it), attempt one real, doomed network call against the test's already-closed loopback port, adding multi-second delays and stderr noise to the whole test session's teardown.
- **Fix:** Every test that configures a real endpoint calls `{metrics,tracing}._provider.shutdown()` as its own last line, synchronously within the test body (guaranteed to run before any fixture teardown, including `monkeypatch`'s own) -- `.shutdown()` both performs one final flush and unregisters the `atexit` handler.
- **Files modified:** `tests/unit/observability/test_metrics.py`, `tests/unit/observability/test_tracing.py`, `tests/integration/test_metrics_otlp.py`
- **Verification:** Timed test runs show no multi-second delay or "Failed to export" stderr noise (`tests/unit/observability` -- 0.31-0.63s wall time for 9 tests; `test_metrics_otlp.py` -- 2.3-7.0s for 1 test, no timeout retries).
- **Committed in:** `e594719`, `d26d381` (Tasks 1 and 3 commits, where each affected test was authored)

---

**Total deviations:** 4 auto-fixed (3 Rule 1 bug fixes, 1 Rule 1 self-correction)
**Impact on plan:** All four were necessary for correctness (genuine no-op/reconfiguration semantics, no `AttributeError` regressions, the plan's own machine-checkable `key_links` contract actually matching, no test-session teardown flakiness). No scope creep -- every fix stayed within files this plan's tasks already touch or explicitly lists.

## Issues Encountered

- **`tests/unit/test_pipeline_errors.py`/`tests/unit/test_logging_config.py` carry pre-existing, out-of-scope mypy gaps.** While diligence-checking `mypy` on files beyond Task 1's own scoped acceptance criterion (`mypy packages/dataplat/src/dataplat/observability`), found `tests/unit/test_pipeline_errors.py`'s `# type: ignore[arg-type]` comments use a malformed-per-mypy syntax (same bug class already logged in `06-universal-csv-engine-schema-contracts-normalization/deferred-items.md` for a different file) and `tests/unit/test_logging_config.py:101-103` trips mypy's `func-returns-value` check on `metrics.increment(...) is None`. Both confirmed pre-existing via `git stash` (identical errors reproduce against the pre-plan file state) and out of `make typecheck`'s actual scope (`TYPECHECK_PATHS` never includes `tests/`). Logged to `.planning/phases/07-observability-metrics-tracing-lineage/deferred-items.md`, not fixed (SCOPE BOUNDARY).

## User Setup Required

None - no external service configuration required. `configure(otlp_endpoint=...)` is a library-level function; wiring it to a real, deployed OTel Collector endpoint is a later Phase 7 plan's responsibility (this plan proves the library half works against a throwaway test-local receiver only).

## Next Phase Readiness

- `dataplat.observability.metrics`/`tracing` are ready for a later plan to point `configure(otlp_endpoint=...)` at the real OTel Collector Service once it's deployed (D-01's OTLP-not-StatsD decision for `dataplat`'s own metrics is now implemented, not just decided).
- `RunContext.trace_id`/`span_id` (already modeled per Phase 3, per 07-CONTEXT.md's canonical refs) remain unpopulated -- W3C `traceparent` extraction on the KPO-pod entrypoint side (Pattern 2 in 07-RESEARCH.md) and the Airflow-side `TracingKubernetesPodOperator` injection (Pattern 1) are explicitly out of this plan's scope, deferred to whichever later Phase 7 plan builds the Airflow→pod trace-context bridge (07-RESEARCH.md names this as needing an early spike given its own MEDIUM-confidence assumption A3).
- No blockers for later plans in this phase: the two library-side seams this plan wires are exactly the foundation 07-CONTEXT.md's Phase Boundary describes ("wire the existing no-op `dataplat.observability.metrics`/`tracing` seams to real backends").

## Self-Check: PASSED

All 11 claimed files verified present via `ls -la` (deferred-items.md, pyproject.toml,
metrics.py, tracing.py, engine.py, test_metrics_otlp.py, observability/__init__.py,
test_metrics.py, test_tracing.py, test_pipeline_errors.py, uv.lock). All 4 claimed
commit hashes (`e594719`, `dd4efe8`, `d26d381`, `80be5de`) verified present via
`git log --oneline -6`.

---
*Phase: 07-observability-metrics-tracing-lineage*
*Completed: 2026-08-15*
