---
phase: 07-observability-metrics-tracing-lineage
plan: 04
subsystem: infra
tags: [airflow, docker, kubernetes, opentelemetry, tracing, helm, kpo, w3c-traceparent]

# Dependency graph
requires:
  - phase: 07-observability-metrics-tracing-lineage (plan 07-03)
    provides: "the standalone OTel Collector + Tempo charts, live in the monitoring namespace, with a proven real Service DNS (otel-collector-opentelemetry-collector.monitoring.svc.cluster.local)"
  - phase: 07-observability-metrics-tracing-lineage (plan 07-02)
    provides: "dataplat.observability.metrics/tracing wired to real OTLP-backed SDK objects, ready to receive a propagated parent trace context"
provides:
  - "docker/airflow/Dockerfile and make image-airflow -- the custom apache-airflow[otel] image, was .gitkeep since Phase 1"
  - "TracingKubernetesPodOperator (airflow/dags/_common/tracing_kpo.py) -- W3C traceparent injection via build_pod_request_obj() override"
  - "csv_ingest_customers.py's ingest task as the OBS-10 trace root (D-12); discover stays a plain KubernetesPodOperator"
  - "Airflow's own metrics on StatsD in both local/ci profiles (D-02); OTel tracing enabled local-only (D-16)"
affects: [07-05, 07-08]

# Tech tracking
tech-stack:
  added: ["apache-airflow[otel]==3.3.0 (custom Airflow image only, not the workspace uv.lock)"]
  patterns:
    - "KubernetesPodOperator subclass overriding build_pod_request_obj() to inject per-execution values at task-run time, not DAG-parse time (RESEARCH.md Pitfalls 2/3)"
    - "Genuinely-static env vars (collector endpoint) live in common_kpo_kwargs(); genuinely-per-execution values (TRACEPARENT) live in an operator subclass -- never mixed"

key-files:
  created:
    - docker/airflow/Dockerfile
    - airflow/dags/_common/tracing_kpo.py
    - tests/unit/test_tracing_kpo.py
  modified:
    - Makefile
    - helm/values/local/airflow.yaml
    - helm/values/ci/airflow.yaml
    - helm/versions.env
    - airflow/dags/_common/kpo.py
    - airflow/dags/csv_ingest_customers.py
    - tests/policy/test_dag_thinness.py
    - tests/policy/test_values_profiles.py
    - tests/unit/conftest.py

key-decisions:
  - "AIRFLOW_IMAGE_TAG/defaultAirflowTag pinned to 8b01cc1 (the git SHA of the commit introducing docker/airflow/Dockerfile) -- a real, reproducible reference, but NOT yet a pushed image (no docker/kubectl access in this sandboxed worktree); a follow-up `make image-airflow` + re-sync of the three tag sources is required before `helm upgrade` runs live (see Next Phase Readiness)."
  - "OTel Collector OTLP/HTTP endpoint resolved to the REAL live Service DNS (otel-collector-opentelemetry-collector.monitoring...) from 07-03-SUMMARY.md's own verified Key Decisions, not the shorter placeholder name the plan's own prose guessed -- confirms the plan's own 'do not guess without checking' instruction mattered."
  - "Extended tests/policy/test_values_profiles.py's _is_monitoring_enablement predicate (Rule 1/3) to recognize config.traces.* and the observability-only top-level env key as the same already-argued divergence axis -- the plan called this axis 'already-permitted' but the actual predicate did not yet implement that for this chart."

patterns-established:
  - "TDD RED/GREEN commit pair for a tdd=true task: temporarily move the not-yet-committed implementation file aside, commit the test alone (genuine collection-time ModuleNotFoundError, not asserted in prose), restore, commit the implementation separately."

requirements-completed: [OBS-10]

duration: 35min
completed: 2026-08-16
---

# Phase 7 Plan 04: Airflow-Side W3C Trace Propagation Summary

**Custom `apache-airflow[otel]` image plus a `KubernetesPodOperator` subclass that injects a real, per-execution W3C `traceparent` into `ingest`'s launched pods via `build_pod_request_obj()`, proven by 4 offline unit tests against the real `opentelemetry-sdk` and `apache-airflow-providers-cncf-kubernetes` packages.**

## Performance

- **Duration:** ~35 min
- **Started:** 2026-08-16T04:19:00Z (approx.)
- **Completed:** 2026-08-16T04:53:10Z
- **Tasks:** 3 completed (Task 3 is `tdd="true"`, executed as a genuine RED→GREEN pair)
- **Files modified:** 13 (3 created, 10 modified), across 5 commits

## Accomplishments

- `docker/airflow/Dockerfile` exists for the first time since Phase 1's `.gitkeep` placeholder, layering `apache-airflow[otel]==3.3.0` (under Airflow's own pinned constraints URL) on the stock image -- closing the gap RESEARCH.md verified live: the stock `apache/airflow:3.3.0-python3.12` image contains zero `opentelemetry` packages.
- `make image-airflow` mirrors `image-csv-processor`'s exact build/tag/push shape (two inline `git rev-parse --short HEAD` calls, never a floating tag).
- `TracingKubernetesPodOperator` (`airflow/dags/_common/tracing_kpo.py`) overrides `build_pod_request_obj()` -- the only mechanism proven viable in RESEARCH.md (not `common_kpo_kwargs()`, evaluated at DAG-parse time before any span exists; not `env_vars` Jinja templating, with a documented multi-year history of not working reliably). `ingest` (D-12's trace root) now uses it; `discover` stays a plain `KubernetesPodOperator`, byte-for-byte unchanged at every other argument.
- Airflow's own metrics are on StatsD in **both** `local` and `ci` profiles (D-02); OTel tracing (`config.traces.otel_on`) is enabled **local-only** (D-16, no OTel Collector deployed in CI).
- `make manifests` (live, network-verified in this session) renders and `kubeconform -strict`-validates both profiles across all 8 pinned charts: 181 resources, 159 valid, 0 invalid, 0 errors.

## Task Commits

Each task was committed atomically (Task 3 is `tdd="true"`, so it has a genuine RED/GREEN pair):

1. **Task 1: docker/airflow/Dockerfile and make image-airflow** - `8b01cc1` (feat)
2. **Task 2: Helm values wiring** - `0d1f6ec` (feat)
3. **Task 3, RED: failing test for TracingKubernetesPodOperator** - `42269fd` (test)
4. **Task 3, GREEN: TracingKubernetesPodOperator implementation** - `c341c46` (feat)
5. **Task 3 follow-up fix: ORCH-06 line-budget regression** - `0edfd0a` (fix)

_No separate plan-metadata commit yet -- this SUMMARY.md is committed as part of this same execution, per worktree-mode instructions (STATE.md/ROADMAP.md excluded; the orchestrator owns those after merge)._

## Files Created/Modified

- `docker/airflow/Dockerfile` - custom `apache-airflow[otel]==3.3.0` image, git-SHA-tagged OCI labels, no `WORKDIR`/`ENTRYPOINT` override
- `Makefile` - new `image-airflow` target (build/tag/push, added to `.PHONY`)
- `helm/values/local/airflow.yaml` - `defaultAirflowRepository`/`defaultAirflowTag` repointed to the custom image; `statsd.enabled: true` (+resources); new `config.traces` block (`otel_on`, `otel_application`) and top-level `env: OTEL_EXPORTER_OTLP_ENDPOINT`
- `helm/values/ci/airflow.yaml` - same image repoint + `statsd.enabled: true` (+resources); deliberately NO `config.traces`/`env` (D-16)
- `helm/versions.env` - `AIRFLOW_IMAGE_TAG` kept in agreement with both `defaultAirflowTag` values
- `airflow/dags/_common/kpo.py` - new static `OTEL_EXPORTER_OTLP_ENDPOINT` env var in `common_kpo_kwargs()`, for both `discover` and `ingest`
- `airflow/dags/_common/tracing_kpo.py` (new) - `TracingKubernetesPodOperator`, overriding `build_pod_request_obj()`
- `airflow/dags/csv_ingest_customers.py` - `ingest` now uses `TracingKubernetesPodOperator.partial(...)`; re-wrapped two pre-existing comment/docstring paragraphs to stay under the 150-line ORCH-06 budget
- `tests/policy/test_dag_thinness.py` - `tracing_kpo.py` added to `_EXEMPT_FROM_IMPORT_CHECK` by name
- `tests/policy/test_values_profiles.py` - `_is_monitoring_enablement` extended to recognize `config.traces.*` and the top-level `env` key
- `tests/unit/conftest.py` - `airflow/dags` `sys.path` bootstrap hoisted to module level (collection-time, not just fixture-time)
- `tests/unit/test_tracing_kpo.py` (new) - 4 tests proving the plan's 3 `<behavior>` cases plus a false-positive control

## Decisions Made

- **OTel Collector endpoint resolved from 07-03-SUMMARY.md's own verified live value**, not re-derived: `http://otel-collector-opentelemetry-collector.monitoring.svc.cluster.local:4318` -- the plan's own placeholder text (`otel-collector.monitoring...`) was confirmed wrong against the chart's real `<release>-<chart-name>` fullname convention, exactly the "do not guess without checking" case the plan itself flagged.
- **`AIRFLOW_IMAGE_TAG`/`defaultAirflowTag` = `8b01cc1`** (the git SHA of the Dockerfile-introducing commit), chosen because it is a real, existing, reproducible reference rather than an arbitrary placeholder string -- but see Next Phase Readiness: this tag does not yet correspond to a pushed image.
- **`config.traces.otel_application: "airflow"`** added alongside `otel_on` (not explicitly named in the plan's action text, but present in CLAUDE.md's own `[traces] otel_on = True`, `otel_application = airflow` example) -- Rule 2, so exported spans carry a distinguishing `service.name`-equivalent resource attribute for Tempo/Grafana.
- **`statsd.resources` added explicitly** for both profiles (Rule 2) -- the chart deploys statsd-exporter as its own Deployment once enabled, and an unsized container is QoS BestEffort (02-RESEARCH.md Pitfall 5 / `test_manifest_resources.py`'s `test_every_container_is_sized`, part of the permanent `make manifest-policy` gate).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1/3 - Bug/Blocking] `_is_monitoring_enablement` predicate did not cover the new divergence**
- **Found during:** Task 2, verifying `helm/values/{local,ci}/airflow.yaml` against `tests/policy/test_values_profiles.py`
- **Issue:** `test_profiles_diverge_only_on_permitted_axes` (unmarked, part of the offline `make policy`/`make check` gate) failed immediately after adding `config.traces.*` and the top-level `env` key local-only: the plan's own action text called this "the already-permitted 'monitoring enablement' axis," but the actual `_is_monitoring_enablement(path)` predicate only matched paths with a literal `metrics`/`monitoring` **segment** -- `config.traces.otel_on` and `env` matched neither.
- **Fix:** Extended `_is_monitoring_enablement` to also recognize `path == "env"` and `path.startswith("config.traces")`, following the exact same precedented extension pattern already used 3 times in this same file's `_is_resource_sizing` history (documented inline: "the same incomplete-implementation gap... one config-section name later, not a new axis"). Did NOT add a 5th `PERMITTED_AXES` entry (the file asserts `len(PERMITTED_AXES) == 4`).
- **Files modified:** `tests/policy/test_values_profiles.py`
- **Verification:** `uv run pytest tests/policy/test_values_profiles.py -q` -- 6/6 passing (was 5/6 before the fix, confirmed empirically before and after).
- **Committed in:** `0d1f6ec` (Task 2 commit)

**2. [Rule 3 - Blocking] `_common.tracing_kpo` unresolvable at test-collection time**
- **Found during:** Task 3, first attempt at `uv run pytest tests/unit -k tracing_kpo -q`
- **Issue:** `tests/unit/test_tracing_kpo.py` imports `from _common.tracing_kpo import TracingKubernetesPodOperator` at module level. `airflow/dags` is only added to `sys.path` inside the existing `dagbag` fixture's body (`tests/unit/conftest.py`), which runs at test **execution** time -- too late for a module-level import attempted during **collection**.
- **Fix:** Hoisted the identical `sys.path` bootstrap to module level in `conftest.py` (outside the fixture), so it runs unconditionally before pytest collects any sibling test module. The fixture's own internal check is untouched and is now a harmless, idempotent no-op.
- **Files modified:** `tests/unit/conftest.py`
- **Verification:** `uv run pytest tests/unit -k tracing_kpo -q` -- went from `ModuleNotFoundError` at collection to 4/4 passing.
- **Committed in:** `42269fd` (RED commit, since the test file needs this fix to even collect)

**3. [Rule 1 - Bug] `csv_ingest_customers.py` exceeded the ORCH-06 150-line budget**
- **Found during:** Post-Task-3 full offline policy suite run (`tests/policy -q -m "not manifests"`), NOT caught by the plan's own narrower Task-3-scoped acceptance criteria
- **Issue:** `tests/policy/test_dag_line_budget.py::test_csv_ingest_customers_stays_under_150_lines` failed: the file grew from 149 (the pre-existing, zero-headroom ceiling) to 154 lines (+1 mandatory import, +4 explanatory comment lines).
- **Fix:** Re-wrapped two **pre-existing** paragraphs (module docstring, and the "Fan-out is bounded..." comment) tighter -- pure line-wrap, zero content lost, both had slack under the 100-char line-length limit -- and trimmed the new D-12 comment to one line (the full rationale already lives in `tracing_kpo.py`'s own module/class docstrings). Net: 149 lines.
- **Files modified:** `airflow/dags/csv_ingest_customers.py`
- **Verification:** `wc -l` = 149; `uv run pytest tests/policy/test_dag_line_budget.py tests/unit/test_dag_structure.py tests/policy/test_dag_thinness.py tests/unit/test_tracing_kpo.py -q` -- 16/16 passing; `ruff format --check`/`ruff check` both clean.
- **Committed in:** `0edfd0a` (separate fix commit, not amended into GREEN, per git-safety protocol)

---

**Total deviations:** 3 auto-fixed (2 Rule 1/3 blocking-test fixes, 1 Rule 1 bug fix)
**Impact on plan:** All three are narrow, mechanically-verified corrections to this plan's own changes (a test predicate, a test-collection path, a line-budget regression) -- no scope creep, no architectural change, no Rule 4 decision needed.

## Issues Encountered

**No `docker`/`kubectl` access in this sandboxed worktree.** Both binaries are symlinks into `/mnt/wsl/docker-desktop/...` and return `Input/output error` on every invocation, confirmed even with the sandbox explicitly disabled for a probe command -- this is an environment/mount-namespace constraint of this particular worktree-isolated agent, not a permissions issue this session could resolve. Concretely, this means:

- `docker build -f docker/airflow/Dockerfile ...` and `docker run --rm airflow:test pip list | grep -i opentelemetry` (the plan's own Task 1 acceptance criteria, and its top-level `<verification>` block's first bullet) were **not independently executed**. The Dockerfile's content was verified structurally (matches the plan's exact specified recipe: base image, pinned `apache-airflow[otel]==3.3.0`, pinned constraints URL, OCI labels) and by static grep-based acceptance criteria, but the actual build was never run and no image was ever pushed to `localhost:5001/airflow`.
- `AIRFLOW_IMAGE_TAG`/`defaultAirflowTag` = `8b01cc1` is therefore a **structural placeholder**, not a proof that an image with that exact tag exists in the registry (see Next Phase Readiness).

**What WAS independently, live-verified in this session** (network egress and the pinned `helm`/`kubeconform` binaries worked normally, unlike `docker`/`kubectl`):
- `make manifests` -- full live run, both profiles, all 8 pinned charts (including the just-modified `airflow` chart): `helm lint` clean, `helm template` succeeded, `kubeconform -strict` reported 181 resources / 159 valid / 0 invalid / 0 errors. This proves the Airflow chart accepts the new `config.traces`, top-level `env`, and `statsd.resources` keys without any schema-validation error, and that `defaultAirflowRepository: localhost:5001/airflow` / `defaultAirflowTag: "8b01cc1"` render correctly.
- The full offline test suite: `tests/unit tests/regression` (401/401), `tests/policy -m "not manifests"` (124/124), `tests/unit -k tracing_kpo` (4/4), plus `ruff check .` / `ruff format --check .` (2 pre-existing, unrelated files flagged and logged to `deferred-items.md`, not fixed -- out of scope) and `lint-imports` (2/2 contracts kept).
- `TracingKubernetesPodOperator.build_pod_request_obj(context=None)` was confirmed to build a complete `V1Pod` with no live Kubernetes connection required (`KubernetesHook.is_in_cluster` is a local check, not a network call) -- proven directly against the real, pinned `apache-airflow-providers-cncf-kubernetes==10.19.0` source, not assumed.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

**Ready:**
- `TracingKubernetesPodOperator`'s injection logic is fully proven offline (RESEARCH.md's own Assumption A3 -- "the active-span assumption... recommend an early spike to confirm empirically" -- is now closed for the *mechanism*: `propagate.inject()` genuinely reads whatever span is current via `start_as_current_span()`, verified with a real SDK `TracerProvider`, not a mock). What remains for a live cluster is whether the **Airflow-managed task span** is genuinely active at the exact point `execute()` calls `build_pod_request_obj()` in a real KubernetesExecutor run -- tracked for Plan 07-08 ("proves the whole chain live"), not re-litigated here.
- Both Helm values profiles render and validate cleanly with every new key this plan introduced.

**Blockers / required follow-up before the next live-cluster verification pass:**
1. **Build and push the real image.** Run `make image-airflow` against a live Docker daemon and the local registry (`localhost:5001`). This was structurally impossible in this sandboxed worktree (no `docker` access).
2. **Re-sync the three `AIRFLOW_IMAGE_TAG` sources.** `make image-airflow` computes `git rev-parse --short HEAD` *at invocation time* -- it will almost certainly NOT equal `8b01cc1` once run against the merged tip of `main`. After that build, update `helm/versions.env`'s `AIRFLOW_IMAGE_TAG` **and** both `helm/values/{local,ci}/airflow.yaml`'s `defaultAirflowTag` to the actual produced SHA, keeping all three in agreement (`tests/policy/test_supply_chain_guards.py::test_every_image_tag_agrees_with_versions_env` will fail loudly if they drift).
3. **Live-verify the built image actually contains `opentelemetry` packages** (`docker run --rm <image> pip list | grep -i opentelemetry`) -- structurally very likely correct (the Dockerfile's `RUN pip install` line is unambiguous), but never executed this session.
4. **Deploy and confirm a real `TRACEPARENT` reaches a real `ingest` pod** -- the live end-to-end proof, explicitly deferred to Plan 07-08 per this plan's own objective statement ("Plan 07-08 proves the whole chain live").

---
*Phase: 07-observability-metrics-tracing-lineage*
*Completed: 2026-08-16*

## Self-Check: PASSED

All created files confirmed present on disk (`docker/airflow/Dockerfile`,
`airflow/dags/_common/tracing_kpo.py`, `tests/unit/test_tracing_kpo.py`, and
every modified file). All 5 cited commit hashes (`8b01cc1`, `0d1f6ec`,
`42269fd`, `c341c46`, `0edfd0a`) confirmed present in `git log 1a619c4..HEAD`,
exact match, no discrepancies.
