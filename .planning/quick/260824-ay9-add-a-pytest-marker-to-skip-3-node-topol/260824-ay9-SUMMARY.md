---
phase: quick-260824-ay9-add-a-pytest-marker-to-skip-3-node-topol
plan: 01
subsystem: testing
tags: [pytest, kubectl, kind, e2e, ci-cd, markers]

# Dependency graph
requires:
  - phase: 11-ci-cd-completion-operations
    provides: e2e-full.yml's make cluster-verify CI gate and its first live merge-triggered run's 5-test failure finding (deferred-items.md, Plan 11-05 finding #1)
provides:
  - "multi_node pytest marker registered in pyproject.toml, --strict-markers-compliant"
  - "tests/e2e/cluster/conftest.py autouse live-detection skip fixture (_skip_multi_node_tests_on_single_node_clusters)"
  - "5 topology/executor-shape tests marked multi_node across 4 test files"
affects: [ci-cd, e2e-full-workflow, cluster-verify]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Live kubectl-probe-based test skip decision instead of environment-variable propagation (same philosophy as quick task 260824-akz's chaos-suite fix)"

key-files:
  created: []
  modified:
    - pyproject.toml
    - tests/e2e/cluster/conftest.py
    - tests/e2e/cluster/test_node_capacity.py
    - tests/e2e/cluster/test_postgres_topology.py
    - tests/e2e/cluster/test_doctor_live_mount_detection.py
    - tests/e2e/cluster/test_airflow_workloads.py

key-decisions:
  - "Skip decision made via a live `kubectl get nodes` probe inside conftest.py, never a PROFILE env-var read — PROFILE is set inline only on make cluster-up's own command line in e2e-full.yml and never exported into the later cluster-verify step, the same propagation gap already found and avoided for make chaos-verify in quick task 260824-akz"
  - "Fixture is function-scoped and autouse, not session-scoped, because request.node (and therefore which marker applies) differs per test; it early-returns for every unmarked test so the extra kubectl call only fires for the 5 multi_node-marked tests"
  - "No Makefile or workflow YAML changes made — node count is a structural signal (kind/cluster.yaml always declares 3, kind/cluster-ci.yaml always declares 1) requiring zero configuration threading"

requirements-completed: []

# Metrics
duration: 20min
completed: 2026-08-24
---

# Phase quick-260824-ay9: Add multi_node pytest marker Summary

**Registered a `multi_node` pytest marker with a live-kubectl-probe autouse skip fixture, then applied it to the 5 `tests/e2e/cluster` tests whose assertions are about cluster-wide topology SHAPE (node count, Postgres-primary placement, node-name list, full Deployment set) — letting `make cluster-verify` pass cleanly under CI's single-node profile without weakening what these tests prove about the local 3-node cluster.**

## Performance

- **Duration:** ~20 min
- **Completed:** 2026-08-24T06:05:05Z
- **Tasks:** 2 completed
- **Files modified:** 6

## Accomplishments
- `pyproject.toml` now registers `multi_node` as a valid marker in the established one-line-docstring style, satisfying `--strict-markers`
- `tests/e2e/cluster/conftest.py` gained an autouse, function-scoped `_skip_multi_node_tests_on_single_node_clusters` fixture that live-probes node count via the existing `kubectl_json` fixture and skips only `multi_node`-marked tests when the live cluster reports fewer than 3 nodes
- Exactly the 5 named tests (`test_exactly_three_nodes`, `test_every_node_allocatable_is_positive_and_within_its_declared_ceiling`, `test_two_distinct_clusters_no_shared_storage`, `test_doctor_live_passes_on_the_real_host`, `test_four_workloads_are_ready`) now carry `@pytest.mark.multi_node`; no other test in these 4 files was touched
- `tests/e2e/observability` and `scripts/stages/85-monitoring.sh` left untouched, per plan scope (that CI gap is tracked separately)

## Task Commits

Each task was committed atomically:

1. **Task 1: Register the multi_node marker and add its live-detection skip fixture** - `741a116` (feat)
2. **Task 2: Mark the 5 topology/executor-shape tests and verify the whole change set** - `adbcc3f` (test)

_Note: Task 2 is a `test` commit because it only adds `@pytest.mark.multi_node` decorators — no behavior-adding source code was written._

## Files Created/Modified
- `pyproject.toml` - Registers the `multi_node` marker under `[tool.pytest.ini_options]`
- `tests/e2e/cluster/conftest.py` - Adds `_skip_multi_node_tests_on_single_node_clusters`, an autouse fixture between `_require_cluster` and `kubectl`
- `tests/e2e/cluster/test_node_capacity.py` - Marks `test_exactly_three_nodes` and `test_every_node_allocatable_is_positive_and_within_its_declared_ceiling`
- `tests/e2e/cluster/test_postgres_topology.py` - Marks `test_two_distinct_clusters_no_shared_storage`
- `tests/e2e/cluster/test_doctor_live_mount_detection.py` - Marks `test_doctor_live_passes_on_the_real_host`
- `tests/e2e/cluster/test_airflow_workloads.py` - Marks `test_four_workloads_are_ready`

## Decisions Made
- Live `kubectl get nodes` probe chosen over a `PROFILE` environment-variable read, for the exact propagation-gap reason already documented and proven in quick task 260824-akz (see `key-decisions` above) — confirmed fresh this session that `.github/workflows/e2e-full.yml`'s `cluster-verify` step never receives `PROFILE` either.
- No Makefile/workflow changes needed — deliberately avoided a `-m "not multi_node"` deselection flag threaded through CI config, since that would reintroduce the same environment-propagation fragility this plan exists to avoid.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- `mypy tests/e2e/cluster/test_node_capacity.py tests/e2e/cluster/test_airflow_workloads.py` reports 2 pre-existing errors (missing `types-PyYAML` stub on `test_node_capacity.py` line 25; an `Any | None` indexing issue on `test_airflow_workloads.py` line ~243-244) — confirmed via `git stash`/re-run against the pre-plan base commit (`5b25d6a`) that both errors exist identically before this plan's changes and are unrelated to the `@pytest.mark.multi_node` decorators added here. Out of scope per the deviation rules' scope boundary (pre-existing issues in unrelated code); not auto-fixed, not introduced by this plan.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- `make cluster-verify` (and therefore `.github/workflows/e2e-full.yml`) is now expected to pass its `tests/e2e/cluster` leg cleanly under CI's single-node profile, since the 5 previously-failing topology/executor-shape tests will now be live-skipped rather than fail.
- Live re-verification against a genuine CI-profile (1-node) cluster is deferred to a separate follow-up, per this plan's own explicit scope (no live cluster/CI run required to close this plan — `ruff check`, `mypy` (on changed files, modulo the 2 pre-existing unrelated errors noted above), and `pytest --collect-only` all pass cleanly).
- The other `cluster-verify` CI gap (`tests/e2e/observability` needing a live monitoring stack unconditionally disabled in CI) remains open, tracked separately as noted in the plan's objective.

---
*Phase: quick-260824-ay9-add-a-pytest-marker-to-skip-3-node-topol*
*Completed: 2026-08-24*

## Self-Check: PASSED

All 6 modified files and the SUMMARY.md itself confirmed present on disk. Both task commits (`741a116`, `adbcc3f`) confirmed present in `git log`.
