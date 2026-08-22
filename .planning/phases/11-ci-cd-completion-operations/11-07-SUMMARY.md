---
phase: 11-ci-cd-completion-operations
plan: 07
subsystem: infra
tags: [retention, pydantic, minio, iam, adr, dry-run, tdd]

# Dependency graph
requires:
  - phase: 02-kind-cluster-core-infrastructure
    provides: The MinIO `etl-app` IAM policy's `Deny` statement on `s3:DeleteObject`/`s3:DeleteObjectVersion` against `raw/*` (D-08), the exact live control D-40 verifies here
  - phase: 10-slowly-changing-dimensions
    provides: The `RejectionRateCircuitBreaker`/`MassDeleteCircuitBreaker` "argument-parameterized totals, trivial-pass empty-input guard, threshold/observed dict findings" shape this plan's evaluator mirrors (diverging on report-vs-raise)
provides:
  - "RetentionConfig: a fail-closed-by-construction Pydantic contract (config/model.py), registered as DatasetConfig.retention"
  - "customers.yaml and orders.yaml retention: blocks with D-37's tiered defaults (processed 60d, quarantine 180d, validation_reports/ingestion_metadata 730d, logs 30d), raw indefinite"
  - "dataplat.retention.policy.evaluate_retention: a pure, never-raising, zero-I/O dry-run/enforce evaluator ready for plan 11-08's platform_retention DAG to call"
  - "D-40 verified live (not rebuilt) and formally closed out via ADR-0011"
affects: [11-08-platform-retention-dag, future-retention-consumers]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Free-function decision evaluator deliberately decoupled from pipeline.protocol.BarrierStage -- retention is a maintenance-DAG concern (D-35), not an ingest-pipeline stage, so it takes no PipelineContext and implements no Protocol"
    - "Per-layer structured report combining concrete named counters (candidate_count/would_delete_count/deleted_count) with ValidationResult's established threshold/observed dict-shape convention"

key-files:
  created:
    - packages/dataplat/src/dataplat/retention/__init__.py
    - packages/dataplat/src/dataplat/retention/policy.py
    - tests/unit/test_retention_policy.py
    - docs/adr/0011-raw-immutability-iam-not-worm.md
  modified:
    - packages/dataplat/src/dataplat/config/model.py
    - configs/datasets/customers.yaml
    - configs/datasets/orders.yaml
    - helm/values/local/minio.yaml
    - helm/values/ci/minio.yaml
    - docs/adr/README.md

key-decisions:
  - "evaluate_retention is a free function, not a RetentionEvaluator class -- the plan explicitly allowed either; a function was chosen because retention has no PipelineContext/BarrierStage to construct against, and a future DAG task calling a plain function once per run needs no instance to build and discard (documented in the module's own docstring)"
  - "LayerRetentionReport.deleted_count is structurally pinned to 0 in every case, including enforce=True -- the evaluator performs literally zero I/O regardless of the enforce flag, which is a stronger guarantee than merely dry-running by default"
  - "raw_days is omitted entirely from both dataset YAMLs (not set to an explicit null) so RetentionConfig's own None default is the visible, structural source of the indefinite window, matching the plan's stated preference"

patterns-established:
  - "Pattern: a decision evaluator that mirrors an existing BarrierStage's constructor/trivial-guard/structured-findings shape without actually implementing BarrierStage, when the caller lives outside the ingest pipeline entirely"

requirements-completed: [INFRA-11]

# Metrics
duration: ~10min
completed: 2026-08-22
---

# Phase 11 Plan 07: Retention Contract + Evaluator, D-40 Closure Summary

**RetentionConfig (Pydantic, fail-closed `enforce: bool = False`) wired into customers.yaml/orders.yaml with D-37's tiered day windows, a pure never-raising `dataplat.retention.policy.evaluate_retention` dry-run evaluator built TDD, and Phase 2's MinIO raw deny-delete IAM policy re-verified live and closed out via ADR-0011.**

## Performance

- **Duration:** ~10 min (commit-to-commit span; this was a retry of a prior attempt that stalled after Task 1 and was discarded — this run redid all three tasks from a clean worktree reset to the same base commit)
- **Started:** 2026-08-22T19:54:21+02:00 (first commit)
- **Completed:** 2026-08-22T20:04:04+02:00 (last commit)
- **Tasks:** 3/3 completed
- **Files modified:** 10 (4 created, 6 modified)

## Accomplishments

- `RetentionConfig` added to `config/model.py`: six opt-in `*_days` window fields plus `enforce: bool = False` as a genuine Pydantic field default (verified by instantiating `RetentionConfig()` with zero arguments), registered as `DatasetConfig.retention: RetentionConfig | None = None`
- `customers.yaml` and `orders.yaml` both extended with real `retention:` blocks inside D-37's locked numeric ranges; `raw_days` deliberately omitted so D-36's indefinite raw default stays structural
- `dataplat.retention.policy.evaluate_retention` built via strict TDD (RED commit with a real `ModuleNotFoundError` failure, then GREEN): a pure, zero-I/O, never-raising function that judges already-queried `RetentionCandidate`s against `RetentionConfig`'s windows and returns a structured, always-fully-populated `RetentionReport` — 6 tests covering dry-run-never-deletes, zero-candidates, `None`-window indefinite retention, the boundary-exclusive threshold, `enforce=True` still performing no I/O, and the size/age `observed` summary
- D-40 (Phase 2's MinIO raw-immutability IAM-deny-delete policy) re-verified live against the real kind cluster (`test_raw_delete_is_denied_for_app_credential` / `test_raw_delete_is_permitted_for_admin_credential`, both passing) rather than re-implemented, then formally closed out with `docs/adr/0011-raw-immutability-iam-not-worm.md`

## Task Commits

Each task was committed atomically:

1. **Task 1: RetentionConfig contract + two dataset YAMLs** - `e25a4b3` (feat)
2. **Task 2: Retention policy evaluator (TDD) — RED** - `7d8b9eb` (test)
2. **Task 2: Retention policy evaluator (TDD) — GREEN** - `212d07e` (feat)
3. **Task 3: Verify D-40 + close the loop with ADR-0011** - `66951b2` (docs)

_TDD task (Task 2) produced two commits (test → feat); no refactor commit was needed — the GREEN implementation passed ruff/mypy cleanly on first pass._

## Files Created/Modified

- `packages/dataplat/src/dataplat/config/model.py` - Added `RetentionConfig(BaseModel)` and `DatasetConfig.retention` field + docstring entry
- `configs/datasets/customers.yaml` - Added `retention:` block (processed 60d, quarantine 180d, validation_reports/ingestion_metadata 730d, logs 30d, `enforce: false`, `raw_days` omitted)
- `configs/datasets/orders.yaml` - Same `retention:` block shape as customers.yaml
- `packages/dataplat/src/dataplat/retention/__init__.py` - Package-marker docstring, matching `dataplat/scd/__init__.py`'s shallow re-export convention
- `packages/dataplat/src/dataplat/retention/policy.py` - `RetentionCandidate`, `LayerRetentionReport`, `RetentionReport`, `evaluate_retention()`
- `tests/unit/test_retention_policy.py` - 6 unit tests, RED-then-GREEN
- `helm/values/local/minio.yaml` - D-08 comment corrected: no longer names Phase 11 as an open revisit point; points to ADR-0011
- `helm/values/ci/minio.yaml` - Parallel resolution note added to its own D-08 header reference
- `docs/adr/0011-raw-immutability-iam-not-worm.md` - The closed-out D-08/D-40 decision record
- `docs/adr/README.md` - Added the new ADR-0011 index row, plus a pre-existing missing ADR-0010 row found in the same table

## Decisions Made

- **Free function over a class-based evaluator.** Task 2's own instructions explicitly permitted either shape and asked for the choice to be documented. `evaluate_retention(config, candidates) -> RetentionReport` was chosen and documented in `policy.py`'s module docstring: retention is deliberately outside the CSV ingest pipeline (D-35), so there is no `PipelineContext`/`BarrierStage` to construct against, and a future DAG task calling a plain function once per run needs no instance lifecycle.
- **`LayerRetentionReport` combines concrete named counters with `ValidationResult`'s `threshold`/`observed` dict-shape convention**, rather than literally instantiating `ValidationResult` (whose `rule_id`/`outcome`/`severity` fields don't map cleanly onto a per-layer retention summary) or using only dicts (which would have buried the plan's own explicitly-required `candidate_count`/`would_delete_count`/`deleted_count` fields inside opaque dict keys). This satisfies both 11-PATTERNS.md's explicit "reuse the threshold/observed convention" guidance and Task 2's own concrete field list.
- **`deleted_count` is pinned to `0` unconditionally**, including when `enforce=True`. The evaluator never performs I/O under any input — this is a stricter guarantee than "dry-run by default" (which could imply conditional deletion); deletion mechanics stay entirely in the future DAG's hands, matching Task 2's explicit architectural boundary.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Shortened two TDD test function names that exceeded ruff's 100-char line-length gate**
- **Found during:** Task 2 (writing the RED test file)
- **Issue:** Two test names taken directly from the plan's `<behavior>` block
  (`test_a_candidate_older_than_the_window_is_selected_and_one_exactly_at_the_boundary_is_not`,
  `test_enforce_true_marks_candidates_for_deletion_but_the_evaluator_itself_still_never_performs_io`)
  produced a `def ...() -> None:` line longer than the project's configured 100-char limit
  (`ruff` `E501`), with no precedent elsewhere in the test suite for a name this long.
- **Fix:** Renamed to `test_boundary_age_is_not_selected_but_one_day_older_is` and
  `test_enforce_true_never_performs_io_itself`, preserving each test's exact assertions and adding
  a one-line docstring on each stating the fuller behavior the shorter name no longer spells out.
- **Files modified:** tests/unit/test_retention_policy.py
- **Verification:** `uv run ruff check tests/unit/test_retention_policy.py` passes; all 6 tests still pass.
- **Committed in:** 7d8b9eb (Task 2 RED commit)

**2. [Rule 1 - Bug] Corrected `helm/values/ci/minio.yaml`'s D-08 comment too, not only `local`'s**
- **Found during:** Task 3
- **Issue:** The plan's action text assumed both `minio.yaml` files carried the identical "Phase 11
  ... is the named place to revisit it" sentence. Only `helm/values/local/minio.yaml` actually has
  it; `helm/values/ci/minio.yaml`'s own D-08 mention is a terser header reference with no such
  claim to correct verbatim.
- **Fix:** Edited `local`'s exact sentence as instructed, and added a parallel, consistent
  resolution note to `ci`'s own D-08 header reference (pointing back to `local`'s fuller comment,
  matching this file's own established convention elsewhere of doing exactly that) so neither file
  is left implying the question is still open.
- **Files modified:** helm/values/local/minio.yaml, helm/values/ci/minio.yaml
- **Verification:** `git diff` on both files shows comment-only changes; both files still parse as
  valid YAML; the live D-40 proof (`test_raw_delete_is_denied_for_app_credential` /
  `test_raw_delete_is_permitted_for_admin_credential`) still passes against the real cluster.
- **Committed in:** 66951b2 (Task 3 commit)

**3. [Rule 1 - Bug] Added the missing ADR-0010 index row while adding ADR-0011's**
- **Found during:** Task 3
- **Issue:** `docs/adr/README.md`'s Records table jumps from row `0009` straight to nothing —
  ADR-0010 (`0010-dbt-silver-layer-boundary.md`, committed in Phase 08.1) was never added to this
  index, even though the ADR file itself exists and "Add the row to the table above" is this same
  README's own documented step for adding a record.
- **Fix:** Added both the missing `0010` row and the new `0011` row in the same edit, plus one
  sentence each in the table's own explanatory paragraph, rather than adding `0011` next to a
  table that would then skip straight from `0009` to `0011` — a still-broken, more visibly
  inconsistent index than not touching it at all.
- **Files modified:** docs/adr/README.md
- **Verification:** Visual review — the table is now monotonic 0001 through 0011 with no gaps in
  the index (gaps in the underlying ADR numbering itself remain permitted per this file's own
  numbering rules, but none exist here).
- **Committed in:** 66951b2 (Task 3 commit)

---

**Total deviations:** 3 auto-fixed (3 Rule 1 — all lint/documentation-consistency bugs directly adjacent to files this plan was already touching)
**Impact on plan:** All three are minor, low-risk corrections with no scope creep — no new files beyond what the plan specified, no architectural changes, no behavior changes to `RetentionConfig` or `evaluate_retention`.

## Issues Encountered

- The prior attempt at this same plan stalled with no progress for 600s after committing Task 1, leaving potentially-incomplete files under `packages/dataplat/src/dataplat/retention/` and `tests/unit/test_retention_policy.py` uncommitted. Per the orchestrator's instructions, that worktree was discarded entirely (including the uncommitted files) and this execution started fresh from the same base commit (`0bcc465`), redoing all three tasks from scratch. No content from the stalled attempt was reused or inspected.
- `ruff`'s isort briefly grouped `from dataplat.retention.policy import ...` into its own import block, separate from `from dataplat.config.model import ...`, while `dataplat.retention` did not yet exist as an importable module (during the RED phase). This resolved itself automatically once the `retention` package was created in the GREEN phase — both imports then sorted into one alphabetical block as expected. No manual workaround was needed beyond writing the test file's final import order once the module existed.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `RetentionConfig` and `evaluate_retention` are ready for plan 11-08's `platform_retention` maintenance DAG to call: the DAG's job is to query each layer's real candidates (MinIO object listings, `meta.*` row ages), construct `RetentionCandidate`s, call `evaluate_retention`, log/persist the resulting `RetentionReport`, and — only when a dataset's `RetentionConfig.enforce` is `True` — actually issue the deletes this module deliberately never performs itself.
- D-40 is fully closed: no further work on raw-immutability mechanism selection is expected; ADR-0011 is the permanent record, with a concrete, non-empty migration trigger (a genuine tamper-proof compliance requirement) should this ever need revisiting toward WORM.
- No blockers. `uv run pytest tests/unit/test_retention_policy.py tests/unit -k config -q` (51 tests across both filters) and `uv run --group cluster pytest tests/e2e/cluster/test_minio_buckets.py -q -m cluster -k delete` (2 tests, live cluster) both pass with zero failures.

---
*Phase: 11-ci-cd-completion-operations*
*Completed: 2026-08-22*
