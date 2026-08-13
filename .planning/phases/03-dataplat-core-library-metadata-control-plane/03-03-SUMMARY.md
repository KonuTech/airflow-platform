---
phase: 03-dataplat-core-library-metadata-control-plane
plan: 03
subsystem: observability
tags: [structlog, secrets-resolver, redaction, contextvars, sec-15, obs-05, dataplat]

# Dependency graph
requires:
  - phase: 03-dataplat-core-library-metadata-control-plane
    provides: "dataplat.errors.SecretResolutionError and the DataPlatformError hierarchy (plan 03-01)"
provides:
  - "dataplat.observability.logging.configure() -- dual JSON/console structlog renderer, contextvars-merge, redaction processor (OBS-02, OBS-04, OBS-05)"
  - "dataplat.observability.logging.{bind_contextvars,clear_contextvars,get_logger} -- the one re-export seam every later call site imports instead of reaching into structlog directly"
  - "dataplat.observability.metrics.increment() / dataplat.observability.tracing.start_span() -- no-op call sites with real, stable signatures for Phase 7 (D-03)"
  - "dataplat.secrets.resolver.resolve_secret() -- env:// and file:// resolution, fail-closed on every other scheme including vault:// (SEC-15)"
  - "Proof that a resolved secret logged under a secret-pattern key never appears in captured output, both by observed output and by structural processor-chain-order assertion"
affects: [03-05, 03-06, 03-07, 03-08]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "structlog processor chain: merge_contextvars -> add_log_level -> TimeStamper -> _redact -> renderer, with _redact cast to structlog.typing.Processor only at the list-literal call site (mirrors 03-01's dataclasses.replace() cast precedent)"
    - "One re-export module per cross-cutting concern (dataplat.observability.logging re-exports bind_contextvars/clear_contextvars/get_logger) so call sites never import structlog directly, matching storage/db.py's single-factory precedent"
    - "No-op call sites with real, stable signatures (metrics.increment, tracing.start_span) so a future backend wiring touches only the seam's internals, never a caller"
    - "SecretsResolver never returns an unresolved reference string -- every unsupported path raises SecretResolutionError instead"

key-files:
  created:
    - packages/dataplat/src/dataplat/observability/__init__.py
    - packages/dataplat/src/dataplat/observability/logging.py
    - packages/dataplat/src/dataplat/observability/metrics.py
    - packages/dataplat/src/dataplat/observability/tracing.py
    - packages/dataplat/src/dataplat/secrets/__init__.py
    - packages/dataplat/src/dataplat/secrets/resolver.py
    - tests/unit/test_logging_config.py
    - tests/unit/test_secrets_resolver.py
    - tests/unit/test_logging_redaction.py
  modified: []

key-decisions:
  - "Cast _redact to structlog.typing.Processor only at the processors-list call site (typing.cast is a runtime no-op) rather than widening _redact's own parameter type, keeping the plan's exact-specified signature (_logger: object, _name: str, event_dict: dict[str, object]) -> dict[str, object]"
  - "Did not run requirements.mark-complete for SEC-15: plan 03-05 also declares SEC-15 in its own frontmatter and contributes the live end-to-end integration proof (resolve_secret() output feeding create_pool() against a real Postgres connection) that this plan's unit-only proof doesn't cover -- same reasoning 03-01 applied to QUAL-03"
  - "Marked OBS-02, OBS-04, OBS-05 complete -- only this plan's frontmatter declares them"

patterns-established:
  - "Structural chain-order assertions (structlog.get_config()['processors']) alongside observed-output assertions, so a future processor reordering that happens to still pass today's specific test values by coincidence is still caught"
  - "# noqa: RULE - short reason inline suppressions for lint rules that are false positives against deliberate design (matches tools/corpus's existing S311/PLR2004/S603 precedent)"

requirements-completed: [OBS-02, OBS-04, OBS-05]

# Metrics
duration: 19min
completed: 2026-08-13
---

# Phase 3 Plan 3: Structured Logging & SecretsResolver Summary

**structlog dual JSON/console renderer with contextvars and secret redaction, paired end-to-end with an env://+file:// SecretsResolver that fails closed on vault://**

## Performance

- **Duration:** 19 min
- **Started:** 2026-08-13T00:22:57Z
- **Completed:** 2026-08-13T00:42:08Z
- **Tasks:** 3
- **Files modified:** 9 (9 created)

## Accomplishments

- `observability.logging.configure()` builds structlog's real processor chain: JSON in-cluster, console locally, contextvars merged automatically, redaction positioned immediately before the renderer (OBS-02, OBS-04, OBS-05) -- the same call site works unmodified across local/Docker/Kubernetes/Airflow task-pod contexts
- `observability.metrics.increment()` / `observability.tracing.start_span()` -- no-op today, with the real, stable call-site signatures Phase 7 will wire a backend behind (D-03)
- `secrets.resolver.resolve_secret()` -- `env://` and `file://` resolution; every other scheme (including `vault://`, Phase 5's) fails closed with `SecretResolutionError` imported from `dataplat.errors`, never redefined locally (SEC-15)
- The SEC-15/OBS-05 pair proven end to end in one test: a secret resolved via `resolve_secret()` and logged under a secret-pattern key never appears in captured stdout -- proven both by observed output and by a structural assertion on `structlog.get_config()["processors"]`'s ordering, so a future reordering can't silently defeat the guarantee
- Manually confirmed (then restored) that removing the redaction processor makes all four `test_logging_redaction.py` tests fail, including the resolver pairing test -- the tests have real teeth
- `make check` green: 105 policy tests, 76 unit+regression tests (16 of them new in this plan, 97-100% coverage on every module this plan added), 70/70 corpus fixtures verified

## Task Commits

Each task was committed atomically (Tasks 1-2 are `tdd="true"`, so each has a RED + GREEN pair):

1. **Task 1: Structured logging -- configure(), contextvars, redaction, no-op metrics/tracing seams**
   - RED: `a413bf7` (test) -- failing tests, `ModuleNotFoundError: No module named 'dataplat.observability'`
   - GREEN: `ad8f325` (feat) -- implementation, 5/5 tests pass
2. **Task 2: SecretsResolver -- env:// and file://, fail closed on anything else**
   - RED: `42ec3c8` (test) -- failing tests, `ModuleNotFoundError: No module named 'dataplat.secrets'`
   - GREEN: `a02d5c5` (feat) -- implementation, 7/7 tests pass
3. **Task 3: Prove the pair -- a resolved secret never reaches a captured log line** -- `739c3e3` (test), 4/4 tests pass on first run

_TDD gate sequence verified in git log: each `test(...)` commit precedes its `feat(...)` commit, both tasks._

## Files Created/Modified

- `packages/dataplat/src/dataplat/observability/__init__.py` - empty package marker, module docstring only
- `packages/dataplat/src/dataplat/observability/logging.py` - `configure()`, `_redact` processor, `_SECRET_KEY_PATTERN`/`_TRUNCATE_KEYS`/`_TRUNCATE_AT` constants, re-exports `bind_contextvars`/`clear_contextvars`/`get_logger`
- `packages/dataplat/src/dataplat/observability/metrics.py` - `increment(name, value=1, **labels) -> None`, no-op
- `packages/dataplat/src/dataplat/observability/tracing.py` - `start_span(name) -> AbstractContextManager[None]`, returns `contextlib.nullcontext()`
- `packages/dataplat/src/dataplat/secrets/__init__.py` - empty package marker, module docstring only
- `packages/dataplat/src/dataplat/secrets/resolver.py` - `resolve_secret(ref) -> str`; imports `SecretResolutionError` from `dataplat.errors`
- `tests/unit/test_logging_config.py` - 5 tests: dual renderer, contextvars propagation/clearing, no-op metrics/tracing
- `tests/unit/test_secrets_resolver.py` - 7 tests: env://, file://, vault:// fail-closed, malformed-ref fail-closed, never-returns-raw-ref
- `tests/unit/test_logging_redaction.py` - 4 tests: key redaction, raw_line/record truncation, end-to-end resolver+logging pairing, structural chain-order assertion

## Decisions Made

- **Cast `_redact` to `structlog.typing.Processor` only at the `processors=[...]` call site.** mypy strict rejects a bare reference: `structlog.typing.Processor`'s `event_dict` parameter is `MutableMapping[str, Any]`, and `Callable` parameters are checked contravariantly, so `_redact`'s plan-specified `dict[str, object]` parameter is not a valid substitute without help. `typing.cast()` is a runtime no-op, so this changes nothing at runtime and keeps `_redact`'s own signature exactly as the plan's action text specifies. Directly mirrors 03-01-SUMMARY.md's documented `RecordChunk.replace()` cast precedent for the same class of mypy special-casing.
- **`SEC-15` left `Pending` in `REQUIREMENTS.md`; `OBS-02`/`OBS-04`/`OBS-05` marked `Complete`.** `03-05-PLAN.md` also declares `requirements: [META-01, SEC-15]` and contributes a distinct, necessary proof this plan doesn't cover: a live integration test where `resolve_secret("env://...")`'s output feeds `create_pool()` against a real Postgres connection. Calling `requirements.mark-complete` for SEC-15 now would falsely mark a two-plan requirement done after only the first plan, the same reasoning 03-01-SUMMARY.md documented for `QUAL-03`. `OBS-02`/`OBS-04`/`OBS-05` are declared only here (confirmed via `grep` across every phase-3 `PLAN.md`'s `requirements:` frontmatter field), so those three were marked complete.
- **No pytest test files beyond the plan's declared `files_modified`.** All three tasks' test files are exactly the ones the plan's frontmatter lists; no additional test modules were added.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] mypy strict rejects `_redact` in the `processors` list literal**
- **Found during:** Task 1 (`configure()`)
- **Issue:** `uv run --frozen mypy packages/dataplat/src` failed: `List item 3 has incompatible type "Callable[[object, str, dict[str, object]], dict[str, object]]"; expected "Callable[[Any, str, MutableMapping[str, Any]], ...]"`. `Callable` parameter types are checked contravariantly, and `dict[str, object]` is not a supertype of `MutableMapping[str, Any]`.
- **Fix:** `cast("structlog.typing.Processor", _redact)` at the list-literal call site only (imports `structlog.typing` explicitly and `from typing import cast`). `_redact`'s own declared signature is untouched, exactly as the plan's action text specifies.
- **Files modified:** `packages/dataplat/src/dataplat/observability/logging.py`
- **Verification:** `uv run --frozen mypy packages/dataplat/src` exits 0 (confirmed both immediately after the fix and again in the final `make check` run)
- **Committed in:** `ad8f325` (Task 1 GREEN commit)

**2. [Rule 1 - Bug] ruff ARG001 on `tracing.start_span`'s unused `name` parameter**
- **Found during:** Task 1 (`tracing.py`)
- **Issue:** `start_span(name: str) -> AbstractContextManager[None]: return contextlib.nullcontext()` doesn't reference `name` in its body (unlike `metrics.increment`, whose body is docstring-only and is exempt from ARG001 as a recognized stub pattern -- verified empirically). ruff flagged `ARG001 Unused function argument: name`.
- **Fix:** `# noqa: ARG001 -- no-op today` inline on the `def` line. Renaming the parameter to `_name` was rejected as a fix because it would change the plan's literally-specified public signature (`start_span(name: str)`).
- **Files modified:** `packages/dataplat/src/dataplat/observability/tracing.py`
- **Verification:** `uv run --frozen ruff check packages/dataplat/src/dataplat/observability/` passes
- **Committed in:** `ad8f325` (Task 1 GREEN commit)

**3. [Rule 1 - Bug] ruff S106 on deliberately-fake secret values in a redaction test**
- **Found during:** Task 3 (`test_redaction_processor_drops_secret_pattern_keys`)
- **Issue:** `password="hunter2-fake-password"` and `api_token="fake-token-abc123"` triggered `S106 Possible hardcoded password assigned to argument`. These are the test's entire point -- deliberately fake values proving redaction -- not real credentials.
- **Fix:** `# noqa: S106 - fake value, proves redaction` inline on both flagged lines, matching the repo's existing `# noqa: RULE - reason` convention (`tools/corpus/generators.py`'s `# noqa: S311`, `tools/security/gitleaks_selftest.py`'s `# noqa: S603`).
- **Files modified:** `tests/unit/test_logging_redaction.py`
- **Verification:** `uv run --frozen ruff check tests/unit/test_logging_redaction.py` passes
- **Committed in:** `739c3e3` (Task 3 commit)

**4. [Rule 1 - Bug] Autouse fixture generators typed `-> None` instead of `-> Iterator[None]`**
- **Found during:** Task 3, while running `mypy` directly against the new test file as extra diligence beyond the plan's own acceptance criteria (the Makefile's `TYPECHECK_PATHS` excludes `tests/`, so this is not part of any enforced gate)
- **Issue:** Both `test_logging_config.py`'s (Task 1) and `test_logging_redaction.py`'s (Task 3) `_clear_bound_context` autouse fixtures contain `yield`, making them generator functions, but were annotated `-> None`. mypy: `error: The return type of a generator function should be "Generator" or one of its supertypes [misc]`.
- **Fix:** Retyped both to `-> Iterator[None]`, importing `collections.abc.Iterator` inside a `TYPE_CHECKING` block (matching `tests/unit/test_corpus_manifest.py`'s existing pattern, since `from __future__ import annotations` makes the import type-only).
- **Files modified:** `tests/unit/test_logging_config.py`, `tests/unit/test_logging_redaction.py`
- **Verification:** `uv run --frozen mypy tests/unit/test_logging_config.py tests/unit/test_logging_redaction.py tests/unit/test_secrets_resolver.py` -- this specific error gone (one pre-existing, out-of-scope `func-returns-value` note remains on asserting a `-> None` function's return value in `test_logging_config.py`; not fixed, see Issues Encountered)
- **Committed in:** `739c3e3` (Task 3 commit, bundled with Task 3's own new file since Task 1's file was already committed)

---

**Total deviations:** 4 auto-fixed (1 blocking/mypy-typing, 3 bug/lint-false-positive)
**Impact on plan:** All four are necessary for a clean `mypy`/`ruff` gate or are correctness polish found via diligence beyond the plan's stated acceptance criteria. None changed any public signature the plan specifies. No scope creep.

## Issues Encountered

- **Worktree base was stale at spawn time.** The mandatory `<worktree_branch_check>` found this worktree's HEAD (`78edd19`, pre-wave-1) behind the expected base (`5133a72`, post-wave-1-merge). Corrected via `git reset --hard 5133a72606d5a3b9d5f5d58d2f41fba67514e029` per the check's own instructions, with `git status --short` confirming no uncommitted work would be lost first. Not a plan-execution problem -- an environment-setup correction before any task work began.
- **`mypy` on `test_logging_config.py` reports `func-returns-value` for `assert metrics.increment(...) is None`.** mypy specifically flags any use of a `-> None`-typed function's return value, even a trivial `is None` check. Left as-is: not part of any enforced gate (`TYPECHECK_PATHS` excludes `tests/`), and rewriting the assertion to dodge this would weaken the test's literal proof of Task 1's own behavior spec ("execute without raising and return `None`").
- **The supplementary `make check` gate took an unusually long wall-clock time** (background task launched ~00:42 UTC, completed ~04:24 UTC) despite its own internally-reported step durations summing to roughly 10 minutes (`tests/policy`: 482.30s; `tests/unit tests/regression`: 9.61s; lint/format/mypy/imports/fixtures-verify: seconds each). Repeated `ps` checks during the wait confirmed the process was genuinely active (not deadlocked) throughout, including a corpus-verify step observed at 98-99% CPU. This reflects CPU contention from other concurrently-running parallel wave/plan executors sharing this host (a sibling worktree agent's own `tests/policy` run was observed active at the same time), not a problem with this plan's code. `make check` finished with exit code 0 and every step green. The Duration reported above reflects actual coding/commit activity (worktree-correction through the last task commit), not this wait.

## User Setup Required

None - no external service configuration required. This plan touches no live database, object store, or cluster (by design -- `configure()`, `resolve_secret()` and their tests are pure-Python/filesystem/env-var, no Docker or network dependency).

## Next Phase Readiness

- `03-05` (`depends_on: ["03-01", "03-02", "03-03"]`) can now `from dataplat.secrets.resolver import resolve_secret` for its live end-to-end SEC-15 integration proof, and `from dataplat.observability import logging` for structured logging in its own code.
- `03-07` (`depends_on: ["03-01", "03-02", "03-03"]`) and, transitively through `03-05`, `03-06`/`03-08` can import the same two seams without further scaffolding.
- `SEC-15` stays `Pending` in `REQUIREMENTS.md` until `03-05` lands (see Decisions Made above) -- expected, not a blocker.
- No blockers. `make check` is green on this plan's five commits atop the post-wave-1 base.

## Self-Check: PASSED

- FOUND: packages/dataplat/src/dataplat/observability/__init__.py
- FOUND: packages/dataplat/src/dataplat/observability/logging.py
- FOUND: packages/dataplat/src/dataplat/observability/metrics.py
- FOUND: packages/dataplat/src/dataplat/observability/tracing.py
- FOUND: packages/dataplat/src/dataplat/secrets/__init__.py
- FOUND: packages/dataplat/src/dataplat/secrets/resolver.py
- FOUND: tests/unit/test_logging_config.py
- FOUND: tests/unit/test_secrets_resolver.py
- FOUND: tests/unit/test_logging_redaction.py
- FOUND commit: a413bf7 (Task 1 RED)
- FOUND commit: ad8f325 (Task 1 GREEN)
- FOUND commit: 42ec3c8 (Task 2 RED)
- FOUND commit: a02d5c5 (Task 2 GREEN)
- FOUND commit: 739c3e3 (Task 3)

---
*Phase: 03-dataplat-core-library-metadata-control-plane*
*Completed: 2026-08-13*
