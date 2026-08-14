---
phase: 04-vertical-slice-csv-to-analytical-postgresql
plan: 11
subsystem: cli

# Dependency graph
requires:
  - phase: 04-vertical-slice-csv-to-analytical-postgresql
    provides: "04-05's csv_processor.cli.ingest() (the discover/ingest click commands and their Receipt/XCom-writing except/finally pairing), and 04-REVIEW.md's WR-01 finding plus 04-VERIFICATION.md's failed truth #6 that elevated it"
provides:
  - "csv_processor.cli._failure_receipt(doc) — the shared FAILED-Receipt-construction helper both of ingest()'s exception branches call"
  - "csv_processor.cli.ingest()'s new except Exception: clause — a Receipt is now written to the XCom path for ANY exception, not only DataPlatformError"
  - "tests/unit/test_csv_processor_cli.py — the first dedicated unit-test file for csv_processor.cli, regression-proving WR-01"
affects: ["any future ingest() caller or reader relying on the Receipt-on-every-exit-path contract (Airflow DAG downstream tasks, operational runbooks)", "04-REVIEW.md's remaining WR-02/WR-03/WR-04 findings, if a later gap-closure plan schedules them"]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "_failure_receipt(doc) helper factors out one Receipt(status=\"FAILED\", ...) construction shared by multiple except branches, so they cannot drift apart"
    - "ordered except clauses (except DataPlatformError: before except Exception:), both always re-raising after their one side effect — Python's except-clause evaluation order guarantees the narrower type is matched first, so the broader clause only ever sees what the narrower one did not, and never intercepts BaseException-only families like KeyboardInterrupt/SystemExit"

key-files:
  created:
    - tests/unit/test_csv_processor_cli.py
  modified:
    - packages/csv-processor/src/csv_processor/cli.py

key-decisions:
  - "Dropped the plan-prescribed `# noqa: BLE001` on the new except Exception: clause — ruff's own RUF100 rule flagged it as an unused directive, since BLE001 (blind-except) does not fire on a branch that always re-raises rather than swallowing the exception; the test_run_ingest.py precedent the plan cited binds `as exc` and does not re-raise, which is a materially different shape"
  - "Reworded _failure_receipt()'s docstring to avoid the literal substring \"except Exception:\" appearing in prose (it originally referenced both branches by their literal `except X:` spelling), which was silently inflating the plan's own `grep -c \"except Exception:\"` acceptance check from the required 1 to 2"

patterns-established:
  - "Any future new exception branch added to ingest() should call the existing _failure_receipt(doc) helper rather than inlining a new Receipt(...) literal"

requirements-completed: [META-03]

# Metrics
duration: ~15min
completed: 2026-08-14
---

# Phase 04 Plan 11: ingest() Receipt-on-Every-Exit-Path Fix (WR-01) Summary

**`csv_processor.cli.ingest()` now writes a `status="FAILED"` `Receipt` to the XCom path for any exception, not only `DataPlatformError` — closing 04-REVIEW.md's WR-01 finding with a new `except Exception:` clause and the first dedicated unit-test file for this module.**

## Performance

- **Duration:** ~15 min
- **Completed:** 2026-08-14T06:08:19Z
- **Tasks:** 1
- **Files modified:** 2 (1 created, 1 modified)

## Accomplishments

- `ingest()`'s docstring has claimed since 04-05 that "a `Receipt` is written to the XCom path on every exit path, success or failure" — but the implementation only ever caught `DataPlatformError`, so a raw `psycopg.errors.DataError`, an unwrapped network error, `MemoryError`, or anything else outside that hierarchy propagated with zero Receipt ever written. That contract violation (04-REVIEW.md WR-01, elevated to a FAILED truth by 04-VERIFICATION.md) is now closed: a second `except Exception:` clause, listed after `except DataPlatformError:`, writes the identical `FAILED` Receipt shape before re-raising, for any exception class.
- Factored the Receipt construction both branches need into a new `_failure_receipt(doc: AssignmentDocument | None) -> Receipt` helper, so the two branches cannot drift apart from each other over time.
- `tests/unit/test_csv_processor_cli.py` (new) proves both directions: a plain `RuntimeError` raised inside `_build_common()` still propagates out of `ingest()`/`main()` (Airflow still sees the pod fail) AND still leaves a `status="FAILED"`, `run_id=-1` Receipt on disk; a `ConfigurationError` raised the same way is unaffected by the new clause and produces the identical outcome, proving `except DataPlatformError:` still catches its own hierarchy first per normal Python except-clause ordering.
- Full acceptance criteria verified live: `pytest tests/unit/test_csv_processor_cli.py tests/unit/test_cli_error_handling.py tests/unit -x -q` (138 tests, all pass), `ruff check` (clean, no warnings), `mypy packages/csv-processor/src` (clean), `lint-imports` (both contracts kept), `grep -c "except Exception:"` == 1, `grep -c "_failure_receipt(doc)"` == 2.

## Task Commits

Each task was committed atomically:

1. **Task 1: ingest() writes a Receipt on every exit path, not only DataPlatformError (WR-01)** - `ee3d591` (fix, tdd)

## Files Created/Modified

- `packages/csv-processor/src/csv_processor/cli.py` - Added `_failure_receipt(doc)` helper; added `except Exception:` clause after `except DataPlatformError:` in `ingest()`; updated `ingest()`'s docstring to name WR-01 and state the guarantee now covers any exception
- `tests/unit/test_csv_processor_cli.py` - New file: two tests proving the WR-01 fix (non-`DataPlatformError` path) and the pre-existing `DataPlatformError` path's Receipt-writing behavior is unaffected

## Decisions Made

- Kept the fix scoped to `ingest()` only, per the plan's explicit instruction — `discover()`'s own docstring never claims an "every exit path" Receipt guarantee (its XCom write is documented as forensic-only, never read by Airflow), and both `04-VERIFICATION.md`'s failed truth and `04-REVIEW.md`'s WR-01 finding are scoped to `ingest()` specifically.
- See `key-decisions` in frontmatter for the two ruff-driven adjustments to the plan's literal prescribed code (dropped noqa, reworded docstring).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Removed the plan-prescribed `# noqa: BLE001` — it failed the plan's own `ruff check` acceptance criterion**
- **Found during:** Task 1, running `uv run --frozen ruff check packages/csv-processor/src/csv_processor/cli.py` as specified in the task's acceptance criteria
- **Issue:** The plan's `<action>` text instructed annotating the new `except Exception:` clause with `# noqa: BLE001`, mirroring `tests/integration/test_run_ingest.py`'s `except Exception as exc:  # noqa: BLE001 -- captured so the main thread can assert on it`. But ruff's `RUF100` rule (unused-noqa) flagged the directive as unused on this exact clause: ruff's `BLE001` (blind-except) does not fire on a branch that always re-raises rather than swallowing the exception. The cited precedent binds `as exc` and appends it to a list without re-raising — a genuinely blind catch — which is a materially different shape from this plan's clause, which unconditionally re-raises after its one side effect.
- **Fix:** Removed the `# noqa: BLE001` directive, kept the full explanatory prose (renamed slightly to note that no suppression is needed and why), and re-ran `ruff check` to confirm a clean pass with zero warnings.
- **Files modified:** `packages/csv-processor/src/csv_processor/cli.py`
- **Verification:** `uv run --frozen ruff check packages/csv-processor/src/csv_processor/cli.py` → `All checks passed!`, exit code 0, no warnings
- **Committed in:** `ee3d591` (Task 1 commit)

**2. [Rule 1 - Bug] Reworded `_failure_receipt()`'s docstring — it was silently doubling the plan's own `grep -c "except Exception:"` acceptance check**
- **Found during:** Task 1, running the plan's specified `grep -c "except Exception:" packages/csv-processor/src/csv_processor/cli.py` acceptance check, which returned `2` instead of the required `1`
- **Issue:** The first draft of `_failure_receipt()`'s docstring described "both of `ingest()`'s exception branches -- `` `except DataPlatformError:` `` and `` `except Exception:` `` (WR-01)" — that prose sentence contained the literal substring `except Exception:` inside backticks, which `grep -c "except Exception:"` counted as a second match alongside the real code line, breaking the plan's own acceptance gate.
- **Fix:** Reworded the sentence to refer to "the `DataPlatformError` branch and the broader `Exception` branch added by WR-01" instead of reproducing the literal `except X:` spelling, preserving the same explanation without the accidental substring match.
- **Files modified:** `packages/csv-processor/src/csv_processor/cli.py`
- **Verification:** `grep -c "except Exception:" packages/csv-processor/src/csv_processor/cli.py` → `1`
- **Committed in:** `ee3d591` (Task 1 commit)

---

**Total deviations:** 2 auto-fixed (both Rule 1 — bugs in the plan's literal prescribed code relative to its own acceptance criteria, not in the underlying design intent)
**Impact on plan:** Both fixes are cosmetic/lint-mechanical, not behavioral — the exception-handling logic, Receipt shape, and test coverage match the plan's intent exactly. No scope creep.

## Issues Encountered

None beyond the two auto-fixed deviations above.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- WR-01 is closed. `04-REVIEW.md`'s WR-02 (no code path ever writes `meta.ingestion_runs.status = 'FAILED'`), WR-03 (`AIRFLOW_TASK_TRY_NUMBER` never set by the KPO pod spec), and WR-04 (a single cast failure at publish time aborts the whole file) remain open and out of this plan's scope — each would need its own gap-closure plan if scheduled.
- `META-03` was already marked `Complete` in `REQUIREMENTS.md` from earlier Phase 4 work (the single publication transaction it requires); `requirements.mark-complete META-03` was re-run as a safe no-op (`already_complete`, `updated: false`) and `REQUIREMENTS.md` was left untouched, so nothing further to commit there.

---
*Phase: 04-vertical-slice-csv-to-analytical-postgresql*
*Completed: 2026-08-14*
