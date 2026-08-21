---
phase: 10-slowly-changing-dimensions
plan: 02
subsystem: database
tags: [scd, scd2, hashing, tdd, hypothesis, python]

# Dependency graph
requires: []
provides:
  - "dataplat.scd.hashing.tracked_attribute_hash -- deterministic normalized-hash change detection (SCD-05)"
  - "dataplat.scd.recompute.recompute_version_chain -- full-history SCD2 version-chain recomputation with Type-0/1/2 dispatch (SCD-01/02/04)"
  - "dataplat.scd.recompute.BronzeRecord / VersionRow frozen dataclasses"
affects: [10-04-scd-publisher, 10-slowly-changing-dimensions]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Pure-function-first TDD spike: prove an algorithm (LEAD()/change-point recompute shape) as plain Python against concrete edge cases before a later plan assembles a DB-touching pipeline around it"
    - "Python's own `!=` is already NULL-safe/IS-DISTINCT-FROM-equivalent for `bytes | None` comparisons -- no special-casing needed outside SQL contexts"

key-files:
  created:
    - packages/dataplat/src/dataplat/scd/__init__.py
    - packages/dataplat/src/dataplat/scd/hashing.py
    - packages/dataplat/src/dataplat/scd/recompute.py
    - tests/unit/test_scd_hashing.py
    - tests/unit/test_scd_recompute.py
  modified: []

key-decisions:
  - "tracked_attribute_hash reuses staging.py's exact _record_hash recipe (pipe-join, None -> \"\", SHA-256) rather than inventing a second convention"
  - "hash_version accepted but not yet mixed into the hash bytes -- documented extension point, mirrors _record_hash_version's stored-alongside-not-baked-in convention (META-02)"
  - "recompute_version_chain never re-normalizes (no unicodedata import) -- the caller's UnicodeNormalizer (D-15) already guarantees NFC-normalized input"

patterns-established:
  - "VersionRow deliberately carries no surrogate/id field -- proven structurally via a dataclasses.fields() assertion, not just documentation"

requirements-completed: [SCD-01, SCD-02, SCD-04, SCD-05]

# Metrics
duration: 25min
completed: 2026-08-21
---

# Phase 10 Plan 02: SCD Recompute & Hashing Building Blocks Summary

**Pure-function SCD2 recompute (Type-0/1/2 dispatch) and deterministic tracked-attribute hashing, both TDD-proven with zero database dependency, de-risking plan 10-04's SCDPublisher assembly.**

## Performance

- **Duration:** 25 min
- **Started:** 2026-08-21T11:49:00Z
- **Completed:** 2026-08-21T12:14:59Z
- **Tasks:** 2
- **Files modified:** 5 (3 created source, 2 created test)

## Accomplishments

- `tracked_attribute_hash(*values, hash_version=1) -> bytes` — deterministic SHA-256 over
  pipe-joined, None-safe tracked values, byte-for-byte matching `staging.py`'s own
  `_record_hash` recipe, proven with a hypothesis property test plus an explicit NFC/NFD
  caller-normalization test.
- `recompute_version_chain(history, *, valid_to_sentinel) -> list[VersionRow]` — full-history
  SCD2 recomputation dispatching Type-0 (earliest-wins), Type-1 (latest-wins), and Type-2
  (change-point versioning) columns, proven against 8 concrete behaviors including both edge
  cases RESEARCH.md's own Assumption A2 (identical `event_ts` tie-break) and Open Question 3
  (schema-evolution-on-untracked-column irrelevance) explicitly flagged as needing verification.
- Both functions independently confirmed via structural AST-based guards (not string search) to
  have zero I/O / zero `unicodedata`/`psycopg` coupling, matching the plan's "pure function, no
  ctx, no DB connection" contract.

## Task Commits

Each task followed strict RED -> GREEN TDD discipline:

1. **Task 1: tracked_attribute_hash**
   - `89a893e` test(10-02): add failing test for tracked_attribute_hash (SCD-05) — RED, confirmed `ModuleNotFoundError`
   - `ae21751` feat(10-02): implement tracked_attribute_hash (SCD-05) — GREEN, all 6 tests pass
2. **Task 2: recompute_version_chain**
   - `062a634` test(10-02): add failing test for recompute_version_chain (SCD-01/02/04) — RED, confirmed `ModuleNotFoundError`
   - `f550a8e` feat(10-02): implement recompute_version_chain (SCD-01/02/04, Assumption-A2 spike) — GREEN, all 9 tests pass

**Plan metadata:** this commit (docs: complete plan, worktree mode — SUMMARY.md only, STATE.md/ROADMAP.md excluded)

## Files Created/Modified

- `packages/dataplat/src/dataplat/scd/__init__.py` — package marker
- `packages/dataplat/src/dataplat/scd/hashing.py` — `tracked_attribute_hash`
- `packages/dataplat/src/dataplat/scd/recompute.py` — `BronzeRecord`, `VersionRow`, `recompute_version_chain`
- `tests/unit/test_scd_hashing.py` — 6 tests (5 behaviors + 1 structural guard)
- `tests/unit/test_scd_recompute.py` — 9 tests (8 behaviors + 1 structural guard)

## Decisions Made

- Reused `staging.py`'s exact hash recipe (pipe-join, `None -> ""`, SHA-256 over UTF-8) rather
  than a new one — one auditable hash-computation convention across the codebase.
- Python's native `!=` on `bytes | None` is already NULL-safe/IS-DISTINCT-FROM-equivalent — no
  special-casing was added in `recompute.py`; the module docstring documents explicitly why a
  future SQL port of this logic would need different handling than this Python version does.

## Deviations from Plan

None functionally — the plan's described algorithm and hash recipe were implemented exactly as
specified. Two test-authoring bugs were found and fixed during the GREEN step (both are test
fixes, not implementation deviations, so tracked here for completeness rather than as Rule 1-4
deviations against plan-specified behavior):

**1. [Test bug, found during GREEN] Hypothesis property test asserted a false collision as a failure**
- **Found during:** Task 1 GREEN run
- **Issue:** The property test asserted any two unequal `(name, country)` tuples must hash
  differently, but `(None, None)` and `(None, "")` are a DELIBERATE, documented hash collision
  (matching `staging.py`'s own `None -> ""` convention) — the test's own assumption was wrong,
  not the implementation.
- **Fix:** Added `assume(normalized_a != normalized_b)` (mapping `None -> ""` before comparing)
  to exclude this known-equivalent pair from the distinctness assertion.
- **Files modified:** `tests/unit/test_scd_hashing.py`
- **Committed in:** `ae21751` (part of Task 1 GREEN commit)

**2. [Test bug, found during GREEN] Structural no-`unicodedata` guard used raw text search**
- **Found during:** Task 1 GREEN run
- **Issue:** The original guard checked whether the literal string `"unicodedata"` appeared
  anywhere in the file's text, which fired on the module's own docstring (which legitimately
  discusses why the function does NOT call `unicodedata.normalize`).
- **Fix:** Rewrote the guard to walk the module's AST and check for actual `Import`/`ImportFrom`/
  `unicodedata.normalize` attribute-access nodes, matching the plan's stated grep-style intent
  ("no `psycopg`/`Connection` import") without penalizing explanatory prose.
- **Files modified:** `tests/unit/test_scd_hashing.py`, `tests/unit/test_scd_recompute.py`
  (same pattern applied to the `psycopg` guard for consistency)
- **Committed in:** `ae21751`, `f550a8e`

---

**Total deviations:** 0 implementation deviations; 2 test-authoring fixes during GREEN (both
found and corrected within the same TDD cycle, before commit).
**Impact on plan:** None — plan executed exactly as specified; both fixes tightened test
correctness rather than changing scope.

## Issues Encountered

None beyond the two test-authoring bugs above (already documented). Lint (ruff, `select = ["ALL"]`)
and `mypy --strict` both required minor adjustments during GREEN (moving `datetime`/`Sequence`
imports into a `TYPE_CHECKING` block, replacing a manual for-loop with `list.extend`, replacing
`open()` with `Path.read_text()`, moving function-local `import ast` to module level, and
shortening three over-100-character docstring lines) — all mechanical, zero behavior change,
folded into the same GREEN commits.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- `dataplat.scd.hashing.tracked_attribute_hash` and `dataplat.scd.recompute.recompute_version_chain`
  are both fully unit-tested, DB-free, and ready for plan 10-04's `SCDPublisher` to assemble the
  real read-from-bronze/write-to-warehouse pipeline around.
- `python -c "import dataplat.scd.recompute, dataplat.scd.hashing"` succeeds with no DB/network
  dependency; the full unit suite (526 tests) passes with zero import-time breakage introduced.
- No blockers or concerns for downstream plans.

---
*Phase: 10-slowly-changing-dimensions*
*Completed: 2026-08-21*
