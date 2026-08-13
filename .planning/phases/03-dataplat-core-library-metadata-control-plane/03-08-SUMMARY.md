---
phase: 03-dataplat-core-library-metadata-control-plane
plan: 08
subsystem: etl
tags: [csv, streaming, chunking, hypothesis, itertools-batched, protocol-implementation]

# Dependency graph
requires:
  - phase: 03-05
    provides: "ObjectStore.get_object() / open_text_stream() — the StreamingBody-to-io.TextIOWrapper bridge this Source's text stream ultimately comes from"
  - phase: 03-06
    provides: "dataplat.sources.protocol.Source/RecordStream Protocol contracts and dataplat.models.record.RecordChunk"
provides:
  - "csv_processor.source.chunked_records() — the CSV-13 core loop: one csv.reader over a newline=\"\" text stream, chunked in records via itertools.batched, never lines or byte offsets"
  - "csv_processor.source.CsvSource/CsvRecordStream — the first concrete implementations of Wave-3's Source/RecordStream protocol"
  - "tests/unit/test_csv_chunking.py — 6 fixed-size (1/2/3) proofs: embedded LF/CRLF survival, NUL filtering, ragged-row passthrough, contiguous ordinals"
  - "tests/property/test_chunking_properties.py — a hypothesis property generalizing record-preservation and ordinal-contiguity across arbitrary chunk sizes (1-10) and record sets"
affects: [phase-04-vertical-slice-dag-and-pod]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "chunked_records(): single csv.reader over an io.TextIOWrapper(..., newline=\"\"), chunked via itertools.batched(reader, chunk_size) — the record-ordinal chunking shape every later CSV-reading code in this project follows (CSV-13)"
    - "_strip_nul(): a generator filtering NUL characters per physical line, inserted as csv.reader's source iterable instead of the raw text_stream — preserves csv.reader's own multiline-quoted-field line-granularity while still being 'the one csv.reader'"
    - "@contextlib.contextmanager method + explicit Protocol inheritance (class CsvSource(Source), class CsvRecordStream(RecordStream)) for implementing a Source.open() -> AbstractContextManager[RecordStream] contract — mirrors storage/objectstore.py's S3ObjectStore(ObjectStore) precedent"

key-files:
  created:
    - packages/csv-processor/src/csv_processor/source.py
    - tests/unit/test_csv_chunking.py
    - tests/property/__init__.py
    - tests/property/test_chunking_properties.py
  modified: []

key-decisions:
  - "Task 1 (tdd=true, no <behavior>/<implementation> split): executed as direct implementation verified by its own acceptance-criteria commands (mypy + grep), since there was no <behavior> spec to drive a literal failing-test-first cycle within that task."
  - "Property test's chunk-size assertion: initial draft only checked flattened-record-equality and ordinal-contiguity, both of which stay true even if chunk_size were silently ignored (every chunk forced to size 1) — added an explicit 'every chunk but the last has exactly chunk_size rows' assertion after manually confirming the gap via an injected chunk_size-ignoring bug, satisfying Task 3's own non-vacuousness acceptance criterion."
  - "Used itertools.pairwise() instead of zip(chunks, chunks[1:]) for the ordinal-contiguity check, after ruff RUF007 flagged the zip form and an earlier zip(..., strict=True) attempt proved always-wrong for deliberately-unequal-length sequences."

requirements-completed: [CSV-13]

# Metrics
duration: ~35min
completed: 2026-08-13
---

# Phase 3 Plan 8: CSV Record-Ordinal Chunking Source Summary

**`csv_processor.source.chunked_records()`: one `csv.reader` over a `newline=""` stream, chunked into `RecordChunk`s via `itertools.batched`, proven at fixed sizes 1/2/3 (unit) and generalized across chunk sizes 1-10 (hypothesis property) — plus `CsvSource`/`CsvRecordStream`, the first concrete `Source`/`RecordStream` implementations.**

## Performance

- **Duration:** ~35 min (approximate — exact start time not captured before the mandatory worktree-branch-check step)
- **Completed:** 2026-08-13
- **Tasks:** 3 completed
- **Files modified:** 4 (4 created, 0 modified)

## Accomplishments

- `chunked_records()` streams CSV records through exactly one `csv.reader` instance over an already-decoded `io.TextIOWrapper(..., newline="")`, chunked by record ordinal via `itertools.batched` — never a line count, never a byte offset (CSV-13, PITFALLS.md E1).
- Embedded `\n` and `\r\n` inside quoted fields survive chunking byte-identical at chunk sizes 1, 2 and 3 (`tests/unit/test_csv_chunking.py`), and the `newline=""` sensitivity was manually confirmed: removing it from the test's own stream construction demonstrably corrupts the CRLF field (`\r\n` → `\n`).
- NUL bytes are filtered per physical line, ahead of `csv.reader`, via `_strip_nul()` — verified a NUL byte embedded in raw source bytes never reaches a parsed field (cpython #71767, T-03-18).
- `csv.field_size_limit(FIELD_SIZE_LIMIT)` is set to an explicit, documented 1 MiB bound before the first `next()` call — never an unbounded limit (T-03-17).
- A ragged row (field count differing from the header's) passes through `RecordChunk.rows` completely unpadded and untruncated, with `expected_field_count` still reflecting the header's true column count.
- A hypothesis property (`tests/property/test_chunking_properties.py`) generalizes the fixed-size proof: across arbitrary well-formed CSV tables (1-5 columns, up to 25 rows) and chunk sizes 1-10, the flattened chunk output always equals the generated input exactly, every chunk but the last holds exactly `chunk_size` rows, and ordinals stay contiguous and non-overlapping. Manually confirmed non-vacuous by injecting a "`chunk_size` silently ignored, every chunk forced to size 1" bug and observing the property fail.
- `CsvSource`/`CsvRecordStream` are the first concrete implementations of `dataplat.sources.protocol.Source`/`RecordStream` (structurally verified via `mypy --strict` and explicit Protocol inheritance, matching the `S3ObjectStore(ObjectStore)` precedent). `CsvRecordStream.chunks(start_ordinal=...)` re-streams from the top and discards whole chunks ending at or before `start_ordinal` (CSV cannot seek into a quoted multiline field). `CsvSource.open()` closes the underlying text stream in a `finally` block regardless of how the context manager exits.
- Full `make check` gate passes: `uv lock --check`, ruff check/format, mypy strict (44 source files), import-linter (`dataplat` still does not depend on `csv_processor`), 112 policy tests, 98 unit+regression tests, and fixture-corpus byte-identity verification — all green.

## Task Commits

Each task was committed atomically:

1. **Task 1: `chunked_records()` and the `CsvSource`/`CsvRecordStream` implementation** - `b7fe9b0` (feat)
2. **Task 2: Prove embedded newlines survive chunking at sizes 1, 2 and 3** - `d1b6439` (test)
3. **Task 3: Property test — chunking never drops, reorders or splits records** - `60a475b` (test)

**Plan metadata:** (this commit, made after this SUMMARY)

_Tasks 1-2 are `tdd="true"`; Task 1 had no `<behavior>`/`<implementation>` split (only `<action>`), so it was executed as a single implementation commit verified by its own acceptance-criteria commands rather than a literal RED/GREEN pair — see Decisions Made._

## Files Created/Modified

- `packages/csv-processor/src/csv_processor/source.py` - `chunked_records()`, `_strip_nul()`, `CsvRecordStream`, `CsvSource` — the CSV-13 chunking core and the first concrete `Source`/`RecordStream`
- `tests/unit/test_csv_chunking.py` - 6 tests: embedded LF at size 1, chunk-size-invariant content at sizes 2/3, embedded CRLF round-trip, NUL filtering, ragged-row passthrough, contiguous ordinals
- `tests/property/__init__.py` - empty package marker, matching `tests/unit/__init__.py`
- `tests/property/test_chunking_properties.py` - `test_chunking_preserves_record_set_and_order`: a hypothesis property generalizing record-preservation, chunk-size-respecting grouping and ordinal contiguity across generated tables and chunk sizes 1-10 (`max_examples=200`, ~1s runtime)

## Decisions Made

- **Task 1's tdd="true" without a `<behavior>`/`<implementation>` split:** treated as a direct implementation task, verified by its own stated acceptance-criteria commands (`mypy` + two `grep` checks) rather than forcing an artificial write-a-failing-test-first cycle the task text never specified. Tasks 2 and 3 are where actual test behavior is specified and exercised.
- **`FIELD_SIZE_LIMIT` comment wording:** the first draft's explanatory comment contained the literal substring `sys.maxsize` (in "never sys.maxsize"), which the Task 1 verify script's `! grep -n "sys.maxsize"` correctly flagged as a false-positive-triggering match against the file as a whole, not just code. Reworded to "never left unbounded" — same meaning, no longer matches the literal token the grep exists to catch in *code*.
- **Property test's grouping assertion:** Task 3's acceptance criteria explicitly requires manually verifying the property fails when `chunk_size` is silently ignored (every chunk forced to size 1) — the first draft's assertions (flattened-record-equality, ordinal-contiguity) both stay true even under that bug, since flattening discards grouping and per-chunk-of-size-1 ordinals are still contiguous. Added an explicit `len(chunk.rows) == chunk_size` check for every chunk but the last, then re-confirmed both that the strengthened property still passes against the real implementation and that it fails against an injected "`itertools.batched(reader, 1)` hardcoded" bug.
- **`itertools.pairwise()` over `zip(chunks, chunks[1:])`:** ruff's `RUF007` flagged the zip form; `zip(..., strict=True)` is categorically wrong here since `chunks[1:]` is deliberately one element shorter than `chunks` (pairing each chunk with its successor), so `itertools.pairwise(chunks)` is both the idiomatic and the correct fix.

## Deviations from Plan

None - plan executed exactly as written. (The three items in "Decisions Made" above are self-caught refinements made to my own not-yet-committed draft code/tests during development — none of them were ever committed in a broken form, so none rise to a Rule 1-4 deviation against previously-existing code or plan text, unlike 03-06's genuine plan-text action/verify contradiction.)

## Issues Encountered

- At startup, the worktree's HEAD was found on `78edd19` — a stale commit that predates all of Phase 3's planning and is an ancestor of the assigned base `7075cac9fc074d85d211c5744c0084313b64892d` — rather than the assigned base itself. Confirmed `git status --short` showed no uncommitted changes before running `git reset --hard 7075cac9fc074d85d211c5744c0084313b64892d` to correct it, per the `<worktree_branch_check>` protocol's sanctioned startup-only exception.
- `make check`'s `policy` step took ~12 minutes, entirely inside `tests/policy/test_corpus_determinism.py::test_two_generations_in_one_process_agree`, which is `@pytest.mark.slow` by design (it materializes the ~293 MB large-profile fixture twice, per its own module docstring) — not a hang and not related to this plan's changes. Diagnosed by running the same suite verbosely with a bounded timeout in a parallel, read-only invocation before letting the original background run finish naturally.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `csv_processor.source.CsvSource` is ready for Phase 4's vertical-slice DAG to plug in as a real `Source` on day one, per this plan's stated purpose.
- **Known coverage gap, in-scope for a future plan rather than this one:** `tests/unit`/`tests/regression` coverage for `packages/csv-processor/src/csv_processor/source.py` is 59% (`chunked_records()` itself is 100% covered; the uncovered lines are entirely inside `CsvRecordStream.__init__`/`.chunks()` and `CsvSource.__init__`/`.open()`). This matches the plan's own explicit test scope exactly — both Task 2's and Task 3's `<action>` blocks call `chunked_records()` directly and never instantiate `CsvSource`/`CsvRecordStream` — and `CsvSource`/`CsvRecordStream`'s protocol conformance is independently verified structurally via `mypy --strict`. Phase 4 is the first real caller of `CsvSource.open()` against a live `PipelineContext`; a fast-follow unit test using a fake `ObjectStore`/`PipelineContext` (the `tests/unit/test_pipeline_errors.py::_make_context()` pattern) would close this gap without waiting for Phase 4 if desired sooner.
- No blockers. This was the phase's final plan (Wave 5 of 5).

## Self-Check: PASSED

All 4 files claimed above verified present on disk, plus this SUMMARY. All 3
claimed commit hashes (`b7fe9b0`, `d1b6439`, `60a475b`) verified present in
`git log --oneline --all`. No missing items.

---
*Phase: 03-dataplat-core-library-metadata-control-plane*
*Completed: 2026-08-13*
