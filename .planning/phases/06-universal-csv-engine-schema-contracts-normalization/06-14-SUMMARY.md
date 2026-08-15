---
phase: 06-universal-csv-engine-schema-contracts-normalization
plan: 14
subsystem: etl
tags: [csv, detection, streaming, compression, gzip, zip, encoding, dialect, header]

# Dependency graph
requires:
  - phase: 06-universal-csv-engine-schema-contracts-normalization (wave 2)
    provides: "csv_processor.detect.filename (06-03), .encoding (06-04), .dialect (06-05), .header (06-06), csv_processor.compression (06-08) -- five independently-built, independently-tested detectors/the compression layer this plan aggregates"
provides:
  - "dataplat.models.profile.CsvProfile -- the plain-data aggregate every detector's finding assembles into"
  - "Source.inspect(ctx) -> CsvProfile added to the dataplat.sources.protocol.Source Protocol"
  - "CsvSource.inspect() -- the real aggregation point: detect_compression -> detect_encoding -> decode_strict -> detect_dialect -> detect_header -> (opt-in) parse_filename, in that order"
  - "CsvSource.open() now builds its csv.reader from CsvSource.inspect()'s detected profile (encoding/dialect/header row/max_field_bytes/compression) instead of Phase 3's D-01 hardcoded UTF-8/comma/header-row-0 shape"
  - "chunked_records()/CsvRecordStream generalized to accept dialect/preamble_rows/max_field_bytes parameters, backward-compatible with every existing caller"
affects: [06-15, 06-16, 06-17, 06-18]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Source.inspect(ctx) -> Profile as a one-time, whole-file detection pass (Pattern 1, 06-RESEARCH.md) -- never a per-chunk StreamingStage"
    - "TextIOWrapper.buffer as the sanctioned raw-byte access point for an already-open ObjectStore.get_object() result (matches dataplat.discovery's own established precedent), routed through open_compressed_stream so a sample read is decompression-aware"

key-files:
  created:
    - packages/dataplat/src/dataplat/models/profile.py
    - tests/unit/test_csv_source_inspect.py
  modified:
    - packages/dataplat/src/dataplat/sources/protocol.py
    - packages/dataplat/src/dataplat/sources/__init__.py
    - packages/csv-processor/src/csv_processor/source.py
    - packages/csv-processor/src/csv_processor/compression.py

key-decisions:
  - "CsvSource.open() always calls self.inspect(ctx) internally (never accepts a pre-computed CsvProfile) -- the one real production call site (StagingLoader.load()'s `with source.open(ctx) as stream:`) has no profile to pass in, so a dual-path design would add complexity with no real consumer"
  - "Raw-byte sample reads (both inspect()'s sample and open()'s full content stream) go through ctx.objects.get_object(...).buffer -- TextIOWrapper's own public binary-buffer attribute, not a new ObjectStore protocol method -- reusing dataplat.discovery.discover_files's own already-reviewed precedent for the identical need"
  - "footer_patterns/skip_footer_rows are NOT threaded from ctx.config.csv into detect_header() in this plan -- the plan's own literal Task 1 action text omits them, footer_patterns has no CsvParsingConfig field yet (06-06-SUMMARY.md's own deferred note), and none of this plan's 5 fixtures exercise footer detection. footer_row_count still gets a real value from detect_header's built-in field-count-mismatch heuristic."

patterns-established:
  - "Pattern 1 (06-RESEARCH.md) is now real code, not a sketch: every detector runs once in Source.inspect(), before any RecordStream exists."

requirements-completed: [CSV-02, CSV-11, LOAD-07, QUAL-04]

# Metrics
duration: 34min
completed: 2026-08-15
---

# Phase 6 Plan 14: Wire Detectors into CsvSource.inspect()/open() Summary

**`CsvSource.inspect()` aggregates all five Wave-2 detectors plus compression dispatch into a real `CsvProfile`, and `CsvSource.open()` now actually builds its `csv.reader` from that profile — Phase 3's `03-CONTEXT.md` D-01 hardcoded UTF-8/comma/header-row-0 shape is gone from the real call path.**

## Performance

- **Duration:** ~34 min
- **Started:** 2026-08-15T14:03Z
- **Completed:** 2026-08-15T14:37Z
- **Tasks:** 2
- **Files modified:** 6 (2 created, 4 modified)

## Accomplishments

- `dataplat.models.profile.CsvProfile` — a plain-data dataclass (no `csv_processor` imports; import-linter contract 1 stays green) holding every field `CsvSource.open()`/chunking need.
- `Source.inspect(ctx) -> CsvProfile` landed on the `dataplat.sources.protocol.Source` Protocol, closing the seam `03-CONTEXT.md` explicitly deferred to Phase 6.
- `CsvSource.inspect()` is the real convergence point Pattern 1 (06-RESEARCH.md) named: `detect_compression` → `detect_encoding` → `decode_strict` → `detect_dialect` → `detect_header` → (opt-in) `parse_filename`, aggregated into one `CsvProfile`. Reads a single, fixed 64 KiB decompressed sample via the already-open `ObjectStore.get_object()` result's own `TextIOWrapper.buffer` attribute (T-06-30's mitigation — a fixed, documented, comment-visible bound).
- `CsvSource.open()` now consumes that profile: the real detected/contract dialect (raising `CsvDialectDetectionError` when genuinely undecidable, never silently defaulting to comma), the real detected header-row offset, real decompression for `.gz`/`.zip` objects, and the contract's own `max_field_bytes` — not the retired module constant.
- `tests/unit/test_csv_source_inspect.py` proves the real, wired call chain against 5 corpus fixtures: `01_simple.csv` (clean control), `06_windows1250.csv` (non-UTF-8, semicolon), `68_utf8_bom_semicolon_pl_excel.csv` (BOM + semicolon + comma-decimal amounts — proving dialect detection runs before any numeric interpretation, not just in `dialect.py`'s own isolated tests), and both `61_gzipped.csv.gz`/`71_zipped.csv.zip` recovering `01_simple.csv`'s exact 20 rows through the real `open()` call path.

## Task Commits

Each task was committed atomically:

1. **Task 1: CsvProfile + Source.inspect() protocol addition + CsvSource.inspect() aggregation** - `530b742` (feat)
2. **Task 2: CsvSource.open() consumes the detected profile; FIELD_SIZE_LIMIT retired** - `76e4695` (feat)

_Note: worktree mode — no separate plan-metadata commit for STATE.md/ROADMAP.md; the orchestrator owns those after merge. This SUMMARY.md/REQUIREMENTS.md commit is the plan-metadata commit for this worktree._

## Files Created/Modified

- `packages/dataplat/src/dataplat/models/profile.py` - New: `CsvProfile` frozen dataclass, the plain-data aggregate every detector's finding assembles into
- `packages/dataplat/src/dataplat/sources/protocol.py` - `Source.inspect(ctx) -> CsvProfile` added to the Protocol; module docstring updated to reflect Phase 6 landing it
- `packages/dataplat/src/dataplat/sources/__init__.py` - Docstring updated (no longer claims `inspect()` is absent)
- `packages/csv-processor/src/csv_processor/source.py` - `CsvSource.inspect()` added; `CsvSource.open()`/`chunked_records()`/`CsvRecordStream` rewired to consume the detected `CsvProfile` instead of D-01's hardcoded constants
- `packages/csv-processor/src/csv_processor/compression.py` - `_DecompressionBombGuard.flush()` no-op added (Rule 1 bug fix, see Deviations)
- `tests/unit/test_csv_source_inspect.py` - New: corpus-fixture proof of the wired `inspect()`/`open()` call chain

## Decisions Made

- `CsvSource.open()` always calls `self.inspect(ctx)` internally rather than accepting an optional pre-computed profile — the sole real call site (`StagingLoader.load()`) never has one to pass, so every real `open()` performs two separate `ctx.objects.get_object` fetches (one bounded sample inside `inspect()`, one full fetch in `open()`). Documented in `open()`'s own docstring as an accepted cost: the sample is bounded, so the duplicated read is cheap regardless of the object's real size.
- Raw-byte access (both the sample read and the full-content stream fed to `open_compressed_stream`) goes through `ctx.objects.get_object(...).buffer` — `TextIOWrapper`'s own public binary-buffer attribute. This is not a new pattern: `dataplat.discovery.discover_files` already uses the identical `stream.buffer.read(...)` idiom for its own raw-bytes content hash, with the same "public attribute, never `StreamingBody`'s forbidden private internal state" framing already documented in `storage/objectstore.py`'s module docstring. No `ObjectStore` protocol change was needed.
- `footer_patterns`/`skip_footer_rows` are not threaded from `ctx.config.csv` into `detect_header()` in this plan — the plan's own Task 1 action text gives the literal `detect_header()` call with only `contract_header_row`/`header_trim`, `footer_patterns` has no `CsvParsingConfig` field yet (`06-06-SUMMARY.md`'s own recorded deferral), and none of this plan's 5 fixtures exercise footer detection. `footer_row_count` still gets a real value from `detect_header`'s built-in field-count-mismatch heuristic, so nothing is silently dropped — a future plan adds the config field and threads it through when a footer-bearing fixture needs the override.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `_DecompressionBombGuard` had no `flush()` method — every gzip/zip stream this codebase ever built raised `AttributeError` the moment it was closed**
- **Found during:** Task 1, while validating the `TextIOWrapper.buffer`-based sample-read design against real gzip/zip bytes before writing `inspect()`'s implementation
- **Issue:** `io.TextIOWrapper.close()` unconditionally calls `self.buffer.flush()` before closing the buffer it wraps. `compression.py`'s `.gz`/`.zip` paths hand `_DecompressionBombGuard` to `io.TextIOWrapper` directly as its `buffer` (no intermediate `io.BufferedReader`), and the guard had no `flush()` method — so a fully successful gzip/zip read still raised `AttributeError: '_DecompressionBombGuard' object has no attribute 'flush'` the instant a caller closed the stream. This is a pre-existing defect from plan 06-08, invisible until a real caller (`CsvSource.inspect()`/`.open()`, both of which always close their streams in a `finally` block) actually exercised the close path — `tests/unit/test_compression.py`'s own isolated tests read the full content but were not observed to close the returned stream via the exact same path this plan's code does.
- **Fix:** Added a no-op `flush()` method to `_DecompressionBombGuard`, matching a read-only stream's correct behavior (mirrors `io.RawIOBase.flush()`'s own default no-op).
- **Files modified:** `packages/csv-processor/src/csv_processor/compression.py`
- **Verification:** Verified live (sandbox script) that gzip/zip/uncompressed round trips all complete AND close cleanly after the fix; `tests/unit/test_csv_source_inspect.py`'s `61_gzipped.csv.gz`/`71_zipped.csv.zip` tests (which close their stream via `CsvSource.open()`'s own context manager) pass.
- **Committed in:** `530b742` (Task 1 commit)

**2. [Rule 1 - Bug] `CsvSource.open()`'s first draft double-counted the header-row skip**
- **Found during:** Task 2, via this task's own new test (`test_open_recovers_exact_row_content_for_01_simple` failed: 19 rows instead of 20, missing row `000001`) — caught immediately, before commit
- **Issue:** `open()` computed `preamble_rows = profile.header_row_index + 1` and passed it to `CsvRecordStream`/`chunked_records`, but `chunked_records` itself already discards exactly one more row (the header) after skipping `preamble_rows` rows — so for `header_row_index=0` (a header at row 0, the common case), the code skipped 1 preamble row (actually the header) AND then discarded the first real data row as a second "header", losing it.
- **Fix:** Changed the computation to `preamble_rows = profile.header_row_index` (no `+ 1`) — `chunked_records`'s own unconditional header-row read supplies the `+ 1`.
- **Files modified:** `packages/csv-processor/src/csv_processor/source.py`
- **Verification:** `test_open_recovers_exact_row_content_for_01_simple` and all other `tests/unit/test_csv_source_inspect.py` tests pass; full `tests/unit` suite (373 tests) passes.
- **Committed in:** `76e4695` (Task 2 commit, the fix landed before the task's own commit — never a separately-visible broken intermediate state)

---

**Total deviations:** 2 auto-fixed (2 Rule 1 bugs)
**Impact on plan:** Both fixes were necessary for this plan's own stated acceptance criteria (both compression fixtures recovering exact row content through the real `open()`/close() path) to be true at all. No scope creep — both fixes are narrowly scoped to the exact defect found, in files this plan's own work directly exercises.

## Issues Encountered

- **`ObjectStore.get_object()` returns already-decoded `io.TextIOWrapper`, not raw bytes** — the plan's Task 1 action text says to read "a bounded sample of the object's raw bytes (via `ctx.objects.get_object`", but `ObjectStore.get_object()`'s actual signature returns `io.TextIOWrapper` (hardcoded UTF-8 decode in the real `S3ObjectStore`). Resolved by using `TextIOWrapper.buffer` (the wrapper's own public underlying binary-buffer attribute) for every raw-byte read — verified as the codebase's own established, already-reviewed pattern (`dataplat.discovery.discover_files` already does exactly this for its content hash), not a new workaround. No `ObjectStore` protocol change was needed; not treated as a deviation since it is a direct, literal, minimal reading of what "via `ctx.objects.get_object`" can mean given the actual return type, fully within Task 1/2's declared file scope.
- **Worktree venv:** this worktree had no `.venv` of its own; the shared `/home/konutec/projects/airflow-platform/.venv` resolves `dataplat`/`csv_processor` via editable installs pointing at the MAIN repo's `packages/` tree, not this worktree's. Ran `uv sync --frozen` inside the worktree to build an isolated, worktree-scoped venv before any verification — without this, every test/lint/typecheck run in this session would have silently validated the wrong tree.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `CsvSource.inspect()`/`.open()` are the real, live, wired convergence point every later Phase 6 plan (schema versioning/evolution, normalization wiring) builds on top of — `CsvProfile` is now the one place a later plan reads detected encoding/dialect/header/compression from.
- `dataplat.sources.protocol.Source` Protocol is final-shape for this phase: any future `Source` implementation (a future Kafka/DB CDC source) now has both `open()` and `inspect()` to implement.
- Known, deliberately out-of-scope gap carried forward unchanged (not introduced by this plan): a genuinely headerless file (`CsvProfile.header_row_index is None`, corpus fixture `11_no_header.csv`) still has its first data row silently discarded by `chunked_records`'s own unconditional header read — this reproduces Phase 3's original D-01 limitation for this one untested-by-this-plan case, not a regression. No fixture in this plan's declared scope exercises it; a future plan that wires `11_no_header.csv` through `open()` needs to address it.
- `footer_patterns` still has no `CsvParsingConfig` field (recorded first in `06-06-SUMMARY.md`, unaddressed here too, in scope). `skip_footer_rows` exists on the config model but is not yet threaded into `CsvSource.inspect()`'s `detect_header()` call, per this plan's own literal action text.

---
*Phase: 06-universal-csv-engine-schema-contracts-normalization*
*Completed: 2026-08-15*

## Self-Check: PASSED

- FOUND: packages/dataplat/src/dataplat/models/profile.py
- FOUND: tests/unit/test_csv_source_inspect.py
- FOUND: packages/dataplat/src/dataplat/sources/protocol.py
- FOUND: packages/csv-processor/src/csv_processor/source.py
- FOUND: packages/csv-processor/src/csv_processor/compression.py
- FOUND commit: 530b742
- FOUND commit: 76e4695
