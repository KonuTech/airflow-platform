---
phase: 06-universal-csv-engine-schema-contracts-normalization
plan: 08
subsystem: etl-library
tags: [gzip, zip, decompression, multipart, streaming, csv-processor, dataplat, hypothesis]

# Dependency graph
requires:
  - phase: 06-01
    provides: "71_zipped.csv.zip corpus fixture, zip/gzip generator+manifest support, diagnostics.py catalog with corrupted-archive/decompression-bomb-exceeded/multipart-group-incomplete codes, SourceError/FileInspectionError hierarchy"
  - phase: 06-02
    provides: "DatasetConfig.columns: contract (ColumnContract), the model this plan's multipart_pattern field extends"
provides:
  - "csv_processor.compression.open_compressed_stream() -- .gz true single-pass streaming, .zip D-22a buffered-archive-bytes exception, both decompression-bomb-bounded"
  - "csv_processor.compression.detect_compression() -- extension-based .gz/.zip dispatch"
  - "dataplat.discovery.group_multipart_units()/MultipartGroup -- multi-part object grouping by configured pattern, gap detection"
  - "dataplat.discovery.open_multipart_stream() -- reassembles grouped parts into one logical line iterator"
  - "SourceConfig.multipart_pattern -- opt-in per-dataset multipart regex field"
affects: ["06-18 (wires open_compressed_stream/group_multipart_units into CsvSource/discover_files's real call paths -- explicitly out of scope here)"]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Decompression-bomb guard as a duck-typed binary-stream wrapper (readable/writable/seekable/closed/read/read1/close) accepted directly by io.TextIOWrapper, with read()/read1() both looping over a fixed bounded inner-read chunk size so a caller's size=-1 can never materialize an entire bomb payload in one underlying call"
    - "D-22a's .zip exception: buffer only the compressed archive bytes (bounded by compressed size, D-21's one-member-per-archive scope), then stream the decompressed member exactly like .gz once open"

key-files:
  created:
    - packages/csv-processor/src/csv_processor/compression.py
    - tests/unit/test_compression.py
  modified:
    - packages/dataplat/src/dataplat/config/model.py
    - packages/dataplat/src/dataplat/discovery.py
    - tests/unit/test_discovery.py

key-decisions:
  - "open_multipart_stream does NOT skip the first physical line of subsequent streams, contradicting the plan's literal action text -- this platform's multipart corpus fixture (62_multipart_split) puts a header in the first part ONLY (verified live by generating and reading the real fixture); skipping would have silently dropped a genuine data row, exactly the failure the fixture exists to catch"
  - "open_compressed_stream's max_decompressed_bytes defaults to 512 MiB (comfortably above customers_large.csv's ~55MB, far below a real decompression-bomb attack), contract-overridable"
  - "compression.py omits a 'key' parameter present in no interface spec -- diagnostic context carries diagnostic_code/member_count/bytes_read_before_trip but no per-object identifier, keeping the function signature identical to the plan's verified interface plus Task 2's max_decompressed_bytes addition"

requirements-completed: [CSV-11, LOAD-07, QUAL-04]

# Metrics
duration: ~50min
completed: 2026-08-15
---

# Phase 6 Plan 8: Compression & Multi-Part Delivery Summary

**`.gz`/`.zip` streaming decompression with a decompression-bomb ceiling, plus Spark-style multi-part (`part-00000`/`part-00001`) discovery grouping and reassembly**

## Performance

- **Duration:** ~50 min
- **Completed:** 2026-08-15
- **Tasks:** 3
- **Files modified:** 5 (2 created, 3 modified)

## Accomplishments

- `open_compressed_stream` gives `.gz` a true zero-compromise single-pass stream (wraps the existing `io.BufferedReader(response_body)` seam with `gzip.GzipFile`, no new adapter) and `.zip` D-22a's scoped, user-confirmed exception: only the compressed archive bytes are buffered into `io.BytesIO` (bounded by compressed size, never the decompressed CSV content, never disk), then the one member streams out in small chunks exactly like `.gz`
- A multi-member or corrupted/truncated `.zip` raises `FileInspectionError` with `diagnostic_code="corrupted-archive"` (D-21) -- never a raw `zipfile.BadZipFile` escaping the module
- A decompression-bomb ceiling (`max_decompressed_bytes`, default 512 MiB) is enforced incrementally in 64 KiB bounded chunks on both paths -- a synthetic 10,000,000-byte gzip bomb trips within ~7 reads, never materializing more than ~57 KB, verified both by a fixed test and a Hypothesis property test spanning 1..2,000,000-byte payloads
- `group_multipart_units` partitions listed objects by a dataset's opt-in `multipart_pattern` regex, whole-string anchored, and raises `FileInspectionError` (`diagnostic_code="multipart-group-incomplete"`) on a gap in part indices rather than silently proceeding
- `open_multipart_stream` reassembles `62_multipart_split`'s two real generated parts into one logical 20-row CSV stream, proven via `csv.reader` over the actual fixture files (header consumed once, part 1's first data row present and not dropped)

## Task Commits

Each task was committed atomically (Task 1 is `tdd="true"`, so RED and GREEN are separate commits):

1. **Task 1 RED: failing test for `open_compressed_stream`** - `3e07d81` (test)
2. **Task 1 GREEN: `open_compressed_stream` for `.gz`/`.zip`** - `71d08cb` (feat)
3. **Task 2: decompression-bomb bound** - `315780c` (feat)
4. **Task 3: multi-part delivery grouping** - `17b82c1` (feat)

## Files Created/Modified

- `packages/csv-processor/src/csv_processor/compression.py` - `detect_compression()`, `open_compressed_stream()`, `_DecompressionBombGuard`, `_open_zip_stream()`
- `tests/unit/test_compression.py` - 8 tests: `.gz`/`.zip` streaming, multi-member/corrupted-archive rejection, uncompressed passthrough, fixed + property-based decompression-bomb tests
- `packages/dataplat/src/dataplat/config/model.py` - `SourceConfig.multipart_pattern: str | None = None`
- `packages/dataplat/src/dataplat/discovery.py` - `MultipartGroup`, `group_multipart_units()`, `open_multipart_stream()`
- `tests/unit/test_discovery.py` - 4 new tests for grouping/gap-detection/reassembly, on top of the 7 pre-existing `discover_files` tests (all still passing, no regressions)

## Decisions Made

- **`open_multipart_stream` does not skip subsequent parts' first line** (deviates from the plan's literal action text) -- see Deviations below; this is the plan's own corpus fixture telling the truth about the platform's actual multipart convention.
- **`max_decompressed_bytes` default is 512 MiB**, chosen as "comfortably above the known ~55 MB real file, far below a dangerous expansion ratio" per the plan's own guidance to document the chosen default and why.
- **Extension-based compression dispatch** (`.gz`→`gzip`, `.zip`→`zip`), not magic-byte sniffing -- 06-CONTEXT.md left this to plan discretion; extension dispatch is sufficient for this platform's synthetic, well-formed corpus.
- **`open_compressed_stream` carries no `key`/filename parameter** -- kept the function signature identical to 06-RESEARCH.md's verified interface (plus Task 2's additive `max_decompressed_bytes`), so a later integration plan (06-18) is not surprised by an unplanned required/positional argument.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `open_multipart_stream` does not skip subsequent parts' first physical line**
- **Found during:** Task 3 (multi-part delivery grouping)
- **Issue:** The plan's action text specified "reading the header from the FIRST stream only and skipping the first physical line of every subsequent stream." Generating the real `62_multipart_split` corpus fixture and reading its two actual parts (`tools/corpus/generators.py::_write_multipart`) showed `part-00001` carries ZERO header bytes -- 100% of its content is genuine data (its first row is `000011,Kowalski,91366.06`, not a header duplicate). Implementing the literal "skip" instruction would have silently dropped this row, producing 19 data rows instead of the corpus's declared 20 -- directly failing the plan's own acceptance criterion ("yields exactly 20 data lines") and violating CLAUDE.md's Core Value ("no data is ever silently dropped").
- **Fix:** `open_multipart_stream` concatenates every stream's lines unconditionally (`for stream in streams: yield from stream`), skipping nothing. Only the first stream's first line ends up as the logical header, purely because it is first in iteration order -- not because anything is special-cased.
- **Files modified:** `packages/dataplat/src/dataplat/discovery.py`, `tests/unit/test_discovery.py`
- **Verification:** `test_open_multipart_stream_reassembles_two_real_parts_into_one_twenty_row_dataset` runs `csv.reader` over the real generated fixture parts and asserts exactly 20 data rows, with `data_rows[10][0] == "000011"` (part 1's first row present as data).
- **Committed in:** `17b82c1` (Task 3 commit)

---

**Total deviations:** 1 auto-fixed (1 bug fix)
**Impact on plan:** The fix is required for correctness against the plan's own acceptance criterion and this phase's own corpus fixture -- no scope creep, no architectural change.

## Issues Encountered

- **Executor session was interrupted mid-Task-3 by a transient monthly spend-limit error** (unrelated to this plan's content) and resumed in a fresh context. On resume, verified actual on-disk/git state (`git status`, re-reading all touched files) before continuing rather than trusting the interrupted session's own memory -- confirmed Tasks 1-2 work was fully written but uncommitted, and Task 3 was partially wired (imports added, functions not yet written). No rework was needed; execution continued from the verified state.
- **`io.TextIOWrapper`'s exact duck-typing surface requirements** (`readable`/`writable`/`seekable`/`closed`/`read`/`read1`, not just `read()`) and **`io.BufferedReader`'s requirement for `readinto()` rather than `read()`** were verified empirically via scratch scripts before being relied on in `_DecompressionBombGuard` and the test doubles -- both diverge slightly from what a first read of `objectstore.py`'s docstring might suggest (which only names `readable()`/`readinto()`, the `BufferedReader` half, not the separate `TextIOWrapper` half `_DecompressionBombGuard` also needs).
- **A naive `_DecompressionBombGuard.read(size=-1)` implementation that delegated straight to the inner decompressor's single bounded read would have silently truncated ordinary (non-bomb) content** -- verified via scratch test that `io.TextIOWrapper.read()` trusts a `.read()` call to have already read until EOF, so returning a short read from a single bounded inner call gets treated as "that's everything." Fixed by making `read()` loop over bounded inner reads (checking the ceiling after each) until `size`/EOF is genuinely reached, while `read1()` keeps the single-bounded-call short-read-allowed semantics for `TextIOWrapper`'s own fast incremental path.
- **Fixed a `SyntaxWarning: invalid escape sequence '\d'`** introduced by my own `SourceConfig.multipart_pattern` docstring (a non-raw docstring containing a literal `\d` regex example) -- made the docstring raw (`r"""`). Caught via `python3 -m pytest tests/unit -q`'s warning output before it could reach review.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `open_compressed_stream`/`detect_compression`/`group_multipart_units`/`open_multipart_stream` all exist as standalone, fully tested functions but are deliberately NOT wired into `CsvSource`/`discover_files`'s real call paths yet -- per this plan's own `<verification>` scope note, that integration is explicitly a later-wave job (06-18), once every Wave 2 detector is ready to be aggregated together.
- `SourceConfig.multipart_pattern` is available for any dataset that opts in; `customers.yaml` correctly declares none (confirmed via `grep -n "multipart" configs/datasets/customers.yaml` returning no match).
- No blockers. All acceptance criteria and the plan's overall `<success_criteria>` verified passing: `.gz` verified genuinely chunked (multiple `readinto()` calls on a non-seekable double), `.zip` verified working where a raw `zipfile.ZipFile` over the same stream shape fails, decompression bomb caught within a bounded ceiling, and `62_multipart_split`'s two parts verified reassembling into one logical 20-row dataset both at the grouping layer and the line-reassembly layer.

## Self-Check: PASSED

- FOUND: packages/csv-processor/src/csv_processor/compression.py
- FOUND: tests/unit/test_compression.py
- FOUND: .planning/phases/06-universal-csv-engine-schema-contracts-normalization/06-08-SUMMARY.md
- FOUND commits: 3e07d81, 71d08cb, 315780c, 17b82c1

---
*Phase: 06-universal-csv-engine-schema-contracts-normalization*
*Completed: 2026-08-15*
