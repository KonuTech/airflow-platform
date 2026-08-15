---
phase: 06-universal-csv-engine-schema-contracts-normalization
plan: 16
subsystem: database
tags: [postgresql, normalization, unicode-nfc, idempotency-key, schema-versioning, psycopg]

# Dependency graph
requires:
  - phase: 06-universal-csv-engine-schema-contracts-normalization (Wave 2, plans 06-08 through 06-12)
    provides: DateNormalizer, NumericNormalizer, NullTokenNormalizer, BooleanNormalizer, UnicodeNormalizer (built and unit-tested in isolation, never wired into a real call site) and SchemaRepository (schema-version CRUD, never consulted by discovery)
provides:
  - StagingLoader._build_stages(ctx): the ONE real call site that threads all four normalizers into run_streaming(), with nullable-column NullTokenNormalizer-before-type-specific-normalizer ordering and unconditional-last UnicodeNormalizer, live against customers.birth_date/event_ts
  - discover_files()'s idempotency key extended with a real schema_version term (Pitfall 5), resolved once per call via SchemaRepository.get_current()
  - discover_files()'s filename_facets_by_object parameter (D-11 signature groundwork, no live caller yet)
  - csv_processor.cli.discover() wired to the new required schema= parameter, so the real KPO production entrypoint is never broken
affects: [06-15 (schema-sync wiring, first populates a real schema_version for discover_files to resolve), 06-17 (determinism property test over normalized _record_hash), 06-18 (multipart delivery through the same discover_files signature)]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "StagingLoader._build_stages(ctx) constructs one StreamingStage list per run from ctx.config.columns, matching each ColumnContract to its target_columns index by name lookup (never assumed positional alignment)"
    - "Idempotency-key formulas are extended by string-appending a new term after the existing ones, never reordering/replacing (Pitfall 5's own documented convention)"
    - "A caller-precomputed Mapping[str, Mapping[str, object]] parameter (filename_facets_by_object) keeps dataplat free of a csv_processor import while still accepting caller-derived per-object facets"

key-files:
  created:
    - tests/integration/test_staging_normalization.py
  modified:
    - packages/dataplat/src/dataplat/load/staging.py
    - packages/dataplat/src/dataplat/discovery.py
    - packages/csv-processor/src/csv_processor/cli.py
    - tests/unit/test_discovery.py
    - tests/integration/test_discover_files.py

key-decisions:
  - "NullTokenNormalizer's null_tokens parameter is built as the UNION of ctx.config.normalization.null_tokens and .null_sentinels[column.name] (per NormalizationConfig.null_sentinels's own docstring: 'Checked in addition to null_tokens'), not one replacing the other"
  - "The idempotency key's new schema_version term is SchemaVersionRecord.version (the integer version number, stringified), not schema_hash -- matches ARCHITECTURE.md Q7's 'schema_version' vocabulary distinctly from the separate 'policy_digest'-shaped hash terms"
  - "_record_hash's per-field rendering (None -> '', non-str -> str(value)) mirrors dataplat.normalize.numeric._row_to_raw_line's established convention exactly, rather than inventing a new encoding"
  - "filename_facets_by_object's extracted business_date facet is surfaced only in discover_files' own log lines (observability), never persisted -- MetadataRepository.create_file has no business_date parameter to receive it, and adding one is out of this plan's file scope"

requirements-completed: [CSV-01, CSV-09, CSV-10, CSV-12, SCHEMA-03, QUAL-04]

duration: 35min
completed: 2026-08-15
---

# Phase 06 Plan 16: Wire Normalizers into StagingLoader, Extend Idempotency Key Summary

**`StagingLoader.load()` now runs DateNormalizer/NumericNormalizer/BooleanNormalizer/NullTokenNormalizer/UnicodeNormalizer for real against `customers`, proven live including the empty-`birth_date`-stages-as-NULL regression case; `discover_files()`'s idempotency key gains a real `schema_version` term.**

## Performance

- **Duration:** ~35 min
- **Completed:** 2026-08-15
- **Tasks:** 3
- **Files modified:** 5 modified, 1 created (matches plan's own `files_modified` exactly)

## Accomplishments

- `StagingLoader._build_stages(ctx)` assembles `RaggedRowGuard()` first, then per `ColumnContract` a nullable column's `NullTokenNormalizer` strictly before its type-specific normalizer, then `UnicodeNormalizer()` unconditionally last — replacing the hardcoded `stages=[RaggedRowGuard()]` literal that was the only thing preventing all four Wave-2 normalizers from ever running against real data.
- Found and fixed a real crash `_build_stages`'s own wiring would otherwise introduce: `_record_hash`'s `"|".join(row)` call chokes on the `None`/`bool` fields the newly-wired normalizers now legitimately produce. Fixed to mirror `dataplat.normalize.numeric._row_to_raw_line`'s established defensive rendering.
- `discover_files()`'s idempotency key now appends a real `schema_version` term (Pitfall 5), resolved once per call via a new required `schema: SchemaRepository` parameter — never reordering the original four terms.
- `csv_processor.cli.discover()` (the real `KubernetesPodOperator` entrypoint) and `tests/integration/test_discover_files.py`'s 5 pre-existing tests both gained the new required parameter in this same plan — never left broken across a wave boundary.
- New `tests/integration/test_staging_normalization.py` proves, against a live database, that `customers`' real config normalizes correctly end-to-end: a clean row round-trips without corruption, an invalid date is rejected (not silently staged), an empty `birth_date` value stages as SQL `NULL` (the direct regression proof for this plan's BLOCKER), and an NFC/NFD `name` pair converges to the same staged text and `_record_hash`.

## Task Commits

Each task was committed atomically:

1. **Task 1: Construct the normalizer stage list from ctx.config; wire it into StagingLoader.load()** - `b69fec6` (feat)
2. **Task 2: discovery.py's idempotency-key extension, filename business_date, and wiring the new required schema parameter** - `48d7472` (feat)
3. **Task 3: Integration proof — customers' birth_date/event_ts normalize end-to-end** - `76aab7b` (test)

_Note: Task 3's commit also carries two small lint/mypy fixes to Task 1/Task 2 files, discovered while verifying the new test file — see Deviations below._

## Files Created/Modified

- `packages/dataplat/src/dataplat/load/staging.py` - Adds `StagingLoader._build_stages(ctx)`; wires it into `load()`'s `run_streaming(...)` call; fixes `_record_hash` computation for None/bool fields.
- `packages/dataplat/src/dataplat/discovery.py` - `discover_files()` gains required `schema: SchemaRepository` and optional `filename_facets_by_object` parameters; idempotency key formula appends `schema_version`.
- `packages/csv-processor/src/csv_processor/cli.py` - `discover()` constructs `SchemaRepository(pool)` and passes `schema=schema` into `discover_files(...)`.
- `tests/unit/test_discovery.py` - Adds `_FakeSchemaRepository`/`_fake_schema()`; updates all 9 pre-existing `discover_files(...)` call sites; adds the Pitfall-5 regression test.
- `tests/integration/test_discover_files.py` - Adds a `schema` fixture; updates all 5 pre-existing tests (including two nested `_discover()` helpers) to pass `schema=schema`.
- `tests/integration/test_staging_normalization.py` (new) - End-to-end proof against a live database and real MinIO object.
- `.planning/phases/06-universal-csv-engine-schema-contracts-normalization/deferred-items.md` - Logs an unrelated, pre-existing `test_compression.py` Hypothesis boundary-case failure discovered while running this plan's own required `pytest tests/unit -q` verification.

## Decisions Made

- **`NullTokenNormalizer`'s token set is a union, not an override.** `_null_tokens_for_column()` combines `ctx.config.normalization.null_tokens` (dataset-wide) with `.null_sentinels.get(column.name, [])` (column-specific), per `NormalizationConfig.null_sentinels`'s own docstring ("Checked in addition to `null_tokens`"). Falls back to D-14's `("",)` default when `ctx.config.normalization is None` (`customers`' real case).
- **`NumericNormalizer` receives no `null_sentinels` argument in this wiring.** The plan's own Task 1 action text enumerates exactly which fields feed `NumericNormalizer` ("locale fields plus that column's `fixed_width`/`reject_scientific_notation`") and does not include `null_sentinels` — a nullable numeric column's null handling runs entirely through its own `NullTokenNormalizer`, matching the platform-wide `None`-passthrough convention. Not exercised by `customers` (no numeric columns) this session.
- **Idempotency key's `schema_version` term is the integer `version` number, stringified** (`SchemaVersionRecord.version`), not `schema_hash` — matches ARCHITECTURE.md Q7's formula, which names `schema_version` and `policy_digest` as two textually-distinct terms.
- **`filename_facets_by_object`'s extracted `business_date` facet is surfaced only in `discover_files`'s own log lines**, never persisted. `MetadataRepository.create_file` has no `business_date` parameter (verified by reading the full Protocol — no method on it accepts one), and extending that Protocol plus the underlying SQL is outside this plan's declared file scope. This keeps the new parameter genuinely consulted (not dead code) while staying strictly within "a signature change only, this plan" as the plan's own action text requires.
- **Verified `tests/integration/test_discover_files.py`/`test_staging_normalization.py` without the plan-specified `-m integration` filter**, following the identical precedent already established twice this phase (06-01-SUMMARY.md, 06-12-SUMMARY.md): no `integration` pytest marker is registered anywhere in this codebase (confirmed via repo-wide grep and empirically: `-m integration` deselects all tests, exit code 5). Used the same invocation `make test-integration` performs instead (`--group cluster`, no marker filter).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `_record_hash` computation would crash on the empty-`birth_date` regression case this plan exists to fix**
- **Found during:** Task 1 (wiring `_build_stages` into `load()`)
- **Issue:** `record_hash = hashlib.sha256("|".join(row).encode("utf-8")).digest()` assumed every field was `str`. Once `_build_stages` wires `NullTokenNormalizer`/`DateNormalizer`, a nullable column's field can genuinely be Python `None` — `"|".join(row)` raises `TypeError: sequence item N: expected str instance, NoneType found` the moment any row reaches the hash computation with a `None` field, which is exactly `customers.birth_date`'s empty-value case Task 3 was written to prove works.
- **Fix:** Render each field as `""` for `None` / `str(value)` for non-`str` before joining, mirroring `dataplat.normalize.numeric._row_to_raw_line`'s already-established convention for the identical widened-row-type problem.
- **Files modified:** `packages/dataplat/src/dataplat/load/staging.py`
- **Verification:** `tests/integration/test_staging_normalization.py`'s empty-`birth_date` assertion passes; `pytest tests/unit -x -q` shows no regression.
- **Committed in:** `b69fec6` (Task 1 commit)

**2. [Rule 1 - Bug] Malformed `# type: ignore[arg-type] -- comment` syntax in the new integration test**
- **Found during:** Task 3, running `mypy` on the new test file as part of this plan's own quality verification (not itself a listed acceptance criterion, but standard practice before finishing)
- **Issue:** Copied `tests/integration/test_staging_loader.py`'s established `PipelineContext(metadata=None,  # type: ignore[arg-type] -- unused ...)` placeholder pattern — mypy parses everything after `# type: ignore[code]` on the same line as part of the directive, so the trailing `-- comment` text after the closing `]` produces `error: Invalid "type: ignore" comment [syntax]`, and the underlying `arg-type` error shows through unsuppressed. This exact issue is already documented, pre-existing and deferred for `test_staging_loader.py` itself in this phase's `deferred-items.md` (from plan 06-02) — this plan's new file inherited the same broken convention by copying it.
- **Fix:** Split into two separate `#` comment segments (`# type: ignore[arg-type]  # unused by StagingLoader.load()`), which mypy parses correctly.
- **Files modified:** `tests/integration/test_staging_normalization.py`
- **Verification:** `mypy tests/integration/test_staging_normalization.py` reports "Success: no issues found in 1 source file".
- **Committed in:** `76aab7b` (Task 3 commit)

**3. [Rule 3 - Blocking] `tests/unit/test_discovery.py`'s 9 pre-existing `discover_files(...)` call sites would break under the new required `schema` parameter**
- **Found during:** Task 2, immediately after making `schema` required
- **Issue:** The plan's own action text names only `csv_processor.cli.discover()` and `tests/integration/test_discover_files.py` as "both real call sites" needing this same-task update — but `tests/unit/test_discovery.py` independently calls `discover_files(...)` 9 times across its existing test suite, and this plan's own Task 2 acceptance criteria requires `pytest tests/unit/test_discovery.py -x -q` to exit 0.
- **Fix:** Added `_FakeSchemaRepository`/`_fake_schema()` (a minimal double whose `get_current()` always returns `None`, `cast()` to `SchemaRepository` at each call site — `SchemaRepository` is a concrete class, not a `Protocol` like `MetadataRepository`/`ObjectStore`, so a duck-typed fake needs the same `cast()` narrowing this codebase already uses elsewhere rather than raw structural typing). Updated all 9 call sites with `schema=_fake_schema()`.
- **Files modified:** `tests/unit/test_discovery.py`
- **Verification:** `pytest tests/unit/test_discovery.py -x -q` — 12 passed (9 pre-existing + the new Pitfall-5 regression test). `mypy tests/unit/test_discovery.py` shows zero NEW errors attributable to the schema fake (only the already-documented, pre-existing `_FakeMetadataRepository` structural-typing gap from plan 06-02 remains, unchanged).
- **Committed in:** `48d7472` (Task 2 commit)

---

**Total deviations:** 3 auto-fixed (2 Rule 1 bugs, 1 Rule 3 blocking gap) + 2 small ruff findings (staging.py's ternary formatting, test_discovery.py's explicit `return None`) fixed during Task 3's own verification pass.
**Impact on plan:** All auto-fixes were necessary for correctness (the `_record_hash` crash would have broken this plan's own regression test) or to keep the plan's own required verification commands green (the required-parameter wiring, the type-check hygiene). No scope creep — every fix stayed inside this plan's declared `files_modified` list, with one exception (`.planning/.../deferred-items.md`, sanctioned documentation of an out-of-scope discovery).

## Known Stubs

- **`filename_facets_by_object` (`packages/dataplat/src/dataplat/discovery.py::discover_files`)** — accepted as a keyword-only parameter, defaults to `None`, and when populated is consulted only to surface a `business_date` value in the function's own discovery log line. `meta.files.business_date` has **no write path anywhere in this codebase** — `MetadataRepository.create_file`'s Protocol has no `business_date` parameter to receive it (verified by reading its full signature). This is not a silent gap: the plan's own action text explicitly scopes this as "a signature change only in this plan" — no dataset declares a filename mask yet (D-10), so there is no live caller this session, and D-11's actual fallback/priority logic (a filename-derived date must never override a data-derived one) has no consuming implementation anywhere yet. **Resolving plan:** whichever future phase first onboards a dataset that declares a `filename:` mask — at that point `csv_processor.cli.discover()` would build the real facet map via `csv_processor.detect.filename.parse_filename`, and `MetadataRepository.create_file`/the underlying SQL would need a `business_date` column write path added.

## Issues Encountered

None beyond the auto-fixed deviations documented above.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `StagingLoader.load()` is now the live, proven normalization entrypoint for `customers` — any future dataset onboarded with `columns:` declaring `decimal`/`integer`/`boolean` types will exercise `NumericNormalizer`/`BooleanNormalizer` through the identical `_build_stages(ctx)` path for the first time (constructed correctly today, but genuinely un-exercised against real data, exactly as the plan's own objective states).
- `discover_files()`'s `schema` parameter is ready for plan 06-15's schema-sync wiring to populate a real, non-empty `schema_version` term — today every call resolves `schema.get_current(dataset_id)` to `None` for `customers` (no schema version has ever been synced for it), so the idempotency key's 5th term is currently always an empty string in practice; this is the documented, expected "no schema yet" fallback, not a bug.
- No blockers for 06-15/06-17/06-18, which build on this plan's `discover_files` signature and `_build_stages` wiring.

---
*Phase: 06-universal-csv-engine-schema-contracts-normalization*
*Completed: 2026-08-15*

## Self-Check: PASSED

All 7 claimed files verified present on disk (`tests/integration/test_staging_normalization.py`,
`packages/dataplat/src/dataplat/load/staging.py`, `packages/dataplat/src/dataplat/discovery.py`,
`packages/csv-processor/src/csv_processor/cli.py`, `tests/unit/test_discovery.py`,
`tests/integration/test_discover_files.py`, `.planning/phases/06-universal-csv-engine-schema-contracts-normalization/deferred-items.md`).
All 4 claimed commit hashes verified present in `git log --oneline --all`
(`b69fec6`, `48d7472`, `76aab7b`, `540150b`). No missing items.
