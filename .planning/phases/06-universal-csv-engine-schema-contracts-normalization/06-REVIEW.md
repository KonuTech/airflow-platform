---
phase: 06-universal-csv-engine-schema-contracts-normalization
reviewed: 2026-08-15T16:17:56Z
depth: standard
files_reviewed: 69
files_reviewed_list:
  - configs/datasets/customers.yaml
  - configs/defaults.yaml
  - migrations/versions/0009_meta_schema_versions.py
  - packages/csv-processor/pyproject.toml
  - packages/csv-processor/src/csv_processor/cli.py
  - packages/csv-processor/src/csv_processor/compression.py
  - packages/csv-processor/src/csv_processor/detect/__init__.py
  - packages/csv-processor/src/csv_processor/detect/dialect.py
  - packages/csv-processor/src/csv_processor/detect/encoding.py
  - packages/csv-processor/src/csv_processor/detect/filename.py
  - packages/csv-processor/src/csv_processor/detect/header.py
  - packages/csv-processor/src/csv_processor/detect/schema.py
  - packages/csv-processor/src/csv_processor/source.py
  - packages/dataplat/src/dataplat/config/model.py
  - packages/dataplat/src/dataplat/diagnostics.py
  - packages/dataplat/src/dataplat/discovery.py
  - packages/dataplat/src/dataplat/errors.py
  - packages/dataplat/src/dataplat/load/staging.py
  - packages/dataplat/src/dataplat/models/assignment.py
  - packages/dataplat/src/dataplat/models/profile.py
  - packages/dataplat/src/dataplat/models/record.py
  - packages/dataplat/src/dataplat/normalize/__init__.py
  - packages/dataplat/src/dataplat/normalize/boolean_null.py
  - packages/dataplat/src/dataplat/normalize/dates.py
  - packages/dataplat/src/dataplat/normalize/numeric.py
  - packages/dataplat/src/dataplat/normalize/unicode.py
  - packages/dataplat/src/dataplat/pipeline/engine.py
  - packages/dataplat/src/dataplat/schema/__init__.py
  - packages/dataplat/src/dataplat/schema/evolution.py
  - packages/dataplat/src/dataplat/schema/repository.py
  - packages/dataplat/src/dataplat/schema/versioning.py
  - packages/dataplat/src/dataplat/sources/__init__.py
  - packages/dataplat/src/dataplat/sources/protocol.py
  - pyproject.toml
  - tests/fixtures/CORPUS.sha256
  - tests/fixtures/corpus.yaml
  - tests/integration/test_discover_files.py
  - tests/integration/test_migrations.py
  - tests/integration/test_run_ingest.py
  - tests/integration/test_schema_resolution.py
  - tests/integration/test_staging_loader.py
  - tests/integration/test_staging_normalization.py
  - tests/property/test_determinism.py
  - tests/property/test_dst_correctness.py
  - tests/unit/detect/__init__.py
  - tests/unit/detect/test_dialect.py
  - tests/unit/detect/test_encoding.py
  - tests/unit/detect/test_filename.py
  - tests/unit/detect/test_header.py
  - tests/unit/detect/test_schema.py
  - tests/unit/normalize/__init__.py
  - tests/unit/normalize/test_boolean_null.py
  - tests/unit/normalize/test_dates.py
  - tests/unit/normalize/test_numeric.py
  - tests/unit/normalize/test_unicode.py
  - tests/unit/schema/__init__.py
  - tests/unit/schema/test_evolution.py
  - tests/unit/schema/test_versioning.py
  - tests/unit/test_batching_config.py
  - tests/unit/test_compression.py
  - tests/unit/test_config_hashing.py
  - tests/unit/test_corpus_semantic_fixtures.py
  - tests/unit/test_csv_source_inspect.py
  - tests/unit/test_csv_source_multipart.py
  - tests/unit/test_dataset_config_columns.py
  - tests/unit/test_diagnostics.py
  - tests/unit/test_discovery.py
  - tools/corpus/generators.py
  - tools/corpus/manifest.py
findings:
  critical: 1
  warning: 4
  info: 0
  total: 5
status: clean
original_status: issues_found
resolved: 2026-08-15T18:35:00Z
---

# Phase 06: Code Review Report

**Reviewed:** 2026-08-15T16:17:56Z
**Depth:** standard
**Files Reviewed:** 69
**Status:** clean (all 5 findings fixed post-review — see Resolution below)

## Resolution

All 5 findings were fixed by the orchestrator immediately after this review, given
CR-01's direct conflict with the project's stated Core Value. Each fix is
live-verified against a real database (reverting it reproduces the original failure)
plus full `make test`/`make test-integration`/`tests/property`/`ruff check`/`make
typecheck` — all green after every fix.

| Finding | Outcome | Commit |
|---|---|---|
| CR-01 (column reorder → silent corruption) | fixed | `e58d83e` |
| WR-01 (`header_case_sensitive` dead config) | fixed (docstring corrected to stop overclaiming; full remapping deferred — genuinely new feature, out of review-fix scope) | `7b708dc` |
| WR-02 (`ColumnContract.type` unconstrained) | fixed | `7b708dc` |
| WR-03 (`multipart-group-too-large` missing from catalog) | fixed | `a804f44` |
| WR-04 (`NumericNormalizer.null_sentinels` unwired) | fixed | `e7ed985` |

## Summary

Phase 6 adds real detection (encoding/dialect/header/compression/filename), schema
versioning/evolution, and value-level normalization on top of the Phase 3/4
skeleton. The code is unusually well-documented (every module/function
docstring cites the corpus fixture or locked decision it implements) and the
`str | bool | None` widening of `RecordChunk.rows` that was flagged as a
cross-plan risk area is handled correctly and consistently everywhere it
matters (`dates.py`, `numeric.py`, `boolean_null.py`, `unicode.py`,
`staging.py` all defensively narrow or pass through non-`str` fields). The
multipart wiring (06-18: `discover_files` → `group_multipart_units` →
`CsvSource.open` → `open_multipart_stream`, and `cli.py`'s `additional_keys`
derivation) is correct and is exercised end-to-end by both a unit test
(`tests/unit/test_csv_source_multipart.py`) and a real-database integration
test (`tests/integration/test_discover_files.py::test_multipart_delivery_becomes_one_logical_dataset`).

The one serious defect found is in the schema-resolution wiring (06-15): the
SCHEMA-06 "does this file match a historical schema version" branch in
`CsvSource._resolve_schema` hashes the wrong column list, and — combined with
`classify_schema_change`'s deliberately position-blind comparison and
`StagingLoader`'s pure positional column mapping — a CSV file whose physical
column order differs from the dataset's `columns:` contract (but uses the
same names/types) is silently accepted as fully COMPATIBLE and then has its
values written into the wrong target columns during staging, with the run
reported `SUCCEEDED`. This is exactly the kind of failure the platform's own
stated Core Value ("no data is ever silently dropped, duplicated, or
corrupted") rules out. Three further warnings cover dead/unwired
configuration surface that weakens the platform's own "config typos fail
loudly" design principle.

## Critical Issues

### CR-01: Column-reordered CSV files are silently accepted as compatible and then staged into the wrong columns

**File:** `packages/csv-processor/src/csv_processor/source.py:791-807`
**Also implicated:** `packages/dataplat/src/dataplat/schema/evolution.py:82-165`, `packages/dataplat/src/dataplat/load/staging.py:192-321`

**Issue:**

`CsvSource._resolve_schema` classifies a file's observed header against the
dataset's contract via `classify_schema_change(old_columns, new_columns)`.
That function's own docstring/tests are explicit that it compares **by
column name only, never by position** — "a column list that is merely
reordered, with every name and type otherwise unchanged, is correctly
treated as no change at all" (`evolution.py:90-92`, proven by
`tests/unit/schema/test_evolution.py`).

When `findings` comes back empty, `_resolve_schema` assumes "`new_columns ==
old_columns == the CONTRACT`" (the comment at `source.py:791`) and computes:

```python
observed_hash, _hash_version = hash_schema(old_columns)   # source.py:796
```

This hashes `old_columns` — the **contract's own column list**, built from
`ctx.config.columns` — never `new_columns`, the file's actual **observed**
header (built from `header`, the real detected column order,
`source.py:754-761`). The variable is misleadingly named `observed_hash`;
it is not derived from the file at all once `findings` is empty. Meanwhile
`hash_schema` is explicitly **position-sensitive** by design — "Column
POSITION is part of a schema's identity" — proven directly by
`tests/unit/schema/test_versioning.py::test_reordering_the_same_columns_changes_the_hash`.

So: for a file whose header has the **same column names/types as the
contract but in a different order** (a common real-world "messy CSV"
pattern, and exactly what this phase's "universal ingestion" charter and
CSV-01→CSV-13 requirements target):

1. `classify_schema_change` returns `[]` (no name/type mismatch) — no
   `IncompatibleSchemaError`, no `SchemaChangeFinding`.
2. `hash_schema(old_columns)` (the contract's hash, independent of the real
   file) is compared against `current.schema_hash`. Since `current` was
   itself produced by an earlier `derived_from="CONTRACT"` sync (the common
   case), this comparison is tautological and always matches.
3. `_resolve_schema` returns `compatibility="COMPATIBLE"` and a real
   `schema_version_id` — no diagnostic, no rejection, no schema-version bump.
4. `CsvSource.open()`/`StagingLoader.load()` then run. `StagingLoader.
   _build_stages` (`staging.py:192-321`) maps a row's field at
   `target_columns.index(column.name)` **assuming the CSV's physical column
   order already equals `target_columns`' order** — there is no
   header-to-contract name-based remapping anywhere in `CsvSource`/
   `CsvRecordStream`/`StagingLoader` (confirmed also by
   `tests/property/test_determinism.py`'s own comment: "StagingLoader has no
   header-to-column name mapping, only positional correspondence").

The net effect: `customer_id,name,country,birth_date,event_ts` delivered as
`name,customer_id,country,birth_date,event_ts` (say) passes schema
validation as a complete non-event, and every row's `name`/`customer_id`
values are silently swapped into each other's target columns. The
`_record_hash` computed in `staging.py` is computed over this already-
misaligned data, so even the audit hash cannot detect the swap. The
ingestion run is reported `SUCCEEDED`.

This is not merely a theoretical gap: `classify_schema_change`'s own
docstring makes an explicit, tested *positive claim* of safety
("reordering... is correctly treated as no change at all") that is only
true for the classifier in isolation — it is false as an end-to-end system
property once `StagingLoader`'s positional-only assumption is taken into
account, and nothing in the reviewed code cross-checks the two.

**Fix:**

At minimum, fix the `_resolve_schema` bug so the branch it lives in
actually verifies what it claims to (SCHEMA-06 "does this file match some
historical schema" should be checked against the file's real structure):

```python
# source.py:796 — hash what was actually observed, not the contract:
observed_hash, _hash_version = hash_schema(new_columns)
```

That alone still leaves the underlying design problem: because
`classify_schema_change` is name-only, a reordered file will still resolve
to `observed_hash != current.schema_hash` (since `new_columns`' `position`
values now differ from `old_columns`') and fall into
`schema_repo.resolve_by_hash`, which will raise `StorageError` for a
never-before-seen hash — turning a currently-silent corruption into a loud,
diagnosable failure, which is a large improvement, but still not a good
message ("no schema_versions row for this hash" rather than "columns are
reordered").

The complete fix needs one of:
- Make `StagingLoader`/`CsvRecordStream` remap each row by the **detected
  header's column names** against `target_columns` before values are
  written (i.e., stop assuming physical order == contract order), so a
  reordered-but-otherwise-compatible file loads correctly instead of merely
  failing loudly; or
- Make `classify_schema_change` (or a new check alongside it) treat a pure
  reordering as its own explicit, named outcome — e.g. a `"columns_reordered"`
  finding — that `_resolve_schema` handles explicitly (reject with a named
  diagnostic, since the load path cannot honor it) instead of silently
  falling through the "no findings" branch.

Either way, `evolution.py`'s docstring claim needs to be corrected to no
longer claim end-to-end safety it does not have, or the described gap needs
to be closed.

## Warnings

### WR-01: `CsvParsingConfig.header_case_sensitive` is fully unused/dead configuration

**File:** `packages/dataplat/src/dataplat/config/model.py:289-303`

**Issue:** `CsvParsingConfig.header_case_sensitive` is documented as
governing "header-to-`columns:` name matching" (its own docstring), and
`csv_processor/detect/header.py`'s module docstring explicitly references it
("`header_case_sensitive` therefore governs a different, later concern
(header-to-contract name matching) and is deliberately not a parameter of
this function"). A repo-wide search shows the field is read **nowhere**:
not passed to `detect_header`, not read in `CsvSource.inspect()`/
`_resolve_schema`, not read in `StagingLoader`. A dataset author setting
`csv.header_case_sensitive: false` in a config YAML gets no observable
effect at all — the field silently does nothing (and is directly related to
CR-01: it is the missing piece that would be needed to actually implement
header-to-contract name matching).

**Fix:** Either wire this field into the (currently nonexistent)
header-to-contract name-matching step this class's own docstring promises
(see CR-01's fix), or remove the field and its docstring claim until that
matching step exists, so the config surface does not advertise behavior
that is not implemented.

### WR-02: `ColumnContract.type` accepts any string — a typo silently degrades to no validation

**File:** `packages/dataplat/src/dataplat/config/model.py:117-169`

**Issue:** `ColumnContract.type` is declared `type: str` with no closed-set
constraint (no `Literal[...]`, no `field_validator`). Its own docstring
calls it "a closed set expressed as plain `str`... see the module
docstring's 'config not code' convention," implying resolution "through
string-keyed registries elsewhere" the way `SourceConfig.type`/
`DeduplicationConfig.strategy`/`LoadConfig.strategy` are — but no such
registry or validator exists for `ColumnContract.type` anywhere in the
codebase (`grep` for `_DATE_LIKE_TYPES`/`_NUMERIC_TYPES`/`"boolean"` shows
the only place `type` is interpreted is `StagingLoader._build_stages`,
which has no `else` branch). A misspelled type (e.g. `type: "sting"` instead
of `"string"`, or `type: "date"` mistyped as `"dat"`) passes `DatasetConfig`
validation cleanly and then silently receives **zero** type-specific
normalization/validation in `_build_stages` (falls through every `if`/`elif`
exactly like a legitimate `"string"` column) — the opposite of this
project's own stated design goal that `extra="forbid"` config validation
"catches config typos, which is the single most common ETL outage cause"
(CLAUDE.md). A `date`/`timestamp`/`decimal`/`integer`/`boolean` column
mistyped this way would stage completely unvalidated, unparsed raw text
with no error anywhere.

**Fix:**

```python
from typing import Literal

_COLUMN_TYPES = Literal["string", "integer", "decimal", "date", "timestamp", "boolean"]

class ColumnContract(BaseModel):
    ...
    type: _COLUMN_TYPES
```

or an explicit `field_validator` raising on any value outside the closed
set — matching the "config not code" convention's own stated intent, and
matching the safety this project already gives `SourceConfig.multipart_pattern`/
`NormalizationConfig.negative_style`/`ambiguous_time_policy` style fields
(several of which likewise remain plain `str`, but `type` is the field whose
value directly gates whether a column receives correctness-critical
normalization at all).

### WR-03: `"multipart-group-too-large"` diagnostic code is missing from the shared `DIAGNOSTIC_CODES` catalog

**File:** `packages/dataplat/src/dataplat/diagnostics.py:110-124`
**Also:** `packages/csv-processor/src/csv_processor/source.py:356-375`

**Issue:** `CsvSource.__init__` raises `FileInspectionError` with
`context={"diagnostic_code": "multipart-group-too-large", ...}` when a
multipart group exceeds `_MAX_MULTIPART_PARTS` (T-06-34, plan 06-18 — the
newest code path in this phase). `dataplat.diagnostics.DIAGNOSTIC_CODES`
(the module whose own docstring states "every detector/validation failure
this phase adds carries a stable, documented diagnostic code" from *one
shared catalog*, D-23/D-24/D-25) does not contain this string in either
`_CORPUS_DERIVED_CODES` or `_NEW_THIS_PHASE_CODES`. `"multipart-group-
incomplete"` (the sibling diagnostic from the same plan,
`discovery.py:group_multipart_units`) *is* correctly present. The project's
own established convention for this — a dedicated drift-guard test per
raise site, e.g.
`tests/unit/detect/test_filename.py::test_filename_does_not_match_mask_diagnostic_code_is_in_the_shared_catalog`
— was not applied to this raise site either, and `tests/unit/
test_diagnostics.py` only checks that corpus-derived codes are a *subset* of
the corpus's own declarations, so it cannot catch a code that was simply
never added to the catalog.

**Fix:**

```python
_NEW_THIS_PHASE_CODES: Final[frozenset[str]] = frozenset(
    {
        ...,
        "multipart-group-too-large",
    },
)
```

and add a drift-guard test in `tests/unit/test_csv_source_multipart.py`
mirroring `test_filename_does_not_match_mask_diagnostic_code_is_in_the_shared_catalog`:
`assert "multipart-group-too-large" in DIAGNOSTIC_CODES`.

### WR-04: `NumericNormalizer.null_sentinels` is implemented and unit-tested but never wired from the real pipeline

**File:** `packages/dataplat/src/dataplat/load/staging.py:248-297`
**Also:** `packages/dataplat/src/dataplat/normalize/numeric.py:128-192`

**Issue:** `NumericNormalizer.__init__` accepts a `null_sentinels: tuple[str,
...] = ()` parameter, fully implemented (`apply()` checks
`raw_value in self._null_sentinels` before parsing, `numeric.py:245`) and
directly unit-tested
(`tests/unit/normalize/test_numeric.py::test_numeric_null_sentinels_match_exactly_never_by_substring`).
However, `StagingLoader._build_stages` — the one real place `NumericNormalizer`
is constructed in the actual pipeline — never passes `null_sentinels=` when
building it (`staging.py:270-297`); every real `NumericNormalizer` therefore
runs with the default empty tuple. Per-column numeric null sentinels only
take effect today via `NullTokenNormalizer` (which is wired through
`_null_tokens_for_column`, `staging.py:111-136`, and only exists at all when
`column.nullable` is `True`), making `NumericNormalizer`'s own dedicated
mechanism dead code in production: unreachable for a `nullable: false`
decimal/integer column with a declared `null_sentinels` entry (that
sentinel would fail to parse and be rejected as `invalid-numeric-value`
instead of recognized as absent), and simply redundant for a `nullable:
true` one.

**Fix:** Either wire it through:

```python
elif column.type in _NUMERIC_TYPES:
    stages.append(
        NumericNormalizer(
            ...,
            null_sentinels=tuple(
                normalization.null_sentinels.get(column.name, [])
            ) if normalization is not None else (),
        ),
    )
```

or remove the parameter (and its test) if `NullTokenNormalizer` is meant to
be the sole null-sentinel mechanism, to avoid a maintainer reasonably
assuming (from the class's own docstring and tests) that this capability is
live in the deployed pipeline.

---

_Reviewed: 2026-08-15T16:17:56Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
