---
phase: 03-dataplat-core-library-metadata-control-plane
reviewed: 2026-08-13T09:46:29Z
depth: standard
files_reviewed: 62
files_reviewed_list:
  - .github/workflows/ci.yml
  - configs/datasets/customers.yaml
  - configs/defaults.yaml
  - docs/README.md
  - docs/adr/0008-pipeline-composition-seam.md
  - docs/adr/README.md
  - migrations/alembic.ini
  - migrations/env.py
  - migrations/script.py.mako
  - migrations/versions/0001_meta_datasets_config_versions.py
  - migrations/versions/0002_meta_files.py
  - migrations/versions/0003_meta_batches_batch_files.py
  - migrations/versions/0004_meta_ingestion_runs.py
  - migrations/versions/0005_normalized_customers.py
  - packages/csv-processor/src/csv_processor/source.py
  - packages/dataplat/pyproject.toml
  - packages/dataplat/src/dataplat/cli.py
  - packages/dataplat/src/dataplat/config/__init__.py
  - packages/dataplat/src/dataplat/config/hashing.py
  - packages/dataplat/src/dataplat/config/loader.py
  - packages/dataplat/src/dataplat/config/model.py
  - packages/dataplat/src/dataplat/config/registry.py
  - packages/dataplat/src/dataplat/errors.py
  - packages/dataplat/src/dataplat/load/publish/protocol.py
  - packages/dataplat/src/dataplat/metadata/__init__.py
  - packages/dataplat/src/dataplat/metadata/postgres.py
  - packages/dataplat/src/dataplat/metadata/repository.py
  - packages/dataplat/src/dataplat/models/__init__.py
  - packages/dataplat/src/dataplat/models/identity.py
  - packages/dataplat/src/dataplat/models/record.py
  - packages/dataplat/src/dataplat/models/report.py
  - packages/dataplat/src/dataplat/observability/__init__.py
  - packages/dataplat/src/dataplat/observability/logging.py
  - packages/dataplat/src/dataplat/observability/metrics.py
  - packages/dataplat/src/dataplat/observability/tracing.py
  - packages/dataplat/src/dataplat/pipeline/engine.py
  - packages/dataplat/src/dataplat/pipeline/protocol.py
  - packages/dataplat/src/dataplat/secrets/__init__.py
  - packages/dataplat/src/dataplat/secrets/resolver.py
  - packages/dataplat/src/dataplat/sources/protocol.py
  - packages/dataplat/src/dataplat/storage/__init__.py
  - packages/dataplat/src/dataplat/storage/db.py
  - packages/dataplat/src/dataplat/storage/objectstore.py
  - schemas/dataset-config.schema.json
  - tests/integration/__init__.py
  - tests/integration/conftest.py
  - tests/integration/test_config_registry.py
  - tests/integration/test_docker_image.py
  - tests/integration/test_metadata_repository.py
  - tests/integration/test_migrations.py
  - tests/integration/test_objectstore.py
  - tests/policy/test_no_latest_image_tag.py
  - tests/property/__init__.py
  - tests/property/test_chunking_properties.py
  - tests/unit/test_cli_error_handling.py
  - tests/unit/test_config_hashing.py
  - tests/unit/test_csv_chunking.py
  - tests/unit/test_errors.py
  - tests/unit/test_logging_config.py
  - tests/unit/test_logging_redaction.py
  - tests/unit/test_pipeline_errors.py
  - tests/unit/test_secrets_resolver.py
findings:
  critical: 0
  warning: 0
  info: 1
  total: 1
status: clean
---

# Phase 3: Code Review Report (Re-Review After Fix Pass)

**Reviewed:** 2026-08-13T09:46:29Z
**Depth:** standard
**Files Reviewed:** 62
**Status:** clean

## Summary

This is a re-review of the same 62-file scope as `03-REVIEW.md`'s prior pass,
after a `gsd-code-fixer` agent addressed all 3 Critical + 5 Warning findings
(CR-01/02/03, WR-01 through WR-05) across 8 separate commits. Every file in
scope was re-read in full (not diffed against memory of the prior review),
and each of the 8 fixes was independently traced against its exact commit
diff (`git show <sha>`) as well as its final on-disk state, specifically
looking for: an incomplete fix, a fix that solved the symptom but not the
root cause, a regression introduced elsewhere, or a new defect the original
review missed.

Findings, verified independently against the code (not just the fix
commit's own claims):

- **CR-01** (raw traceback on bad CLI invocation): `dataplat/cli.py`'s
  `main()` now wraps the click dispatch in `except click.exceptions.Exit`,
  `except click.exceptions.ClickException`, `except click.exceptions.Abort`
  in addition to the existing `except DataPlatformError`. Verified against
  the installed `click` package's actual exception hierarchy
  (`ClickException(Exception)`, `Abort(RuntimeError)`, `Exit(RuntimeError)`
  — three genuinely disjoint families, so catch order cannot mask one
  case with another) and exercised live: `--help`, `--bogus-option`,
  `no-such-command`, and a bare invocation all now exit with a controlled
  code and no traceback. Confirmed fixed.
- **CR-02** (`RuntimeError: generator raised StopIteration` on an empty
  CSV): `chunked_records()` now wraps `next(reader)` in `try/except
  StopIteration: return`. Confirmed fixed and covered by
  `test_empty_stream_yields_no_chunks_and_does_not_raise`.
- **CR-03** (TOCTOU race in `get_or_create_dataset`/`_resolve_dataset_id`):
  both `metadata/postgres.py` and `config/registry.py` now use a single
  `INSERT ... ON CONFLICT (dataset_name) DO UPDATE SET dataset_name =
  EXCLUDED.dataset_name RETURNING dataset_id` instead of
  check-then-insert (`postgres.py`) or `SELECT ... FOR UPDATE` then
  `INSERT` (`registry.py`, which had the more severe gap: `FOR UPDATE`
  cannot lock a row that does not yet exist, so it never protected a
  brand-new dataset's first sync). Verified the `UNIQUE(dataset_name)`
  constraint the `ON CONFLICT` target depends on actually exists in
  `migrations/versions/0001_meta_datasets_config_versions.py`. Confirmed
  fixed, with no leftover check-then-insert code paths.
- **WR-01** (`S3ObjectStore.get_object` missed `BotoCoreError`
  connectivity failures): now catches `(ClientError, BotoCoreError)` as a
  tuple. Confirmed fixed.
- **WR-02** (`create_pool()`'s dead `except`/misleading `Raises` claim):
  the dead branch and inaccurate docstring were removed; the function is
  now a single `return ConnectionPool(..., open=False)` with an accurate
  docstring. Confirmed fixed.
- **WR-03** (reserved-context-key collision could `TypeError` inside the
  one handler that must never raise): `DataPlatformError.__init__` now
  rejects `context` keys `{"error_type", "error_message"}` at construction
  with a `ValueError`, inherited by every subclass. Confirmed fixed and
  covered by the new `tests/unit/test_errors.py`, including a test that
  reproduces `cli.py`'s exact `log.error(..., error_type=..., **exc.context)`
  call shape end to end.
- **WR-04** (`RaggedRowGuard`'s hardcoded `","` join delimiter): a
  `field_delimiter` constructor parameter was added, defaulting to `","`
  for backward compatibility, and `RejectedRecord.raw_line`'s docstring
  was extended to state plainly that the reconstruction is not
  necessarily the true source bytes. Confirmed fixed.
- **WR-05** (silent NUL-byte stripping): `_strip_nul` now calls
  `metrics.increment("lines_with_nul_stripped")` exactly once per
  physical line that contained a NUL. Confirmed fixed and covered by two
  new tests, one proving the metric fires for the affected line and one
  proving it does not fire for a NUL-free file.

No regression was introduced by any of the 8 fixes, and no fix was found
to be superficial (symptom-only) rather than addressing the described root
cause. I independently re-examined the surrounding code each fix touches
(not just the changed lines) for new edge cases the fix itself might have
opened — e.g., whether the `ON CONFLICT DO UPDATE` upsert could deadlock,
starve, or return a stale `dataset_id` under concurrent load (it cannot: a
single row-level lock correctly serializes all concurrent callers for a
given `dataset_name`, first-time or repeat); whether the widened
`click.exceptions` catch in `cli.py` could accidentally swallow a
`DataPlatformError` raised through click's own machinery (it cannot: the
`except DataPlatformError` clause is evaluated first and the two exception
families are disjoint); and whether the new `_RESERVED_CONTEXT_KEYS` guard
in `errors.py` could reject a legitimate call site (it only rejects the
exact two keys `cli.py`'s handler hardcodes, and every existing raise site
in this codebase was checked and uses neither).

I also did a fresh pass across the full 62-file scope independent of the
fix commits, re-checking config/model/loader/hashing, the metadata
repository and its Postgres implementation, the pipeline engine and
protocols, secrets resolution, observability (logging/redaction/metrics/
tracing), the Alembic migrations, CI workflow, and every test file in
scope. No new Critical or Warning finding surfaced. One minor,
non-blocking Info-level observation is recorded below.

## Info

### IN-01: `DataPlatformError.context` aliases the caller's dict instead of copying it

**File:** `packages/dataplat/src/dataplat/errors.py:63,72`
**Issue:** `__init__` does `context = context if context is not None else {}`
and then `self.context: dict[str, object] = context` — the exact object the
caller passed in, not a copy. If a raise site builds a `context` dict and
continues mutating it after constructing the exception (or reuses the same
dict across multiple raises, which is an easy mistake in a loop), the
exception's `.context` — including whatever `cli.py`'s catch-once handler
later logs — reflects the mutated state at logging time, not the state at
raise time. This is speculative today (no current raise site in this
codebase reuses or mutates a `context` dict after raising), so it is not a
proven bug against any existing call site, but it is a foot-gun for the
next contributor who adds one, and it is a one-line fix.
**Fix:**
```python
self.context: dict[str, object] = dict(context)
```

---

_Reviewed: 2026-08-13T09:46:29Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
