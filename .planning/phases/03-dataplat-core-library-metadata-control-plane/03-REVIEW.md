---
phase: 03-dataplat-core-library-metadata-control-plane
reviewed: 2026-08-13T00:00:00Z
depth: standard
files_reviewed: 61
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
  - tests/unit/test_logging_config.py
  - tests/unit/test_logging_redaction.py
  - tests/unit/test_pipeline_errors.py
  - tests/unit/test_secrets_resolver.py
findings:
  critical: 3
  warning: 5
  info: 2
  total: 10
status: issues_found
---

# Phase 3: Code Review Report

**Reviewed:** 2026-08-13T00:00:00Z
**Depth:** standard
**Files Reviewed:** 61
**Status:** issues_found

## Summary

Reviewed the `dataplat` core library, the `csv_processor` plugin's first
`Source` implementation, the five META-01/META-02 Alembic migrations, and
the integration/unit/property test suites that exercise them. This is a
disciplined codebase: no hardcoded secrets, no `eval`/shell-injection
patterns, no bare `except:`, SQL is parameterized everywhere except one
allow-listed column-name assembly site (`update_ingestion_run_status`,
correctly guarded), and the cross-plan integration seams called out for
special attention are genuinely clean — `ConfigRegistry` takes its pool
from `dataplat.storage.db.create_pool()` rather than constructing its own
(verified against the single `ConnectionPool(` construction site in the
whole package), `csv_processor.source.CsvSource`/`CsvRecordStream`
structurally satisfy `dataplat.sources.protocol.Source`/`RecordStream`, and
a repository-wide scan for duplicate top-level `class`/`def` names across
`dataplat` and `csv_processor` found zero collisions.

That discipline makes the three bugs below more notable, not less: each is
a real, empirically reproduced defect (not a style nit) in exactly the kind
of "first real code on a new seam" path a reviewer should distrust most.
All three were verified by direct execution against the project's own
pinned dependencies (`click==8.4.2` per `uv.lock`) and, for the concurrency
bug, against a real throwaway PostgreSQL 18 container — not inferred from
reading alone.

1. The actual `dataplat` console-script entrypoint crashes with a raw
   Python traceback on the most basic possible invocations (no arguments,
   an unknown option, an unknown subcommand) — precisely the failure mode
   its own design doc and test suite claim is impossible. The existing test
   exercises the wrong call path, so CI is green despite the bug.
2. The CSV chunking function crashes with an opaque `RuntimeError` on a
   genuinely empty (zero-byte) input file — a realistic real-world CSV
   ingestion scenario for a platform whose stated purpose is handling messy
   real-world files.
3. The metadata control plane's "get or create" primitives have an
   unguarded time-of-check/time-of-use race: two concurrent first-time
   calls for the same new dataset name reproducibly crash one caller with
   a raw, unwrapped `psycopg.errors.UniqueViolation`.

## Critical Issues

### CR-01: `dataplat` CLI entrypoint crashes with a raw traceback on any usage error (no args, bad option, unknown command)

**File:** `packages/dataplat/src/dataplat/cli.py:79-92`

**Issue:** `main()` calls `cli.main(args=argv, prog_name="dataplat", standalone_mode=False)` and only catches `DataPlatformError`. With `standalone_mode=False`, click does **not** intercept its own `UsageError`/`ClickException` family (`NoArgsIsHelpError`, `NoSuchOption`, `NoSuchCommand`, missing required arguments, etc.) — those propagate as raw Python exceptions out of `cli.main()`, are not `DataPlatformError` instances, and are therefore not caught by `main()`'s `except` clause. They propagate all the way out of `main()`.

This is not hypothetical — verified directly against the pinned `click==8.4.2` (the exact version locked in `uv.lock`):

```
$ python3 -c 'from dataplat.cli import main; main([])'
Traceback (most recent call last):
  ...
  File ".../dataplat/cli.py", line 80, in main
    cli.main(args=argv, prog_name="dataplat", standalone_mode=False)
  File ".../click/core.py", line 1924, in parse_args
    raise NoArgsIsHelpError(ctx)
click.exceptions.NoArgsIsHelpError: Usage: dataplat [OPTIONS] COMMAND [ARGS]...
```

The same happens for `main(["no-such-command"])` (raises `NoSuchCommand`) and `main(["--bogus-option"])` (raises `NoSuchOption`).

This is the literal `ENTRYPOINT ["dataplat"]` of the `csv-processor` Docker image (per `docker/csv-processor/Dockerfile`, exercised by `tests/integration/test_docker_image.py`). Running that container with no arguments — the single most natural first sanity check of a new image — crashes with a raw traceback instead of printing usage help and exiting 2, even though `no_args_is_help=True` was explicitly configured on the `cli` group specifically to make that impossible, and `cli.py`'s own module docstring states any non-`DataPlatformError` exception "is a bug to surface loudly" (usage errors are not bugs — they are the single most expected form of user/operator error a CLI has to handle).

`tests/unit/test_cli_error_handling.py::test_zero_arguments_does_not_crash` gives false confidence here: it calls `CliRunner().invoke(cli, [])`, not `main([])`. `CliRunner.invoke()` has its own independent exception-catching wrapper around the click dispatch, so it observes `exit_code == 2` regardless of what `main()` itself does with the same exception. The test's docstring literally asserts "never an unhandled Python traceback," but it never actually calls the function (`main()`) that is supposed to provide that guarantee.

**Fix:** Catch click's own control-flow/usage exceptions alongside `DataPlatformError`, converting them the same way `standalone_mode=True` would have:

```python
import click

try:
    cli.main(args=argv, prog_name="dataplat", standalone_mode=False)
except DataPlatformError as exc:
    log.error(
        "dataplat command failed",
        error_type=type(exc).__name__,
        error_message=str(exc),
        **exc.context,
    )
    return 1
except click.exceptions.Exit as exc:
    return exc.exit_code
except click.exceptions.ClickException as exc:
    exc.show()
    return exc.exit_code
except click.exceptions.Abort:
    return 1
return 0
```

Then fix the test to actually exercise `main([])` (and `main(["--bogus"])`/`main(["no-such-command"])`) directly, not only `CliRunner.invoke()`, so this exact regression cannot silently return.

---

### CR-02: `chunked_records()` crashes with an opaque `RuntimeError` on an empty (zero-byte) CSV file

**File:** `packages/csv-processor/src/csv_processor/source.py:99` (root cause), function `chunked_records` (lines 72-109)

**Issue:** `header = next(reader)` is called eagerly, unguarded, directly inside a generator function. When the input stream is genuinely empty (zero bytes — no header, no rows), `next(reader)` raises `StopIteration`. Because `chunked_records` is itself a generator (it contains `yield`), PEP 479 converts that `StopIteration` into `RuntimeError: generator raised StopIteration` at the point the generator is first driven — not a `dataplat.errors.DataPlatformError`, not a `RejectedRecord`, not anything this codebase's error-handling design accounts for.

Verified directly:

```python
>>> import io
>>> from csv_processor.source import chunked_records
>>> stream = io.TextIOWrapper(io.BytesIO(b""), encoding="utf-8", newline="")
>>> list(chunked_records(stream, chunk_size=10))
RuntimeError: generator raised StopIteration
```

An empty file is a realistic, common real-world CSV ingestion scenario (an empty export, a placeholder object, a zero-byte upload) for a platform whose explicit purpose is "real-world messy CSV files." No test in `tests/unit/test_csv_chunking.py` or `tests/property/test_chunking_properties.py` exercises a header-less, zero-byte stream — the property test's own generator (`_csv_table`) always writes a header row via `csv.writer.writerow(header)` even when the generated row list is empty, so the empty-rows-with-header case is covered but the genuinely-empty-stream case is not.

**Fix:** Guard the header read explicitly and raise (or otherwise signal) a typed condition instead of letting `StopIteration` escape uncaught inside the generator:

```python
def chunked_records(text_stream: TextIOWrapper, *, chunk_size: int) -> Iterator[RecordChunk]:
    csv.field_size_limit(FIELD_SIZE_LIMIT)
    reader = csv.reader(_strip_nul(text_stream), dialect=DIALECT)
    try:
        header = next(reader)
    except StopIteration:
        return  # or raise a typed dataplat error, per the project's empty-file policy
    expected_field_count = len(header)
    ...
```

Whether an empty file should yield zero chunks or raise a `DataPlatformError` subclass is a product decision this review does not make — but *some* explicit, typed, tested behavior is required; an uncaught `RuntimeError` from generator internals is not an acceptable outcome either way.

---

### CR-03: Unguarded time-of-check/time-of-use race in "get or create dataset" — concurrent first-time calls crash with a raw `UniqueViolation`

**File:** `packages/dataplat/src/dataplat/metadata/postgres.py:77-93` (`PostgresMetadataRepository.get_or_create_dataset`); same root cause, partially mitigated, in `packages/dataplat/src/dataplat/config/registry.py:181-207` (`ConfigRegistry._resolve_dataset_id`)

**Issue:** `get_or_create_dataset()` runs a plain `SELECT ... WHERE dataset_name = %s` followed by, if no row was found, a plain `INSERT INTO meta.datasets (dataset_name) VALUES (%s)`. There is no row lock, no `ON CONFLICT` clause, and no retry. When two concurrent callers both invoke this method for the same **new** `dataset_name` (entirely plausible under `KubernetesExecutor`, where concurrent per-task pods are the normal execution model — e.g. a backfill that fans out multiple files of a brand-new dataset in parallel), both can observe "no row exists" before either commits its `INSERT`, and the loser's `INSERT` raises a raw, unwrapped `psycopg.errors.UniqueViolation` against `meta.datasets`' `UNIQUE(dataset_name)` constraint (migration `0001`). That exception is not a `StorageError`/`DataPlatformError`, so it is not caught by `cli.py`'s catch-once boundary either — it crashes the run with a raw traceback, contradicting the method's own docstring: "creating the row if absent" (implying idempotent, safe concurrent use).

Verified against a real, throwaway PostgreSQL 18 container (matching the pinned analytical-database major) running the exact SQL sequence used by `get_or_create_dataset`, with two threads racing on the same never-before-seen `dataset_name`:

```
RESULTS:
  OK-INSERTED: 1
  RAISED: psycopg.errors.UniqueViolation: duplicate key value violates unique constraint "datasets_dataset_name_key"
DETAIL:  Key (dataset_name)=(concurrent_new_dataset) already exists.
FINAL ROWS: [(1, 'concurrent_new_dataset')]
```

(Data integrity itself is fine — Postgres's unique constraint guarantees exactly one row — but the losing caller's run crashes ungracefully instead of resolving to the winner's row.)

`ConfigRegistry._resolve_dataset_id()` has the identical structural defect for a brand-new dataset: its `SELECT ... FOR UPDATE` only takes a lock when a row already exists to lock. When zero rows match, `FOR UPDATE` locks nothing, so two concurrent first-time `sync()` calls for the same new dataset race exactly the same way. The module's own docstring explicitly reasons about serializing "concurrent `sync()` calls for that dataset" (T-03-10) but that reasoning only holds once the dataset row already exists — the first-ever sync of a given dataset is exactly the case left unguarded.

Neither `tests/integration/test_metadata_repository.py::test_full_slice_round_trip` nor `tests/integration/test_config_registry.py` exercises concurrent invocation — both call these methods sequentially, so this gap is untested.

**Fix:** Use an atomic upsert instead of select-then-insert, e.g.:

```python
def get_or_create_dataset(self, dataset_name: str) -> int:
    with self._pool.connection() as conn:
        row = conn.execute(
            """
            INSERT INTO meta.datasets (dataset_name) VALUES (%s)
            ON CONFLICT (dataset_name) DO UPDATE
                SET dataset_name = EXCLUDED.dataset_name
            RETURNING dataset_id
            """,
            (dataset_name,),
        ).fetchone()
        ...
```

(`DO UPDATE SET dataset_name = EXCLUDED.dataset_name` is a standard no-op-update idiom that still returns a row via `RETURNING`, unlike `DO NOTHING`, which returns no row on conflict and would need a fallback `SELECT`.) Apply the same fix to `ConfigRegistry._resolve_dataset_id`.

## Warnings

### WR-01: `S3ObjectStore.get_object()` only catches `ClientError`; connectivity failures escape unwrapped, contradicting its own docstring

**File:** `packages/dataplat/src/dataplat/storage/objectstore.py:130-135`

**Issue:** The `try/except` around `self._client.get_object(...)` catches only `botocore.exceptions.ClientError` (S3-service-level errors: no such key, access denied, etc.). `botocore.exceptions.EndpointConnectionError`, `ConnectTimeoutError`, and the rest of the `BotoCoreError` family — raised when MinIO is unreachable, DNS fails, or the connection times out — are **not** subclasses of `ClientError` (verified directly: `issubclass(EndpointConnectionError, ClientError)` is `False`). A MinIO outage or network blip therefore raises a raw botocore exception straight out of `get_object()`, contradicting the method's own docstring: "Raises: StorageError: ... any other `botocore.exceptions.ClientError` occurs. The raw boto3/botocore exception type never escapes this method." That claim is false for the entire connectivity-failure class, which is one of the more common real failure modes for a networked object store in a Kubernetes environment.

**Fix:** Widen the catch to `botocore.exceptions.BotoCoreError` (the common base of both `ClientError`'s sibling connectivity errors and `ClientError` itself is `Exception`, so catch both explicitly):

```python
from botocore.exceptions import BotoCoreError, ClientError

try:
    response = self._client.get_object(Bucket=bucket, Key=key)
except (ClientError, BotoCoreError) as exc:
    msg = "failed to get object from object storage"
    raise StorageError(msg, context={"bucket": bucket, "key": key}) from exc
```

### WR-02: `create_pool()`'s `except psycopg.OperationalError` is dead code — the documented "raises `StorageError`" behavior never fires

**File:** `packages/dataplat/src/dataplat/storage/db.py:47-51`

**Issue:** `ConnectionPool(dsn, ..., open=False)` never parses or validates `dsn` at construction time (confirmed by reading `psycopg_pool.pool.ConnectionPool.__init__`: with `open=False`, `self._open()` is never called, and no DSN parsing happens beforehand) and therefore cannot raise `psycopg.OperationalError` from this call. Verified directly: `create_pool("not a dsn at all")`, `create_pool("")`, `create_pool("postgresql://")`, and `create_pool("://bad")` all construct a pool object successfully with no exception at all. The docstring's `Raises: StorageError: If the pool cannot be constructed` is therefore never actually true in practice — a malformed DSN silently produces a working-looking `ConnectionPool` object, and the real failure only surfaces later, deep inside whatever code first calls `.connection()` (e.g. inside `ConfigRegistry.sync()` or `PostgresMetadataRepository`), as a raw, un-wrapped exception from a completely different, unrelated call site.

**Fix:** Either remove the misleading `try/except`/docstring claim (since there is genuinely nothing to catch here), or move the validation to where it can actually observe a failure — e.g. eagerly opening the pool (`pool.open(wait=True)`) inside `create_pool()` if construction-time validation is actually wanted, wrapping *that* call in the `except psycopg.OperationalError` handler instead of the constructor call.

### WR-03: `cli.py`'s catch-once handler crashes itself if a future `DataPlatformError.context` uses the key `error_type` or `error_message`

**File:** `packages/dataplat/src/dataplat/cli.py:85-90`

**Issue:** `log.error("dataplat command failed", error_type=..., error_message=..., **exc.context)` spreads `exc.context` alongside two fixed keyword arguments. If any `DataPlatformError` (now or in a future phase) is raised with `context={"error_type": ...}` or `context={"error_message": ...}`, this call raises `TypeError: ...meth() got multiple values for keyword argument 'error_type'` — verified directly. That `TypeError` is not a `DataPlatformError`, so it is not caught by `main()`'s own `except` clause either: the very error-handling boundary whose entire purpose is "never a raw Python traceback" would itself produce one. No current raise site in `errors.py`'s call sites uses either key today, so this is dormant, not yet triggered — but it is a landmine for the next contributor who adds a `DataPlatformError(..., context={"error_type": ...})` somewhere, and the failure mode (the error boundary crashing on the very thing it exists to handle) is the worst possible place for this kind of collision to surface.

**Fix:** Namespace the caller-supplied context so it cannot collide with fixed keys, e.g. `log.error("dataplat command failed", error_type=..., error_message=..., context=exc.context)` (nest it under one key), or reserve `error_type`/`error_message` as disallowed context keys and assert/strip them in `DataPlatformError.__init__`.

### WR-04: `RaggedRowGuard` reconstructs `RejectedRecord.raw_line` via `",".join(row)`, which is not the row's actual original text

**File:** `packages/dataplat/src/dataplat/pipeline/engine.py:75`

**Issue:** `RejectedRecord.raw_line` (`packages/dataplat/src/dataplat/models/record.py:80`) is documented as "The row's original, unparsed text." `RaggedRowGuard.apply()` populates it with `",".join(row)` — but `row` is already the *parsed* tuple of fields (quoting and escaping already resolved by `csv.reader`). Naively rejoining with `,` is not the original raw text whenever a field itself contained a comma, an embedded newline, or a quoted delimiter — exactly the cases CSV quoting exists to handle in the first place, and exactly the cases most likely to need to distinguish "field count differs because of a genuine ragged row" from "field count differs because a field's content confused the parser." A human (or downstream tooling) inspecting a `RejectedRecord` for audit purposes — the platform's stated Core Value is "every ... record ... can be traced, explained, reprocessed and trusted" — will see a reconstructed line that can look like it has a different shape than the actual source row.

**Fix:** Either rename/re-scope the field's contract to make clear it is a *reconstruction*, not the original text (e.g. `reconstructed_line`), or thread the true raw physical line text through the chunking layer so `RaggedRowGuard` (and any future stage populating `RejectedRecord`) has access to it. At minimum, join using the dialect's actual delimiter (`DIALECT.delimiter`) rather than a hardcoded `","`, so the reconstruction does not silently drift out of sync the moment delimiter detection (Phase 6) makes the delimiter configurable.

### WR-05: `_strip_nul()` silently discards NUL bytes with zero observability

**File:** `packages/csv-processor/src/csv_processor/source.py:44-69`

**Issue:** `_strip_nul()` removes every `\x00` character from each physical line before it reaches `csv.reader`, with no counter increment, no log line, and no `RejectedRecord`/finding recorded anywhere. This is a deliberate, well-reasoned mitigation for `_csv.Error: line contains NUL` (documented at length in the module docstring), but it is also a silent modification of row content — the project's stated Core Value is explicit that "no data is ever silently dropped, duplicated, or corrupted." A file containing NUL bytes today is ingested with those bytes quietly stripped and no trace anywhere (not in metrics, not in logs, not in the run's `meta.ingestion_runs` counters) that it happened.

**Fix:** At minimum, increment a metric (`metrics.increment("rows_with_nul_stripped", ...)`, mirroring the pattern `RaggedRowGuard` already establishes) when a line actually contained a NUL byte, so the run's observability surface reflects that a mutation occurred, even if the row itself is still accepted.

## Info

### IN-01: `run_streaming()`'s docstring says "Returns:" for a generator function

**File:** `packages/dataplat/src/dataplat/pipeline/engine.py:103`

**Issue:** `run_streaming` is a generator (contains `yield`, declared `Iterator[tuple[int, StageResult]]`), but its docstring uses a `Returns:` section. Every other generator in the reviewed scope (`chunked_records`, `_strip_nul`) uses `Yields:`, per this codebase's own established Google-docstring convention.

**Fix:** Rename the section to `Yields:` for consistency.

### IN-02: No lower-bound validation on `chunk_size`

**File:** `packages/csv-processor/src/csv_processor/source.py:72` (`chunked_records`), and its callers `CsvRecordStream.__init__`/`CsvSource.__init__`

**Issue:** `chunk_size` is typed `int` with no validation. `itertools.batched(reader, chunk_size)` raises a raw `ValueError: batched(): n must be at least one` for `chunk_size <= 0`, rather than a `dataplat`-domain error. Low likelihood today (both constructors default to `1000`, and nothing in this phase makes `chunk_size` externally configurable yet), but worth a guard before a later phase threads a config-supplied value through.

**Fix:** Validate `chunk_size >= 1` at the `CsvSource`/`CsvRecordStream` constructor boundary and raise a `ConfigurationError` for an invalid value.

---

_Reviewed: 2026-08-13T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
