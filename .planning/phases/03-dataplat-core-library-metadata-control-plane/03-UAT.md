---
status: diagnosed
phase: 03-dataplat-core-library-metadata-control-plane
source: [03-01-SUMMARY.md, 03-02-SUMMARY.md, 03-03-SUMMARY.md, 03-04-SUMMARY.md, 03-05-SUMMARY.md, 03-06-SUMMARY.md, 03-07-SUMMARY.md, 03-08-SUMMARY.md]
started: 2026-08-13T09:49:35Z
updated: 2026-08-13T10:14:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Cold Start Smoke Test
expected: |
  From a clean environment (no leftover containers, no cached state): `make test-integration`
  spins up throwaway PostgreSQL 18 + MinIO containers, runs `alembic upgrade head` against the
  fresh database, and the full integration suite passes with no manual setup step.
result: issue
reported: "Claude ran `make test-integration` on the user's behalf (clean containers, fresh throwaway PostgreSQL 18 + MinIO). 13 tests passed, but tests/integration/test_metadata_repository.py::test_full_slice_round_trip FAILED: psycopg.errors.UniqueViolation: duplicate key value violates unique constraint \"uq_config_versions_dataset_version\" — Key (dataset_id, version)=(1, 1) already exists. This is the first time in this phase's execution that the whole tests/integration/ directory has run together in one pytest session — during Wave 3, plans 03-04 and 03-05 ran in separate isolated worktrees and each only ran its own test file, never exercising this cross-file interaction."
severity: blocker

### 2. CLI prints its version
expected: `uv run dataplat --version` prints a real version string (not the `0.0.0+unknown` placeholder) and exits 0.
result: pass

### 3. CLI shows usage help instead of crashing on bad invocation
expected: |
  `uv run dataplat` (no arguments), `uv run dataplat --bogus-option`, and
  `uv run dataplat no-such-command` each print usage/help text and exit non-zero — none of them
  print a raw Python traceback. (This was CR-01, found and fixed earlier this session.)
result: pass

### 4. Docker image builds and runs, tagged by git SHA
expected: |
  `make image-csv-processor` builds an image tagged `csv-processor:<current-git-short-sha>`
  (never `:latest`), and `docker run --rm csv-processor:<sha> --version` prints the version.
result: pass

### 5. Migrations create the whole meta/normalized schema
expected: |
  Against a throwaway PostgreSQL 18 (testcontainers), `alembic upgrade head` creates
  `meta.datasets`, `meta.config_versions`, `meta.files`, `meta.batches`, `meta.batch_files`,
  `meta.ingestion_runs`, and `normalized.customers` — running it a second time is a no-op.
result: pass

### 6. Dataset config syncs to a versioned row
expected: |
  Loading `configs/datasets/customers.yaml`, hashing it, and calling `ConfigRegistry.sync()`
  creates one `meta.datasets` row and one `meta.config_versions` row. Calling `sync()` again
  with the unchanged file is a no-op — no new row, no version bump.
result: pass

### 7. CSV chunking never splits an embedded newline
expected: |
  A CSV row with a quoted field containing an embedded newline (e.g. `"line1\nline2"`) survives
  chunking completely intact — the field's content is identical whether the chunk size is 1, 2,
  or 3 rows.
result: pass

### 8. Secrets resolver fails closed on unrecognized schemes
expected: |
  `resolve_secret("vault://kv/data/etl/db")` raises `SecretResolutionError` rather than silently
  returning the literal string `"vault://kv/data/etl/db"` or crashing with an unrelated exception.
result: pass

### 9. Logging never leaks a secret value
expected: |
  Logging an event with a field named `password` (or `token`, `secret`, `dsn`, `credential`)
  never shows the raw value in the captured output — only the literal string `***REDACTED***`.
result: pass

## Summary

total: 9
passed: 8
issues: 1
pending: 0
skipped: 0
blocked: 0

## Gaps

- truth: "make test-integration passes cleanly when the whole tests/integration/ directory runs together in one pytest session (as CI's `integration` job actually runs it)"
  status: failed
  reason: "Claude reported: tests/integration/test_metadata_repository.py::test_full_slice_round_trip fails with psycopg.errors.UniqueViolation on uq_config_versions_dataset_version (dataset_id, version)=(1, 1) — a cross-file test-isolation collision between test_config_registry.py (03-04) and test_metadata_repository.py (03-05), both of which independently create a 'customers' dataset + version-1 config_versions row against the same session-scoped Postgres fixture, never exercised together until this run."
  severity: blocker
  test: 1
  root_cause: "test_metadata_repository.py::_insert_config_version() hardcodes version=1 with no ON CONFLICT/version derivation, and test_full_slice_round_trip reuses the literal dataset name \"customers\" that test_config_registry.py also creates. Both files share one session-scoped Postgres fixture (tests/integration/conftest.py, by deliberate design). pytest's default alphabetical file collection always runs test_config_registry.py before test_metadata_repository.py (no ordering plugin configured), so test_config_registry.py deterministically creates (dataset_id=1, version=1) first every time, and test_metadata_repository.py's hardcoded version=1 insert always collides. get_or_create_dataset() itself is correct (idempotent ON CONFLICT DO UPDATE) and is not the bug."
  artifacts:
    - path: "tests/integration/test_metadata_repository.py"
      issue: "_insert_config_version() (lines 33-53) hardcodes version=1 with no conflict handling; test_full_slice_round_trip (line 79) reuses dataset name \"customers\" shared with test_config_registry.py"
    - path: "tests/integration/test_config_registry.py"
      issue: "creates the real \"customers\" dataset + version-1 config_versions row (lines 61-82) — not itself buggy, but the source of the pre-existing row the other file collides with"
  missing:
    - "test_metadata_repository.py should use a dataset name that cannot collide with any other file's fixture data (e.g. \"customers_slice_proof\") instead of \"customers\""
    - "_insert_config_version() should derive the next version for dataset_id (e.g. COALESCE(MAX(version), 0) + 1) instead of hardcoding version=1"
  debug_session: ""
