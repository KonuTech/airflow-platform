---
phase: 3
slug: dataplat-core-library-metadata-control-plane
status: planned
nyquist_compliant: true
wave_0_complete: true
created: 2026-08-12
---

# Phase 3 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.1.1 (already configured, `[tool.pytest.ini_options]` in root `pyproject.toml`) |
| **Config file** | `pyproject.toml` (`[tool.pytest.ini_options]`, `testpaths = ["tests"]`) |
| **Quick run command** | `uv run --frozen pytest tests/unit -q` |
| **Full suite command** | `make check && make test-integration` (`test-integration` is a new Makefile target this phase adds, per CONTEXT.md D-04) |
| **Estimated runtime** | ~5s quick (no Docker) / ~60-90s full (testcontainers Postgres + MinIO startup included) |

---

## Sampling Rate

- **After every task commit:** Run `uv run --frozen pytest tests/unit -q`
- **After every plan wave:** Run `make check && make test-integration`
- **Before `/gsd:verify-work`:** Full suite must be green, plus `alembic upgrade head` proven fresh
  against a throwaway testcontainers Postgres (not a warm/reused one) at least once
- **Max feedback latency:** ~90 seconds (testcontainers container startup is the dominant cost)

---

## Per-Task Verification Map

Filled in after planning + plan-checker verification (2 rounds; 0 blockers, all warnings resolved
or dispositioned — see 03-DISCUSSION-LOG.md-adjacent plan-checker history). Task references are
`{plan}-T{n}` (task position within that plan's `<tasks>` block — plans do not carry global task
IDs).

| Task | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 03-02-T1/T3 | 03-02 | 2 | META-01 | — | `alembic upgrade head` creates schemas `meta`/`normalized`, the five slice tables + `batch_files` + `normalized.customers`, nothing else | integration | `pytest tests/integration/test_migrations.py -x` | ✅ | ⬜ pending execution |
| 03-02-T1 | 03-02 | 2 | META-02 | — | `meta.files.hash_version`, `meta.config_versions.hash_version`, `normalized.customers._record_hash_version` all exist | integration | `pytest tests/integration/test_migrations.py::test_hash_version_columns -x` | ✅ | ⬜ pending execution |
| 03-07-T3 | 03-07 | 3 | INFRA-08 | — | Built image tag equals `git rev-parse --short HEAD`, never `latest` | integration/build | `pytest tests/policy/test_no_latest_image_tag.py -x` | ✅ | ⬜ pending execution |
| 03-03-T? | 03-03 | 2 | SEC-15 | T-03-01 | `resolve_secret()` handles `env://`/`file://`; unrecognized scheme (incl. `vault://`) fails closed | unit | `pytest tests/unit/test_secrets_resolver.py -x` | ✅ | ⬜ pending execution |
| 03-05-T3 | 03-05 | 3 | SEC-15 / META-01 | T-03-01 | `resolve_secret("env://...")`'s output, passed into `create_pool()`, yields a real live connection (added in revision to close the isolation-only-testing gap) | integration | `pytest tests/integration/test_metadata_repository.py::test_resolved_env_secret_yields_a_live_metadata_connection -x` | ✅ | ⬜ pending execution |
| 03-03-T? | 03-03 | 2 | SEC-15 / OBS-05 | T-03-02 | A credential value passed through the resolver never appears in a captured log line | unit | `pytest tests/unit/test_logging_redaction.py -x` | ✅ | ⬜ pending execution |
| 03-08-T1 | 03-08 | 5 | CSV-13 | — | Embedded-newline records survive chunking at chunk sizes 1, 2, 3; NUL characters filtered at the character level post-decode | unit + property | `pytest tests/unit/test_csv_chunking.py tests/property/test_chunking_properties.py -x` | ✅ | ⬜ pending execution |
| 03-04-T? | 03-04 | 3 | SCHEMA-07 | T-03-03 | Identical config (reordered keys) hashes identically; changed content hashes differently | unit | `pytest tests/unit/test_config_hashing.py -x` | ✅ | ⬜ pending execution |
| 03-04-T? | 03-04 | 3 | SCHEMA-07 | — | Config-sync round trip writes the expected `meta.config_versions` row for the seeded `configs/datasets/customers.yaml` | integration | `pytest tests/integration/test_config_registry.py -x` | ✅ | ⬜ pending execution |
| 03-03-T? | 03-03 | 2 | OBS-02 / OBS-04 | — | `structlog` emits JSON in-cluster, console locally; bound context appears on every subsequent event | unit | `pytest tests/unit/test_logging_config.py -x` | ✅ | ⬜ pending execution |
| 03-03-T? | 03-03 | 2 | OBS-05 | T-03-02 | Redaction processor drops secret-pattern keys and truncates `raw_line`/`record` | unit | `pytest tests/unit/test_logging_redaction.py -x` | ✅ | ⬜ pending execution |
| 03-06-T? | 03-06 | 4 | QUAL-03 | — | A malformed row produces a `RejectedRecord` via `StageResult.rejected`, never raises | unit | `pytest tests/unit/test_pipeline_errors.py -x` | ✅ | ⬜ pending execution |
| 03-07-T? | 03-07 | 3 | QUAL-03 | — | `DataPlatformError` subclasses carry `context: dict`; caught exactly once in `cli.py`'s `main(argv) -> int` | unit | `pytest tests/unit/test_cli_error_handling.py -x` | ✅ | ⬜ pending execution |
| 03-01-T? | 03-01 | 1 | QUAL-03 | — | `DataPlatformError` hierarchy exists with only this phase's actually-raised branches (`ConfigurationError`, `StorageError`, `SecretResolutionError`) | unit | `pytest tests/unit/ -k errors -x` | ✅ | ⬜ pending execution |
| 03-02-T1 | 03-02 | 2 | SEC-15 (V4) | T-03-04 | Migrations `GRANT` `etl_app` only `SELECT, INSERT, UPDATE` per table, never DDL/superuser | integration | `pytest tests/integration/test_migrations.py::test_etl_app_grants -x` | ✅ | ⬜ pending execution |

*Status: ⬜ pending execution (plans verified, not yet run) · ✅ green · ❌ red · ⚠️ flaky.
"File Exists" now reflects task specification, not actual test-file presence on disk — files are
created during `/gsd:execute-phase`, per each task's own `<automated>` verify command.*
*Some `-T?` task positions are approximate (plan bodies don't number tasks with global IDs) —
resolve to exact task order from each `PLAN.md`'s `<tasks>` block during execution if precision
is needed.*

---

## Wave 0 Requirements

All items below are now real tasks inside the 8 verified plans (not separate pre-staged Wave-0
work) — this phase co-locates each test with the task that makes it meaningful, rather than
front-loading empty test stubs. Confirmed present as of plan-checker verification:

- [x] `tests/integration/conftest.py` — `postgres_dsn`, `minio_config`, `s3_client` session-scoped
      testcontainers fixtures (03-02)
- [x] `tests/integration/test_migrations.py` — the META-01/META-02 proof, incl. `etl_app` grants
      (03-02)
- [x] `tests/unit/test_secrets_resolver.py` — SEC-15 (03-03)
- [x] `tests/unit/test_logging_redaction.py` — SEC-15 / OBS-05, shared file (03-03)
- [x] `tests/unit/test_logging_config.py` — OBS-02/04 (03-03)
- [x] `tests/unit/test_csv_chunking.py` + `tests/property/test_chunking_properties.py` — CSV-13
      (03-08)
- [x] `tests/unit/test_config_hashing.py`, `tests/integration/test_config_registry.py` —
      SCHEMA-07 (03-04)
- [x] `tests/unit/test_pipeline_errors.py` — QUAL-03 (03-06); `tests/unit/test_cli_error_handling.py`
      — QUAL-03 (03-07)
- [x] `tests/integration/test_metadata_repository.py::test_resolved_env_secret_yields_a_live_metadata_connection`
      — SEC-15 live-wiring proof, added during plan revision (03-05)
- [x] Makefile target `test-integration` (D-04) — `$(RUN_CLUSTER) pytest tests/integration -q`,
      reusing the `RUN_CLUSTER`/`cluster` dependency group; never a prerequisite of `check`/`ci`
      (03-02)
- [x] `.github/workflows/ci.yml` — new `integration` job running `make test-integration`, separate
      from the existing `check` job (03-02)
- [x] `tests/policy/test_no_latest_image_tag.py` — INFRA-08, mirrors existing `tests/policy/` style
      (03-07)

*This phase has no pre-existing test infrastructure for its own domain — only the repo-wide
pytest/ruff/mypy scaffolding Phase 1 built, which every item above plugs into.*

---

## Manual-Only Verifications

*None — all phase behaviors have automated verification. `docker run csv-processor:<git-sha>
dataplat --version` (success criterion 3) is exercised as an integration/build test, not left to
manual confirmation.*

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies — confirmed by gsd-plan-checker
      (24/24 tasks across 8 plans, both verification passes)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify — confirmed (every
      3-task window is 3/3)
- [x] Wave 0 covers all MISSING references — N/A, no `<automated>MISSING</automated>` references
      exist; tests are co-located with implementation per task, not pre-staged
- [x] No watch-mode flags — confirmed
- [x] Feedback latency < 90s — quick command ~5s (no Docker); full suite ~60-90s (testcontainers
      startup dominates); several individual task-level verify commands exceed 30s by design
      (testcontainers/Docker-build tasks whose entire purpose is proving that infrastructure
      works) — disclosed and accepted, not a violation of the phase-level 90s budget
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-08-13 (gsd-plan-checker VERIFICATION PASSED, revision iteration 1 of 3,
0 blockers)
