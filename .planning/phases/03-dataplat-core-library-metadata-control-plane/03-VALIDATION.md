---
phase: 3
slug: dataplat-core-library-metadata-control-plane
status: draft
nyquist_compliant: false
wave_0_complete: false
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

Task ID / Plan / Wave columns are filled in by the planner as it creates `PLAN.md` files — this
table pre-registers the requirement → test mapping the planner's tasks must satisfy.

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| TBD | TBD | TBD | META-01 | — | `alembic upgrade head` creates the complete slice schema | integration | `pytest tests/integration/test_migrations.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | META-02 | — | `files`/`config_versions`/`normalized.customers` carry `hash_version`/`_record_hash_version` | integration | `pytest tests/integration/test_migrations.py::test_hash_version_columns -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | INFRA-08 | — | Built image tag equals `git rev-parse --short HEAD`, never `latest` | integration/build | `pytest tests/policy/test_no_latest_image_tag.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | SEC-15 | T-03-01 | `resolve_secret()` handles `env://`/`file://`; unrecognized scheme fails closed | unit | `pytest tests/unit/test_secrets_resolver.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | SEC-15 / OBS-05 | T-03-02 | A credential value passed through the resolver never appears in a captured log line | unit | `pytest tests/unit/test_logging_redaction.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | CSV-13 | — | Embedded-newline records survive chunking at chunk sizes 1, 2, 3 | unit + property | `pytest tests/unit/test_csv_chunking.py tests/property/test_chunking_properties.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | SCHEMA-07 | T-03-03 | Identical config (reordered keys) hashes identically; changed content hashes differently | unit | `pytest tests/unit/test_config_hashing.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | SCHEMA-07 | — | Config-sync round trip writes the expected `meta.config_versions` row | integration | `pytest tests/integration/test_config_registry.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | OBS-02 / OBS-04 | — | `structlog` emits JSON in-cluster, console locally; bound context appears on every subsequent event | unit | `pytest tests/unit/test_logging_config.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | OBS-05 | T-03-02 | Redaction processor drops secret-pattern keys and truncates `raw_line`/`record` | unit | `pytest tests/unit/test_logging_redaction.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | QUAL-03 | — | A malformed row produces a `RejectedRecord`, never raises | unit | `pytest tests/unit/test_pipeline_errors.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | QUAL-03 | — | `DataPlatformError` subclasses carry `context: dict`; caught exactly once in `cli.py` | unit | `pytest tests/unit/test_cli_error_handling.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | SEC-15 (V4) | T-03-04 | Migrations `GRANT` `etl_app` only SELECT/INSERT/UPDATE per table, never DDL/superuser | integration | `pytest tests/integration/test_migrations.py::test_etl_app_grants -x` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/integration/conftest.py` — `postgres_dsn`, `minio_config`, `s3_client` session-scoped
      testcontainers fixtures
- [ ] `tests/integration/test_migrations.py` — the META-01/META-02 proof (schema creation +
      `hash_version` columns + `etl_app` grants)
- [ ] `tests/unit/test_secrets_resolver.py` — SEC-15
- [ ] `tests/unit/test_logging_redaction.py` — SEC-15 / OBS-05 (shared file, both requirements)
- [ ] `tests/unit/test_logging_config.py` — OBS-02/04
- [ ] `tests/unit/test_csv_chunking.py` + `tests/property/test_chunking_properties.py` — CSV-13
- [ ] `tests/unit/test_config_hashing.py`, `tests/integration/test_config_registry.py` — SCHEMA-07
- [ ] `tests/unit/test_pipeline_errors.py`, `tests/unit/test_cli_error_handling.py` — QUAL-03
- [ ] Makefile target `test-integration` (D-04) — `$(RUN_CLUSTER) pytest tests/integration -q`,
      reusing the existing `RUN_CLUSTER` variable from the `cluster` dependency group
- [ ] `.github/workflows/ci.yml` — new `integration` job running `make test-integration`, separate
      from the existing `check` job (Docker is available by default on `ubuntu-latest` runners)
- [ ] `tests/policy/test_no_latest_image_tag.py` — INFRA-08, mirrors existing `tests/policy/` style

*This phase has no pre-existing test infrastructure for its own domain — only the repo-wide
pytest/ruff/mypy scaffolding Phase 1 built, which every item above plugs into.*

---

## Manual-Only Verifications

*None — all phase behaviors have automated verification. `docker run csv-processor:<git-sha>
dataplat --version` (success criterion 3) is exercised as an integration/build test, not left to
manual confirmation.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 90s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
