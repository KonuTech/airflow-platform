---
phase: 04-vertical-slice-csv-to-analytical-postgresql
plan: 05
subsystem: database

# Dependency graph
requires:
  - phase: 04-vertical-slice-csv-to-analytical-postgresql
    provides: "04-01's RunContext.file_id/batch_id + claim_ingestion_run, 04-03's discover_files/AssignmentDocument, 04-04's StagingLoader/resolve_publisher/MergePublisher"
provides:
  - "dataplat.pipeline.run.run_ingest — claim, stage (with a live rows_read/rows_parsed heartbeat), one atomic publish transaction, Receipt on every exit path"
  - "csv_processor.cli's discover/ingest click commands, attached to the shared dataplat.cli.cli group via an importlib.metadata entry point (never a static import)"
  - "MetadataRepository.get_ingestion_run_status — a pure read distinguishing SUCCEEDED from RUNNING-with-live-lease"
  - "docker/csv-processor/Dockerfile ships configs/ and declares a real runtime WORKDIR /app"
affects: ["04-07 (the DAG's KubernetesPodOperator tasks invoke exactly these discover/ingest commands, unmodified)", "04-08/E2E (pod-kill retry and idempotency tests exercise run_ingest's claim/lease protocol directly)"]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "entry-point plugin loading: dataplat.cli.main() iterates importlib.metadata.entry_points(group=\"dataplat.plugins\") and calls .load() before dispatching — the only place dataplat causes csv_processor code to load, with no static import token anywhere in dataplat's source (import-linter contract 1 stays green)"
    - "Two connections per run_ingest call, deliberately never the same one: staging commits on its own connection (pool.connection()'s own context-manager commit-on-clean-exit), publication is pg_advisory_xact_lock + Publisher.publish + finalize_publication inside one conn.transaction() block"
    - "A background heartbeat thread (threading.Event.wait(interval) as both sleep and stop-signal) refreshes lease_expires_at and rows_read/rows_parsed from a shared, lock-free progress holder, stopped unconditionally in a finally"
    - "CLI commands write a Receipt/dict to the XCom sidecar path on every exit path (success, skip, and DataPlatformError failure) via one _write_xcom helper; run_ingest itself adds no receipt-writing boundary of its own — that is each CLI command's job"

key-files:
  created:
    - packages/dataplat/src/dataplat/pipeline/run.py
    - packages/csv-processor/src/csv_processor/cli.py
    - tests/integration/test_run_ingest.py
  modified:
    - packages/dataplat/src/dataplat/cli.py
    - packages/dataplat/src/dataplat/metadata/repository.py
    - packages/dataplat/src/dataplat/metadata/postgres.py
    - packages/csv-processor/pyproject.toml
    - docker/csv-processor/Dockerfile
    - uv.lock

key-decisions:
  - "run_ingest hardcodes _CUSTOMERS_TARGET_COLUMNS (customer_id, name, country, birth_date, event_ts) for StagingLoader — mirrors MergePublisher's own already-hardcoded customers column list; this vertical slice is deliberately single-dataset and DatasetConfig has no generic business-column field to resolve this from yet"
  - "run_ingest(ctx, *, heartbeat_interval_seconds: float = 60.0) — a keyword-only override the plan's own text left as illustrative (\"e.g. every 60s\"); added so tests can shrink it far below 60s and observe a heartbeat write without a real minute-long wait, while every production call site uses the default"
  - "The claim-refused branch resolves SKIPPED_DUPLICATE vs SKIPPED_CONCURRENT via a new, dedicated repository read (get_ingestion_run_status(run_id=...)) rather than re-deriving dataset_id/config_version_id just to reuse get_or_create_ingestion_run for a read — the plan explicitly offered this as one of two valid choices (\"a small helper read, or infer from context\")"

patterns-established:
  - "rows_deduplicated = max(staging_result.rows_parsed - result.rows_affected, 0) — this phase does not separately track DISTINCT-ON collapses from WHERE-suppressed no-op writes; a finer split is Phase 9's meta.dedup_decisions territory"
  - "CLI-level untrusted-input Pydantic validation (AssignmentDocument.model_validate_json) is always re-raised as dataplat.errors.ConfigurationError, never left to escape as a raw pydantic.ValidationError — the same discipline dataplat.config.loader.load_config already established for YAML config parsing"

requirements-completed: [META-03, LOAD-01, LOAD-02, LOAD-05, LOAD-08, LOAD-09]

# Metrics
duration: ~50min
completed: 2026-08-13
---

# Phase 04 Plan 05: run_ingest Orchestration and discover/ingest CLI Wiring Summary

**`dataplat.pipeline.run.run_ingest` (claim → live-heartbeat staging → one atomic publish transaction → Receipt) plus `csv_processor.cli`'s `discover`/`ingest` commands, wired onto the shared `dataplat` CLI entirely through an `importlib.metadata` entry point so `dataplat`'s own source never contains the text `import csv_processor`.**

## Performance

- **Duration:** ~50 min
- **Completed:** 2026-08-13T15:50:00Z
- **Tasks:** 2
- **Files modified:** 9 (3 created, 6 modified)

## Accomplishments

- `run_ingest` is the complete, source-agnostic pod-side orchestration: claims the run (`claim_ingestion_run`), stages on its own connection with a live `rows_read`/`rows_parsed` heartbeat thread, then runs `pg_advisory_xact_lock` + `Publisher.publish` + `finalize_publication` inside one `conn.transaction()` block so publication and status-flip commit or roll back together (META-03), then explicitly drops the staging table after commit (never `ON COMMIT DROP`). No `except` clause anywhere in the module — only a `finally` that stops the heartbeat — so a run-fatal exception propagates to the caller uncaught, exactly as the plan specifies.
- Resolved the architectural conflict this plan's own Interfaces section flagged: `csv_processor.cli`'s `discover`/`ingest` attach to `dataplat.cli.cli` via `importlib.metadata.entry_points(group="dataplat.plugins")`, loaded by `dataplat.cli.main()` before dispatch — a runtime, string-based lookup import-linter's static AST scan cannot see, keeping `setup.cfg` contract 1 (`dataplat` must not depend on `csv_processor`) green while still letting the two subcommands share one entrypoint.
- `docker/csv-processor/Dockerfile` now ships `configs/` for `discover`'s config-resolution needs, and the runtime stage declares `WORKDIR /app` explicitly — it was silently absent before this plan (verified by reading the whole file, then confirmed live: `docker run --rm --entrypoint pwd csv-processor:test` printed `/` before the fix and `/app` after).
- 6 new integration tests against real testcontainers PostgreSQL + MinIO (using a real `CsvSource`, never a fake) prove every `<behavior>` bullet: the full success path, `SKIPPED_DUPLICATE`, `SKIPPED_CONCURRENT`, a simulated crash between staging and publish followed by a clean retry (proving `staging.py`'s own `DROP TABLE IF EXISTS`-first behavior composes correctly with `run_ingest`'s retry path), the live heartbeat, and — the closest analogue to 04-04's deferred concurrency test — the publish transaction's four effects (the `normalized.customers` insert plus the three `meta.*` status flips) staying invisible to a second connection until commit.

## Task Commits

Each task was committed atomically:

1. **Task 1: run_ingest — claim, stage, publish-transaction, receipt** - `1ea985a` (feat)
2. **Task 2: discover/ingest CLI commands, entry-point wiring, Dockerfile configs/** - `244c7a7` (feat)

## Files Created/Modified

- `packages/dataplat/src/dataplat/pipeline/run.py` - `run_ingest`: claim/stage/publish-transaction/receipt orchestration, heartbeat thread
- `packages/csv-processor/src/csv_processor/cli.py` - `discover`/`ingest` click commands, `_build_common`/`_write_xcom`/`_parse_s3_uri` helpers
- `tests/integration/test_run_ingest.py` - 6 tests against real testcontainers Postgres + MinIO
- `packages/dataplat/src/dataplat/cli.py` - `main()` loads `dataplat.plugins` entry points before dispatching to `cli`
- `packages/dataplat/src/dataplat/metadata/repository.py` - new `get_ingestion_run_status` Protocol method; `finalize_publication.report_uri` widened to `str | None`
- `packages/dataplat/src/dataplat/metadata/postgres.py` - `get_ingestion_run_status` implementation; matching `report_uri` widening
- `packages/csv-processor/pyproject.toml` - `click>=8.4,<9` as a direct dependency; `[project.entry-points."dataplat.plugins"]`
- `docker/csv-processor/Dockerfile` - `COPY configs/ configs/` (builder, post-dependency-layer) + `COPY --from=builder .../configs /app/configs` and `WORKDIR /app` (runtime)
- `uv.lock` - regenerated after the `click` direct-dependency addition (resolved version unchanged; confirmed via `uv lock --check`)

## Decisions Made

- **`_CUSTOMERS_TARGET_COLUMNS` is a module-level constant, not derived from `DatasetConfig`.** The plan's own action text left `StagingLoader(target_columns=(...))` as a literal ellipsis. `DatasetConfig` carries no generic "business columns" field yet, and `MergePublisher` (plan 04-04) already hardcodes the identical column list against `normalized.customers` for the same reason — this vertical slice is deliberately single-dataset. Keeping both hardcoded lists in sync is a known constraint, documented in `run.py`'s own comment.
- **`heartbeat_interval_seconds` is a keyword-only parameter, not a hardcoded 60s literal.** The plan's text describes "an interval well under the 5-minute lease, e.g. every 60s" without specifying how a test would observe a heartbeat firing without a real-time wait. Making it overridable (default `60.0`, matching the plan's own suggestion) let `test_heartbeat_writes_a_live_nonzero_rows_read_while_running_before_return` set it to `0.05` and assert deterministically, instead of either waiting a real minute or leaving the behavior unverified.
- **`SKIPPED_DUPLICATE`/`SKIPPED_CONCURRENT` disambiguation reads via `ctx.run.run_id` directly, not a re-lookup by `idempotency_key`.** `ctx.run.run_id` is already known before `run_ingest` is ever called (frozen into the `AssignmentDocument` at discovery time and threaded through by the `ingest` CLI's `RunContext` construction), so the new `get_ingestion_run_status(run_id=...)` read needs no `idempotency_key`-based lookup or index scan.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical Functionality] Added `MetadataRepository.get_ingestion_run_status`**
- **Found during:** Task 1, implementing the claim-refused branch's SKIPPED_DUPLICATE/SKIPPED_CONCURRENT disambiguation.
- **Issue:** The plan's own text names this exact need ("determine SKIPPED_DUPLICATE vs SKIPPED_CONCURRENT by re-reading the run's current status — a small helper read, or infer from context — document whichever is chosen") but no existing `MetadataRepository` method could answer "what is this run's current status" without either mutating (`claim_ingestion_run`) or requiring `dataset_id`/`config_version_id`/`processor_image_digest` the caller does not have at that point (`get_or_create_ingestion_run`).
- **Fix:** Added `get_ingestion_run_status(*, run_id: int) -> str | None` to the `MetadataRepository` Protocol and `PostgresMetadataRepository` — a pure `SELECT status FROM meta.ingestion_runs WHERE run_id = %s` read, matching this module's own established "one parameterized statement per method" discipline.
- **Files modified:** `packages/dataplat/src/dataplat/metadata/repository.py`, `packages/dataplat/src/dataplat/metadata/postgres.py`
- **Verification:** `test_already_succeeded_run_returns_skipped_duplicate_and_touches_no_staging_table` and `test_running_run_with_a_live_lease_returns_skipped_concurrent` in `tests/integration/test_run_ingest.py` both pass; `mypy packages/dataplat/src` passes.
- **Committed in:** `1ea985a` (Task 1 commit)

**2. [Rule 3 - Blocking Issue] Widened `finalize_publication`'s `report_uri` from `str` to `str | None`**
- **Found during:** Task 1, writing `run_ingest`'s publish-transaction call to `finalize_publication(..., report_uri=None)`, exactly as the plan's own action text specifies.
- **Issue:** The existing Protocol signature (from plan 03) declared `report_uri: str` (no `None`), which would make `mypy packages/dataplat/src` fail — Task 1's own acceptance criterion. The backing `meta.ingestion_runs.report_uri` column is already nullable (migration 0004), and `Receipt.report_uri`'s own docstring already documents `None` as "no such report exists" — the type was simply too narrow for a case the schema and the sibling model both already anticipated.
- **Fix:** Widened `report_uri: str` to `report_uri: str | None` in both the Protocol (`repository.py`) and the implementation (`postgres.py`), with a docstring note explaining why `None` is a real, intended value here (this phase never generates a validation report).
- **Files modified:** `packages/dataplat/src/dataplat/metadata/repository.py`, `packages/dataplat/src/dataplat/metadata/postgres.py`
- **Verification:** `mypy packages/dataplat/src` passes; the existing `test_finalize_publication_updates_are_invisible_until_the_callers_commit` test (which passes a real string `report_uri`) still passes unmodified.
- **Committed in:** `1ea985a` (Task 1 commit)

**3. [Rule 1 - Bug] Added the missing `WORKDIR /app` to the Dockerfile's runtime stage**
- **Found during:** Task 2, following the plan's own explicit instruction to "read the full Dockerfile to confirm the runtime stage's WORKDIR before deciding" between adding a WORKDIR or using an absolute path in the CLI code.
- **Issue:** The `runtime` stage never declared `WORKDIR` at all (confirmed by reading the complete file). Docker does not carry a `WORKDIR` across `FROM ... AS <stage>` boundaries, so the container's default working directory was `/`, not `/app` — `discover`'s relative `Path(f"configs/datasets/{dataset}.yaml")` would have resolved to `/configs/datasets/customers.yaml`, which does not exist, even though `COPY --from=builder .../configs /app/configs` correctly placed the files at `/app/configs`. This would have silently broken `discover` at runtime despite the image building successfully and `--help` still working.
- **Fix:** Added `WORKDIR /app` to the `runtime` stage, immediately after the `useradd` line.
- **Files modified:** `docker/csv-processor/Dockerfile`
- **Verification:** `docker build -f docker/csv-processor/Dockerfile . -t csv-processor:test` succeeds; `docker run --rm --entrypoint pwd csv-processor:test` prints `/app`; `docker run --rm --entrypoint ls csv-processor:test -la /app/configs/datasets/customers.yaml` confirms the file is present and readable; `docker run --rm csv-processor:test discover --help` and `... ingest --help` both exit 0.
- **Committed in:** `244c7a7` (Task 2 commit)

**4. [Rule 2 - Missing Critical Functionality] `ingest` re-raises `AssignmentDocument.model_validate_json`'s `ValidationError` as `ConfigurationError`**
- **Found during:** Task 2, writing `ingest`'s try/except around the assignment-document read — T-04-02's own threat-model entry names this document as "technically attacker-influenceable".
- **Issue:** `pydantic.ValidationError` is not a `dataplat.errors.DataPlatformError` subclass, so a malformed/attacker-influenceable assignment document would have escaped `ingest`'s own `except DataPlatformError` block (breaking the plan's "a Receipt is written to the XCom path on every exit path" contract for this specific failure) and also escaped `dataplat.cli.main()`'s catch-once boundary entirely, crashing with a raw, unhandled traceback instead of a logged, structured failure.
- **Fix:** Wrapped the `AssignmentDocument.model_validate_json(raw_text)` call in its own `try/except ValidationError`, re-raising as `ConfigurationError` — the identical discipline `dataplat.config.loader.load_config` already established for the same class of untrusted-input Pydantic validation.
- **Files modified:** `packages/csv-processor/src/csv_processor/cli.py`
- **Verification:** `mypy packages/dataplat/src packages/csv-processor/src` passes; `uv run ruff check` passes; manual review confirms the exception now flows through `ingest`'s existing `except DataPlatformError` receipt-writing path instead of escaping raw (this specific path has no dedicated integration test — it requires a live MinIO object containing deliberately-malformed JSON, which is out of this plan's stated test scope; the wrapping mirrors `load_config`'s own already-tested pattern).
- **Committed in:** `244c7a7` (Task 2 commit)

**5. [Rule 3 - Blocking Issue] Regenerated `uv.lock` after adding `click` as a direct `csv-processor` dependency**
- **Found during:** Task 2, after editing `packages/csv-processor/pyproject.toml` to add `click>=8.4,<9` to `dependencies` (T-04-SC's own named mitigation).
- **Issue:** `uv lock --check` failed immediately after the `pyproject.toml` edit — the lockfile no longer matched the declared dependencies, which would break `uv sync --frozen` (the Dockerfile's own dependency-layer command) and CI's own lock-check gate.
- **Fix:** Ran `uv lock`, then re-verified `uv lock --check` passes. Confirmed via `git diff uv.lock` that only `csv-processor`'s own `dependencies`/`requires-dist` entries changed (`click` added to both) — no new `[[package]]` block and no version change anywhere else in the file, exactly matching T-04-SC's stated expectation that this addition "changes no resolved version and introduces no new package into the lockfile" (since `click` was already present, transitively, via `dataplat`).
- **Files modified:** `uv.lock`
- **Verification:** `uv lock --check` passes; `uv sync --frozen` succeeds; the Docker build (which runs `uv sync --frozen --no-install-workspace ...` then `uv sync --locked ...`) succeeds end-to-end.
- **Committed in:** `244c7a7` (Task 2 commit)

---

**Total deviations:** 5 auto-fixed (2 missing-critical-functionality, 1 bug-fix, 2 blocking-issue)
**Impact on plan:** All five were necessary for correctness (the SKIPPED_* disambiguation and the ValidationError wrapping), for the plan's own stated acceptance criteria to pass (`mypy`, `uv lock --check`), or for the Dockerfile's `configs/` addition to actually work as intended rather than merely build successfully. No scope creep — nothing beyond what this plan's two tasks require.

## Issues Encountered

- An early draft of `csv_processor/cli.py`'s `ingest` function accidentally left a nonsensical placeholder line (`raise exc from None if False else exc`) from an in-progress edit. Caught immediately on the next file read, before any lint/test run, and replaced with the intended bare `raise`.
- `ruff` flagged the `_S3_SECRET_KEY_REF = "env://DATAPLAT_S3_SECRET_KEY"` module constant as `S105` (possible hardcoded password) — a false positive, since the value is an opaque `SecretsResolver` reference string, never a secret value itself. Suppressed with a narrow, justified `# noqa: S105`.
- Several test functions initially tripped `PLR0913`/`PLR0917` (too many arguments) from accepting 6-7 pytest fixtures directly. Refactored `tests/integration/test_run_ingest.py` to bundle this file's own fixtures into one `_Env` dataclass + one `env` fixture, so every test takes one or two parameters instead of six or seven — a pure style change, not a behavior change.
- Early customer_id ranges in `test_run_ingest.py` risked colliding with `test_publish_merge.py`'s own already-occupied ranges (both files share the same session-scoped `normalized.customers` table across the whole `tests/integration/` collection). Resolved by choosing a distinct, clearly-out-of-range base (`9_100_0xx`+) before writing any assertions, rather than discovering the collision via a flaky test later.

## User Setup Required

None - no external service configuration required. (The four `DATAPLAT_*` env vars `_build_common` resolves are supplied by the DAG's `KubernetesPodOperator` pod spec, established in this phase's other plans — nothing for a human to configure as a consequence of this plan specifically.)

## Next Phase Readiness

- `csv_ingest_customers.py` (a later plan's DAG file) can invoke `dataplat discover --dataset customers` and `dataplat ingest --assignment <uri>` via `KubernetesPodOperator` exactly as built here — both commands are proven working from the built `csv-processor:test` image, `--help` exits 0 for both, and the entry-point wiring is mechanically verified by `lint-imports`, not merely asserted.
- The pod-kill/retry E2E test (ROADMAP success criterion 3, D-09..D-11) can reuse `run_ingest`'s claim/lease protocol directly: `test_crash_between_staging_and_publish_leaves_no_partial_state_and_retry_succeeds` already proves the exact mechanism (expired-lease reclaim, clean staging-table retry) that test will exercise against a real `kubectl delete pod`.
- No blockers. The one open design note (`_CUSTOMERS_TARGET_COLUMNS` staying in sync with `MergePublisher`'s own hardcoded column list) is documented in both source files and this summary so a future reader adding a second dataset knows both places need updating together.

---
*Phase: 04-vertical-slice-csv-to-analytical-postgresql*
*Completed: 2026-08-13*
