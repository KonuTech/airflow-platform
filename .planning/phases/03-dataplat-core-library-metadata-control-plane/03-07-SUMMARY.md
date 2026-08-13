---
phase: 03-dataplat-core-library-metadata-control-plane
plan: 07
subsystem: infra
tags: [cli, click, structlog, docker, uv-workspace, oci-labels, policy-test]

# Dependency graph
requires:
  - phase: 03-01
    provides: "dataplat.errors exception hierarchy (DataPlatformError, ConfigurationError) and dataplat.version.resolve_version()"
  - phase: 03-03
    provides: "dataplat.observability.logging.configure()/get_logger() structured logging"
provides:
  - "dataplat console script (`dataplat` on PATH) wired via packages/dataplat/pyproject.toml's [project.scripts]"
  - "cli.py's main(argv) -> int: the catch-exactly-once DataPlatformError -> exit 1 boundary every future subcommand inherits (D-06)"
  - "docker/csv-processor/Dockerfile: a buildable, runnable, non-root (UID 1000), git-SHA-labeled pod image with no Airflow distribution"
  - "make image-csv-processor: builds + tags the image by git rev-parse --short HEAD, never :latest"
  - "tests/policy/test_no_latest_image_tag.py: INFRA-08 enforced statically in make check"
  - "tests/integration/test_docker_image.py: automated, mechanical proof of ROADMAP success criterion 3"
affects: [phase-04-vertical-slice-dag-and-pod, phase-11-ci-cd-observability-and-hardening]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Cluster O CLI shape: click.group() + version_option, plus a plain main(argv) -> int wrapping cli.main(standalone_mode=False) in try/except DataPlatformError -- the console-script entry point"
    - "Multi-stage Dockerfile: --no-install-workspace --frozen dependency-only layer (cacheable across source changes) -> --locked --all-packages --no-editable full layer -> slim non-root runtime stage copying only .venv"
    - "Makefile recipe-body regex scanning (mirrors test_pinned_tool_versions_agree.py/test_supply_chain_guards.py) with explicit comment-stripping before any substring/regex check"

key-files:
  created:
    - packages/dataplat/src/dataplat/cli.py
    - docker/csv-processor/Dockerfile
    - tests/unit/test_cli_error_handling.py
    - tests/policy/test_no_latest_image_tag.py
    - tests/integration/test_docker_image.py
  modified:
    - packages/dataplat/pyproject.toml
    - Makefile
    - docs/README.md

key-decisions:
  - "uv sync in the Dockerfile needs --all-packages, not bare --no-dev: dataplat/csv-processor are workspace members listed ONLY inside the root's `dev` dependency-group, so --no-dev alone silently installed neither package nor its dependencies"
  - "uv sync's full-install layer also needs --no-editable: uv installs local workspace members editable by default (a pointer back to packages/*/src), but the runtime stage copies only .venv, never packages/, so an editable install left dataplat unimportable at runtime"
  - "The integration test runs `docker run --rm <image> --version`, not `docker run --rm <image> dataplat --version`: ENTRYPOINT is already [\"dataplat\"], so repeating the binary name is parsed as an unknown click subcommand and fails -- reconciles an internal inconsistency between the plan's own must_haves prose (copied from ROADMAP's pre-ENTRYPOINT-design wording) and its equally-explicit ENTRYPOINT [\"dataplat\"] requirement"
  - "DATAPLAT_LOG_JSON env var (unset/falsy -> console renderer, truthy -> JSON) controls main()'s in_cluster logging choice; defaults to console so a local `dataplat --version` stays human-readable, with Kubernetes pod specs opting into JSON"
  - "OCI image org.opencontainers.image.version reuses the git SHA (same value as .revision): there is no semver release for this image yet, and INFRA-08 already mandates the tag itself be the git SHA"
  - "docker/csv-processor/.gitkeep removed now that the directory holds real content, matching the precedent plan 03-04 already set for configs/.gitkeep and schemas/.gitkeep"

patterns-established:
  - "Console-script entry points in this repo: click.group() (production surface) + a plain main(argv) -> int (the actual [project.scripts] target) that owns logging setup and the D-06 catch-once boundary, matching tools/corpus/__main__.py's main(argv)->int shape and tools/security/gitleaks_selftest.py's catch-domain-error-log-return-1 shape"
  - "Policy tests that scan Makefile recipe bodies must strip comments before any substring/regex check (RUF100-style false positives are real: this plan's own explanatory comments about ':latest' and 'except Exception' tripped blunt greps until reworded/stripped)"

requirements-completed: [INFRA-08, QUAL-03]

# Metrics
duration: 45min
completed: 2026-08-13
---

# Phase 3 Plan 07: CLI Entrypoint & csv-processor Docker Image Summary

**`dataplat` console script with a click-based `--version` + D-06 catch-once error boundary, plus a buildable, non-root, git-SHA-tagged `csv-processor` Docker image proven end-to-end by an actual `docker build` + `docker run` integration test.**

## Performance

- **Duration:** ~45 min
- **Started:** 2026-08-13T06:33Z (worktree base correction)
- **Completed:** 2026-08-13T07:14Z
- **Tasks:** 3 (plus one post-hoc test-coverage strengthening commit)
- **Files modified:** 8 (5 created, 3 modified, 1 removed)

## Accomplishments

- `dataplat --version` works end to end via the installed console script, resolving the version from installed distribution metadata (never a hardcoded literal)
- A `DataPlatformError` raised anywhere inside a CLI command is caught exactly once in `main()`, logged with structured `context`, and produces exit code 1 with no raw Python traceback; an undeclared exception is proven to propagate instead (D-06/QUAL-03 complete)
- `docker/csv-processor/Dockerfile` builds a real, runnable, non-root (numeric UID 1000) image with OCI `revision`/`source`/`version` labels, following ADR-0004's exact `--no-install-workspace --frozen` then `--locked` layer ordering
- `make image-csv-processor` always tags by `git rev-parse --short HEAD`, never `:latest`; a static policy test (`tests/policy/test_no_latest_image_tag.py`) fails the build if that regresses
- ROADMAP success criterion 3 is now mechanically proven by `tests/integration/test_docker_image.py`, not asserted or manually eyeballed

## Task Commits

Each task was committed atomically:

1. **Task 1: cli.py -- `--version` and the catch-exactly-once error boundary** - `ab8a115` (feat)
2. **Task 2: The csv-processor Dockerfile, its Make target, and the no-`:latest` policy test** - `49d1b9d` (feat)
3. **Task 3: Prove success criterion 3 -- build, run, read the version back** - `da73ff1` (test)
4. **Post-hoc: close a coverage gap on `main()`'s own success return path** - `cd65bf4` (test)

**Plan metadata:** this commit (SUMMARY.md)

_Note: no TDD tasks in this plan; Task 4 above is a same-plan follow-up test addition prompted by `make check`'s coverage report, not a separate plan task._

## Files Created/Modified

- `packages/dataplat/src/dataplat/cli.py` - click group (`--version`) + `main(argv) -> int`: configures logging once, dispatches to the group inside `try/except DataPlatformError`, logs structured context and returns 1 on catch, 0 on success
- `packages/dataplat/pyproject.toml` - `[project.scripts] dataplat = "dataplat.cli:main"`
- `docker/csv-processor/Dockerfile` - multi-stage build: `uv`/`python:3.12-slim-bookworm` builder, dependency-only layer, full workspace-member layer, non-root runtime stage with OCI labels and `ENTRYPOINT ["dataplat"]`
- `Makefile` - `image-csv-processor` target, tag always computed inline from `git rev-parse --short HEAD`
- `docs/README.md` - `docker/csv-processor/` row corrected from "Phase 4" to "Phase 3"
- `tests/unit/test_cli_error_handling.py` - 5 tests: `--version`, catch-once logging/exit-1, undeclared-exception propagation, zero-arg no-crash, `main()`'s own success return value
- `tests/policy/test_no_latest_image_tag.py` - 7 tests statically reading the Makefile recipe body (comment-stripped) for the git-SHA-twice / no-`:latest` / no-mutable-tag invariants, each paired with a non-vacuity check
- `tests/integration/test_docker_image.py` - `docker build` + `docker run --rm <image> --version` against the real Dockerfile, asserting a genuine (non-sentinel) version string, with `docker rmi` cleanup in a `finally` block
- `docker/csv-processor/.gitkeep` - removed (directory now holds real content)

## Decisions Made

- **`uv sync --all-packages`, not bare `--no-dev`, in the Dockerfile.** `dataplat`/`csv-processor` are workspace members listed only inside the root `pyproject.toml`'s `dev` dependency-group (by design, so a bare `uv sync` is sufficient for local dev per ROADMAP criterion 4). A Dockerfile using `--no-dev` alone therefore excludes the only group naming either package, so `uv sync --frozen --no-install-workspace --no-dev` installed almost nothing. `--all-packages` selects workspace members directly, bypassing the dependency-group indirection, so combined with `--no-dev` it installs exactly `dataplat` + `csv-processor` and their own `[project.dependencies]` -- never the dev-only tooling (pytest, ruff, mypy, alembic, sqlalchemy, boto3-stubs, pre-commit, import-linter).
- **`--no-editable` on the full-install layer.** uv installs local path/workspace dependencies editable by default (a pointer back to `packages/*/src`, not a copied package). The `runtime` stage copies only `/app/.venv` out of `builder`, never `/app/packages`, so an editable install left `dataplat` unimportable at runtime (`ModuleNotFoundError: No module named 'dataplat'`, observed directly via `docker run`). `--no-editable` installs the workspace members as real, self-contained packages under `.venv/site-packages`.
- **Integration test invokes `docker run --rm <image> --version`, not `... <image> dataplat --version`.** The Dockerfile's `ENTRYPOINT ["dataplat"]` (explicitly required by the plan's own `<interfaces>` section, sourced from `03-RESEARCH.md`'s verified code example) means passing "dataplat" again as a `docker run` argument is parsed as `dataplat dataplat --version` -- click resolves the second "dataplat" as an unknown subcommand and raises `NoSuchCommand`, observed directly. The plan's own `must_haves.truths` bullet literally reads `docker run <image>:<git-sha> dataplat --version`, copied verbatim from `ROADMAP.md`'s success criterion 3, which predates the (also plan-mandated) `ENTRYPOINT` design decision. Resolved in favor of the more rigorously-sourced `ENTRYPOINT` requirement; the integration test proves the actual observable intent (the image reports its version when run) via the invocation that is consistent with that entrypoint.
- **`DATAPLAT_LOG_JSON` env var chooses `main()`'s logging renderer**, defaulting to the human-readable console renderer (JSON is opt-in via an explicit truthy value), so a local `dataplat --version` stays pleasant to read while a future Kubernetes pod spec can opt into JSON.
- **OCI `.version` label reuses the git SHA** (same value as `.revision`): there is no semver release for this image yet, and INFRA-08 already mandates the image tag itself be the git SHA, so the version label matches that convention rather than inventing a second scheme.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `uv sync --no-dev` alone installed neither workspace member nor their dependencies in the Dockerfile**
- **Found during:** Task 2, manual verification (`docker run <image> dataplat --version` immediately after the first successful `docker build`)
- **Issue:** The verbatim Dockerfile shape from `03-RESEARCH.md`/`03-PATTERNS.md` uses `uv sync --frozen --no-install-workspace --no-dev` and `uv sync --locked --no-dev`. In this repo's actual root `pyproject.toml`, `dataplat`/`csv-processor` are listed only inside the `dev` dependency-group, so `--no-dev` excluded the only group naming either package -- the build "succeeded" but installed almost nothing (1 file's worth of bytecode compiled per layer, vs. the expected dozens of packages), and `docker run <image> dataplat --version` failed with `executable file not found in $PATH`.
- **Fix:** Added `--all-packages` to both `uv sync` invocations, which selects workspace members directly (bypassing the root's dependency-group indirection) and is compatible with `--no-dev` excluding dev-only tooling.
- **Files modified:** `docker/csv-processor/Dockerfile`
- **Verification:** `docker build` + `docker run --rm <image> --version` (after also fixing deviation 2 below) prints `dataplat, version 0.1.0`
- **Committed in:** `49d1b9d` (Task 2 commit)

**2. [Rule 1 - Bug] Default editable install left `dataplat` unimportable in the runtime stage**
- **Found during:** Task 2, manual verification, immediately after fixing deviation 1
- **Issue:** With `--all-packages` added, the build installed `dataplat`/`csv-processor` but as uv's default EDITABLE install (a path-based pointer to `/app/packages/*/src`). The `runtime` stage copies only `/app/.venv` from `builder`, never `/app/packages`, so the editable pointer resolved to nothing at runtime: `docker run <image> --version` raised `ModuleNotFoundError: No module named 'dataplat'`.
- **Fix:** Added `--no-editable` to the second (full) `uv sync` invocation, so workspace members install as real, self-contained packages under `.venv/site-packages` rather than editable pointers.
- **Files modified:** `docker/csv-processor/Dockerfile`
- **Verification:** `docker run --rm <image> --version` prints `dataplat, version 0.1.0`; `docker run --rm --entrypoint id <image>` prints `uid=1000(app) gid=1000(app) groups=1000(app)`
- **Committed in:** `49d1b9d` (Task 2 commit)

**3. [Rule 1 - Bug] Two of my own explanatory comments tripped the plan's own blunt substring/grep checks**
- **Found during:** Task 1 (`grep -c "except Exception" cli.py` returned 1, not 0) and Task 2 (`grep -A3 "^image-csv-processor:" Makefile | grep -q ":latest"` matched)
- **Issue:** `cli.py`'s module docstring quoted ARCHITECTURE.md's rule using the literal phrase "except Exception" as prose; the Makefile's `##` help text for `image-csv-processor` used the literal phrase "never :latest" as prose. Both acceptance-criteria checks are literal substring greps with no comment-awareness, so prose mentioning the forbidden string tripped them even though no executable code was affected.
- **Fix:** Reworded both comments to convey the same meaning without the literal substring (e.g. "bans a blanket catch-all clause", "never a mutable tag"). `tests/policy/test_no_latest_image_tag.py`'s own checks additionally strip comments before scanning (mirroring `test_supply_chain_guards.py`'s documented defense against this exact class of false positive), so the richer Python-level test is robust even where the plan's blunt one-line grep is not.
- **Files modified:** `packages/dataplat/src/dataplat/cli.py`, `tests/unit/test_cli_error_handling.py`, `Makefile`
- **Verification:** `grep -c "except Exception" packages/dataplat/src/dataplat/cli.py` returns 0; `grep -A3 "^image-csv-processor:" Makefile | grep -q ":latest"` fails to match
- **Committed in:** `ab8a115` (Task 1), `49d1b9d` (Task 2)

---

**Total deviations:** 3 auto-fixed (all Rule 1 -- bugs found and fixed during the plan's own verification steps, not scope creep)
**Impact on plan:** All three were necessary for the Dockerfile/CLI to actually work as the plan's own acceptance criteria require. No architectural changes, no new dependencies, no scope expansion beyond what Tasks 1-3 already specified.

## Issues Encountered

- **Plan-internal inconsistency between `must_haves.truths` and the `<interfaces>`-mandated `ENTRYPOINT`.** The plan's frontmatter asserts both "whose ENTRYPOINT is `dataplat`" and "`docker run <image>:<git-sha> dataplat --version` succeeds" as separate `must_haves.truths` bullets; given real Docker CLI semantics these cannot both be literally true (see Decisions Made above). Traced to `ROADMAP.md`'s success criterion 3 predating the ENTRYPOINT design. Resolved by keeping the ADR-0004-verified `ENTRYPOINT` and implementing/testing the invocation that is actually consistent with it (`docker run --rm <image> --version`), which achieves the criterion's real intent. Not filed as a Rule-1 "bug in my own code" deviation because the actual code (the Dockerfile) is correct as specified in `<interfaces>`; it is the plan's own prose that is imprecise.
- Docker daemon was already live this session (confirmed per `03-RESEARCH.md`'s own Environment Availability note) with a running kind cluster and local registry from Phase 2 -- neither interfered with plain `docker build`/`docker run` against the new Dockerfile.

## Known Stubs

None. This plan's deliverables (CLI entrypoint, Dockerfile, policy test, integration test) have no data-rendering or UI surface, and no placeholder/mock wiring was introduced.

## User Setup Required

None - no external service configuration required. `make image-csv-processor` and the new integration test both run entirely against the local Docker daemon already confirmed live this session.

## Next Phase Readiness

- The `csv-processor` image is now a real, provably-runnable artifact: Phase 4's `KubernetesPodOperator` work can reference `csv-processor:<git-sha>` with `ENTRYPOINT ["dataplat"]` as a known-good starting point, and its `ingest`/other future subcommands inherit the D-06 catch-once boundary and one-time logging configuration `main()` already sets up.
- `org.opencontainers.image.revision`/`.version` labels (populated from `ARG GIT_SHA`) are ready for Phase 4 to read back into `meta.ingestion_runs.processor_image_digest` once a real ingestion run exists -- this plan only builds the label, wiring it into that column is explicitly Phase 4's job (per the Dockerfile's own comments).
- No blockers. `docker/csv-processor/.gitkeep` removal and the `docs/README.md` Phase 4 -> Phase 3 correction keep the repository map consistent with what actually exists on disk after this plan.

---
*Phase: 03-dataplat-core-library-metadata-control-plane*
*Completed: 2026-08-13*

## Self-Check: PASSED

All claimed files verified present on disk:
- FOUND: packages/dataplat/src/dataplat/cli.py
- FOUND: tests/unit/test_cli_error_handling.py
- FOUND: docker/csv-processor/Dockerfile
- FOUND: tests/policy/test_no_latest_image_tag.py
- FOUND: tests/integration/test_docker_image.py
- FOUND: packages/dataplat/pyproject.toml
- FOUND: Makefile
- FOUND: docs/README.md

All claimed commit hashes verified present in `git log`:
- FOUND: ab8a115 (Task 1)
- FOUND: 49d1b9d (Task 2)
- FOUND: da73ff1 (Task 3)
- FOUND: cd65bf4 (post-hoc coverage test)

No missing items.
