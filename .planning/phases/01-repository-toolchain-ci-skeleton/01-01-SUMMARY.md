---
phase: 01-repository-toolchain-ci-skeleton
plan: 01
subsystem: toolchain
tags: [uv, workspace, ruff, mypy, import-linter, pytest, github-actions, quality-gate]
status: complete

requires: []
provides:
  - uv virtual workspace with two members (dataplat, csv-processor)
  - Makefile as the single definition of every quality gate
  - GitHub Actions workflow `CI` with the `Quality gate` job
  - dataplat.resolve_version() — the first typed, documented public API
  - tests/policy/ with the LOAD-12 COPY-CSV ban, live before any loader exists
affects:
  - every later plan in this phase and every commit in phases 2–11

tech-stack:
  added:
    - uv 0.12.3 (host tool, upgraded from 0.8.11)
    - ruff 0.16.2 (pinned with == — it is a gate)
    - mypy 2.3.0 (pinned with == — it is a gate)
    - pytest 9.1.1, pytest-cov 7.1.0, pytest-xdist 3.8.0, hypothesis 6.165.3
    - import-linter 2.13, pre-commit 4.6.2, PyYAML 6.0.3
    - hatchling (build backend for both members)
  patterns:
    - One gate definition, two callers — CI invokes only `make`
    - Architecture policy expressed as pytest, not as a CI grep step
    - src-layout preserved inside each workspace member under packages/

key-files:
  created:
    - pyproject.toml
    - uv.lock
    - setup.cfg
    - Makefile
    - .dockerignore
    - packages/dataplat/pyproject.toml
    - packages/dataplat/src/dataplat/__init__.py
    - packages/dataplat/src/dataplat/version.py
    - packages/dataplat/src/dataplat/py.typed
    - packages/csv-processor/pyproject.toml
    - packages/csv-processor/src/csv_processor/__init__.py
    - packages/csv-processor/src/csv_processor/py.typed
    - tests/conftest.py
    - tests/unit/test_dataplat_public_api.py
    - tests/unit/test_csv_processor_package.py
    - tests/policy/test_no_postgres_csv_parsing.py
    - .github/workflows/ci.yml
    - .github/dependabot.yml
    - .github/pull_request_template.md
    - docs/README.md
  modified:
    - .gitignore

decisions:
  - Both workspace members are listed in the root `dev` dependency group with
    `[tool.uv.sources] … workspace = true`, so bare `uv sync` installs both
    editable and ROADMAP success criterion 4 is true exactly as worded.
  - ruff is scoped away from `*.md` and `.planning/`; ruff 0.16 formats Python
    code blocks inside Markdown and would otherwise rewrite the 3,386-line
    README specification.
  - The test tree carries `__init__.py` files rather than relaxing INP001, so
    same-named test modules in different directories cannot collide.
  - `D417` is restated in `extend-select` even though `convention = "google"`
    already enables it, so changing the convention cannot silently drop it.
  - import-linter contract 2 (nothing imports the DAG folder) is deferred to
    Phase 4; a forbidden contract over an empty package passes for the wrong
    reason.

metrics:
  duration: ~35 min
  completed: 2026-08-11
  tasks: 3
  commits: 3

actuals:
  tokens: 24000
  tasks: 3
  commits: 3
---

# Phase 1 Plan 01: Repository, Toolchain & CI Skeleton — Tracer Summary

A uv workspace with two members whose quality gate is defined once in a Makefile
and called unchanged by both a developer and GitHub Actions, proven by observing
each of ruff, mypy, import-linter and pytest reject a deliberate violation.

## What was built

**Task 1 — one commit through the local gate** (`bf2e5da`)

A virtual uv workspace root (`package = false`) with two explicitly named
members — never a glob, so a future `packages/airflow-dags/` cannot join by
accident and drag Airflow's ~600 constraint pins into this lockfile. Both
members are listed in the root `dev` group and resolved through
`[tool.uv.sources] … { workspace = true }`, which is what makes bare `uv sync`
install both editable.

`Makefile` defines `help`, `uv-guard`, `install`, `lock-check`, `lint`,
`format`, `typecheck`, `imports`, `test`, `policy`, `fixtures`,
`fixtures-verify`, `gitleaks`, `gitleaks-selftest`, `check`, `ci` and `clean`.
`check` chains `uv-guard lock-check lint format typecheck imports policy test`.
The `fixtures*` and `gitleaks*` targets are written now and implemented by plans
01-02 and 01-03; the gate is defined once, not accreted.

First real code: `dataplat.resolve_version()`, fully annotated with a Google
docstring, reading the installed distribution metadata so provenance never
depends on a literal that can drift from `pyproject.toml`.

**Task 2 — the repository skeleton** (`40e2341`)

Sixteen directories with `.gitkeep` markers, plus `docs/README.md` naming the
owning phase for each one and recording the three departures from README §75.

**Task 3 — CI calls the gate** (`10add78`)

Workflow `CI` with the `Quality gate` job on `pull_request` and `push` to main.
Both action references are 40-character commit SHAs with trailing version
comments. The job's only `run:` steps are `make install` and `make check`.

## Verification performed

The four gates were each observed rejecting a deliberate violation, and the tree
was restored to green after every probe:

| Violation | Gate | Observed |
|---|---|---|
| `print("x")` appended to `version.py` | `make lint` | `T201 print found`, exit 2 |
| Docstring removed from the public function | `make lint` | `D103 Missing docstring in public function`, exit 2 |
| Return + parameter annotations removed | `make typecheck` | `error: Function is missing a type annotation [no-untyped-def]`, exit 2 |
| `import csv_processor` added to a dataplat module | `make imports` | contract BROKEN, `dataplat.version -> csv_processor (l.40)`, exit 2 |
| `COPY … FORMAT csv` in a `.sql` file | `make policy` | LOAD-12 assertion naming `migrations/_probe_violation.sql:1`, exit 2 |
| `UV` pointed at a stale 0.8.11 shim | `make uv-guard` | `ERROR: uv 0.12.3 is required; found '0.8.11'`, exit 2 |

Clone-equivalence proof: the tree was copied to a scratch directory with no
`.git` and no pre-existing virtual environment, `uv sync` (bare, no
`--all-packages`) was run, and both `dataplat` and `csv_processor` imported
successfully; `make check` was then green in that copy. ROADMAP success
criterion 4 is therefore true as written, and no amendment to its wording was
needed.

Offline predicate: `UV_OFFLINE=1 make check` passes after one sync, confirming
`check` needs no network service.

Config assertions: `grep -c CPY001 pyproject.toml` = 1; `grep -Ec '^\s*strict\s*=\s*false' pyproject.toml` = 0;
`grep -c apache-airflow uv.lock` = 0; `T20` appears zero times in the top-level
ruff ignore list and only in the `scripts/**` and `tools/corpus/__main__.py`
per-file carve-outs; `grep -Ec 'secrets\.' .github/workflows/ci.yml` = 0;
`git ls-files tests/fixtures/csv` is empty.

Both `pytest tests/unit` and `pytest tests/policy` collect and pass (5 and 2
tests); neither exits 5.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 3 — Blocking] ruff INP001 rejected the test tree**

- **Found during:** Task 1, first `make check`.
- **Issue:** `select = ["ALL"]` fires `INP001` on `tests/unit/` and
  `tests/policy/`, which the research config's `tests/**` per-file-ignores
  (`S101`, `PLR2004`, `ANN`, `D`) does not cover. The gate could not go green.
- **Fix:** added `__init__.py` to `tests/`, `tests/unit/` and `tests/policy/`
  rather than adding `INP001` to the ignore list. This keeps the rule enforced,
  makes the mypy `module = ["tests.*"]` override name real modules, and removes
  the basename-collision hazard pytest's default import mode has for
  identically-named test files in different directories. A future plan adding a
  test directory gets a readable `INP001` telling it exactly what to add.
- **Files:** `tests/__init__.py`, `tests/unit/__init__.py`, `tests/policy/__init__.py`
- **Commit:** `bf2e5da`

**2. [Rule 1 — Bug] `ruff format --check` would have rewritten the README spec**

- **Found during:** Task 1, `make format`.
- **Issue:** ruff 0.16.2 formats Python code blocks embedded in Markdown. Five
  files, including `README.md` — this project's 3,386-line specification, which
  the research structure explicitly marks "untouched" — were reported as "would
  be reformatted". A gate that rewrites the specification it is measured against
  is a bug, not a style preference.
- **Fix:** `extend-exclude = [..., "*.md", ".planning"]` in `[tool.ruff]`, with a
  comment stating why.
- **Files:** `pyproject.toml`
- **Commit:** `bf2e5da`

**3. [Rule 1 — Bug] `uv-guard` produced an unreadable message**

- **Found during:** Task 1 acceptance check with `UV=/bin/false`.
- **Issue:** coreutils `false --version` prints multi-line version text and exits
  0, so `awk '{print $2}'` over the whole output produced garbage inside the
  error message. The guard exited non-zero correctly, but its message — the
  entire point of the target — was noise.
- **Fix:** `head -n1` before `awk`. Re-verified against a shim printing
  `uv 0.8.11`, which now reports `found '0.8.11'`.
- **Files:** `Makefile`
- **Commit:** `bf2e5da`

**4. [Rule 2 — Missing critical functionality] generated files left untracked**

- **Found during:** Task 1, after the first `make test` and `make imports`.
- **Issue:** `.coverage` and `.import_linter_cache/` are generated on every gate
  run and were not ignored, so `git status` would never be clean and a future
  commit could sweep them in.
- **Fix:** added `__pycache__/`, `.coverage`, `.coverage.*` and
  `.import_linter_cache/` to `.gitignore`.
- **Files:** `.gitignore`
- **Commits:** `bf2e5da`, `40e2341`

**5. [Rule 2 — Missing critical functionality] `D417` pinned explicitly**

- **Found during:** Task 1, resolving the plan's "confirm the google convention
  leaves D417 enabled" instruction.
- **Issue:** D417 *is* enabled under `convention = "google"` (verified by probe),
  so the plan's conditional `extend-select` was not strictly required. But the
  rule's presence then depends entirely on one convention string; changing it to
  `pep257` would silently delete QUAL-02's parameter half with no signal.
- **Fix:** `extend-select = ["D417"]` with a comment recording both the
  verification and the limitation below.
- **Files:** `pyproject.toml`
- **Commit:** `bf2e5da`

**6. [Rule 3 — Blocking] `make typecheck` referenced a path that does not exist**

- **Found during:** Task 1, writing the Makefile.
- **Issue:** the research `typecheck` target names `tools`, which plan 01-03
  creates. Hard-coding it would fail the gate for two plans.
- **Fix:** `TYPECHECK_PATHS := packages/dataplat/src packages/csv-processor/src $(wildcard tools)`
  — self-healing when `tools/` lands, no edit required.
- **Files:** `Makefile`
- **Commit:** `bf2e5da`

**7. [Addition] `tests/unit/test_csv_processor_package.py`**

Not in the plan's file list. `make test` emitted a `CoverageWarning: Module
csv_processor was never imported` on every run because the second member had no
test exercising it. A warning printed on every gate run is a broken window;
asserting the member is importable and exports nothing is the honest fix and
keeps it inside the coverage report from the first commit.

### Verified corrections to the research

- **D417's real scope.** Probed both cases: a docstring with an incomplete
  `Args:` section is flagged; a docstring with **no** `Args:` section at all is
  **not**. So "every parameter is documented" is only partly mechanical, which
  matches 01-RESEARCH.md's assessment of QUAL-02 as partial — but the boundary is
  narrower than the phrase "D417 partially covers parameters" suggests, and is
  now recorded in `pyproject.toml` next to the rule.
- **`uv sync` on a virtual root.** The plan required proving rather than
  assuming that a bare sync installs both members. It does, given the root `dev`
  group + `[tool.uv.sources]` arrangement. No ROADMAP amendment was needed.

## Authentication gates

None. This plan touches no authenticated service.

## Known stubs

The following Makefile targets are written but not yet implemented, by design —
the Makefile is the gate's single definition and is written once:

| Target | File | Resolved by |
|---|---|---|
| `fixtures`, `fixtures-verify` | `Makefile:54-61` | Plan 01-03 (corpus generator) |
| `gitleaks`, `gitleaks-selftest` | `Makefile:63-68` | Plan 01-02 (secret scanning) |

None of these is reachable from `make check` in this plan, so the gate never
invokes an unimplemented target. `make ci` does chain `gitleaks` and
`gitleaks-selftest` and will fail until plan 01-02 lands the binary download and
the self-test module; this is the plan's stated sequencing, not an accident.

`import-linter` contract 2 is deliberately absent until Phase 4 — recorded as a
comment in `setup.cfg` rather than added as a vacuous passing contract.

## Threat flags

None. This plan introduces no network endpoint, auth path, file-access pattern
or schema at a trust boundary. Every mitigation the plan's threat register
assigned to this plan was applied and asserted: T-01-01 (SHA-pinned actions),
T-01-02 (`pull_request` trigger + `contents: read`), T-01-03 (`==` pins for the
two gate tools, committed `uv.lock`, `uv lock --check` inside `make check`,
Dependabot), T-01-04 (Makefile is the only gate definition; CI calls only
`make`), T-01-05 (zero `secrets.*` references).

## Requirements addressed

| ID | Mechanism |
|---|---|
| QUAL-01 | mypy strict over both member `src` trees; observed rejecting an untyped public def |
| QUAL-02 | ruff `D` with `convention = "google"` + explicit `D417`; observed rejecting a missing docstring. Assumptions, exceptions and side effects remain review-time (PR template) |
| QUAL-07 | **Review-time half only** — the PR template's `tests/regression/` checkbox. The mechanical half (the regression tree and its provenance-enforcing collection hook) is plan 01-04's |
| OBS-03 | ruff `T20`, never in the top-level ignore list, relaxed only for `scripts/**` and `tools/corpus/__main__.py`; observed rejecting `print()` |
| CICD-01 | `.github/workflows/ci.yml` triggers on `pull_request` and `push` to main |
| CICD-02 | The `check` job's only substantive step is `make check`, so the PR gate is structurally the full local gate |
| CICD-03 | `make lint` inside `make check`; observed failing the build |
| CICD-04 | `make typecheck` inside `make check`; observed failing the build |

The LOAD-12 policy test landed in this plan at the ROADMAP's explicit
instruction. It is an architecture ban, not part of QUAL-07, and is not claimed
as such.

## Self-Check: PASSED

All 20 created files verified present on disk. All three commits verified in
`git log`. Working tree clean at `10add78`; `make check` green; no file
deletions in any commit.
