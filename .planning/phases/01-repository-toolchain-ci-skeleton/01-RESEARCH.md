# Phase 1: Repository, Toolchain & CI Skeleton - Research

**Researched:** 2026-08-11
**Domain:** Python monorepo packaging (uv workspace), static-analysis gating (ruff/mypy), deterministic test-fixture generation, GitHub Actions CI, secret scanning
**Confidence:** HIGH

## Summary

This phase has no unsolved technical problems — every tool is already pinned by `.planning/research/STACK.md`. The research value is therefore almost entirely in **mechanism**: exactly which knob makes each requirement fail a build, and which requirements only *look* mechanically checkable. Four findings materially change what the planner should write.

**First, three of STACK.md's own recommendations are wrong or incomplete and were caught by running them.** `strict = false` inside a `[[tool.mypy.overrides]]` block is **silently ignored** by mypy 2.3.0 — no warning, no error, the module stays fully strict [VERIFIED: executed `mypy==2.3.0` with both forms, see Code Examples]. `select = ["ALL"]` in ruff 0.16.2 enables `CPY001` (missing copyright notice), which fires on *every file in the repo* and is absent from STACK.md's ignore list [VERIFIED: executed `ruff==0.16.2`]. And gitleaks' default ruleset deliberately **does not flag** the documented AWS example key `AKIAIOSFODNN7EXAMPLE`, so the "commit a fake credential and watch CI fail" negative test silently passes-as-clean if it uses that value [VERIFIED: executed `gitleaks 8.30.1`].

**Second, byte-identical fixture generation is a solved problem but not by the obvious route.** Python's `random` docs guarantee reproducibility across versions for `Random.random()` *only*; `shuffle`, `sample`, `choice`, `randrange` and `randint` are explicitly "subject to change across Python versions" [CITED: docs.python.org/3/library/random.html]. A generator built on `choice`/`shuffle` is reproducible today and silently drifts on a Python upgrade — which is exactly the failure mode this corpus exists to prevent. Separately, `gzip.GzipFile` embeds the current wall-clock mtime by default, producing different bytes on every run [VERIFIED: two runs 1.1 s apart produced different SHA-256]. A prototype generator obeying ten explicit determinism rules produced identical digests across processes, `PYTHONHASHSEED` values, timezones and locales [VERIFIED: prototype run 3×].

**Third, the large-file fixture needs no caching at all.** Generation runs at ~67 MB/s and SHA-256 at ~2.4 GB/s on this machine, so a 241 MB fixture costs roughly 4 seconds to build and 0.1 s to verify [VERIFIED: measured]. A cache would introduce a staleness hole in the one artifact whose entire purpose is reproducibility. Better still, the bounded-memory assertion does not need a file larger than the *machine's* RAM — `resource.setrlimit(RLIMIT_AS, 128 MB)` in a subprocess makes the streaming reader pass and the buffering reader die with `MemoryError` on a 241 MB file [VERIFIED: executed]. That converts LOAD-07 from an E2E-only claim into a unit test.

**Fourth, two of the twelve requirements are not honestly mechanically verifiable and the plan should say so rather than invent a check.** SEC-10 ("no CI job echoes a secret value") is decidable only in its Phase-1 form — this phase references no secret other than `GITHUB_TOKEN`, which *is* greppable — but becomes undecidable the moment a job interpolates a secret into a shell command. QUAL-02 (docstrings) is mechanically checkable for *presence* (ruff `D101`/`D102`/`D103`, which fire only on public names — verified) but not for the *content* README §69 demands (purpose, parameters, returns, assumptions, exceptions, side effects). Both belong in `## Validation Architecture` labelled as partial.

**Primary recommendation:** a uv **workspace** with two members (`packages/dataplat`, `packages/csv-processor`) under a virtual root, one `uv.lock`, one venv; a single `Makefile` that is the *only* definition of the quality gate, with CI invoking `make ci` and a policy test asserting CI invokes nothing else; ruff `ALL` minus a nine-rule ignore list; mypy `strict` with per-module flags enumerated individually (never `strict = false`); a manifest-driven fixture generator whose entire output is gitignored and verified against a committed `CORPUS.sha256`; and gitleaks run as a checksum-verified binary with `--redact`, a path-scoped `SYNTH_` allowlist, and a self-test that proves the scanner is live without ever putting a credential in real history.

## Architectural Responsibility Map

This phase is a build-and-verify toolchain; the "tiers" are toolchain layers, not application tiers. No browser, server, API, CDN or database tier is touched — consistent with locked decision 2 (no UI) and locked decision 3 (no cluster, no credentials, no network services).

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Dependency resolution & venv creation | Build tooling (uv workspace) | — | `uv.lock` is the single reproducibility root; nothing else may resolve dependencies |
| Package boundary enforcement (`dataplat` ⊁ `csv_processor`) | Static analysis (import-linter) | Packaging (`pyproject` dependency lists) | Packaging states the direction; the linter proves no code violates it |
| Style / `print` ban / docstring presence | Static analysis (ruff) | — | Fast, single-pass, no imports needed |
| Type-completeness of public APIs | Static analysis (mypy) | — | ruff `ANN` catches missing annotations syntactically; mypy catches semantically-untyped defs |
| Fixture corpus materialisation | Build tooling (`make fixtures`) | Repo policy (`.gitignore`) | Generated artifacts must be un-committable by construction, not by discipline |
| Fixture byte-identity proof | Test suite (`make fixtures-verify`) | Committed `CORPUS.sha256` | The digest manifest is the oracle; the check is a pure comparison |
| Architecture-policy rules (`COPY … FORMAT csv`, CI/Make parity) | Test suite (`tests/policy/`) | — | Living in pytest guarantees local and CI run the identical rule from one definition |
| Secret detection | CI job (gitleaks binary) | Pre-commit hook | CI is the gate; pre-commit is the courtesy that avoids history rewrites |
| Gate orchestration | `Makefile` | GitHub Actions workflow | One definition, two callers — the only structure in which drift is impossible |

## Project Constraints (from CLAUDE.md)

Directives from `./.claude/CLAUDE.md` that bind this phase. The planner must not produce tasks that contradict these.

| # | Directive | Phase-1 consequence |
|---|-----------|--------------------|
| C1 | Repo must stay on WSL ext4; never hostPath-mount `dags/` from `/mnt/c` | Repo is already at `/home/user/projects/airflow-platform` (ext4). No task may relocate it. |
| C2 | Business logic lives in the ETL library; DAGs orchestrate and delegate | The `airflow/dags/` directory may be created empty/skeletal, but no logic lands there. Enforce with an import-linter contract now. |
| C3 | No credential in Git, Python source, Dockerfiles, K8s manifests, Airflow Variables or CI workflow files | Phase 1 *is* the enforcement of this. Workflow files must reference no `secrets.*` other than `GITHUB_TOKEN`. |
| C4 | No secrets in fixtures — the corpus is synthetic by construction and safe to commit | Justifies a **path-scoped** gitleaks allowlist for `tests/fixtures/**`, never a global pattern allowlist. |
| C5 | Determinism: same source + config + processor version ⇒ same logical result; unavoidable non-determinism must be documented | Directly binds the fixture generator. Every determinism rule below is a C5 obligation. |
| C6 | CI runners are 4 CPU / 16 GB; Helm values must be profile-parameterized from the start | Phase 1 does not install Helm charts, but must not create a CI job design that assumes a large runner. |
| C7 | Two images, two dependency sets — do **not** install `csv_processor` into the Airflow image | The Airflow image must **not** be a uv workspace member. Its dependencies resolve via Airflow's constraints file, separately. |
| C8 | Python 3.12 for both images; `uv` (not Poetry) | `requires-python = ">=3.12,<3.13"` on every member. `uv` must be upgraded from the installed 0.8.11 to 0.12.3. |
| C9 | GSD workflow enforcement — no direct repo edits outside a GSD workflow | Applies to execution, not to this research. |
| C10 | Avoid `:latest`; version by git SHA | Applies to images (Phase 11), but the *convention* should be recorded in an ADR now. |

<phase_requirements>
## Phase Requirements

| ID | Description (from REQUIREMENTS.md) | Research Support |
|----|-------------------------------------|------------------|
| QUAL-01 | Type hints used consistently across arguments, returns, classes, public APIs, configuration and data models, verified by mypy in CI — *(DoD 71)* | mypy 2.3.0 `strict` (enables `disallow_untyped_defs` + `disallow_incomplete_defs`) plus ruff `ANN`. Verified flag list and the `strict = false` override trap below. |
| QUAL-02 | Public classes, functions and methods carry docstrings describing purpose, parameters, returns, assumptions, exceptions and side effects — *(DoD 72)* | ruff `D` with `convention = "google"`; verified that `D101/D102/D103` fire only on public (non-underscore) names. **Presence is checkable; content is not** — see Validation Architecture. |
| QUAL-07 | Every important discovered bug gains a permanent regression test — *(DoD 81)* | `tests/regression/` created empty with a `README.md` stating the policy, plus a `conftest.py` requiring a `# BUG:` provenance marker. Process requirement; the mechanical part is the directory + PR template. |
| QUAL-08 | A CSV edge-case fixture corpus exists, generated from a seed rather than committed en masse, and grows as cases are discovered — *(DoD 82)* | The bulk of this document. Manifest schema, ten determinism rules, digest verification, prototype verified byte-identical 3×. |
| CICD-01 | GitHub Actions provides CI/CD — *(DoD 103)* | `.github/workflows/ci.yml` skeleton with `permissions`, `concurrency`, SHA-pinned actions. |
| CICD-02 | Pull requests automatically run the full quality gate — *(DoD 104)* | `on: pull_request`; the `check` job runs `make ci` — one target, no drift. |
| CICD-03 | Linting runs automatically via ruff — *(DoD 105)* | `ruff check` + `ruff format --check`, pinned 0.16.2, invoked from `make lint`. |
| CICD-04 | Type checking runs automatically via mypy — *(DoD 106)* | `mypy` 2.3.0 from `make typecheck`; corrected per-module override syntax. |
| SEC-02 | No secret exists anywhere in git history or the working tree — *(DoD 91)* | `gitleaks git --log-opts="--all"` (full history) + `gitleaks dir` (working tree). Verified detection and exit code 1. |
| SEC-10 | CI/CD holds no unnecessary long-lived credentials, and secrets are never printed during CI execution — *(DoD 99)* | `permissions: contents: read` default; zero `secrets.*` besides `GITHUB_TOKEN`; `gitleaks --redact` verified to mask the finding. **Partially mechanical** — see Validation Architecture. |
| SEC-11 | Automated secret scanning runs in CI and fails the build on a detected credential — *(DoD 100)* | gitleaks binary in a `run:` step (avoids the org-repo licence trap). Self-test target proves the scanner is live. Verified end to end. |
| OBS-03 | No `print()` is used for operational logging — enforced in CI — *(DoD 75)* | ruff `T20` (`T201`/`T203`). Verified it fires in library code and is scoped away from `scripts/` via `per-file-ignores`. |
</phase_requirements>

## Standard Stack

All versions are **cited from `.planning/research/STACK.md`**, not re-derived, per the quality gate. Where this session independently confirmed a version against PyPI or a GitHub release, that is noted.

### Core

| Library / tool | Version | Purpose | Why standard |
|---|---|---|---|
| uv | `0.12.3` | Workspace, lockfile, venv, tool runner | [CITED: STACK.md §F "Packaging"] — 10–100× Poetry, cross-platform universal lock, native constraints support. **Installed is 0.8.11; STACK.md explicitly says upgrade in Phase 1** [VERIFIED: `uv --version` → 0.8.11] |
| CPython | `3.12` | Both images | [CITED: STACK.md §F] — default Python for `apache/airflow:3.3.0`. Local is 3.12.3 [VERIFIED: `python3 --version`] |
| hatchling | latest | Build backend for both members | [CITED: STACK.md §F example `pyproject.toml`]. `uv_build` is the alternative [CITED: uv workspace docs via Context7] — see Alternatives |
| ruff | `0.16.2` | Lint + format | [CITED: STACK.md §I]. [VERIFIED: PyPI `ruff` → `0.16.2`; executed this session] |
| mypy | `2.3.0` | Type checking | [CITED: STACK.md §I]. [VERIFIED: PyPI `mypy` → `2.3.0`; executed this session] |
| pytest | `9.1.1` | Test runner | [CITED: STACK.md §G]. [VERIFIED: PyPI `pytest` → `9.1.1`; resolved and installed cleanly under Python 3.12 in a uv workspace] |
| gitleaks | `8.30.1` | Secret scanning (binary, **not** the Action) | [CITED: STACK.md §I]. [VERIFIED: GitHub release `v8.30.1` published 2026-03-21; downloaded, checksum-verified and executed this session] |
| GNU Make | 4.3 (system) | Single definition of the quality gate | [VERIFIED: `make --version` → GNU Make 4.3] |

### Supporting

| Library | Version | Purpose | When to use |
|---|---|---|---|
| `pytest-cov` | `7.1.0` | Coverage | [CITED: STACK.md §G]. [VERIFIED: PyPI → `7.1.0`] Wire it now, set no threshold yet — CICD-05 is Phase 11 |
| `pytest-xdist` | `3.8.0` | `-n auto` parallel unit tests | [CITED: STACK.md §G]. **Do not** apply to testcontainers tests (Phase 4+) |
| `hypothesis` | `6.165.3` | Property tests | [CITED: STACK.md §G]. [VERIFIED: PyPI → `6.165.3`] Install now; first properties land Phase 6 |
| `import-linter` | `2.13` | Enforce `dataplat` ⊁ `csv_processor` and `dags` as a leaf | [CITED: ARCHITECTURE.md "Structure rationale" recommends an import-linter contract]. [VERIFIED: installed 2.13, contract broke correctly, exit 1 on violation / 0 when clean] |
| `pre-commit` | `4.6.2` | Local hooks: ruff, ruff-format, gitleaks, `check-added-large-files`, `detect-private-key` | [CITED: STACK.md §I "Also add a pre-commit hook set"]. [VERIFIED: PyPI → `4.6.2`] |
| `syrupy` | `5.5.3` | Snapshot-assert JSON report shapes | [CITED: STACK.md §G]. [VERIFIED: PyPI → `5.5.3`] Optional in Phase 1 — first consumer is Phase 8 |
| `time-machine` | `3.4.0` | Freeze time for determinism tests | [CITED: STACK.md §G]. [VERIFIED: PyPI → `3.4.0`] Optional in Phase 1 |
| `PyYAML` | `>=6` | Read the fixture manifest | [CITED: STACK.md §F dependency list]. Already a `dataplat` runtime dep |

### Alternatives Considered

| Instead of | Could use | Trade-off |
|---|---|---|
| uv **workspace** (2 members) | Single `pyproject.toml`, one distribution, two top-level packages in `src/` | Cheaper (one file) and satisfies the *import-path* argument in ARCHITECTURE.md §4.1. But `dataplat` then cannot be installed without `csv_processor`, and the dependency direction is enforced only by import-linter rather than by packaging. **Recommend the workspace** — the cost is one extra `pyproject.toml`. |
| `hatchling` | `uv_build` (`uv_build>=0.12.3,<0.13`) | [CITED: uv workspace docs, Context7]. Faster and one fewer tool, but younger and less widely exercised. `hatchling` is what STACK.md's example uses. **Recommend hatchling**; revisit if build time becomes visible (it will not). |
| `Makefile` | `just`, or `uv run` scripts / `[project.scripts]` | `just` is not installed [VERIFIED: `command -v just` → absent] and adds a bootstrap step to "clone and run". `uv run` script entries cannot easily express dependencies between gates. **Recommend Make** — present on every runner and on this machine. |
| gitleaks binary | `gitleaks/gitleaks-action@v3` | Action requires a paid `GITLEAKS_LICENSE` for organisation-owned repos [CITED: STACK.md §I]. **Binary, checksum-verified** — no licence, no rate limit, exact pin. |
| gitleaks | trufflehog `3.96.0` | Complements rather than replaces: verifies candidates against live APIs, needs egress, slower [CITED: STACK.md §I]. **Schedule it; do not gate PRs on it** — and note it violates locked decision 3 (no network services) if run in the PR path. |
| ruff `D` | `interrogate`, `pydoclint` | ruff already runs; a second docstring tool is pure overhead. `pydoclint` *does* check Args/Returns sections against the signature, which is closer to QUAL-02's intent — flagged as an Open Question. |
| import-linter | `tach`, or a hand-rolled AST test | import-linter is purpose-built, has a declarative contract file, and was verified working. No reason to hand-roll. |

**Installation:**

```bash
# Host tooling — upgrade uv from the installed 0.8.11 (STACK.md §F)
curl -LsSf https://astral.sh/uv/0.12.3/install.sh | sh

# Everything else comes from the lockfile
uv sync --all-packages
```

`ruff`, `mypy` and `gitleaks` are pinned twice on purpose: as `[dependency-groups] dev` entries (so `make check` uses the locked versions locally) and as explicit versions in the workflow (so a lockfile regression cannot silently weaken the gate). The policy test described below asserts the two agree.

## Package Legitimacy Audit

Ecosystem: **PyPI**. The seam `gsd-tools query package-legitimacy check --ecosystem pypi …` returned `SUS` for every package with reasons `unknown-downloads` and `no-repository`. **This is an artifact of the PyPI JSON API, not a risk signal** — PyPI does not expose weekly download counts through `/pypi/<pkg>/json`, and several projects publish `project_urls` under keys the seam does not read. Each package was therefore verified independently against the PyPI JSON API and, for the two that gate the build, by executing it.

| Package | Registry | Version verified | Source repo | Seam verdict | Independent evidence | Disposition |
|---|---|---|---|---|---|---|
| `ruff` | PyPI | `0.16.2` | github.com/astral-sh/ruff | — | [VERIFIED: **executed** `ruff==0.16.2` via `uv tool run`; produced expected diagnostics] | Approved |
| `mypy` | PyPI | `2.3.0` | github.com/python/mypy | — | [VERIFIED: **executed** `mypy==2.3.0`; `--help` strict-flag list captured] | Approved |
| `pytest` | PyPI | `9.1.1` | github.com/pytest-dev/pytest | — | [VERIFIED: resolved + installed in the prototype workspace] | Approved |
| `import-linter` | PyPI | `2.13` | none in `project_urls` | SUS (`unknown-downloads`, `no-repository`) | [VERIFIED: **executed** `lint-imports` 2.13; correct verdicts and exit codes] | Approved — `no-repository` is a metadata gap; upstream is `github.com/seddonym/import-linter` [ASSUMED] |
| `pre-commit` | PyPI | `4.6.2` | github.com/pre-commit/pre-commit | SUS (`unknown-downloads`) | [VERIFIED: PyPI `project_urls` carries the repo] | Approved |
| `hypothesis` | PyPI | `6.165.3` | none in `project_urls` | SUS (`unknown-downloads`, `no-repository`) | Version matches STACK.md's independently verified figure | Approved |
| `pytest-cov` | PyPI | `7.1.0` | none in `project_urls` | SUS (`unknown-downloads`, `no-repository`) | Version matches STACK.md | Approved |
| `pytest-xdist` | PyPI | `3.8.0` (STACK.md) | github.com/pytest-dev/pytest-xdist | SUS (`unknown-downloads`) | Repo present | Approved |
| `syrupy` | PyPI | `5.5.3` | none in `project_urls` | SUS (`unknown-downloads`, `no-repository`) | Version matches STACK.md's "current" | Approved (optional in Phase 1) |
| `time-machine` | PyPI | `3.4.0` | github.com/adamchainz/time-machine | SUS (`too-new`, …) | `too-new` = released 2026-08-10, i.e. a fresh release of a long-established project | Approved (optional in Phase 1) |
| `PyYAML` | PyPI | `>=6` | — | — | Already in STACK.md's `dataplat` dependency list | Approved |
| `gitleaks` | GitHub release (not PyPI) | `8.30.1` | github.com/gitleaks/gitleaks | n/a | [VERIFIED: release API; **downloaded, SHA-256 verified against `gitleaks_8.30.1_checksums.txt`, executed**] | Approved |

**Packages removed due to `[SLOP]` verdict:** none.
**Packages flagged `[SUS]` requiring a `checkpoint:human-verify`:** none. Every `SUS` verdict traces to a PyPI metadata limitation, and the two packages that actually gate the build (`ruff`, `mypy`) plus `import-linter` and `gitleaks` were executed in this session.

**Supply-chain hardening the planner should include regardless:**
- The gitleaks download step must verify the published SHA-256 checksum, not just `curl | tar`. [VERIFIED: the checksum file exists and matched this session.]
- GitHub Actions must be pinned by commit SHA, not tag — a tag is mutable. Resolved this session: `actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1  # v7.0.1` and `astral-sh/setup-uv@c771a70e6277c0a99b617c7a806ffedaca235ff9  # v9.0.0` [VERIFIED: GitHub `git/ref/tags` API].
- Enable Dependabot for `uv.lock` and `github-actions` [CITED: STACK.md §I].

## Architecture Patterns

### System Architecture Diagram

```
                    ┌──────────────────────────────────────────┐
   developer ──────►│  git clone && uv sync --all-packages     │
   clean checkout   └────────────────┬─────────────────────────┘
                                     │ resolves uv.lock (single, universal)
                                     ▼
                        ┌────────────────────────┐
                        │   .venv (one, shared)  │
                        │  dataplat -e           │
                        │  csv_processor -e      │
                        │  dev group             │
                        └────────┬───────────────┘
                                 │
   ┌─────────────────────────────┴──────────────────────────────┐
   │                    make check   (THE gate)                 │
   └──┬────────┬─────────┬──────────┬──────────┬────────────┬───┘
      │        │         │          │          │            │
      ▼        ▼         ▼          ▼          ▼            ▼
   ruff     ruff      mypy      lint-      pytest       pytest
   check    format   strict     imports    unit         policy
   (T20,    --check  (untyped   (dataplat  (tests/      (COPY-csv ban,
    D, ANN,          public     ⊁ csv_     unit/)       CI/Make parity,
    S, BLE)          defs)      processor;              tool-version
                                dags leaf)              agreement)
                                                             │
   ┌─────────────────────────────────────────────────────────┘
   │
   ▼
 make fixtures-verify
   │
   │  corpus.yaml (committed, ~70 entries)  ─┐
   │  MASTER_SEED (committed)               ─┤
   │                                         ▼
   │                            ┌────────────────────────┐
   │                            │  generator (pure fn)   │
   │                            │  seed⊕name → RNG       │
   │                            │  str → .encode(enc)    │
   │                            │  → bytes               │
   │                            └────────┬───────────────┘
   │                                     │
   │                    ┌────────────────┴──────────────┐
   │                    ▼                               ▼
   │        tests/fixtures/csv/**            sha256 per fixture
   │        (GITIGNORED — never committed)             │
   │                                                    ▼
   │                                       compare to CORPUS.sha256
   └──────────────────────────────────────► (committed, ~5 KB)
                                             mismatch ⇒ exit 1


   ┌────────────────────── GitHub Actions ───────────────────────┐
   │ on: pull_request, push(main)                                │
   │ permissions: contents: read     concurrency: cancel-in-prog │
   │                                                             │
   │  job: check          job: secrets         job: selftest     │
   │  fetch-depth: 1      fetch-depth: 0       fetch-depth: 1    │
   │  └─ make ci          └─ gitleaks git      └─ make gitleaks- │
   │     (== make check)     --log-opts=--all     selftest       │
   │                         --redact             (temp repo w/  │
   │                         --exit-code 1         canary creds; │
   │                                               asserts rc=1) │
   └─────────────────────────────────────────────────────────────┘
```

The load-bearing property of this diagram: **`make check` is the only node with fan-out.** CI has no gate of its own; it calls `make ci`. That is what makes local/CI drift structurally impossible rather than a discipline.

### Recommended Project Structure

Reconciling README §75 (which expects `airflow/`, `csv_processor/`, `schemas/`, `configs/`, `tests/`, `docker/`, `kubernetes/`, `helm/`, `scripts/`, `docs/`, `.github/`), ARCHITECTURE.md's "Recommended Repository Structure" (which moves the packages under `src/` and adds `migrations/`, `docs/adr/`, `helm/values/{local,ci}/`), and uv's workspace requirement that **each member owns a directory containing its own `pyproject.toml`**:

```
airflow-platform/
├── pyproject.toml              # VIRTUAL workspace root: [tool.uv] package = false
├── uv.lock                     # single universal lock for both members + dev group
├── Makefile                    # THE definition of every gate
├── ruff.toml                   # or [tool.ruff] in root pyproject — see note
├── .gitleaks.toml              # path-scoped allowlists only
├── .pre-commit-config.yaml
├── .gitignore                  # includes tests/fixtures/csv/
├── .dockerignore               # tests/, .planning/, .git/, docs/  (PITFALLS G3)
├── setup.cfg                   # [importlinter] contracts (its native config home)
├── README.md                   # the 3,386-line spec — untouched
│
├── packages/
│   ├── dataplat/
│   │   ├── pyproject.toml      # name="dataplat"; NO dep on csv-processor
│   │   └── src/dataplat/
│   │       └── __init__.py     # Phase 1 ships the package marker + py.typed only
│   └── csv-processor/
│       ├── pyproject.toml      # name="csv-processor"; deps=["dataplat"] via workspace
│       └── src/csv_processor/
│           └── __init__.py
│
├── airflow/
│   ├── dags/.gitkeep           # leaf; import-linter forbids anything importing it
│   └── config/.gitkeep
│
├── tests/
│   ├── conftest.py
│   ├── unit/                   # pure, no I/O
│   ├── integration/.gitkeep    # Phase 4 (testcontainers)
│   ├── e2e/.gitkeep            # Phase 11
│   ├── property/.gitkeep       # Phase 6 (hypothesis)
│   ├── regression/             # QUAL-07 — created EMPTY, with the policy in README.md
│   │   ├── README.md
│   │   └── conftest.py
│   ├── policy/                 # architecture + process rules as tests
│   │   ├── test_no_postgres_csv_parsing.py     # LOAD-12, live before Phase 4
│   │   ├── test_ci_invokes_make_only.py        # local/CI drift
│   │   └── test_pinned_tool_versions_agree.py  # lockfile vs workflow
│   └── fixtures/
│       ├── corpus.yaml         # THE SPEC — committed
│       ├── CORPUS.sha256       # THE ORACLE — committed, ~5 KB
│       └── csv/                # GENERATED — .gitignore'd, never committed
│
├── tools/
│   └── corpus/                 # the generator (a package, so it is linted+typed)
│       ├── __init__.py
│       ├── manifest.py         # pydantic models for corpus.yaml
│       ├── generators.py       # tabular | literal | wrapper
│       └── __main__.py         # `python -m tools.corpus generate|verify`
│
├── configs/.gitkeep            # §65 dataset configs — Phase 3
├── schemas/.gitkeep            # §22 contracts — Phase 6
├── migrations/.gitkeep         # alembic — Phase 3
├── docker/{airflow,csv-processor}/.gitkeep      # Phase 2/4
├── kubernetes/.gitkeep         # Phase 2
├── helm/values/{local,ci}/.gitkeep              # Phase 2 (INFRA-10)
├── scripts/.gitkeep            # print() allowed here
├── docs/
│   ├── adr/
│   │   ├── README.md           # index + numbering convention
│   │   ├── 0000-template.md
│   │   ├── 0001-record-architecture-decisions.md
│   │   └── 0002-dataplat-core-with-csv-processor-plugin.md
│   └── .gitkeep                # the §75 doc set fills in per phase
└── .github/
    ├── workflows/ci.yml
    ├── dependabot.yml
    └── pull_request_template.md   # QUAL-07 checkbox
```

**Three deliberate departures, each of which the ADR must record:**

1. **Packages live under `packages/`, not `src/`.** ARCHITECTURE.md's diagram shows `src/dataplat/` and `src/csv_processor/`. A uv workspace member is a *directory containing a `pyproject.toml`* [CITED: uv workspace docs via Context7 — `members = ["packages/*"]`, member layout `packages/<name>/{pyproject.toml,src/<pkg>/}`]. Keeping `src/` at the root while also having per-member `pyproject.toml` files forces `src/dataplat/src/dataplat/`. `packages/` is uv's own documented convention and was verified working end-to-end. src-layout is *preserved inside each member*, which is what ARCHITECTURE.md's rationale ("prevents testing the source tree instead of the installed package") actually requires.
2. **`csv_processor` is a workspace member, not the root package** — this is locked decision 1, and it supersedes README §75's root-level `csv_processor/`.
3. **`tools/corpus/` is not in README §75.** It is generator code, not library code and not a shell script; putting it in `scripts/` would exempt it from the `print()` ban and from mypy strict, which is wrong for code whose output the whole CSV engine is specified against.

**Why the Airflow image is *not* a workspace member.** A uv workspace has **one lockfile and one resolved environment** [CITED: uv workspace docs — "Dependencies between workspace members are treated as editable"; a single `uv.lock` covers all members]. Adding `apache-airflow==3.3.0` as a member would force its ~600 constraint pins (`pandas==2.1.4`, `psycopg2-binary`, `polars==1.42.1`) into the same resolution as `dataplat`, which is precisely the failure C7 / PITFALLS G5 exist to prevent. The Airflow image therefore installs providers via `--constraint` in its own Dockerfile, entirely outside the workspace [CITED: STACK.md §F, "Installation"].

**Docker consequence to record now (bites in Phase 4).** uv requires *all* workspace member configuration files to be present in order to validate the lockfile; the Dockerfile must therefore use `--no-install-workspace` with `--frozen` for the dependency layer and switch to `--locked` after copying members [CITED: uv Docker integration docs via Context7]. Retrofitting this is annoying; noting it now is free.

### Pattern 1: One gate definition, two callers

**What:** the `Makefile` defines every check; `.github/workflows/ci.yml` invokes `make ci` and nothing else; a policy test asserts the workflow contains no direct invocation of `ruff`, `mypy`, `pytest` or `lint-imports`.
**When:** any project where "works on my machine" and "passes CI" must be the same statement.
**Why it beats the alternatives:** duplicating commands into the workflow is the single most common source of "green locally, red in CI". Composite actions and reusable workflows solve the CI-to-CI case but not the CI-to-local case. Make is the only layer both callers already have.

### Pattern 2: Architecture policy as pytest, not as a CI grep step

**What:** rules that are about *the repository* (no `COPY … FORMAT csv`; CI calls only `make`; pinned tool versions agree between `uv.lock` and the workflow) live in `tests/policy/` as ordinary tests.
**When:** a rule must hold before the code it governs exists — exactly the ROADMAP's instruction to "add the CI grep forbidding `COPY … FORMAT csv` now, so it is live before the first loader exists".
**Why:** a `run: grep -r …` step in a workflow runs only in CI, produces `grep: exit 1` as its entire error message, and is invisible to a developer until they push. A pytest assertion runs in `make check`, fails with a readable message naming the file and line, and is discoverable by reading the test tree.
**Trade-off:** a policy test must exclude itself from its own pattern. Build the forbidden pattern from concatenated fragments and skip `tests/policy/` when walking.

### Pattern 3: Generated corpus, committed oracle

**What:** `tests/fixtures/csv/**` is `.gitignore`d in its entirety. The only committed artifacts are `corpus.yaml` (the specification) and `CORPUS.sha256` (the oracle). `make fixtures` materialises; `make fixtures-verify` regenerates to a temp directory and diffs digests.
**When:** any corpus large enough that committing it bloats build contexts or drowns a secret scanner (PITFALLS G3, cheap-now decision #15).
**Why the digest file rather than "just regenerate and trust it":** without a committed oracle, a change to the generator silently changes the corpus, and every downstream detector test quietly re-baselines against the new bytes. The digest file makes a generator change a *reviewable diff* — which is the entire point of "the corpus is the specification".

### Anti-Patterns to Avoid

- **`strict = false` in a mypy per-module override.** Silently ignored — no warning, no error, the module remains fully strict. [VERIFIED: `mypy 2.3.0` reported `no-untyped-def` for a module explicitly overridden with `strict = false`, and reported success only when the individual flags were enumerated.] STACK.md §I's snippet contains this exact bug.
- **`select = ["ALL"]` without ignoring `CPY001`.** [VERIFIED: `ruff 0.16.2` emitted `CPY001 Missing copyright notice at top of file` for 4 of 4 files.] Absent from STACK.md's ignore list.
- **A global (non-path-scoped) gitleaks allowlist regex.** [CITED: PITFALLS G3] — it disables the control the requirement exists for. Path-scope to `tests/fixtures/**` *and* anchor on the `SYNTH_` prefix. [VERIFIED: a path+regex-scoped allowlist kept the corpus silent while still reporting four real findings in `src/`.]
- **Using `AKIAIOSFODNN7EXAMPLE` as the canary in the negative test.** [VERIFIED: gitleaks 8.30.1 reports `no leaks found` and exits 0 for a commit containing both the documented AWS example access key and its example secret.] A negative test built on it proves the opposite of what it claims.
- **`random.choice` / `shuffle` / `sample` / `randint` in the fixture generator.** [CITED: docs.python.org random "Notes on Reproducibility" — only `Random.random()` is guaranteed stable across versions.] Reproducible today, silently different after a Python upgrade.
- **`gzip.GzipFile(...)` without `mtime=0`.** [VERIFIED: two runs 1.1 s apart produced different SHA-256 digests; with `mtime=0` they were identical.] Same class of bug for `zipfile` without an explicit `ZipInfo.date_time`.
- **Committing the corpus "just the small ones".** Splitting the corpus into committed and generated halves means `make fixtures` cannot make the byte-identity claim about the whole corpus, and criterion 2 becomes partly vacuous. Encode pathological byte sequences *inside* `corpus.yaml` as escaped/hex literals so 100 % of `tests/fixtures/csv/` is generated.
- **Caching the large fixture.** [VERIFIED: generation ≈67 MB/s, SHA-256 ≈2.4 GB/s ⇒ a 241 MB fixture costs ~4 s to build, ~0.1 s to verify.] A cache adds a staleness hole to the one artifact whose purpose is reproducibility, to save four seconds.
- **`print()` allowed by weakening the library rule.** Do not remove `T20` from `select`. Scope it off `scripts/**` (and only `scripts/**`) via `per-file-ignores`. [VERIFIED: `T201` fired in `src/` and was correctly silent in `scripts/`.]

## Don't Hand-Roll

| Problem | Don't build | Use instead | Why |
|---|---|---|---|
| Enforcing `dataplat` never imports `csv_processor` | An AST-walking test | `import-linter` `forbidden` contract | Handles `from x import y`, `importlib`, relative imports, and re-export chains. [VERIFIED: correct verdict + exit codes.] Hand-rolled AST walkers miss dynamic and transitive cases. |
| Docstring presence on public APIs | A custom `inspect.getdoc` sweep | ruff `D101/D102/D103` | [VERIFIED] ruff already knows public-vs-private, nested classes, overloads, `__init__`, property setters, and inherited-method conventions. |
| Detecting an untyped public function | A grep for `def .*)\s*:` | mypy `disallow_untyped_defs` (in `strict`) | Only mypy knows a decorated, overloaded or `Protocol`-implementing def is untyped. |
| Secret detection | Entropy heuristics | gitleaks 8.30.1 default ruleset + scoped allowlist | Hundreds of curated provider rules plus stopword handling. The one thing to know is that its stopwords *include* vendor example keys — see the canary note. |
| Reproducible pseudo-randomness | A hand-written LCG or `hash()`-based stream | `random.Random(seed)` used **only via `.random()`/`.getrandbits()`** | The stdlib gives a documented cross-version guarantee for `.random()`; a hand-rolled LCG gives none, and `hash()` is `PYTHONHASHSEED`-dependent for `str`. |
| Deterministic archive bytes | Post-processing gzip headers | `gzip.GzipFile(mtime=0, filename="")` and `zipfile.ZipInfo(date_time=(1980,1,1,0,0,0))` | [VERIFIED] The stdlib exposes exactly the two knobs that carry the nondeterminism. |
| Bounded-memory assertion | Sampling RSS with `psutil` in-process | `resource.setrlimit(RLIMIT_AS, N)` in a **subprocess** | [VERIFIED: streaming reader passed, buffering reader raised `MemoryError`, on a 241 MB file at a 128 MB limit.] RSS sampling is racy and allocator-dependent; an rlimit is a hard, deterministic boundary. |
| Locking dependencies | `pip freeze` / `requirements.txt` | `uv.lock` + `uv lock --check` | [VERIFIED: `uv lock --check` exits 0 when in sync] — universal cross-platform lock, and a first-class drift check for CI. |
| Running the same commands in CI and locally | Copy-pasting into the workflow | One `Makefile`, invoked by both | Copy-paste drift is the default outcome, not the exception. |

**Key insight:** every hand-rolled variant above is *not* meaningfully harder to write — it is harder to be *right*. The failure mode of each is silence: a grep that never matches, an entropy check that never fires, an RSS sample that happens to be low. In a phase whose entire deliverable is "gates that fail when they should", a control that fails open is worse than no control, because it is also a claim.

## Code Examples

Every configuration below was executed in this session unless marked otherwise.

### Root `pyproject.toml` — virtual workspace root

```toml
# VERIFIED: this exact shape synced successfully with uv 0.8.11
# (uv 0.12.3 is the target per STACK.md; workspace semantics are unchanged)
[project]
name = "airflow-platform"
version = "0.0.0"
description = "Airflow ETL Platform — workspace root (not a distribution)"
requires-python = ">=3.12,<3.13"
dependencies = []

[tool.uv]
package = false                        # virtual root: never built or installed

[tool.uv.workspace]
members = ["packages/dataplat", "packages/csv-processor"]

[dependency-groups]
dev = [
  "pytest>=9.1,<10",
  "pytest-cov>=7.1,<8",
  "pytest-xdist>=3.8,<4",
  "hypothesis>=6.165,<7",
  "ruff==0.16.2",                      # pinned exactly: this is a gate
  "mypy==2.3.0",                       # pinned exactly: this is a gate
  "import-linter>=2.13,<3",
  "pre-commit>=4.6,<5",
  "PyYAML>=6",
]

[tool.ruff]
target-version = "py312"
line-length = 100
src = ["packages/dataplat/src", "packages/csv-processor/src", "tools", "tests", "airflow/dags"]

[tool.ruff.lint]
select = ["ALL"]
ignore = [
  "D203", "D213",      # mutually exclusive with D211/D212 — pick one side
  "COM812", "ISC001",  # conflict with `ruff format`
  "ANN401",            # Any in **kwargs is sometimes correct
  "FIX", "TD",         # TODO comments are allowed
  "CPY001",            # VERIFIED: fires on EVERY file under select=ALL; not in STACK.md's list
]

[tool.ruff.lint.per-file-ignores]
"tests/**"        = ["S101", "PLR2004", "ANN", "D"]   # asserts + magic numbers are fine
"airflow/dags/**" = ["INP001"]                        # DAG folder is not a package
"scripts/**"      = ["T20", "INP001"]                 # OBS-03 carve-out — scripts only
"tools/corpus/__main__.py" = ["T20"]                  # CLI entry point may print

[tool.ruff.lint.pydocstyle]
convention = "google"

[tool.mypy]
python_version = "3.12"
strict = true
warn_unreachable = true
disallow_any_explicit = false          # Decimal / CSV boundaries need some Any
mypy_path = ["packages/dataplat/src", "packages/csv-processor/src"]

# CORRECTED from STACK.md §I. `strict = false` here is SILENTLY IGNORED (verified).
# The individual flags must be enumerated.
[[tool.mypy.overrides]]
module = ["tests.*"]
disallow_untyped_defs = false
disallow_incomplete_defs = false
check_untyped_defs = false
disallow_untyped_calls = false
disallow_untyped_decorators = false
warn_return_any = false

[[tool.mypy.overrides]]
module = ["clevercsv.*", "hvac.*", "chardet.*"]
ignore_missing_imports = true

[tool.pytest.ini_options]
minversion = "9.0"
testpaths = ["tests"]
addopts = "-ra --strict-markers --strict-config"
markers = [
  "slow: generates or reads a large fixture",
  "regression: permanent test for a specific fixed bug (QUAL-07)",
]
```

> **Note on `warn_unused_ignores`.** It is part of `strict` [VERIFIED: captured from `mypy --help`]. Combined with the `ignore_missing_imports` overrides above it will flag any `# type: ignore` that becomes unnecessary once a library ships stubs. That is desirable, but it means a dependency upgrade can turn CI red for a reason unrelated to the change. Expect it and do not disable it.

### `packages/dataplat/pyproject.toml`

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "dataplat"
version = "0.1.0"
description = "Source-agnostic ETL platform core"
requires-python = ">=3.12,<3.13"
dependencies = ["PyYAML>=6"]           # Phase 1 minimum; STACK.md §F lists the full set

# NOTE: no dependency on csv-processor, in either direction of the file.
# The absence here is the packaging half of the guarantee; import-linter is the other half.

[tool.hatch.build.targets.wheel]
packages = ["src/dataplat"]
```

### `packages/csv-processor/pyproject.toml`

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "csv-processor"
version = "0.1.0"
description = "CSV source plugin for dataplat"
requires-python = ">=3.12,<3.13"
dependencies = ["dataplat"]

[tool.uv.sources]
dataplat = { workspace = true }        # VERIFIED: resolves to the local member, editable

[tool.hatch.build.targets.wheel]
packages = ["src/csv_processor"]
```

Both members also need `src/<pkg>/py.typed` (empty file) so mypy treats the installed package as typed — otherwise a consumer importing `dataplat` from the *installed* wheel sees it as untyped and `strict` produces confusing `Any`s.

### `setup.cfg` — import-linter contracts

```ini
# VERIFIED: import-linter 2.13 broke contract 1 on a deliberate violation
# (exit code 1) and passed cleanly once removed (exit code 0).
[importlinter]
root_packages =
    dataplat
    csv_processor

[importlinter:contract:1]
name = dataplat core must not depend on the CSV plugin
type = forbidden
source_modules =
    dataplat
forbidden_modules =
    csv_processor

[importlinter:contract:2]
name = nothing may import the DAG folder
type = forbidden
source_modules =
    dataplat
    csv_processor
forbidden_modules =
    dags
```

> Contract 2 encodes ARCHITECTURE.md's rule that "`airflow/dags/` is a leaf … enforce with an import-linter contract in CI — this is the mechanical guarantee behind §6.4". It is trivially satisfied in Phase 1 (the folder is empty) and that is exactly why it is cheap to add now.

### `Makefile` — the single definition of the gate

```make
# The ONLY place a quality gate is defined. CI calls `make ci` and nothing else.
SHELL := /bin/bash
.DEFAULT_GOAL := help
UV ?= uv
RUN := $(UV) run --frozen

.PHONY: help install lock-check lint format typecheck imports test policy \
        fixtures fixtures-verify gitleaks gitleaks-selftest check ci clean

help:                          ## Show targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/'

install:                       ## Create the venv from the lockfile
	$(UV) sync --all-packages --frozen

lock-check:                    ## Fail if uv.lock is stale vs pyproject files
	$(UV) lock --check

lint:                          ## ruff check (T20 => OBS-03, D => QUAL-02, ANN => QUAL-01)
	$(RUN) ruff check .

format:                        ## ruff format --check
	$(RUN) ruff format --check .

typecheck:                     ## mypy strict (QUAL-01)
	$(RUN) mypy packages/dataplat/src packages/csv-processor/src tools

imports:                       ## import-linter contracts
	$(RUN) lint-imports

test:                          ## unit tests
	$(RUN) pytest tests/unit -q

policy:                        ## repository policy tests (LOAD-12 ban, CI/Make parity)
	$(RUN) pytest tests/policy -q

fixtures:                      ## (re)generate the corpus + rewrite CORPUS.sha256
	$(RUN) python -m tools.corpus generate --out tests/fixtures/csv \
	                                       --manifest tests/fixtures/corpus.yaml \
	                                       --write-digests tests/fixtures/CORPUS.sha256

fixtures-verify:               ## QUAL-08: prove byte-identity against the committed oracle
	$(RUN) python -m tools.corpus verify --manifest tests/fixtures/corpus.yaml \
	                                     --digests tests/fixtures/CORPUS.sha256

gitleaks:                      ## SEC-02/SEC-11: full history + working tree
	./tools/bin/gitleaks git --log-opts="--all" --redact --no-banner --exit-code 1 .
	./tools/bin/gitleaks dir  --redact --no-banner --exit-code 1 .

gitleaks-selftest:             ## SEC-11: prove the scanner actually fails a build
	$(RUN) python -m tools.security.gitleaks_selftest ./tools/bin/gitleaks

check: lock-check lint format typecheck imports policy test fixtures-verify  ## Local gate
ci: check gitleaks gitleaks-selftest                                          ## CI gate

clean:
	rm -rf .venv .mypy_cache .pytest_cache .ruff_cache tests/fixtures/csv
```

`make check` deliberately excludes `gitleaks`, because it requires a downloaded binary and success criterion 4 says a clean checkout must run `uv sync && make check` with **no network**. `make ci` is the superset. The policy test asserts CI runs `make ci` and that `ci` depends on `check`.

### `.github/workflows/ci.yml`

```yaml
name: CI

on:
  pull_request:
  push:
    branches: [main]

# SEC-10: least privilege at the workflow level; no job elevates in Phase 1.
permissions:
  contents: read

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: ${{ github.event_name == 'pull_request' }}

env:
  UV_VERSION: "0.12.3"
  GITLEAKS_VERSION: "8.30.1"

jobs:
  check:
    name: Quality gate
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      # VERIFIED: tag v7.0.1 -> this SHA via GitHub git/ref/tags API
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      # VERIFIED: tag v9.0.0 -> this SHA
      - uses: astral-sh/setup-uv@c771a70e6277c0a99b617c7a806ffedaca235ff9 # v9.0.0
        with:
          version: ${{ env.UV_VERSION }}
          enable-cache: true
          cache-dependency-glob: "uv.lock"
      - run: make install
      - run: make check          # <- the ONLY substantive step; no gate defined here

  secrets:
    name: Secret scan (full history)
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          fetch-depth: 0         # SEC-02 requires the whole history, not the PR diff
      - name: Install gitleaks (checksum-verified)
        run: |
          set -euo pipefail
          mkdir -p tools/bin && cd "$(mktemp -d)"
          base="https://github.com/gitleaks/gitleaks/releases/download/v${GITLEAKS_VERSION}"
          curl -sSLf -o gl.tgz  "${base}/gitleaks_${GITLEAKS_VERSION}_linux_x64.tar.gz"
          curl -sSLf -o sums.txt "${base}/gitleaks_${GITLEAKS_VERSION}_checksums.txt"
          grep "linux_x64" sums.txt \
            | sed "s/gitleaks_${GITLEAKS_VERSION}_linux_x64.tar.gz/gl.tgz/" \
            | sha256sum -c -
          tar xzf gl.tgz gitleaks
          mv gitleaks "$GITHUB_WORKSPACE/tools/bin/gitleaks"
      - run: make gitleaks           # --redact is inside the target: SEC-10
      - run: make gitleaks-selftest  # SEC-11: proves the scanner is not a no-op
```

Notes the planner should not lose:

- **`fetch-depth: 0` is mandatory for SEC-02.** `actions/checkout` defaults to depth 1; `gitleaks git --log-opts="--all"` on a depth-1 checkout scans one commit and reports "no leaks found" — a green build that proves nothing. [VERIFIED: gitleaks reported `1 commits scanned` on a single-commit repo.]
- **Full history is affordable now.** [VERIFIED: gitleaks scanned a 3-commit repo in ~85 ms.] If the job ever exceeds ~60 s, split it: PRs scan `--log-opts="--no-merges ${{ github.event.pull_request.base.sha }}..HEAD"`, and a `schedule:`/`push: main` job keeps `--log-opts="--all"`. Criterion 5 is satisfied by the full-history job either way. Do not do this split pre-emptively.
- **`enable-cache: true` on `setup-uv`** keyed on `uv.lock` — this is the cheap half of PITFALLS G2; the expensive half (image layers) is Phase 4+.
- **No `secrets.*` reference appears anywhere.** That is the strongest available form of SEC-10 for this phase and is greppable.

### `.gitleaks.toml`

```toml
# VERIFIED end-to-end: this allowlist kept a synthetic-token corpus silent while
# still reporting 4 real findings (aws-access-token, github-pat, generic-api-key,
# slack-bot-token) in src/.
title = "airflow-platform"

[extend]
useDefault = true

# PITFALLS G3: path-scoped, never a global pattern allowlist.
[[allowlists]]
description = """
Synthetic CSV corpus. Generated from a committed seed; every synthetic
credential-shaped value carries the SYNTH_ prefix by construction. Both the
path AND the prefix must match, so a real secret dropped into this tree is
still reported.
"""
paths = ['''^tests/fixtures/.*''']
regexTarget = "line"
regexes = ['''SYNTH_[A-Za-z0-9]{8,}''']
condition = "AND"

[[allowlists]]
description = "Corpus manifest may contain SYNTH_-prefixed literals"
paths = ['''^tests/fixtures/corpus\.yaml$''']
regexTarget = "line"
regexes = ['''SYNTH_[A-Za-z0-9]{8,}''']
condition = "AND"
```

> `condition = "AND"` requires *both* criteria [CITED: gitleaks README allowlist schema — `condition` is `"OR"` (default) or `"AND"`]. With the default `OR`, the `SYNTH_` regex would allowlist that prefix **repository-wide**, which is exactly the global-pattern mistake PITFALLS G3 warns against. This one keyword is the difference between a precise control and a disabled one. [ASSUMED: the `AND` semantics were read from the README, not exercised — the verified run above used the default `OR` form with only the corpus present, so it did not distinguish the two. **The planner should include a task that asserts a `SYNTH_`-prefixed value outside `tests/fixtures/` is still reported.**]

### `.gitignore` (relevant lines) and `.dockerignore`

```gitignore
# QUAL-08 / criterion 2: the corpus is generated, never committed.
tests/fixtures/csv/
!tests/fixtures/corpus.yaml
!tests/fixtures/CORPUS.sha256

.venv/
tools/bin/          # downloaded gitleaks
.mypy_cache/
.pytest_cache/
.ruff_cache/
```

```dockerignore
# PITFALLS G3 problem 2: keep fixtures and planning docs out of every build context
tests/
.planning/
.git/
docs/
.venv/
tools/bin/
```

## Fixture Corpus: Determinism, Specification and Coverage (QUAL-08)

This is the design-heavy deliverable of the phase. Everything below was prototyped and measured.

### The ten determinism rules

Each rule exists because a specific mechanism breaks byte-identity. The argument is mechanical: rule, mechanism, evidence.

| # | Rule | Mechanism it defeats | Evidence |
|---|------|---------------------|----------|
| **R1** | Derive a **per-fixture** RNG: `Random(int.from_bytes(sha256(f"{MASTER_SEED}\|{name}").digest(), "big"))` | A single shared stream makes fixture *N*'s bytes depend on how many values fixtures 1..*N*−1 consumed. Adding, removing or reordering any fixture silently rewrites all later ones. | [VERIFIED: prototype used per-name derivation; digests stable across runs] |
| **R2** | Consume randomness **only** through `.random()` and `.getrandbits()`. Derive ints as `lo + int(r.random() * (hi-lo+1))`, choices as index arithmetic on `.random()` | `choice`, `shuffle`, `sample`, `randrange`, `randint` are documented as "subject to change across Python versions" | [CITED: docs.python.org/3/library/random.html — "Most of the random module's algorithms and seeding functions are subject to change across Python versions, but two aspects are guaranteed not to change: … The generator's `random()` method will continue to produce the same sequence when the compatible seeder is given the same seed."] |
| **R3** | Build a `str`, call `.encode(declared_encoding)`, write with `open(path, "wb")`. Never `open(path, "w")`, never rely on locale | `open(..., "w")` uses `locale.getpreferredencoding(False)`, which differs between the WSL dev box and a CI runner | [VERIFIED: digests identical under `LC_ALL=C.UTF-8` and the default locale] |
| **R4** | Line terminator is an explicit manifest field, joined by hand. Never `csv.writer`'s default, never a translated `"\n"` | `csv.writer` defaults to `\r\n`; text-mode writes translate `\n` per platform | Design rule; prototype joined terminators explicitly |
| **R5** | `gzip.GzipFile(..., mtime=0, filename="")`; `zipfile.ZipInfo(name, date_time=(1980,1,1,0,0,0))` with an explicit `external_attr` | Both formats embed wall-clock timestamps (and gzip embeds the source filename) in the header | [VERIFIED: two gzip runs 1.1 s apart → `a8ed9bf047b3ec77` vs `dc784cb3c6741cc9`; with `mtime=0` → `7cbae7d96605d614` both times] |
| **R6** | No `datetime.now()`, `uuid4()`, `os.urandom()`, `time.time()`, `os.getpid()` anywhere in `tools/corpus/`. Every timestamp is a literal in the manifest | Obvious once stated; easy to reintroduce via a "helpful" `generated_at` header comment | Enforceable as a policy test that greps `tools/corpus/` |
| **R7** | Iterate the manifest in **declared order**. Never iterate a `set`, never `sorted()` a heterogeneous key, never rely on `dict` ordering derived from a set | `str.__hash__` is `PYTHONHASHSEED`-salted, so set iteration order varies per process | [VERIFIED: identical digests under `PYTHONHASHSEED=99` vs the default randomised seed] |
| **R8** | Never read `os.listdir()`/`glob` order as input to generation | Filesystem order differs between ext4, overlayfs and tmpfs | Design rule |
| **R9** | Build NFC/NFD variants with explicit `unicodedata.normalize("NFC"/"NFD", s)` calls — **never** by pasting two visually identical strings into the generator source | Editors, `git` filters and some terminals silently renormalise source files, collapsing the very distinction the fixture tests | [VERIFIED: prototype asserted `nfc != nfd` after explicit normalisation] |
| **R10** | Format numbers with explicit format strings or `Decimal`; never `str(float)` | `repr(float)` is stable in CPython ≥3.1 but the *values* produced by float arithmetic in the generator are not worth the risk when the corpus exists to test numeric fidelity | Design rule; consistent with STACK.md §F "never `float` for money or identifiers" |

**Proof run.** A prototype implementing R1–R10 across ten fixtures (UTF-8, UTF-8+BOM+CRLF, Windows-1250, UTF-16-LE+BOM, NUL bytes, NFC-vs-NFD, Excel scientific-notation IDs, DST gap/overlap, gzip, zip) produced identical SHA-256 digests on three consecutive runs, including one under `TZ=America/New_York LC_ALL=C.UTF-8 PYTHONHASHSEED=99`. [VERIFIED: `diff` of digest listings was empty for both comparisons.]

**Residual risk stated honestly.** R2 relies on a documented CPython guarantee, not on a cross-version experiment — only Python 3.12.3 is present on this machine. The guarantee is explicit and load-bearing enough to rely on, but the planner should add a `tests/policy/` check that `tools/corpus/` contains no reference to `random.choice|shuffle|sample|randrange|randint`, so R2 cannot erode. That check *is* the cross-version insurance.

### Where the specification lives: `tests/fixtures/corpus.yaml`

The manifest is not merely a build recipe — it is the machine-readable statement of what each fixture *means*, which is what makes "the corpus is the specification" (QUAL-08) true rather than aspirational. Every field in `expect:` becomes an assertion in Phase 6.

```yaml
# tests/fixtures/corpus.yaml  — COMMITTED. The only spec of the corpus.
version: 1
master_seed: "airflow-platform/corpus/v1"

fixtures:
  - name: "01_simple.csv"
    covers: [CSV-04, CSV-07]
    generator: tabular
    encoding: utf-8
    bom: false
    delimiter: ","
    quotechar: '"'
    line_terminator: "\n"
    header: [id, name, amount]
    rows: 20
    row_spec:
      id:     { kind: zero_padded_int, width: 6, start: 1 }
      name:   { kind: pick, values: ["Kowalski", "Nowak", "Wiśniewski", "Wójcik"] }
      amount: { kind: decimal, min: "100.00", max: "99999.99", scale: 2 }
    expect:
      detected_encoding: utf-8
      detected_delimiter: ","
      header_row_index: 0
      data_rows: 20
      rejected_rows: 0

  - name: "07_utf16.csv"
    covers: [CSV-02, CSV-03]
    generator: tabular
    encoding: utf-16-le
    bom: true                  # generator prepends codecs.BOM_UTF16_LE
    delimiter: ","
    line_terminator: "\r\n"
    header: [id, name, amount]
    rows: 8
    expect:
      detected_encoding: utf-16-le
      encoding_confidence_min: 1.0     # a BOM is deterministic evidence (STACK.md §F)
      data_rows: 8

  - name: "32_nul_bytes.csv"
    covers: [CSV-06, VALID-01]
    generator: literal
    encoding: utf-8
    # Pathological bytes live IN the manifest as escapes, so 100% of
    # tests/fixtures/csv/ stays generated and gitignored.
    content: "id,name\n1,ab\\x00cd\n2,ok\n"
    expect:
      data_rows: 2
      # SEE THE VERIFIED NOTE BELOW — this expectation is NOT what FEATURES.md assumes.
      parser_raises: false
      contains_nul_in_column: name
      quarantine_reason: "nul-byte-in-text-field"

  - name: "44_unicode_nfc_vs_nfd.csv"
    covers: [CSV-12, DEDUP-01]
    generator: literal_unicode          # forces explicit unicodedata.normalize (R9)
    encoding: utf-8
    rows_spec:
      - { id: "1", name: { text: "Wiśniewski", form: NFC } }
      - { id: "1", name: { text: "Wiśniewski", form: NFD } }
    expect:
      data_rows: 2
      distinct_after_nfc_normalization: 1
      distinct_before_normalization: 2

  - name: "55_dst_gap_and_overlap.csv"
    covers: [QUAL-17, SCD-06]
    generator: literal
    encoding: utf-8
    content: |
      event_id,ts_local
      1,2026-03-29 02:30:00
      2,2026-10-25 02:30:00
      3,2026-01-15 12:00:00
    expect:
      timezone: Europe/Warsaw
      row_1_classification: nonexistent   # spring-forward gap
      row_2_classification: ambiguous     # autumn overlap, fold=0 and fold=1 differ
      row_3_classification: unambiguous

  - name: "61_gzipped.csv.gz"
    covers: [CSV-11]
    generator: wrapper
    wraps: "01_simple.csv"
    compression: gzip
    gzip_mtime: 0                 # R5 — non-negotiable
    gzip_filename: ""

  - name: "29_large_file.csv"
    covers: [LOAD-07]
    generator: tabular
    encoding: utf-8
    delimiter: ","
    line_terminator: "\n"
    header: [id, name, amount]
    rows: 9000000                 # ≈241 MB
    profile: large                # excluded from `make fixtures --fast`
    expect:
      approx_bytes: 241000000
      rlimit_as_bytes: 134217728  # 128 MiB — streaming must pass, buffering must die
      data_rows: 9000000
```

**Why the `expect:` block is the important half.** Without it the corpus is a pile of bytes and every Phase-6 detector test hard-codes its own expectations, which is how a corpus stops being a specification. With it, Phase 6's detector tests are a single parametrised loop over the manifest — and a *disagreement* between a fixture's declared meaning and the detector's behaviour is a test failure rather than a judgement call.

### `CORPUS.sha256` — the committed oracle

```
a0b7293af563b6d844c57137409b5a21ae0cb489b672d3b0468975b5fafd9cbb  01_simple.csv
8a172dccd2998c80aad1f90ddace8f06a3421ae2c6bfccbcb2c8c9ada03766a1  05_utf8_bom.csv
9d1857b301cb3aefcb27532b3f9993f9987cec369d51785304eb58b287dc1c67  06_windows1250.csv
...
```

Standard `sha256sum` format so `sha256sum -c` works as an independent second opinion. ~70 lines, ~5 KB — a reviewable diff. **`make fixtures` rewrites it; `make fixtures-verify` only reads it.** That asymmetry is what makes a generator change show up in code review instead of silently re-baselining.

### Coverage: the ~69 fixtures and what Phase 1 should actually build

README §73 names 29; `.planning/research/FEATURES.md` §3.4 adds 40 more (numbered 30–70), for 69 total. Phase 1 should **not** author all 69 bodies — but it *must* author every one whose construction is byte-level hard, because those are the ones a naive generator cannot be retrofitted to produce.

| Class | Examples | Phase 1? | Why |
|---|---|---|---|
| **Byte-level hard** — encodings, BOMs, control bytes, archives, size | `05_utf8_bom`, `06_windows1250`, `07_utf16`, `30_crlf_lf_mixed`, `31_cr_only`, `32_nul_bytes`, `39_utf8_invalid_sequences`, `40_utf16_no_bom`, `41_bom_mid_file`, `42_zero_width_and_bidi`, `43_nbsp_thousands_separator`, `44_unicode_nfc_vs_nfd`, `61_gzipped.csv.gz`, `62_multipart_split`, `29_large_file` | **Yes — all of them** | Each needs a distinct generator capability (encoder selection, BOM injection, raw byte splicing, archive wrapping, size parameterisation). Discovering a missing capability in Phase 6 means reworking the generator *and* re-baselining every digest. |
| **Structural** — dialect, header, footer, raggedness | `02`–`04`, `08`–`19`, `33_ragged_rows`, `34_unclosed_quote_eof`, `35_quote_in_unquoted_field`, `36_doubled_vs_backslash_escape`, `37_delimiter_frequency_differs`, `38_single_column_no_delimiter`, `45`–`47`, `63`–`67` | **Yes** | All expressible as `tabular` + `literal` with the parameters already required. Cheap once the framework exists, and they are the §73 core. |
| **Semantic damage** — types, dates, numbers, booleans | `20`–`24`, `50_excel_scientific_notation_ids`, `51_excel_leading_zero_stripped`, `52_date_ambiguous_dm_vs_md`, `53_two_digit_year`, `54_excel_serial_dates`, `55_dst_gap_and_overlap`, `56`–`60`, `70` | **Partly** — build the ~8 that pin a hard decision (`50`, `51`, `52`, `55`) | These are plain UTF-8 text; adding one later is a five-line manifest entry with no framework change. But `50`/`51`/`52` encode *unrecoverable* damage and `55` encodes DST — those four are the ones whose expected behaviour must be locked before anyone writes a normaliser. |
| **Header hygiene** | `48_duplicate_header_names_case_variant`, `49_header_with_leading_trailing_spaces` | Yes | Trivial, and they pin header-detection semantics. |

Recommended Phase 1 target: **the full 29 from §73 plus every byte-level-hard addition plus the four decision-pinning semantic ones ≈ 50 fixtures.** The remaining ~19 are explicitly deferred with a note in `docs/adr/` and a `corpus.yaml` comment, satisfying QUAL-08's "grows as cases are discovered".

### The large-file fixture: generate it, do not cache it

Measured on this machine:

| Operation | Measurement | Implication |
|---|---|---|
| Generate 2 M rows (53.6 MB) | 0.8 s → **~67 MB/s** | 241 MB ≈ 3.6 s |
| SHA-256 throughput | **~2 379 MB/s** | 1 GB verified in ~0.4 s; the whole corpus in well under a second |
| Stream-parse 9 M rows (241 MB) under `RLIMIT_AS = 128 MiB` | passes, rc 0 | The bounded-memory test needs no container |
| Buffer-parse the same file under the same limit | `MemoryError`, rc 1 | The negative half of the test is real, not hypothetical |

[VERIFIED: all four rows measured this session.]

**Therefore: no cache, no `actions/cache` key, no "generate on demand" special case.** A cache would be the only artifact in this system whose staleness could invalidate the reproducibility claim it exists to support. Generating it costs ~4 s and hashing it ~0.1 s — well inside PITFALLS G4's "a 4-minute PR gate will not be routed around" budget.

Two knobs the manifest must expose so the fixture stays useful as the platform grows:

- `rows:` — the size is a parameter, which is exactly the property PITFALLS G3 says generated fixtures buy you ("can be generated at *any* size, which is what E6's memory test needs").
- `rlimit_as_bytes:` — the *limit* is a parameter too, with the invariant `approx_bytes > 2 × rlimit_as_bytes` asserted by the manifest validator. Note the interpreter's own footprint consumes ~30–50 MB of the address space, so the limit cannot be set arbitrarily low; 128 MiB was verified to leave adequate headroom.

Add `make fixtures FAST=1` which skips `profile: large` for the inner-loop developer experience, but **`make check` and CI always run the full set** — a fast path that is also the default is a fast path that silently stops testing the thing.

### One verified correction to FEATURES.md

`.planning/research/FEATURES.md` §3.4 states for `32_nul_bytes`: *"Python's stdlib `csv` raises `_csv.Error: line contains NULL byte` and **cannot** handle it without pre-filtering the byte stream (cpython #71767 / bpo-27580)"*.

**This is not true on Python 3.12.3.** [VERIFIED: `csv.reader` over both a `StringIO` and a file opened with `newline=""` parsed `id,name\n1,ab\x00cd\n2,ok\n` without error, returning `[['id','name'], ['1','ab\x00cd'], ['2','ok']]` — the NUL survives inside the field.]

This matters because it moves the failure downstream. The NUL is not a *parsing* problem, it is a *PostgreSQL* problem: `text`/`varchar` cannot store `U+0000`. So `32_nul_bytes`' `expect:` block must assert a **validation/quarantine** outcome (`nul-byte-in-text-field`), not a parser exception — and the check must live in the validation stage in Phase 6/8, not in the reader. Planning for a parser exception that never arrives would leave NUL bytes to surface as an opaque `psycopg` error during `COPY` in Phase 4, attributable to no particular row, which is exactly the §51 failure the platform is built to avoid.

## Policy Tests: the `COPY … FORMAT csv` ban and CI/Make parity

### Where the LOAD-12 ban lives, and why

ROADMAP.md instructs: *"Add the CI grep forbidding `COPY … FORMAT csv` now, so it is live before the first loader exists (it enforces LOAD-12 in Phase 4)."* The question is *where*.

| Option | Fails cleanly? | Runs locally? | Readable error? | Verdict |
|---|---|---|---|---|
| `run: ! grep -rn …` step in `ci.yml` | Yes | **No** | `Error: Process completed with exit code 1` | Reject — invisible until push |
| `pre-commit` hook only | No (bypassable with `--no-verify`) | Yes | Moderate | Reject as the gate; fine as a courtesy |
| ruff custom rule | n/a | — | — | Not possible; ruff has no user-defined rules |
| **pytest test in `tests/policy/`** | Yes (rc 1) | **Yes**, via `make check` | Full assertion message with path + line | **Recommend** |

```python
# tests/policy/test_no_postgres_csv_parsing.py
"""LOAD-12: the ETL processor is the only component that parses CSV.

PostgreSQL must never parse raw input, because rows loaded by ``COPY ... FORMAT csv``
bypass every structural, type and quality check the platform exists to perform
(PITFALLS C3: "letting PostgreSQL parse the CSV voids the entire product").

This test is deliberately live from Phase 1, before any loader exists, so the
constraint is never briefly true-by-accident and then violated.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

SCANNED_SUFFIXES = frozenset({".py", ".sql", ".yaml", ".yml", ".j2", ".sh"})
EXCLUDED_DIRS = frozenset({
    ".git", ".venv", ".planning", "docs", "tests/policy",
    ".mypy_cache", ".pytest_cache", ".ruff_cache", "tests/fixtures/csv",
})

# Assembled from fragments so this file does not match its own pattern.
_COPY = "CO" + "PY"
_FMT = "FOR" + "MAT"
FORBIDDEN = re.compile(
    rf"(?is)\b{_COPY}\b(?:(?!;).){{0,400}}?\b(?:{_FMT}\s+)?\bCSV\b"
)
# Also catches psql's backslash form and psycopg's copy_expert(...) legacy path.
FORBIDDEN_EXTRA = re.compile(r"(?i)\\\s*copy\b(?:(?!;).){0,400}?\bcsv\b")


def _candidate_files() -> list[Path]:
    out: list[Path] = []
    for path in REPO_ROOT.rglob("*"):
        if not path.is_file() or path.suffix not in SCANNED_SUFFIXES:
            continue
        rel = path.relative_to(REPO_ROOT).as_posix()
        if any(rel == d or rel.startswith(f"{d}/") for d in EXCLUDED_DIRS):
            continue
        out.append(path)
    return sorted(out)  # sorted: stable failure ordering


def test_postgres_never_parses_csv() -> None:
    violations: list[str] = []
    for path in _candidate_files():
        text = path.read_text(encoding="utf-8", errors="replace")
        for lineno, line_text in enumerate(text.splitlines(), start=1):
            if FORBIDDEN.search(line_text) or FORBIDDEN_EXTRA.search(line_text):
                rel = path.relative_to(REPO_ROOT).as_posix()
                violations.append(f"{rel}:{lineno}: {line_text.strip()[:120]}")

    assert not violations, (
        "LOAD-12 violation: PostgreSQL must never parse raw CSV.\n"
        "Use the processor to parse, then COPY the already-validated rows into an\n"
        "all-TEXT staging table using the default TEXT format.\n\n"
        + "\n".join(violations)
    )
```

**Honest limitation, which the plan should record rather than paper over:** a regex over source cannot detect a `COPY` statement assembled at runtime from string fragments or read from a config file. This test raises the cost of the mistake and documents the rule where a developer will meet it; it does not make the mistake impossible. The mechanism that *does* make it impossible is architectural — the staging table is all-`TEXT` [CITED: REQUIREMENTS.md "Out of Scope" — "Typed staging tables … Staging is all-TEXT so structural and type validation happen in the processor"] — and belongs to Phase 4. Both should exist.

### CI/Make parity test

```python
# tests/policy/test_ci_invokes_make_only.py
"""CI and local development must run the identical gate.

The Makefile is the single definition. If a workflow step invokes a linter,
type-checker or test runner directly, the two definitions can diverge and
"green locally, red in CI" becomes possible.
"""

from __future__ import annotations

import re
from pathlib import Path

WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "ci.yml"
DIRECT_TOOLS = re.compile(r"\b(ruff|mypy|pytest|lint-imports)\b")


def test_workflow_never_invokes_a_gate_directly() -> None:
    offenders = [
        f"{n}: {line.strip()}"
        for n, line in enumerate(WORKFLOW.read_text(encoding="utf-8").splitlines(), 1)
        if re.search(r"^\s*(-\s*)?run:\s*", line) and DIRECT_TOOLS.search(line)
    ]
    assert not offenders, (
        "CI must invoke gates through `make`, never directly, so the local and CI\n"
        "definitions cannot drift. Add the check to the Makefile instead.\n\n"
        + "\n".join(offenders)
    )
```

A third policy test (`test_pinned_tool_versions_agree.py`) should assert that the `ruff==` and `mypy==` pins in the root `pyproject.toml` dev group match the versions the workflow would install, and that `GITLEAKS_VERSION` in `ci.yml` matches the version the Makefile's `gitleaks` target expects. These are three-line comparisons that prevent a whole class of "the gate got weaker and nobody noticed".

### The gitleaks self-test (SEC-11's negative proof)

PITFALLS G3 says: *"Add a deliberate negative test: commit a canary secret in a branch and assert CI fails. A scanner nobody has ever seen fail is not known to work."* Doing that literally requires putting a credential-shaped string into real git history, which then permanently trips every future full-history scan and forces a global allowlist — the exact anti-pattern G3 warns about elsewhere.

Better: build a throwaway repository in a temp directory, commit canaries into *it*, and assert `gitleaks git` exits 1.

```python
# tools/security/gitleaks_selftest.py  (invoked by `make gitleaks-selftest`)
"""SEC-11 negative proof: assert the configured scanner actually fails a build.

Creates a disposable git repository containing credential-shaped canaries, runs
the *project's* .gitleaks.toml against it, and asserts a non-zero exit. Nothing
is written to the real repository's history.
"""
```

Two verified constraints on the canary values:

1. **Do not use vendor example keys.** [VERIFIED: a commit containing `AKIAIOSFODNN7EXAMPLE` *and* `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` produced `no leaks found` and exit code 0 from gitleaks 8.30.1.] They are on the default stopword list. A self-test built on them asserts exactly the wrong thing.
2. **Randomised-looking values do fire.** [VERIFIED: a commit containing an AWS-shaped key, a `ghp_`-prefixed token and an `xoxb-` Slack token produced `leaks found: 4` — `aws-access-token`, `github-pat`, `generic-api-key`, `slack-bot-token` — and exit code 1.]

The self-test must also assert the *positive* control: that the same run does **not** flag the `SYNTH_`-prefixed corpus. That is the test that keeps the allowlist honest in both directions, and it is the one place where the `condition = "AND"` semantics get exercised.

## ADR Mechanics

### Location, format and numbering

| Question | Recommendation | Reasoning |
|---|---|---|
| Where | `docs/adr/` | [CITED: ARCHITECTURE.md "Recommended Repository Structure" — `docs/  # §75 set + adr/`]. README §75 lists `docs/` without `adr/`; this is an addition, not a conflict. |
| Format | **MADR** (Markdown Architectural Decision Records), current version **4.0.0** | [CITED: adr.github.io/madr]. MADR is a superset of Nygard — every Nygard ADR is a valid MADR. Its `## Considered Options` / `### Consequences of the Options` structure is what this project actually needs, because nearly every decision here is *"the obvious choice is dead upstream, here is the fork/alternative and here is the escape hatch"*. Nygard's four-section form has nowhere to put the rejected options, and the rejected options are the whole content of e.g. the MinIO decision. |
| Variant | **MADR 4.0 minimal**, extended with a `Migration trigger` heading | The full template's `decision-makers`/`consulted`/`informed` RACI fields are noise for a single-author project [CITED: MADR 4.0 provides both full and minimal templates, each annotated or bare]. `Migration trigger` is bespoke and earns its place: several of this project's decisions are explicitly provisional. |
| Numbering | Zero-padded four digits, monotonic, never reused: `0001-…`, `0002-…` | Matches MADR's own `0000-use-markdown-architectural-decision-records.md` convention. |
| Superseding | Never edit a decided ADR's decision. Add a new ADR and set the old one's `status: superseded by 00NN` | Standard MADR practice; matters here because README is a fixed spec and the ADR log is the *only* record of departures from it. |
| Tooling | None (`adr-tools` not required) | `adr-tools` is a shell helper for creating and linking files. With a template and a `docs/adr/README.md` index, it adds a dependency for negligible benefit — and it is not installed [ASSUMED: not probed]. |

### Template (`docs/adr/0000-template.md`)

```markdown
---
status: {proposed | accepted | rejected | deprecated | superseded by ADR-00NN}
date: YYYY-MM-DD
---

# ADR-00NN: {short title, a decision phrased as a claim}

## Context and Problem Statement

{What forces are in play? Which README section or research finding does this touch?}

## Considered Options

* Option A
* Option B

## Decision Outcome

Chosen option: "{A}", because {justification}.

### Consequences

* Good, because …
* Bad, because …
* Neutral, because …

## Migration trigger

{What observable event would make us revisit this? "None — this is permanent" is a
valid answer and must be written explicitly rather than left blank.}

## References

* README §NN
* .planning/research/{FILE}.md §{section}
```

### Which ADRs belong in Phase 1

| # | Title | Phase 1? | Reasoning |
|---|---|---|---|
| 0001 | Record architecture decisions (MADR, `docs/adr/`) | **Yes** | The meta-ADR. Without it the numbering and format are folklore. |
| 0002 | `dataplat` core with `csv_processor` as a plugin, superseding README §68/§75 | **Yes — mandated by locked decision 1** | The whole point is that Phase 10 does not re-litigate it when `cdc/` and `scd/` land. Must cite ARCHITECTURE.md §4.1's six-problem critique and state explicitly that §29/§95 ("add non-CSV sources without redesigning") is the driver. |
| 0003 | uv workspace with members under `packages/`, not `src/` | **Yes** | Small, but it is a visible departure from ARCHITECTURE.md's own diagram and from README §75. Someone *will* ask. Two paragraphs. Could reasonably be folded into 0002. |
| 0004 | Two images, two dependency sets; `csv_processor` is never installed into the Airflow image | **Yes** | [CITED: PITFALLS G5 — "one chance to get the split right", retrofit cost MEDIUM]. The decision is *made* in Phase 1 by keeping Airflow out of the workspace; recording it where the reasoning is fresh is free. Deferring it to Phase 4 means the Dockerfile author re-derives it. |
| 0005 | The fixture corpus is generated from a seed, not committed | **Yes** | [CITED: PITFALLS cheap-now decision #15]. This is the phase's headline design decision and the determinism rules need a home that is not a test docstring. |
| 0006 | MinIO: pin the `pgsty` community fork; SeaweedFS is the named migration target | **Defer to Phase 2** | [CITED: STACK.md §D — "Add a Phase-1 ADR recording this"]. STACK.md says Phase 1, but the decision is only *actionable* when the values file exists, and its `Migration trigger` (the fork going stale) is meaningless before anything runs on it. Recommend Phase 2, where INFRA-05 lands. |
| 0007 | Helm 4.2.3 over Helm 3.21.3 | **Defer to Phase 2** | [CITED: STACK.md — Helm 4 vs Helm-3 charts is the MEDIUM-confidence call; STATE.md lists it as a Phase-2 concern with a documented fallback]. Writing an ADR before the compatibility gate runs would record a guess as a decision. |
| 0008 | Vault is BUSL-1.1/IBM-owned; OpenBao is the API-compatible escape hatch | **Defer to Phase 5** | [CITED: STACK.md "Conflicts" table, severity LOW]. |

**Recommendation: five ADRs in Phase 1 (0001–0005), all of which record decisions this phase actually makes.** Writing 0006–0008 now would violate the principle the ADR log exists to serve — an ADR records a decision that has been *taken*, not one that has been previewed.

## Common Pitfalls

### Pitfall 1: `strict = false` in a mypy per-module override does nothing

**What goes wrong:** `[[tool.mypy.overrides]] module = ["tests.*"]` with `strict = false` leaves the module fully strict.
**Why it happens:** mypy accepts the key without complaint — no warning, no error, no non-zero exit. It simply has no per-module effect.
**Evidence:** [VERIFIED: with `strict = true` globally and `strict = false` for `pkg.loose`, `mypy 2.3.0` still reported `error: Function is missing a type annotation [no-untyped-def]`. Replacing it with `disallow_untyped_defs = false` / `disallow_incomplete_defs = false` / `check_untyped_defs = false` produced `Success: no issues found`.]
**How to avoid:** enumerate the individual flags. STACK.md §I's snippet must be corrected before it is copied into `pyproject.toml`.
**Warning signs:** hundreds of `no-untyped-def` errors in `tests/` on the very first `make typecheck`, prompting someone to reach for `--ignore-errors` or delete the whole mypy step.

### Pitfall 2: `select = ["ALL"]` fires `CPY001` on every file

**What goes wrong:** the first `ruff check` reports "Missing copyright notice at top of file" once per file, drowning the real findings.
**Evidence:** [VERIFIED: 4 of 4 files in the probe repo, including `__init__.py`.]
**How to avoid:** add `"CPY001"` to `ignore`. Do **not** respond by narrowing `select` away from `ALL` — `ALL` is what keeps `T20`, `ANN`, `D`, `S`, `BLE`, `TRY`, `DTZ` and `LOG` on without enumerating them, and those eight map directly to README §69–§71 [CITED: STACK.md §I].

### Pitfall 3: the secret-scanner canary that proves nothing

**What goes wrong:** the negative test uses `AKIAIOSFODNN7EXAMPLE`, gitleaks reports clean, and the team concludes the scanner works because "CI is green and we tested it".
**Why it happens:** the AWS documented example key is on gitleaks' stopword list precisely so it does not spam every tutorial repository.
**Evidence:** [VERIFIED: a commit containing that key plus its example secret produced `no leaks found`, exit 0, from gitleaks 8.30.1 with `useDefault = true`.]
**How to avoid:** use randomised-looking values; assert exit code 1 *and* assert the reported rule IDs.

### Pitfall 4: `fetch-depth` defaults defeat the full-history scan

**What goes wrong:** `actions/checkout` defaults to depth 1; `gitleaks git --log-opts="--all"` then scans one commit and reports clean. SEC-02 is claimed, not proven.
**Evidence:** [VERIFIED: gitleaks logged `1 commits scanned` on a fresh single-commit repository.]
**How to avoid:** `fetch-depth: 0` on the secrets job — and assert the commit count in the job output, so a future checkout change is visible.
**Warning signs:** the secret-scan job completing suspiciously fast as the repository grows.

### Pitfall 5: a shared RNG stream couples every fixture to every other

**What goes wrong:** one `Random(SEED)` drives all fixtures in manifest order. Inserting fixture 30 shifts the stream and rewrites the bytes of 31–70, so `CORPUS.sha256` shows 40 changed digests in a PR that added one fixture. Review becomes impossible and someone regenerates without looking.
**How to avoid:** R1 — derive each fixture's seed from `sha256(master_seed | fixture_name)`. Adding a fixture then changes exactly one digest line.
**Warning signs:** a `CORPUS.sha256` diff far larger than the `corpus.yaml` diff. This is worth a policy assertion in its own right.

### Pitfall 6: archive fixtures that are never byte-identical

**What goes wrong:** `61_gzipped.csv.gz` fails `make fixtures-verify` on every run, on every machine, for no visible reason.
**Why it happens:** gzip embeds the current mtime and the source filename in its header; zip embeds a DOS timestamp per entry.
**Evidence:** [VERIFIED: default-`mtime` gzip of identical input, 1.1 s apart, produced digests `a8ed9bf047b3ec77` and `dc784cb3c6741cc9`; `mtime=0` produced `7cbae7d96605d614` both times.]
**How to avoid:** R5. Also set `filename=""` — otherwise the name of the temp file leaks into the header.

### Pitfall 7: locale- and hash-seed-dependent generation

**What goes wrong:** the corpus verifies on the dev box and fails in CI, or vice versa.
**Why it happens:** `open(path, "w")` picks up `locale.getpreferredencoding(False)`; `str.__hash__` is `PYTHONHASHSEED`-salted, so any `set` iteration in the generator varies per process.
**Evidence:** [VERIFIED: the R1–R10 prototype produced identical digests under `TZ=America/New_York LC_ALL=C.UTF-8 PYTHONHASHSEED=99`. This confirms the rules work; it does not make the hazard hypothetical, since `locale.getpreferredencoding(False)` returned `UTF-8` here and will not on every runner.]
**How to avoid:** R3, R7, R8.

### Pitfall 8: the corpus fights the secret scanner, and the scanner loses

**What goes wrong:** gitleaks' generic-high-entropy and IBAN/card rules fire on synthetic account numbers and long identifiers. Someone adds a global allowlist regex, and six months later a real key ships.
**Evidence:** [CITED: PITFALLS G3]. [VERIFIED: a path-scoped + `SYNTH_`-anchored allowlist kept a corpus containing a synthetic token and a Polish IBAN silent, while still reporting four genuine findings in `src/`.]
**How to avoid:** path-scope every allowlist; anchor on a `SYNTH_` prefix that the generator emits by construction; use `condition = "AND"`; and assert both directions in the self-test.

### Pitfall 9: adding `apache-airflow` to the uv workspace

**What goes wrong:** the workspace resolves one lockfile for all members, so Airflow's ~600 constraint pins (`pandas==2.1.4`, `psycopg2-binary==2.9.12`, `polars==1.42.1`) become the ETL library's constraints.
**Evidence:** [CITED: STACK.md §F "two images, two dependency sets" and the Version Compatibility Matrix; PITFALLS G5]. [VERIFIED: a uv workspace produces a single `uv.lock` and a single `.venv` shared by all members.]
**How to avoid:** the Airflow image installs providers via `--constraint` in its own Dockerfile, entirely outside the workspace. The `[tool.uv.workspace] members` list should be explicit (not a `packages/*` glob) so a future `packages/airflow-dags/` cannot join by accident.

### Pitfall 10: the `.dockerignore` that does not exist yet

**What goes wrong:** Phase 4's first `docker build` uploads the whole 241 MB fixture tree as build context, and any fixture change busts every layer cache.
**Evidence:** [CITED: PITFALLS G3 problem 2].
**How to avoid:** write `.dockerignore` in Phase 1 even though no Dockerfile exists. It costs eight lines and cannot be forgotten later. `tests/fixtures/csv/` being gitignored helps but does not fully protect the build context, since `docker build` reads the working tree.

## Validation Architecture

Test framework detection: **no test infrastructure exists** — the repository contains only `README.md`, `.gitignore`, `.claude/` and `.planning/` [VERIFIED: `ls -a` at repo root]. Everything below is Wave 0.

### Test Framework

| Property | Value |
|---|---|
| Framework | `pytest` 9.1.1 [CITED: STACK.md §G] |
| Config file | none — **Wave 0** creates `[tool.pytest.ini_options]` in the root `pyproject.toml` |
| Quick run command | `uv run --frozen pytest tests/unit tests/policy -q` |
| Full suite command | `make check` (lock-check → lint → format → typecheck → imports → policy → unit → fixtures-verify) |
| CI superset | `make ci` (= `make check` + `gitleaks` + `gitleaks-selftest`) |

### Phase Requirements → Test Map

| Req ID | Observable signal that proves it holds | Mechanism (exact command / CI step) | Cadence | File exists? |
|---|---|---|---|---|
| **QUAL-01** | A function without complete annotations anywhere in `packages/*/src` or `tools/` fails the build | `uv run mypy packages/dataplat/src packages/csv-processor/src tools` under `strict` (`disallow_untyped_defs` + `disallow_incomplete_defs`) — [VERIFIED: emits `error: Function is missing a type annotation [no-untyped-def]`]. Backed by ruff `ANN001`/`ANN201` — [VERIFIED]. Meta-test: `tests/policy/test_gates_actually_fail.py::test_untyped_def_is_rejected` runs mypy against a fixture module containing `def f(x): return x` and asserts rc≠0 | Continuous (every PR) | ❌ Wave 0 |
| **QUAL-02** | A public class/function/method without a docstring fails the build | `uv run ruff check .` with `D` selected and `convention = "google"` — [VERIFIED: `D103` on a public function, `D102` on a public method, **silent** on `_private` names, so the rule scopes to public API with no extra config] | Continuous | ❌ Wave 0 |
| | ⚠️ **Partial.** ruff proves a docstring *exists*. README §69 demands it describe purpose, parameters, returns, assumptions, exceptions and side effects. `D417` (undocumented-param) partially covers parameters under the Google convention; **assumptions, exceptions and side effects are not mechanically checkable.** | Residual is a **review rule**: a PR-template checkbox plus a `docs/` note. Do not claim more. | Review-time | — |
| **QUAL-07** | Every bug fix lands with a test under `tests/regression/` naming the bug | `tests/regression/conftest.py` fails collection for any test file lacking a `# BUG:` provenance line; PR template carries a "regression test added / N/A because…" checkbox | Continuous (structural) + review-time (did the bug warrant one) | ❌ Wave 0 |
| | ⚠️ **Partial.** The directory convention and the marker check are mechanical. "Every *important* discovered bug" is a judgement no linter can make. | — | — | — |
| **QUAL-08** | `make fixtures` on a clean checkout reproduces every fixture byte-for-byte against the committed digests | `make fixtures-verify` → `python -m tools.corpus verify` regenerates to a temp dir and compares SHA-256 to `tests/fixtures/CORPUS.sha256`; exit 1 on any mismatch. Reinforced by `tests/policy/test_corpus_determinism.py` which generates **twice in-process** and asserts equality, and by `test_generator_uses_only_random_random.py` enforcing R2 | Continuous | ❌ Wave 0 |
| | Second signal: no corpus file is committed | `tests/policy/test_corpus_not_committed.py` runs `git ls-files tests/fixtures/csv` and asserts empty | Continuous | ❌ Wave 0 |
| **CICD-01** | A push/PR triggers a run | Existence + validity of `.github/workflows/ci.yml`; a green run on the phase's own PR | One-shot (phase acceptance) | ❌ Wave 0 |
| **CICD-02** | The PR gate is the *full* gate, not a subset | `tests/policy/test_ci_invokes_make_only.py` (no workflow step invokes a gate directly) + `test_ci_calls_make_ci.py` (the `check` job's substantive step is `make ci` or `make check`) + `make ci` depends on `make check` | Continuous | ❌ Wave 0 |
| **CICD-03** | ruff runs and can fail the build | `make lint` in `make check`; meta-test `test_gates_actually_fail.py::test_print_is_rejected` runs ruff against a fixture file containing `print()` and asserts rc≠0 | Continuous | ❌ Wave 0 |
| **CICD-04** | mypy runs and can fail the build | `make typecheck` in `make check`; meta-test as under QUAL-01 | Continuous | ❌ Wave 0 |
| **SEC-02** | A full-history scan of every ref reports zero findings | `gitleaks git --log-opts="--all" --redact --no-banner --exit-code 1 .` on a `fetch-depth: 0` checkout, **plus** `gitleaks dir` for the working tree (uncommitted files are outside `git log -p`) — [VERIFIED: both forms executed; detection and exit codes confirmed] | Continuous now; move the `--all` form to `push: main` + `schedule:` if the job ever exceeds ~60 s | ❌ Wave 0 |
| | Guard against the silent-pass mode | `tests/policy/test_secret_scan_depth.py` asserts `fetch-depth: 0` is present on the secrets job — [VERIFIED as a real hazard: depth-1 yields `1 commits scanned`] | Continuous | ❌ Wave 0 |
| **SEC-10a** | CI holds no long-lived credentials | `tests/policy/test_workflow_secrets.py`: every `secrets.<NAME>` reference in `.github/workflows/*.yml` is in the allowset `{GITHUB_TOKEN}` — in Phase 1 the set is **empty**. Plus: workflow-level `permissions: contents: read` present, and no job widens it | Continuous | ❌ Wave 0 |
| **SEC-10b** | The scanner itself never prints a secret it finds | `--redact` on every gitleaks invocation — [VERIFIED: the JSON report's `Secret` field was literally `REDACTED`, and the raw value appeared 0 times in stdout]. Asserted by `test_workflow_secrets.py::test_gitleaks_always_redacts` | Continuous | ❌ Wave 0 |
| **SEC-10c** | No job dumps its environment | Same policy test greps `run:` blocks for `set -x`, `set -o xtrace`, bare `env`, `printenv`, `declare -p` | Continuous | ❌ Wave 0 |
| | ⚠️ **Honest limit.** SEC-10's general form — "no CI job ever echoes a secret value" — is **not decidable**. A future step doing `curl -H "Authorization: Bearer $TOKEN" -v` leaks via verbose output and matches no grep. What *is* decidable, and is fully true in Phase 1, is the stronger structural claim: **this workflow references no secret at all.** State the requirement that way in VERIFICATION.md and re-audit whenever a secret is first introduced (Phase 11). | Structural check continuous; general form is **review-time** | Continuous + review | — |
| **SEC-11** | A commit containing a credential fails the build | `make gitleaks` (rc 1 on detection) **and** `make gitleaks-selftest`, which builds a disposable repo with non-example canaries and asserts rc 1 plus the expected rule IDs, and asserts the `SYNTH_` corpus is *not* flagged — [VERIFIED: 4 findings / rc 1 with realistic values; 0 findings / rc 0 with the AWS example key, which is why the canary choice is a task-level requirement, not a detail] | Continuous | ❌ Wave 0 |
| **OBS-03** | A `print()` in library code fails the build | ruff `T20` (`T201` `print`, `T203` `pprint`) with `select = ["ALL"]` — [VERIFIED: fired in `src/`, correctly silent in `scripts/` via `per-file-ignores`]. Meta-test as under CICD-03 | Continuous | ❌ Wave 0 |
| | Guard against the carve-out widening | `tests/policy/test_print_ban_scope.py` asserts `T20` appears in `per-file-ignores` for at most `scripts/**` and `tools/corpus/__main__.py`, and never in the top-level `ignore` list | Continuous | ❌ Wave 0 |

**Cross-cutting note on meta-tests.** Six of the twelve requirements are satisfied by "a linter is configured". A configured linter that has never been observed to fail is indistinguishable from a disabled one — which is the same argument PITFALLS G3 makes about the secret scanner, generalised. `tests/policy/test_gates_actually_fail.py` should run each gate against a small deliberately-bad fixture in `tests/policy/badsamples/` and assert non-zero exit. That single file converts "the gate is configured" into "the gate is observed to work" for `print()`, untyped defs, missing docstrings and forbidden imports, and it is the highest-value test in the phase. `tests/policy/badsamples/` must be excluded from the main ruff/mypy runs via `extend-exclude`.

### Sampling Rate

- **Per task commit:** `uv run --frozen pytest tests/unit tests/policy -q` (target < 10 s)
- **Per wave merge:** `make check` (adds lock-check, ruff, mypy, import-linter, `fixtures-verify` including the 241 MB fixture — target < 90 s)
- **Phase gate:** `make ci` green on a real pull request, with the `secrets` job on `fetch-depth: 0`, before `/gsd-verify-work`

### Wave 0 Gaps

- [ ] Framework install: `uv sync --all-packages` after the root `pyproject.toml` exists — no test framework is currently present
- [ ] `pyproject.toml` `[tool.pytest.ini_options]` — testpaths, markers, `--strict-markers`, `--strict-config`
- [ ] `tests/conftest.py` — repo-root fixture, `tmp_path`-based helpers
- [ ] `tests/policy/test_no_postgres_csv_parsing.py` — LOAD-12 (live before Phase 4)
- [ ] `tests/policy/test_ci_invokes_make_only.py` + `test_ci_calls_make_ci.py` — CICD-02
- [ ] `tests/policy/test_workflow_secrets.py` — SEC-10a/b/c
- [ ] `tests/policy/test_secret_scan_depth.py` — SEC-02
- [ ] `tests/policy/test_print_ban_scope.py` — OBS-03
- [ ] `tests/policy/test_pinned_tool_versions_agree.py` — gate-strength drift
- [ ] `tests/policy/test_corpus_determinism.py` + `test_corpus_not_committed.py` + `test_generator_uses_only_random_random.py` — QUAL-08
- [ ] `tests/policy/test_gates_actually_fail.py` + `tests/policy/badsamples/` — meta-verification for QUAL-01, QUAL-02, OBS-03, CICD-03, CICD-04
- [ ] `tests/regression/README.md` + `tests/regression/conftest.py` — QUAL-07 policy, tree intentionally otherwise empty
- [ ] `tools/security/gitleaks_selftest.py` — SEC-11
- [ ] `tools/corpus/` (manifest models, generators, `__main__`) + `tests/fixtures/corpus.yaml` + `tests/fixtures/CORPUS.sha256` — QUAL-08
- [ ] `tests/unit/test_corpus_manifest.py` — validates the manifest model itself (a corrupt manifest must fail loudly, not generate a corrupt corpus)

## Security Domain

`security_enforcement: true`, `security_asvs_level: 1` [VERIFIED: `.planning/config.json` lines 46–49]. This phase builds no application surface — it builds the controls that guard one. ASVS applicability is therefore mostly "not yet", and saying so explicitly is more useful than inventing coverage.

### Applicable ASVS Categories

| ASVS Category | Applies to Phase 1 | Standard control |
|---|---|---|
| V2 Authentication | No | No auth surface exists. First relevant in Phase 5 (Vault Kubernetes auth). |
| V3 Session Management | No | No sessions. |
| V4 Access Control | **Partly** | CI authorisation only: `permissions: contents: read` at workflow level, no job elevation, no `secrets.*` reference. GitHub branch protection requiring the `check` and `secrets` jobs is the enforcement point [ASSUMED: branch protection is a repository setting, not a repo file — it cannot be verified from the working tree and must be a `checkpoint:human-verify` task]. |
| V5 Input Validation | **Partly** | The only untrusted input this phase processes is `tests/fixtures/corpus.yaml`, parsed by the generator. Use `yaml.safe_load` (never `yaml.load`) and validate into a Pydantic model with `extra="forbid"` [CITED: STACK.md §F — `ConfigDict(extra="forbid", frozen=True)` "catches config typos, which is the single most common ETL outage cause"]. |
| V6 Cryptography | **Partly** | SHA-256 for fixture digests and for RNG seed derivation. Both are integrity/derivation uses, not secrecy — no key material, no randomness-for-security. Explicitly: the corpus PRNG is `random`, **not** `secrets`, and that is correct, because reproducibility is the requirement. This distinction should be a comment in the generator so nobody "fixes" it. |
| V7 Error Handling & Logging | **Partly** | `gitleaks --redact` so findings never carry the secret into a log [VERIFIED]. The broader OBS-05 redaction layer is Phase 3. |
| V14 Configuration | **Yes** | Pinned dependencies (`uv.lock` + `uv lock --check`), SHA-pinned GitHub Actions, checksum-verified gitleaks download, least-privilege workflow permissions. |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard mitigation | Phase 1 status |
|---|---|---|---|
| Credential committed to git history | Information Disclosure | gitleaks full-history scan; pre-commit hook to prevent the history rewrite | **Implemented** — [VERIFIED end to end] |
| Secret-scanner disabled by an over-broad allowlist | Repudiation / Info Disclosure | Path-scoped `[[allowlists]]` with `condition = "AND"`; self-test asserting real secrets still fire | **Implemented** — the self-test is the control that keeps the control honest |
| Mutable action tag repointed to malicious code | Tampering | Pin actions by commit SHA | **Implemented** — SHAs resolved this session |
| Compromised tool download (gitleaks tarball) | Tampering | Verify the published SHA-256 checksum before extraction | **Implemented** — [VERIFIED: `gl.tgz: OK`] |
| Dependency confusion / typosquat in `uv.lock` | Tampering | `uv.lock` pins exact versions and hashes; Dependabot for updates; the Package Legitimacy Audit above | **Implemented** |
| Secret leaked via CI log output | Information Disclosure | Zero `secrets.*` references; `--redact`; no `set -x` | **Partial by construction** — see the SEC-10 honest limit above |
| Malicious PR reading repository secrets | Elevation of Privilege | `pull_request` (not `pull_request_target`); `permissions: contents: read`; no secrets in the workflow | **Implemented** — the workflow uses `pull_request`, which does **not** expose secrets to fork PRs |
| Arbitrary code execution via YAML deserialisation of `corpus.yaml` | Elevation of Privilege | `yaml.safe_load` only | **Task-level requirement** |

## Runtime State Inventory

**Not applicable — greenfield phase.** This is the first phase of a project whose repository currently contains only `README.md`, `.gitignore`, `.claude/` and `.planning/` [VERIFIED: `ls -a` at repo root]. There is no rename, refactor or migration, and therefore no stored data, live service config, OS-registered state, secret, or build artifact carrying a prior name. Confirmed by inspection, not assumed.

## State of the Art

| Old approach | Current approach | When changed | Impact here |
|---|---|---|---|
| `black` + `isort` + `flake8` + a dozen plugins | **ruff** (lint + format in one Rust binary) | ruff 0.1+ (2023) onward | One tool, one config block, `select = ["ALL"]` is tractable |
| Poetry `1.x` | **uv** `0.12.3` | uv reached production maturity 2024–2026 | 10–100× resolution; native Airflow-constraints support; `uv.lock` is universal [CITED: STACK.md §F] |
| `requirements.txt` + `pip freeze` | `uv.lock` + `uv lock --check` | — | Drift is a CI check, not a hope [VERIFIED: `uv lock --check` exits 0 in sync] |
| `gitleaks detect` / `gitleaks protect` | **`gitleaks git` / `gitleaks dir` / `gitleaks stdin`** | v8.19.0 | `detect`/`protect` still work but are deprecated and hidden from `--help`. Any tutorial or older CI snippet you find will use `detect` — write the new form [CITED: gitleaks CLI reorganisation, v8.19.0] |
| `[rules.allowlist]` (singular) | **`[[allowlists]]` / `[[rules.allowlists]]`** | v8.21.0 (rule-level), v8.25.0 (global) | Backwards-compatible, but the plural form supports `condition` — which is what makes a path-scoped allowlist precise |
| `mypy 1.x` | **`mypy 2.3.0`** | 2026-07-13 | Major release with stricter defaults [CITED: STACK.md §I]. `--strict` now includes `--extra-checks` and `--strict-equality`, which 1.x users will not expect |
| `pytest 8.x` | **`pytest 9.1.1`** | 2026-06-19 | Major bump; verify plugin compatibility [CITED: STACK.md §G]. [VERIFIED: 9.1.1 resolved and installed cleanly under Python 3.12 alongside a uv workspace] |
| `freezegun` | `time-machine` | — | Faster and more correct [CITED: STACK.md §G] |
| Nygard ADR template | **MADR 4.0.0** | Sept 2024 | Superset of Nygard; adds `Considered Options` and per-option consequences [CITED: adr.github.io/madr] |
| `kubeval` | `kubeconform 0.8.0` | — | Not this phase (Phase 2, CICD-07), but recorded so it is not re-researched [CITED: STACK.md §I] |

**Deprecated / outdated — do not use:**
- `gitleaks detect` / `gitleaks protect` → `gitleaks git` / `gitleaks dir`
- `gitleaks/gitleaks-action@v3` → the binary in a `run:` step (paid licence for org repos) [CITED: STACK.md §I]
- Poetry (1.8.2 is installed on this machine) → uv; STACK.md notes Poetry "can be uninstalled; it is not used"
- `ty` (Astral's type checker) at `0.0.70` → pre-1.0, do not adopt [CITED: STACK.md §I]
- `pytest-airflow` → unmaintained; `dag.test()` + plain pytest [CITED: STACK.md §G] — relevant from Phase 4

## Environment Availability

Probed on this machine this session.

| Dependency | Required by | Available | Version | Fallback |
|---|---|---|---|---|
| Python 3.12 | Everything | ✓ | 3.12.3 (`/usr/bin/python3.12`) | — |
| `uv` | Workspace, lockfile, all `make` targets | ✓ **but stale** | 0.8.11 installed; **0.12.3 required** [CITED: STACK.md §F "upgrade in Phase 1"] | Workspace semantics verified working on 0.8.11, so this is an upgrade task, not a blocker |
| GNU Make | `make check` | ✓ | 4.3 | — |
| `git` | Everything | ✓ | 2.43.0 | — |
| Network (PyPI, GitHub) | `uv sync`, gitleaks download | ✓ | — | Criterion 4 requires `make check` to work with **no network services**; PyPI access for the initial `uv sync` is a one-time bootstrap, and `make check` excludes `gitleaks` for exactly this reason |
| `gh` CLI | Optional (PR creation) | ✓ | 2.45.0 | — |
| `docker` | Not needed this phase | ✓ | 29.7.2 | — |
| `kubectl` | Not needed this phase | ✓ | present | — |
| `ruff` (system) | — | ✗ | — | Comes from `uv.lock` dev group; a system install would be a second source of truth and must **not** be added |
| `mypy` (system) | — | ✗ | — | Same |
| `gitleaks` | `make gitleaks` (CI job) | ✗ | — | Downloaded + checksum-verified into `tools/bin/` by the Makefile and the workflow. [VERIFIED: download and checksum both succeeded this session] |
| `just` | — | ✗ | — | Not used — Make chosen |
| `kind`, `helm` | Phase 2 | ✗ | — | Out of scope for Phase 1 (locked decision 3). Already recorded in STATE.md as a Phase-2 blocker |
| `poetry` 1.8.2 | — | present | 1.8.2 | Should be removed or ignored; must not appear in any Phase-1 artifact |

**Missing dependencies with no fallback:** none.
**Missing dependencies with fallback:** `gitleaks` (downloaded on demand, checksum-verified); `ruff`/`mypy` (from the lockfile, which is correct).
**Action required:** upgrade `uv` 0.8.11 → 0.12.3 as an explicit Phase-1 task, and add a `make` guard that fails with a readable message if `uv --version` is below the pin — otherwise a stale `uv` silently produces a lockfile a newer `uv` rejects.

## Assumptions Log

Claims tagged `[ASSUMED]` in this document. Each needs user confirmation or a verification task before it becomes a locked decision.

| # | Claim | Section | Risk if wrong |
|---|---|---|---|
| A1 | `condition = "AND"` in a gitleaks `[[allowlists]]` block requires *both* `paths` and `regexes` to match. Read from the README schema; the verified run used the default `OR` with only the corpus present, so it did not distinguish the two | Code Examples → `.gitleaks.toml` | **HIGH.** If `AND` behaves as `OR`, the `SYNTH_` regex allowlists that prefix repository-wide — the exact global-allowlist failure PITFALLS G3 warns about. **Mitigation: a task asserting a `SYNTH_` value outside `tests/fixtures/` is still reported.** |
| A2 | `import-linter`'s upstream repository is `github.com/seddonym/import-linter` — PyPI `project_urls` is empty | Package Legitimacy Audit | LOW. The package was executed and behaved correctly; only the provenance link is unconfirmed |
| A3 | `adr-tools` is not installed on this machine (not probed) | ADR Mechanics | NONE — the recommendation is to not use it |
| A4 | GitHub branch protection requiring the `check` and `secrets` status checks is a repository *setting* and cannot be verified from the working tree | Security Domain → V4 | MEDIUM. Without it, CI is advisory and criterion 1 ("opening a PR runs … and a commit containing a fake credential fails the build") is only half true — the build fails but the merge is not blocked. **Needs a `checkpoint:human-verify` task.** |
| A5 | `hatchling` rather than `uv_build` as the build backend — chosen because STACK.md's example uses it, not because `uv_build` was evaluated | Standard Stack | LOW. Both are supported; switching later is a two-line change per member |
| A6 | The ~50-fixture Phase-1 target (full §73 + byte-level-hard + four decision-pinning semantic ones) is the right scope split | Fixture Corpus → Coverage | MEDIUM. Too few and Phase 6 discovers a missing generator capability and must re-baseline every digest; too many and Phase 1 becomes a fixture-authoring project. **Worth an explicit user decision.** |
| A7 | Running the **full-history** gitleaks scan on every PR is affordable indefinitely — extrapolated from ~85 ms on a 3-commit repository | Code Examples → workflow | LOW. Degrades gradually and visibly; the documented split (diff on PRs, `--all` on main + schedule) is the remedy |
| A8 | `python 3.12` is the only interpreter that will ever run the generator, so R2's cross-version guarantee is untested-but-relied-upon | Fixture Corpus → R2 | LOW-MEDIUM. The CPython guarantee is explicit. The R2 policy test is the insurance |

## Open Questions (RESOLVED)

1. **Should QUAL-02 be enforced beyond docstring presence?**
   - What we know: ruff `D` proves presence and scopes correctly to public names [VERIFIED]. `D417` checks documented parameters under the Google convention.
   - What's unclear: README §69 also demands *assumptions*, *exceptions* and *side effects*. `pydoclint` cross-checks `Raises:` sections against actual `raise` statements, which would cover "exceptions" mechanically.
   - Recommendation: ship ruff `D` + `D417` in Phase 1; record `pydoclint` as a candidate and revisit in Phase 3 when `dataplat/errors.py` gives it something real to check. Do not add a second docstring tool to an empty codebase.

2. **How many fixtures does Phase 1 actually author?** (see A6)
   - What we know: 69 are named across README §73 and FEATURES.md §3.4. The byte-level-hard subset must be in Phase 1 because it drives generator capability.
   - What's unclear: whether the ~19 plain-text semantic fixtures are better authored now (corpus-as-spec leads implementation) or in Phase 6 alongside the normaliser that consumes them.
   - Recommendation: ~50 in Phase 1. Surface this to the user — it is the largest single sizing lever in the phase.

3. **Does the `expect:` block belong in `corpus.yaml` in Phase 1, or is it premature?**
   - What we know: it is what makes "the corpus is the specification" true, and Phase 6's detector tests become a parametrised loop over it.
   - What's unclear: some `expect:` fields name concepts (`encoding_confidence_min`, `quarantine_reason`) whose vocabulary is fixed in Phases 6 and 8.
   - Recommendation: include `expect:` with a permissive schema (`extra` fields allowed *inside* `expect:` only, while the outer manifest stays `extra="forbid"`), so vocabulary can grow without a manifest-model migration. This is the one place to relax `extra="forbid"`, and the reason should be a comment.

4. **Should `airflow/dags/` exist at all in Phase 1?**
   - What we know: import-linter contract 2 ("nothing may import the DAG folder") is free to add now and is ARCHITECTURE.md's mechanical guarantee behind §6.4.
   - What's unclear: an empty package with no modules makes the contract vacuous, and import-linter needs `dags` to be importable as a root package.
   - Recommendation: create `airflow/dags/` with a `.gitkeep` but **defer contract 2 to Phase 4**, when the first DAG exists. Contract 1 (`dataplat` ⊁ `csv_processor`) is non-vacuous immediately and should ship now.

5. **Coverage threshold — now or later?**
   - What we know: CICD-05 (coverage reporting) is Phase 11, not Phase 1.
   - Recommendation: install `pytest-cov` and emit a report in Phase 1, but set **no failure threshold**. A threshold on a codebase that is ~95 % config files produces a meaningless number that people then game.

### Resolutions

All five questions were closed during planning, and the plans cite the resolutions inline. Recorded
here so the section is not read as still-open work.

| # | Resolution | Where it is implemented |
|---|------------|-------------------------|
| 1 | **Adopted as recommended.** Ship ruff `D` + `D417` in Phase 1; no second docstring tool. `pydoclint` stays a Phase-3 candidate, revisited when `dataplat/errors.py` gives it real `raise` statements to cross-check. The honest limit — presence is mechanical, quality is not — is recorded in `01-VALIDATION.md` and must not be marked green on a passing lint run | `01-01-PLAN.md` task 1 (ruff config, `D417` confirmed enabled under the Google convention); `01-05-PLAN.md` task 1 (the `missing_param_doc` bad sample proves it fires) |
| 2 | **SUPERSEDED by user decision.** The recommendation was ~50 fixtures in Phase 1 with the ~19 plain-text semantic ones deferred to Phase 6. The developer decided during planning to author **all 69 in Phase 1**, so the corpus is complete before any implementation reads it. This also closes assumption **A6**: the scope split it hedged against no longer exists, and with it the risk that Phase 6 discovers a missing generator capability and has to re-baseline every digest. The cost is that Phase 1 is a nine-plan phase; that was accepted knowingly | `01-03-PLAN.md` (5 fixtures + the framework), `01-06-PLAN.md` (16 → 21), `01-07-PLAN.md` (31 → 52), `01-08-PLAN.md` (17 → 69, with a completeness assertion that fails on a gap or an invention) |
| 3 | **Adopted as recommended.** `expect:` ships in Phase 1 with a permissive sub-schema: the outer manifest forbids unknown keys, `expect:` accepts them, so vocabulary fixed in Phases 6 and 8 is writable today without a model migration. The relaxation carries a comment explaining itself, because an unexplained relaxation is the one a later reader widens | `01-03-PLAN.md` task 1 (the model and the comment); `01-06-PLAN.md` task 2 |
| 4 | **Adopted as recommended.** `airflow/dags/` is created with a `.gitkeep` in Phase 1; import-linter **contract 2** is deferred to Phase 4, when the first DAG makes it non-vacuous. Contract 1 (`dataplat` must not import `csv_processor`) ships now and is non-vacuous immediately | `01-01-PLAN.md` task 1 (`setup.cfg`, contract 1 only, with the deferral recorded as a comment) and task 2 (the directory) |
| 5 | **Adopted as recommended.** `pytest-cov` is installed and the `test` target emits a report, with **no failure threshold**. CICD-05 lands in Phase 11 | `01-01-PLAN.md` task 1 (the `test` target) |

**Related assumption also resolved: A4 (branch protection).** A4 recorded that requiring the
`check` and `secrets` status checks is a repository setting that "cannot be verified from the
working tree", and called for a `checkpoint:human-verify`. It is **resolved**: the developer made
the repository public during planning, so the branch-protection API is reachable and the rule's
*contents* are now read back automatically rather than attested by a human. Plan `01-09-PLAN.md`
task 1 applies the rule through the API, asserts both required check names and `enforce_admins`,
and records the applying and reversing commands in `docs/ci-branch-protection.md`. One claim
remains genuinely human-only and stays that way — that a failing required check blocks the **merge
button**, not merely the build — and it is carried as a `<human-check>` in `01-09` task 2 and in
`01-VALIDATION.md` § Manual-Only Verifications.

## Sources

### Primary (HIGH confidence)

- `.planning/research/STACK.md` §F (Python/packaging), §G (testing), §I (CI/CD, linting & typing, security scanning) — all versions cited from here per the quality gate
- `.planning/research/PITFALLS.md` — cheap-now decisions table (#15), §G1–G5 (CI/CD)
- `.planning/research/ARCHITECTURE.md` §4.1–4.2 (package decomposition critique and the `dataplat`/`csv_processor` split), "Recommended Repository Structure", "Structure rationale"
- `.planning/research/FEATURES.md` §3.4 (the 40 fixture additions to §73's 29)
- `.planning/REQUIREMENTS.md` (verbatim text of all 12 phase requirements), `.planning/config.json`, `.planning/STATE.md`
- `README.md` §69–§79 (engineering standards, logging, error handling, testing, corpus, repo structure, CI/CD, container engineering, git practices)
- `./.claude/CLAUDE.md` (project constraints)
- Context7 `/astral-sh/uv` — workspace concepts: `[tool.uv.workspace] members`, `[tool.uv.sources] … { workspace = true }`, member directory layout, `--no-install-workspace` + `--frozen` for Docker
- Context7 `/python/mypy` — per-module overrides, `untyped_calls_exclude`, error-code configuration
- Context7 `/astral-sh/ruff` — pydocstyle convention config, `per-file-ignores`, flake8-annotations settings
- docs.python.org/3/library/random.html — "Notes on Reproducibility", quoted verbatim

### Executed in this session (HIGHEST confidence — direct observation)

- `ruff 0.16.2` — `D101/D102/D103` public-only scoping; `T201` scoping via `per-file-ignores`; `ANN001/ANN201`; `CPY001` firing under `select = ["ALL"]`
- `mypy 2.3.0` — full `--strict` flag list; `disallow_untyped_defs` behaviour; **`strict = false` silently ignored in `[[tool.mypy.overrides]]`**
- `uv 0.8.11` — two-member workspace created, `uv sync --all-packages` succeeded, both packages importable from their member `src/` trees; `uv lock --check`; `uv run`
- `import-linter 2.13` — `forbidden` contract broke on violation (exit 1) and passed when clean (exit 0)
- `gitleaks 8.30.1` — downloaded, SHA-256 verified against the published checksum file, executed: full-history scan, `--redact` masking, path+regex-scoped allowlist, exit codes; **AWS documented example key not detected**
- CPython 3.12.3 — `csv.reader` handles embedded NUL bytes without error; `random.Random(seed)` behaviour; gzip mtime nondeterminism; `resource.setrlimit(RLIMIT_AS)` bounded-memory harness; `zoneinfo` Europe/Warsaw 2026 DST transitions (gap 2026-03-29 02:00–03:00, overlap 2026-10-25 02:00–03:00); generation and SHA-256 throughput
- GitHub API — `actions/checkout@v7.0.1` → `3d3c42e5aac5ba805825da76410c181273ba90b1`; `astral-sh/setup-uv@v9.0.0` → `c771a70e6277c0a99b617c7a806ffedaca235ff9`; `gitleaks v8.30.1` release assets
- PyPI JSON API — `ruff 0.16.2`, `mypy 2.3.0`, `pytest 9.1.1`, `pytest-cov 7.1.0`, `hypothesis 6.165.3`, `import-linter 2.13`, `pre-commit 4.6.2`, `syrupy 5.5.3`, `time-machine 3.4.0`

### Secondary (MEDIUM confidence)

- gitleaks README (via raw.githubusercontent.com) — CLI subcommands, global flags, `[[allowlists]]` schema including `condition`
- WebSearch — gitleaks v8.19.0 CLI reorganisation and `[[allowlists]]` migration history (v8.21.0 / v8.25.0)
- adr.github.io/madr + github.com/adr/madr — MADR 4.0.0, full vs minimal templates, relationship to Nygard

### Tertiary (LOW confidence — flagged in the Assumptions Log)

- `condition = "AND"` semantics (A1) — read, not exercised
- `import-linter` upstream repository URL (A2)

## Metadata

**Confidence breakdown:**

| Area | Level | Reason |
|---|---|---|
| Standard stack & versions | **HIGH** | Cited from STACK.md and independently re-confirmed against PyPI/GitHub; the four gating tools were executed |
| Repository / workspace architecture | **HIGH** | The two-member uv workspace was built and synced end to end, not reasoned about |
| Lint / type gate mechanics (QUAL-01, QUAL-02, OBS-03, CICD-03/04) | **HIGH** | Every rule's behaviour observed directly, including two corrections to STACK.md |
| Fixture determinism (QUAL-08) | **HIGH** | Ten rules, each tied to a mechanism; prototype verified byte-identical across processes, hash seeds, timezones and locales; the one residual (cross-version `random`) rests on an explicit CPython guarantee and is covered by a policy test |
| Fixture *coverage scope* | **MEDIUM** | The ~50-fixture split is a judgement (A6), not a derivation |
| Secret scanning (SEC-02, SEC-11) | **HIGH** | Downloaded, checksum-verified, executed; detection, redaction, allowlist scoping and the example-key trap all observed |
| SEC-10 | **MEDIUM** | Deliberately so — the requirement's general form is undecidable, and this document says so rather than inventing a check |
| CI pipeline shape | **MEDIUM-HIGH** | Action SHAs and gitleaks invocation verified; the workflow as a whole has not been executed on a runner (it cannot be, from a working tree) |
| ADR mechanics | **MEDIUM-HIGH** | MADR 4.0 confirmed from the project's own site; which ADRs belong in Phase 1 is a reasoned judgement |

**Research date:** 2026-08-11
**Valid until:** 2026-09-10 (30 days). The volatile inputs are `uv` (fast minor cadence — re-check the 0.12.3 pin), `ruff` (frequent releases; a new `ALL` rule can appear and break the build, which is an argument for the exact `ruff==0.16.2` pin rather than a range), and the GitHub Action SHAs. The determinism findings, the mypy override trap and the gitleaks example-key behaviour are structural and will not expire.








