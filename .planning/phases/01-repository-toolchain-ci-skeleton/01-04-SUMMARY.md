---
phase: 01-repository-toolchain-ci-skeleton
plan: 04
subsystem: documentation
tags: [adr, madr, decision-log, regression-tests, qual-07, pytest-collection]
status: complete

requires:
  - 01-01 (the walking skeleton whose layout decisions these records justify)
provides:
  - docs/adr/ — MADR 4.0 minimal + `Migration trigger`, with numbering and superseding rules
  - ADR-0001..0005 — every decision this phase actually takes, with rejected options
  - tests/regression/ — QUAL-07 enforced at collection time, tree intentionally empty
affects:
  - Phase 2 (writes the deferred MinIO and Helm records), Phase 4 (inherits ADR-0004's
    Dockerfile ordering), Phase 5 (writes the deferred Vault record), Phase 10 (inherits
    ADR-0002 rather than re-arguing it), and every bug fix from now on

tech-stack:
  added: []
  patterns:
    - An ADR records a decision already taken, never one previewed
    - A decided record's decision is never edited; a reversal is a new superseding record
    - Policy enforced at pytest collection time, not as a warning
    - Every provisional decision states the observable event that would reverse it

key-files:
  created:
    - docs/adr/README.md
    - docs/adr/0000-template.md
    - docs/adr/0001-record-architecture-decisions.md
    - docs/adr/0002-dataplat-core-with-csv-processor-plugin.md
    - docs/adr/0003-uv-workspace-members-under-packages.md
    - docs/adr/0004-two-images-two-dependency-sets.md
    - docs/adr/0005-fixture-corpus-generated-from-a-seed.md
    - tests/regression/README.md
    - tests/regression/conftest.py
    - tests/regression/__init__.py
  modified: []

decisions:
  - MADR 4.0 minimal over Nygard, because Nygard has nowhere to put the rejected
    options and in this project the rejected options are the content.
  - The bespoke `Migration trigger` heading is mandatory; "None — this is permanent"
    must be written out so a blank section always reads as an omission.
  - Only 0001–0005 are written. The object-store fork, the package-manager major and
    the secrets-engine licence are named as deferred with target phases, so their
    absence reads as a decision rather than an oversight.
  - The regression hook uses `pytest_pycollect_makemodule` (verified by probe) rather
    than raising from `pytest_collect_file`, so the error is attributed to the
    offending file and its tests are never collected.
  - The conftest enforces the provenance line only. The `regression` marker stays a
    documented convention, because enforcing two rules invites exceptions to both.
  - pytest's exit-5 on a standalone empty-directory run is documented, not overridden;
    an exit-status override would mask the same status where it means something real.

metrics:
  duration: ~25 min
  completed: 2026-08-11
  tasks: 3
  commits: 3

actuals:
  tokens: 13000
  tasks: 3
  commits: 3
---

# Phase 1 Plan 04: Decision Records & Regression Policy Summary

Five MADR records fixing this phase's departures from README §68/§75 with their
rejected options and reversal triggers, plus QUAL-07 enforced at pytest collection
time while the regression tree is still empty.

## What was built

**Task 1 — ADR mechanics** (`e68eeab`)

`docs/adr/0000-template.md` reproduces the research template verbatim: status
enumeration and date in front matter, then context, considered options, decision
outcome, consequences, **migration trigger**, references.

`docs/adr/README.md` is the index and the rulebook: four-digit monotonic numbering
that is never reused and never renumbered (other records cite these numbers), the
format rationale (MADR is a *superset* of Nygard — every Nygard record is a valid
MADR record — and it adds `## Considered Options`, which is where nearly every
decision in this project lives), and the superseding rule. It also names the three
**deliberately deferred** records with target phases, so their absence is legible as
a decision.

`docs/adr/0001-record-architecture-decisions.md` is the meta-record. Without it the
numbering and format are folklore. Its migration trigger is honest: none foreseen.

No tooling was added. `adr-tools` would be an unpinned dependency for negligible
benefit; a record is created by copying the template.

**Task 2 — records 0002–0005** (`230a249`)

| # | Decision | Migration trigger |
|---|---|---|
| 0002 | `dataplat` core, `csv_processor` as a plugin | None foreseen — deliberately not provisional |
| 0003 | Members under `packages/`, src-layout inside each | If uv changes its workspace convention (cosmetic; low bar to ignore) |
| 0004 | Two images, two dependency sets | The first time a DAG needs a non-trivial library import |
| 0005 | Corpus generated from a seed, oracle committed | `make fixtures-verify` approaching ~30 s |

**0002** is the phase's headline departure. It cites ARCHITECTURE.md §4.1's six
specific problems, names §29/§95 as the driver (an architecture where a future Kafka
CDC source imports through a CSV-named path violates "add non-CSV sources without
redesign" on day one), and weighs the genuinely cheap alternative — one distribution
with `csv_processor/core/` and `csv_processor/sources/csv/` — rejecting it because
that seam is a convention rather than a boundary and the import paths stay CSV-named
in every traceback. Its consequences section states explicitly what **Phase 10
inherits**: CDC and SCD modules live in the core, not under the CSV plugin.

**0003** records the `packages/` layout as departing from ARCHITECTURE.md's diagram
*without departing from its reasoning* — src-layout exists to stop tests importing an
uninstalled source tree, and that property is relative to the distribution root, so
`packages/dataplat/src/dataplat/` preserves it entirely.

**0004** records that the Airflow distribution is deliberately **not** a workspace
member: a workspace resolves one lockfile for all members, so Airflow's ~600
constraint pins would silently become the ETL library's. It carries forward the Phase
4 Dockerfile consequence — dependency layer with `--no-install-workspace --frozen`,
then `--locked` after members are copied — and names the real failure mode as
*drift*, with the third-tiny-distribution remedy rather than "install csv_processor
into the Airflow image".

**0005** records all ten determinism rules by name and mechanism, the
committed-oracle pattern (and why generate-and-trust is the trap that looks
equivalent), and the two things generation buys that a committed corpus cannot: a
size-parameterised large fixture, and a secret scanner that is not drowned. Its
migration trigger carries the measured headroom — generation ~67 MB/s (241 MB ≈ 3.6 s),
SHA-256 ~2 379 MB/s — against the < 90 s `make check` budget, and states that the
response at the trigger is to move the target to the merge gate, **not** to commit
fixtures or add a cache.

**Task 3 — the regression policy** (`ecb1f35`)

`tests/regression/conftest.py` substitutes a failing collector for any test module
lacking a `# BUG:` provenance line, so the file's tests are never collected — a
regression test without provenance is not a test that passes, it is one that must not
land. `tests/regression/README.md` states the policy, the naming convention, what a
regression test is for (reproduce the bug, not the area; do not refactor it later for
elegance), and the honest limitation.

## Verification performed

Every `<verify>` block in the plan was executed.

| Check | Result |
|---|---|
| `ADR_MECHANICS_OK` — template + meta-record exist, template has `Migration trigger` | pass |
| `FIVE_RECORDS` — `ls docs/adr/000[1-5]-*.md \| wc -l` = 5 | pass |
| `ALL_HAVE_TRIGGERS` — `grep -c 'Migration trigger'` on each of 0001–0005 | 5, 1, 1, 1, 1 (0001 documents the heading, hence 5) |
| `Considered Options` present in each of 0001–0005 | 1 each |
| `status: accepted` in each of 0001–0005 | all five |
| `INDEX_UPDATED` — index names 0005 | pass |
| 0002 cites ARCHITECTURE.md §4.1 | 3 citations (body + 2 references) |
| 0002 names Phase 10 | 4 occurrences |
| 0004 states "not a workspace member" | 2 occurrences |
| 0005 enumerates R1–R10 | all ten present |
| No record for the object-store fork / package-manager major / secrets-engine licence | `grep -rln 'pgsty\|SeaweedFS\|OpenBao\|Helm 4' docs/adr/` matches **only** `README.md`'s deferred table |
| `EMPTY_TREE_GREEN` — `make check` with the regression tree empty | pass |
| `PROVENANCE_ENFORCED` — provenance-less module | **rc 2**, `ERROR collecting tests/regression/test_scratch_provenance.py`, message names the file and the exact line to add |
| Same module *with* a provenance line | `1 passed` |
| `TREE_RESTORED_GREEN` — `make check` after removing the scratch module | pass |
| `git ls-files tests/regression` | `README.md`, `__init__.py`, `conftest.py` (see deviation 1) |

**Edge predicate — an empty regression tree must not produce the no-tests-collected
status.** Confirmed by measurement rather than assertion: `make check` runs
`pytest tests/unit` and `pytest tests/policy` explicitly and never names this
directory, and a bare `uv run --frozen pytest` (testpaths = `tests`) exits **0** with
the tree empty. The one case that does report exit 5 — `pytest tests/regression` run
standalone on an empty directory — is documented in the README rather than
suppressed; see deviation 3.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 3 — Blocking] `tests/regression/` needed an `__init__.py`**

- **Found during:** Task 3, first `ruff check tests/regression/`.
- **Issue:** `INP001 File 'tests/regression/conftest.py' is part of an implicit
  namespace package`. Under `select = ["ALL"]` this fails `make lint` for everyone,
  not just for this plan. The inherited findings from 01-01 flagged this exact hazard
  in advance.
- **Fix:** added `tests/regression/__init__.py`, matching `tests/`, `tests/unit/` and
  `tests/policy/`, rather than widening the ignore list. This also removes the
  module-basename collision hazard pytest's default import mode has for identically
  named files in different test directories — which matters more here than elsewhere,
  because regression files are named after bugs and collisions are plausible.
- **Consequence:** the plan's acceptance criterion "`git ls-files tests/regression`
  lists exactly the README and the conftest" now lists three files. The criterion's
  intent — no placeholder tests seeded to make the directory look used — holds: there
  are still zero test modules.
- **Files:** `tests/regression/__init__.py`
- **Commit:** `ecb1f35`

**2. [Rule 1 — Bug avoided] the obvious collection hook would have been the wrong one**

- **Found during:** Task 3, before writing the conftest.
- **Issue:** the natural reading of "fails collection" is to raise from
  `pytest_collectstart`. That hook is invoked *outside* pytest's `CallInfo` wrapper,
  so raising there produces an `INTERNALERROR` — a stack trace, not a policy message.
  The next candidate, raising from `pytest_collect_file`, was probed and does produce
  a clean error, but attributes it to the **directory** (`ERROR collecting regr`) and
  aborts collection of sibling files.
- **Fix:** probed `pytest_pycollect_makemodule` (a `firstresult` hook) returning a
  `pytest.Module` subclass whose `collect()` raises `CollectError`. Verified: the
  error is attributed to the offending **file**, the default collector is replaced so
  none of that file's tests run, and the process exits 2. Both probes were run in the
  scratchpad against pytest 9.1.1 before any repository file was written.
- **Files:** `tests/regression/conftest.py`
- **Commit:** `ecb1f35`

### Judgement calls recorded rather than silently taken

**3. pytest's exit 5 on a standalone empty-directory run is documented, not overridden.**
The plan's edge predicate asks that an empty `tests/regression/` not make "the pytest
run" exit with the no-tests-collected status. Every run the gate performs satisfies
this (measured above). The remaining case — invoking `pytest tests/regression`
directly while it is empty — reports `no tests ran`, exit 5, which is pytest
truthfully reporting the state of the world. Overriding it would mean a
`pytest_sessionfinish` hook rewriting `session.exitstatus`, and that same hook would
mask the identical status in a run where zero tests collected is a genuine failure.
The README states the behaviour and why it is left alone.

**4. The `regression` marker is documented as a convention, not enforced.**
The conftest enforces the provenance line only. Enforcing the marker as well would
have been two lines, and was rejected: the plan's own acceptance criterion requires
that a module *with a provenance line* passes, and one mechanical rule that is always
true is worth more than two that invite exceptions. The README says plainly that the
marker's purpose is selection (`-m regression`) and that it is not a gate.

**5. Verification commands were run as separate invocations.** The plan's `<verify>`
blocks chain steps with `&&`, `;` and `$?` capture. This worktree-isolated agent
refuses compound shell forms, so each was run as a plain command and the exit codes
captured individually. Same checks, same results; no check was skipped or weakened.

## Authentication gates

None. This plan touches no authenticated service.

## Known stubs

None.

`tests/regression/` contains no test modules, and this is **not** a stub: the plan's
own prohibition forbids seeding it with placeholder tests, and its README states that
the emptiness is correct because no bug has been discovered yet. The enforcement
mechanism is live and observed working today.

The three deferred ADRs are likewise not stubs — they are decisions this phase has
not taken. Writing them now would violate the principle the log exists to serve.

## Threat flags

None. This plan creates no network endpoint, auth path, file-access pattern or schema
at a trust boundary.

Every mitigation the plan's threat register assigned was applied:

| Threat | Applied |
|---|---|
| T-01-19 (undocumented departure from the README) | Records 0002–0005 name the departure, the rejected options and the reversal trigger; the index names the three deferred records with target phases |
| T-01-20 (bug fixes landing without a permanent test) | Collection-time provenance enforcement, observed failing; PR checkbox already present from 01-01; the "importance is a judgement" limit is stated, not hidden |
| T-01-21 (a decided record edited in place) | The superseding rule is stated in the index, with `git log docs/adr/` named as the audit trail |
| T-01-22 (a policy hook that never fires) | The hook was observed **rejecting** a provenance-less module (rc 2) and **accepting** a compliant one (1 passed) |

## Requirements addressed

| ID | Mechanism |
|---|---|
| QUAL-07 | Mechanical half complete: `tests/regression/conftest.py` fails collection for any module lacking a `# BUG:` line, observed rejecting one. Review half was already on the PR template (01-01). The judgement half — whether a given bug *warranted* a test — is documented as a review rule in `tests/regression/README.md` and is not claimed as mechanical. |

## Self-Check: PASSED

All 10 created files verified present on disk. All 3 commits verified in `git log`.
Working tree clean at `ecb1f35`; `make check` green; no file deletions in any commit.
