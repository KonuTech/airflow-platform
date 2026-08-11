---
status: accepted
date: 2026-08-11
---

# ADR-0003: Workspace members live under `packages/`, with src-layout preserved inside each member

## Context and Problem Statement

`.planning/research/ARCHITECTURE.md` §4.2 draws the two distributions of ADR-0002
as `src/dataplat/` and `src/csv_processor/` — a single root `src/` directory
holding both packages. That is the conventional src-layout, and its rationale is
sound: an importable package must not be importable from the repository root, so
that tests exercise the *installed* distribution rather than the source tree.

It does not survive contact with uv workspaces. A uv workspace member is
**a directory containing its own `pyproject.toml`**. Two distributions therefore
need two member directories, each with a project file. Keeping a single root
`src/` while also giving each member its own project file produces the member
directory `src/dataplat/` whose package directory is then `src/dataplat/src/dataplat/`
— a doubled path, for nothing.

## Considered Options

* **A — `src/dataplat/`, `src/csv_processor/` as ARCHITECTURE.md draws it,** with
  the per-member `pyproject.toml` files nested inside, accepting the doubled path.
* **B — `packages/dataplat/`, `packages/csv-processor/`,** each member keeping
  src-layout internally (`packages/dataplat/src/dataplat/`). This is uv's own
  documented workspace convention.
* **C — flat layout, no `src/` anywhere:** `dataplat/` and `csv_processor/` at
  the repository root, as README §75 places `csv_processor/`.

## Decision Outcome

Chosen option: **B — members under `packages/`, src-layout preserved inside each
member.** The workspace root declares them explicitly:

```toml
[tool.uv.workspace]
members = ["packages/dataplat", "packages/csv-processor"]
```

Never a glob. A glob would let a future directory — an Airflow DAG package, most
plausibly — join the workspace by accident and drag Airflow's constraint pins into
this lockfile, which is precisely what ADR-0004 exists to prevent.

Option **A** is the same decision as B with a redundant path segment; there is no
argument for it beyond fidelity to a diagram. Option **C** was rejected because it
discards the property that motivated src-layout in the first place: with packages
at the repository root, `import dataplat` succeeds from an uninstalled working
copy, and a test suite can pass against source that was never packaged.

The important point is that **this departs from ARCHITECTURE.md's diagram without
departing from its reasoning.** ARCHITECTURE.md wants src-layout because it
"prevents testing the source tree instead of the installed package". That property
is a function of where the package sits relative to the *distribution root*, not
relative to the repository root — and it is fully preserved by
`packages/dataplat/src/dataplat/`. The rationale is honoured; only the drawing
changed.

### Consequences

* Good, because it is uv's documented convention, verified working end to end
  (plan 01-01: a clean copy with no `.git` and no pre-existing virtualenv,
  `uv sync`, both members importable, `make check` green).
* Good, because the src-layout guarantee holds per member: neither `dataplat` nor
  `csv_processor` is importable from the repository root without installation.
* Good, because `packages/` reads as what it is — a directory of distributions —
  and adding a third one is an obvious, local operation.
* Bad, because the paths in ARCHITECTURE.md §4.2's diagram are now wrong, and that
  document is not edited. Mitigated by this record and by `docs/README.md`'s
  repository map.
* Neutral, because it is invisible to imports. `import dataplat` and
  `import csv_processor` are unaffected by where the member directories sit.

## Migration trigger

**If uv changes its workspace member convention** — for example by supporting a
shared source root across members — this layout would be worth revisiting for the
single directory level it would save. That is a cosmetic gain, so the bar is low
value and the trigger is unlikely to be acted on even if it fires.

Reversal is cheap: it is a directory move plus one line in `[tool.uv.workspace]`,
with no import path changes. That cheapness is itself part of why this record is
two pages shorter than ADR-0002.

## References

* README §75 — Repository Structure (option C's origin)
* `.planning/research/ARCHITECTURE.md` §4.2 — the `src/`-rooted diagram this departs from
* `.planning/phases/01-repository-toolchain-ci-skeleton/01-RESEARCH.md`
  § Architecture Patterns → "Recommended Project Structure", departure 1
* uv workspace documentation — member layout `packages/<name>/{pyproject.toml,src/<pkg>/}`
* `pyproject.toml` — `[tool.uv.workspace] members`, explicit and never a glob
* ADR-0002 — what the two members are
* ADR-0004 — why there is no third member for Airflow
