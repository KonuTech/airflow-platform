---
status: accepted
date: 2026-08-11
---

# ADR-0004: Two images with two dependency sets; the Airflow distribution is not a workspace member

## Context and Problem Statement

The platform runs two distinct Python environments: the **Airflow image**
(scheduler, API server, DAG processor, triggerer) and the **csv-processor image**
(the ETL pod launched by `KubernetesPodOperator`). They have different jobs and
sharply different dependency needs.

A uv workspace has **one lockfile and one resolved environment** for all its
members. Adding `apache-airflow==3.3.0` as a third member would therefore force
its constraint pins — roughly 600 of them, including `pandas==2.1.4`,
`psycopg2-binary==2.9.12`, `polars==1.42.1`, `charset-normalizer==3.4.7` — into
the same resolution as `dataplat` and `csv_processor`. The ETL library's
dependency set would silently become whatever Airflow tolerates.

That is not a hypothetical annoyance. The library's stack deliberately pins
`psycopg` v3 (Airflow's constraints pin psycopg **2**), `charset-normalizer`
3.4.9 (Airflow pins 3.4.7) and no pandas at all (Airflow pins 2.1.4). A shared
resolution either fails outright or resolves to something neither environment
actually wants.

**The decision is made in Phase 1, whether or not it is written down**, because
the workspace membership list is written in Phase 1. `[tool.uv.workspace]`
already names exactly two members and is explicitly commented never to become a
glob. What is missing without this record is the *reason* — which the Phase 4
Dockerfile author would otherwise have to re-derive from first principles, at the
moment when adding Airflow to the workspace looks like a convenient way to share
a lockfile.

## Considered Options

* **A — One image.** Install `csv_processor` into the extended Airflow image and
  run ETL in the Airflow worker.
* **B — Two images, one workspace.** Add an `airflow` distribution as a third
  workspace member so a single `uv.lock` covers everything.
* **C — Two images, two dependency sets.** The workspace covers only `dataplat`
  and `csv_processor`; the Airflow image installs Airflow and its providers via
  `--constraint` in its own Dockerfile, entirely outside the workspace.

## Decision Outcome

Chosen option: **C — two images, two dependency sets.**
`apache-airflow` never appears in `uv.lock`, and `csv_processor` is never
installed into the Airflow image.

Option **A** was rejected on README grounds before dependency grounds: §6.4 and
the platform constraints require heavy processing to run in task pods, never in
the scheduler. It also collapses the resolution problem into its worst form.

Option **B** was rejected because the single lockfile *is* the problem. It offers
one apparent benefit — a shared resolution — which is exactly the thing that must
not be shared.

The mechanism that makes C true is a property of `pyproject.toml`, not of the
Dockerfiles: `[tool.uv.workspace] members` lists two paths explicitly, never a
glob, so a future `packages/airflow-dags/` directory cannot join by being
created. `grep -c apache-airflow uv.lock` returning `0` is the assertion that
this holds, and it was checked in plan 01-01.

### Consequences

* Good, because the ETL library keeps the stack it was designed for —
  `psycopg` v3 with `COPY`, `charset-normalizer` 3.4.9, no pandas — instead of
  inheriting Airflow's.
* Good, because the csv-processor image stays slim. It is a Python base plus a
  wheel, not a build environment carrying Airflow's transitive tree.
* **Phase 4 Dockerfile consequence, recorded now because it bites later:** uv
  requires *all* workspace member configuration files to be present in order to
  validate the lockfile. The Dockerfile must therefore install the dependency
  layer with `--no-install-workspace` **and** `--frozen`, then switch to
  `--locked` after the member sources are copied. Doing it in that order is what
  keeps the dependency layer cacheable across source changes; retrofitting it is
  annoying, and noting it here is free.
* Bad, because two dependency sets can **drift**. If `csv_processor` — or any
  shared code — ends up installed in both images at different versions, a
  DAG-side import and a pod-side import behave differently and a bug reproduces
  in one and not the other. The prevention is that DAG files import almost
  nothing: they orchestrate and delegate (README §6.4), so the DAG image needs
  the processor's modules not at all. **If** the DAG ever genuinely needs shared
  constants or a schema, the correct fix is a third, tiny, dependency-free
  distribution that both images pin to the same version, with version equality
  asserted in CI — not installing `csv_processor` into the Airflow image.
* Bad, because Airflow's dependency set is not covered by `uv.lock` and therefore
  not covered by Dependabot's `uv.lock` updates. The Airflow image's pins are
  governed by Airflow's own constraints file for the pinned release, which is a
  deliberate and separate update path.
* Neutral, because both images are built from the same repository and the same
  commit, so `org.opencontainers.image.revision` still identifies one source
  state for both.

## Migration trigger

**If the two images' shared surface ever grows beyond "nothing".** Concretely: the
first time a DAG file needs to import something from `dataplat` or
`csv_processor` at more than trivial depth, this record's assumption — that the
DAG image needs none of the library — has failed. The response is the third tiny
distribution named above, plus a CI assertion that both images pin it to the same
version. It is *not* a reversal of the two-image split, and it is not a licence to
add Airflow to the workspace.

## References

* README §6.4 — DAG principles; heavy processing runs in task pods
* README §77 — image standards
* `.planning/research/PITFALLS.md` G5 — "Two images, two dependency sets, one
  chance to get the split right"; the failure mode is *drift*, not the split
* `.planning/phases/01-repository-toolchain-ci-skeleton/01-RESEARCH.md`
  § Architecture Patterns → "Why the Airflow image is not a workspace member",
  including the `--no-install-workspace` / `--frozen` → `--locked` Docker note
* `.claude/CLAUDE.md` § Version Compatibility Matrix — Airflow 3.3.0 constraints
  pin `pandas==2.1.4`, `psycopg2-binary==2.9.12`, `polars==1.42.1`
* `pyproject.toml` — `[tool.uv.workspace] members`, two entries, never a glob
* ADR-0002, ADR-0003 — what the workspace contains and where it lives
