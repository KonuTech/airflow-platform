# Walking Skeleton — Airflow ETL Platform

**Phase:** 1
**Generated:** 2026-08-11

## Scope note — read this first

The default walking-skeleton shape (project scaffold + routing + one real database read/write +
one real UI interaction + dev deployment) does not apply to this project at Phase 1, and forcing
it would contradict the roadmap. This platform has **no UI at all**, and its database and data
pipeline arrive in Phases 3 and 4. The ROADMAP places the *data-pipeline* walking skeleton at
**Phase 4**: one UTF-8 comma CSV travelling `CSV → MinIO → TaskFlow DAG → KubernetesPodOperator →
processor → analytical PostgreSQL`, closing only when a re-run produces zero additional rows.

Phase 1's walking skeleton is therefore the **delivery pipeline proving itself end to end**. That
is a genuine thin end-to-end slice of the system this phase builds, and it is the slice every
later phase depends on: no phase after this one can land a commit that the gate has not judged.

## Capability Proven End-to-End

A commit flows to a pull request; CI runs lint → format → type check → import contracts → policy
tests → unit tests → fixture byte-identity → secret scan; the run is green on clean code, red on a
planted synthetic credential, and a red run blocks the merge.

## Architectural Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Package layout | `dataplat` core + `csv_processor` plugin, two uv workspace members under `packages/` | CSV must not sit at the root of the namespace, or a future Kafka CDC source imports through a CSV-named path forever (ARCHITECTURE.md §4.1). Locked by the developer; recorded as ADR-0002. |
| Dependency management | uv workspace, one universal lockfile, one virtual environment, virtual root | Reproducibility root for the whole project; `uv lock --check` makes drift a build failure rather than a surprise |
| Image boundary | Two images, two dependency sets — the Airflow distribution is **not** a workspace member | A workspace resolves one lockfile for all members; admitting Airflow would push its constraint pins onto the ETL library. ADR-0004. |
| Gate definition | A single `Makefile`; CI calls `make` and defines no gate of its own | The only structure in which local and CI drift is impossible rather than merely discouraged |
| Static analysis | ruff with everything selected minus a nine-rule ignore list; mypy strict with per-module flags enumerated individually | `select = ALL` keeps the print ban, annotation, docstring, security and logging families on without enumeration. The blanket per-module strict toggle is silently ignored by mypy 2.3.0, so relaxation must enumerate flags. |
| Architecture policy | pytest tests under `tests/policy/`, not workflow grep steps | A policy test runs locally, fails with a readable message naming file and line, and is discoverable by reading the test tree |
| Test fixtures | Generated from a recorded seed against a committed digest oracle; no fixture bytes committed | Committing the corpus bloats build contexts, drowns the secret scanner, and makes a size-parameterised memory test impossible. ADR-0005. |
| Secret scanning | The scanner binary in a run step, checksum-verified; path-and-prefix scoped allowlists; a self-test that watches it fail | The marketplace action requires a paid licence for organisation repositories; a scanner nobody has seen fail is indistinguishable from a disabled one |
| ADR format | MADR-style records under `docs/adr/`, extended with a migration-trigger heading | Nearly every decision in this project is "the obvious choice is dead upstream, here is the alternative and here is the escape hatch" — the rejected options are the content |
| Python | CPython 3.12, source layout inside each member, typed-package markers | The default interpreter for the Airflow image; source layout prevents testing the source tree instead of the installed package |

## Stack Touched in Phase 1

- [x] Project scaffold — uv workspace, two members, build backend, lockfile, test runner
- [x] The gate — lint, format, type check, import contracts, policy tests, unit tests
- [x] A real artifact pipeline — the fixture corpus generated from a seed and verified against a
      committed oracle
- [x] Secret scanning — full history and working tree, with a negative proof
- [x] Continuous integration — two jobs, both calling only make targets
- [x] Enforcement — required status checks on the default branch, so a red run blocks a merge
- [ ] Database — **deferred to Phase 3** (analytical PostgreSQL, metadata control plane)
- [ ] Data pipeline — **deferred to Phase 4** (the project's data walking skeleton)
- [ ] User interface — **out of scope for the project**; there is no bespoke frontend

## Out of Scope (Deferred to Later Slices)

- Any Kubernetes cluster, Helm chart, or container image — Phase 2 and Phase 4. Nothing in Phase 1
  may require `kind` or `helm`, which are not installed on this machine.
- Any credential, secrets engine or network service — Phase 5. A clean checkout must pass the
  local gate with none of them.
- Library implementation. Phase 1 ships one real typed and documented function purely so the gate
  has something to judge; the pipeline engine, readers, validators and loaders belong to Phases 3
  onward.
- Coverage thresholds — reporting is wired now, the threshold is Phase 11.
- The DAG-folder import contract — vacuous while the folder is empty; Phase 4 adds it.
- Object-store, package-manager-major and secrets-engine ADRs — those decisions are taken in
  Phases 2, 2 and 5, and an ADR records a decision already taken.
- End-to-end tests against an ephemeral cluster — Phase 11.

## Subsequent Slice Plan

Each later phase adds one vertical slice on top of this skeleton without altering its
architectural decisions:

- **Phase 2:** a destroyable and recreatable three-node cluster running the object store, both
  database clusters and the orchestrator, from committed files — with the CI-sized profile written
  from the first infrastructure commit.
- **Phase 3:** the metadata control plane and pipeline engine as a testable library, in parallel
  with Phase 2 and sharing no files with it.
- **Phase 4:** the data walking skeleton — one CSV travelling the whole path, idempotent by
  construction, closing only when a re-run produces zero additional rows.
- **Phase 5:** the secrets engine becomes the only source of runtime credentials, behind the
  resolver seam Phase 3 established.
- **Phase 6:** the universal CSV engine, implemented against the sixty-nine fixtures this phase
  authored — the corpus is the specification it is measured by.
- **Phases 7–11:** observability, validation and quarantine, ETL correctness, change data capture
  and slowly changing dimensions, then the ephemeral-cluster end-to-end pipeline and the runbooks.
