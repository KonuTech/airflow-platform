---
status: accepted
date: 2026-08-11
---

# ADR-0002: `dataplat` is the source-agnostic core; `csv_processor` is a plugin that depends on it

## Context and Problem Statement

README §68 proposes a single package named `csv_processor`, containing
`filename/`, `detector/`, `parser/`, `validation/`, `normalization/`,
`deduplication/`, `incremental/`, `cdc/`, `scd/`, `storage/` and `models/`.
README §75 places that package at the repository root. This is the phase's
**headline departure from the specification**, and it is taken deliberately.

§68's list is a reasonable *taxonomy of CSV concerns*. It is not an architecture.
`.planning/research/ARCHITECTURE.md` §4.1 identifies six specific problems:

| # | Problem | Consequence |
|---|---|---|
| 1 | **CSV is at the root of the namespace.** `cdc/` and `scd/` sit *inside* a package named `csv_processor` | A Kafka CDC source importing `csv_processor.cdc` is an architecture smell that becomes permanent the moment anything imports it |
| 2 | **No composition seam.** The modules are a bag of utilities; nothing says how a run is assembled, ordered, checkpointed or aborted | Composition logic accretes somewhere — most likely `cli.py`, or worse the DAG, violating README §6.4 |
| 3 | **No config layer**, despite §65/§66 making configuration the centre of gravity | Config parsing scatters across every module and §66 versioning has nowhere to live |
| 4 | **No metadata/control-plane package**, despite §24/§37/§62/§82/§83 | The largest subsystem in the project has no home |
| 5 | `storage/{minio,postgres}` conflates two unrelated concerns | Object-store reads and warehouse publication become coupled; the §36 publication strategies have nowhere to go |
| 6 | `models/` as a peer leaf invites cycles | `validation` imports `models`; `models` grows a validation helper; the graph knots |

Problem 1 is the driver. README §29 (CDC) and §95 ("the project should evolve
toward a metadata-driven, production-like ETL platform, not a collection of
individual CSV scripts" — every new source evaluated across file/API/database/CDC)
require that non-CSV sources be added **without redesign**. An import path that
routes every future source through a CSV-named package violates that on day one,
and it violates it in the one dimension that is most expensive to reverse: the
public name of every module.

The cost of getting this wrong is not paid now. It is paid in Phase 10, when
`cdc/` and `scd/` need a home, at exactly the moment when the pressure to "just
put it where the README said" is highest and the budget for a repository-wide
rename is lowest.

## Considered Options

* **A — README §68 verbatim.** One distribution, `csv_processor` at the root,
  with `cdc/`, `scd/` and `storage/` as subpackages.
* **B — One distribution, nested subpackages.** `csv_processor/core/…` plus
  `csv_processor/sources/csv/…`. The same architectural seams, inside a single
  package with a single `pyproject.toml`.
* **C — Two distributions.** `dataplat` (source-agnostic core) and
  `csv_processor` (a CSV source plugin that depends on `dataplat`; never the
  reverse), as two uv workspace members.

## Decision Outcome

Chosen option: **C — two distributions, `dataplat` and `csv_processor`.**

`dataplat` owns everything that is not about CSV: `models/`, `errors.py`,
`config/`, `pipeline/`, `sources/` (the `Source` protocol and its plugin
registry), `validation/`, `normalization/`, `deduplication/`, `incremental/`,
`load/` (staging + publication strategies), `storage/`, `metadata/`,
`observability/`, `secrets/` and `cli.py` — the pod entry point.
`csv_processor` owns `filename/`, `detect/`, `read/` and a `source.py` that
implements the `dataplat` `Source` protocol and registers via an entry point.

**The dependency runs one way only, and this is mechanically enforced.**
`setup.cfg` carries import-linter contract 1, *"dataplat core must not depend on
the CSV plugin"* — a `forbidden` contract from `dataplat` to `csv_processor`,
run by `make imports` inside `make check`. That contract is the mechanical form
of this record; this record is the reasoning behind that contract. It was
observed rejecting a deliberate `import csv_processor` added to a `dataplat`
module (plan 01-01).

Option **A** was rejected because it structurally contradicts §29/§95, as set out
above. It is the cheapest option today and the most expensive one from Phase 10
onward.

Option **B** deserves more than a one-line dismissal, because it is genuinely
cheap: it buys the same seams for the cost of two directory levels and no extra
`pyproject.toml`. It was rejected for two reasons. First, the seam it creates is
a *convention* rather than a boundary — nothing stops `csv_processor/core/` from
importing `csv_processor/sources/csv/`, and import-linter contracts over
subpackages of one distribution are easier to weaken than a contract between two
distributions someone would have to merge to remove. Second, the import paths
stay ugly and stay CSV-named (`csv_processor.core.pipeline.engine`), which means
the §95 property is true in the dependency graph but false in every traceback,
log line and code review. The stated cost of option C is *one extra `pyproject`
entry*; that is a small price for making the property visible rather than merely
provable.

### Consequences

* Good, because §95 becomes **structurally true** rather than aspirational: a
  future Kafka or database CDC source is a second plugin implementing the same
  `Source` protocol, added alongside `csv_processor`, importing nothing from it.
* Good, because the one-way dependency is enforced by a contract in `make check`
  that has been observed to fail, not by a comment nobody re-reads.
* Good, because `dataplat` is installable and importable alone, which makes the
  core testable without any CSV fixture.
* **Phase 10 inherits this:** when CDC and SCD land, they go in
  `dataplat/load/publish/` and the `dataplat` change-model types — **the core,
  not the CSV plugin.** `csv_processor` gains nothing when CDC arrives. This
  sentence exists so that Phase 10 does not re-open the question; the decision
  was taken here, with the evidence above, before any code depended on it.
* Bad, because the package name in `PROJECT.md` and README §68 (`csv_processor`)
  no longer names the whole library. Anyone reading only the README will look for
  business logic that is not there. Mitigated by `docs/README.md`, which maps
  every directory and flags this departure, and by this record.
* Bad, because two distributions mean two `pyproject.toml` files, two version
  numbers and a workspace to keep in step. Accepted: it is the cost that buys the
  boundary.
* Neutral, because README §6.4's rule — business logic in the package, DAGs
  orchestrate and delegate — is unaffected. Both distributions live outside the
  Airflow image either way (see ADR-0004).

## Migration trigger

**None foreseen.** This is the most load-bearing structural decision in the
repository and it is deliberately not provisional.

Reversing it would mean a rename across every module, every import, every DAG and
every metadata record that carries a module path — so if it is ever wrong, the
correct response is almost certainly to absorb the cost rather than to have left
the decision open. The event that would *force* a revisit is the opposite of the
one this record guards against: if, by the end of the project, `csv_processor`
remains the only source plugin and `dataplat` has no second consumer, then the
boundary cost was paid for a property never exercised. That is a retrospective
judgement for the milestone review, not a migration.

## References

* README §68 — Python Library Architecture (the superseded structure)
* README §75 — Repository Structure (`csv_processor/` at the root)
* README §29 — Change Data Capture; README §95 — Overall Architectural Principle
* README §6.4 — DAG principles (business logic out of DAG files)
* `.planning/research/ARCHITECTURE.md` §4.1 — the six-problem critique of §68
* `.planning/research/ARCHITECTURE.md` §4.2 — the recommended structure and the
  naming-deviation note
* `setup.cfg` — import-linter contract 1, the mechanical form of this decision
* ADR-0003 — where the two members physically live
* ADR-0004 — why neither is installed into the Airflow image
