---
status: accepted
date: 2026-08-13
---

# ADR-0008: Pipeline composition is `Source` → `RecordChunk` → `Stage` → `Publisher`, not README §68's flat taxonomy

## Context and Problem Statement

README §68 lists CSV concerns as a flat module list —
`filename/detector/parser/validation/normalization/deduplication/incremental/cdc/scd/storage/models` —
with no composition seam. ADR-0002 already addressed the naming half of this
problem (splitting the flat list into a source-agnostic `dataplat` core and a
`csv_processor` plugin), but it did not settle the piece this record settles:
**how a run is actually assembled, sequenced and checkpointed**, and how the
barrier between "this stage sees one chunk" and "this stage sees the whole
run" is expressed in the type system rather than left to convention.

Without a named seam, that assembly logic accretes somewhere unplanned —
most likely `cli.py`, or worse, an Airflow DAG file, which is exactly what
README §6.4 forbids (business logic in the package, DAGs orchestrate and
delegate only). It also leaves README §29/§95's extensibility promise (new
sources — Kafka, database CDC — added without redesign) unproven: nothing in
§68 states the contract a second source or a second publication strategy
would implement against.

This phase (Wave 4) is the first point in the project where the seam's
inputs all exist — `DatasetConfig` (plan 03-04), `MetadataRepository` and
`ObjectStore` (plan 03-05) — so it is also the first point the seam itself
can be recorded, not merely proposed.

## Considered Options

* **A — Implicit composition.** No named seam. A run is assembled by
  convention inside `cli.py`'s orchestration code (or, if that discipline
  slips, inside a DAG file): read a file, call a sequence of ad hoc
  functions, write a target table. Each new source or publication strategy
  is a new branch of hand-written glue.
* **B — A named `Source` → `RecordChunk` → `Stage` → `Publisher` protocol
  set.** `PipelineContext` composes every subsystem a run needs;
  `Source`/`RecordStream` are the read side; `StreamingStage`/`BarrierStage`
  split "runs once per chunk" from "runs once per run" in the type system;
  `Publisher` is the write side. This is the `Source`/`RecordChunk`/
  `Publisher` set `.planning/research/ARCHITECTURE.md` Question 4 (§4.1's
  critique, §4.3's abstractions) designs and this plan implements.

## Decision Outcome

Chosen option: **B — the named `Source`/`RecordStream`/`StreamingStage`/
`BarrierStage`/`Publisher` protocol set.**

README §29 (CDC) and §95 ("the project should evolve toward a
metadata-driven, production-like ETL platform... every new source evaluated
across file/API/database/CDC") require that non-CSV sources and non-`merge`
publication strategies be added **without redesign**. Option A cannot deliver
that: without a named contract, "add a source without redesign" is only true
by accident, for as long as nobody has yet needed to change the ad hoc glue.
Option B makes it true by construction — a second `Source` or `Publisher` is
a new implementation of an existing `typing.Protocol`, not a new code path
through `cli.py`.

`.planning/phases/03-dataplat-core-library-metadata-control-plane/03-CONTEXT.md`'s
phase-scoping already assumes this seam exists: it defers `merge` to Phase 4
and SCD/CDC publishers to Phase 10 as *implementations of the `Publisher`
protocol*, not as a redesign. `.planning/ROADMAP.md`'s own Phase 3 plan
guidance states the departure directly: "README §68's proposed package
layout does not contain this seam — record the departure as an ADR now so it
is not re-litigated at Phase 10." This record is that ADR.

### Consequences

* Good, because Phase 10's CDC `Source` and SCD `Publisher` are new
  implementations of an existing contract, not a parallel pipeline —
  `.planning/ROADMAP.md`'s Phase 10 plan guidance ("Placement is `Source` /
  `Publisher`, not a new pipeline... which is exactly why the seam was
  established in Phase 3 and recorded as an ADR") depends on this being true
  by the time Phase 10 starts.
* Good, because the `StreamingStage`/`BarrierStage` split keeps
  checkpointing (README §38, only ever between streaming chunks) and atomic
  publication (README §36, barrier-only) from fighting each other. A stage
  that needs the whole run cannot accidentally be checkpointed mid-run,
  because it is not a `StreamingStage` at all.
* Good, because `dataplat.pipeline.engine.run_streaming` gives every later
  `Source`/`Stage` implementation one proven sequencing loop, instead of each
  plan re-deriving chunk-threading and checkpoint-ordinal bookkeeping.
* Bad, because this plan ships five new files (`pipeline/protocol.py`,
  `pipeline/engine.py`, `sources/protocol.py`, `load/publish/protocol.py`,
  plus this record) and their tests for a phase with no real pipeline run
  yet — `RaggedRowGuard` is the only concrete stage, and it exists to prove
  the errors-as-values mechanism (QUAL-03), not because the slice needs row
  validation today. Accepted because retrofitting this seam after Phases 6
  through 10 have already built against an ad hoc orchestration shape would
  be far more expensive than the cost of building it one phase early.
* Neutral, because the protocols defined here are deliberately narrower than
  `ARCHITECTURE.md` Q4.3's original sketch (`Source`/`RecordStream` carry no
  `schema`/`profile` attributes, no `inspect()` method — those depend on
  Phase 6 detection-engine types that do not exist yet). Phase 6 widens this
  file; it does not replace it.

## Migration trigger

**None foreseen — this is permanent.** Reversing this decision means every
future `Source` implementation (Phase 4's CSV read path, Phase 6's richer
detection-aware source, Phase 10's CDC source) and every future `Publisher`
implementation (Phase 4's `merge`, Phase 10's SCD/CDC publishers) loses its
common contract and must be re-derived against whatever replaces it. The
roadmap's own Phase 10 plan guidance is written on the assumption this record
holds; un-making it after Phase 10 has shipped concrete implementations
against it would mean rewriting every one of them, not just this file.

## References

* README §68 — Python Library Architecture (the flat taxonomy this seam
  departs from)
* README §29 — Change Data Capture; README §95 — Overall Architectural
  Principle (non-CSV sources added without redesign)
* README §6.4 — DAG principles (business logic out of DAG files); README §38
  — checkpointing; README §36 — atomic publication
* `.planning/research/ARCHITECTURE.md` Question 4 — §4.1 ("Honest critique of
  §68") and §4.3 ("The core abstractions") — the seam this plan implements
* `.planning/ROADMAP.md` § "Phase 3: `dataplat` Core Library & Metadata
  Control Plane", plan guidance: "README §68's proposed package layout does
  not contain this seam — record the departure as an ADR now so it is not
  re-litigated at Phase 10"
* `.planning/ROADMAP.md` § "Phase 10" plan guidance — "Placement is `Source`
  / `Publisher`, not a new pipeline... exactly why the seam was established
  in Phase 3 and recorded as an ADR"
* `.planning/phases/03-dataplat-core-library-metadata-control-plane/03-CONTEXT.md`
  — deferred section (`merge` to Phase 4, SCD/CDC publishers to Phase 10)
* ADR-0002 — the `dataplat`/`csv_processor` package split this seam lives
  inside
* `packages/dataplat/src/dataplat/pipeline/protocol.py`,
  `packages/dataplat/src/dataplat/sources/protocol.py`,
  `packages/dataplat/src/dataplat/load/publish/protocol.py` — the protocols
  themselves
