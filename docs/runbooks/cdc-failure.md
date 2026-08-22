# CDC failure — not applicable in v1

This is a deliberate, honest stub, not an operational how-to — included so this scenario is never
silently omitted from the runbook set, per `README.md` §89 and this platform's own D-41 convention.

## Symptoms

Not applicable in v1. CDC (Change Data Capture) — the event model, ordering barrier, delivery-
semantics documentation, and its test coverage — was explicitly dropped from scope. See
`REQUIREMENTS.md`'s **Out of Scope** table: *"CDC (Change Data Capture), including a CSV-delivered
change feed ... (DoD 44, 45, 46, 87). No upstream system produces a change feed yet, and SCD 0/1/2
(Phase 10) build entirely from CSV batches without one — dropped 2026-08-21 rather than built for a
format with no real producer."* There is no CDC event model in this codebase to fail.

## Diagnosis

Not applicable. There is no CDC ingestion path, ordering barrier, or delivery-semantics
implementation to diagnose. SCD Type 0/1/2 (Phase 10) build entirely from full CSV batches —
see [`scd-correction.md`](scd-correction.md) for that mechanism, which has no CDC dependency.

## Recovery

Not applicable.

## Reprocessing

Not applicable.

## Verification

Not applicable. If a CDC feed is ever added, it plugs in through the existing `Source`/`Publisher`
composition seam
([`docs/adr/0008-pipeline-composition-seam.md`](../adr/0008-pipeline-composition-seam.md)) — a new
`Source` implementation of the already-defined `typing.Protocol`, without redesigning the pipeline
around it. That seam exists and is proven by the CSV `Source` already built on top of it; it carries
no CDC-specific code today, only the general contract that would admit one later.
