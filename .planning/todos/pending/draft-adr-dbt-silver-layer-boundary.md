---
title: Draft ADR-0010 formalizing the dbt bronze-to-silver boundary decision
date: 2026-08-18
priority: medium
---

# Draft ADR-0010: dbt scoped to bronze→silver, gold stays Python-owned

Write `docs/adr/0010-dbt-silver-layer-boundary.md`, following the existing ADR shape
(`docs/adr/0000-template.md`, and the style of `0008-pipeline-composition-seam.md` /
`0009-openbao-licence-escape-hatch.md`).

## What it needs to supersede/amend

- `PROJECT.md`'s Key Decisions table row: *"dbt excluded — README models transformation and SCD
  in Python (§36, §54–61). Introducing dbt would split the transformation story across two
  paradigms."* — needs to become: dbt is in scope, narrowly, for bronze→silver only; gold (and
  Phase 10's SCD2) stays Python-owned for the reasons below. Don't just delete the old row — the
  ADR should explain what changed and why, since a bare reversal without reasoning invites
  re-litigating it again later.
- `REQUIREMENTS.md`'s Out of Scope table entry: *"dbt or an external transformation framework"*
  — needs the same narrowing, not a blanket removal.

## Content to carry over (full reasoning already written)

`.planning/notes/dbt-silver-layer-architecture-decision.md` has the complete reasoning chain:
PG `MERGE` concurrency bug (BUG #18279) collision with dbt-postgres's `merge` incremental
strategy, META-03's single-transaction guarantee incompatibility with dbt's per-model transaction
model, the DuckDB/MinIO/Delta-Iceberg path considered and rejected in favor of silver-in-Postgres,
the least-privilege dbt role fit, and the Phase 10 dbt-snapshot rejection (dbt Labs' own docs:
"snapshots are not a replacement for CDC or event streaming"). Pull from that note rather than
re-deriving.

## When to do this

Before or during planning of Phase 08.1 (dbt Silver Transformation Layer) — the ADR should exist
before implementation starts, matching this project's existing practice of documenting
architecture decisions before they're built on top of (see ADR-0008/0009's timing relative to
their phases).
