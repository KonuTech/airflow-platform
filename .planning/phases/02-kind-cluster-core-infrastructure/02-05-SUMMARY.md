---
phase: 02-kind-cluster-core-infrastructure
plan: 05
subsystem: docs
tags: [adr, supply-chain, helm, minio, ingress-nginx]

# Dependency graph
requires: [02-01]
provides:
  - "docs/adr/0006-unmaintained-upstream-artifacts.md — the supply-chain risk acceptance for pgsty/minio, the archived ingress-nginx controller and quay.io/minio/mc"
  - "docs/adr/0007-helm-4-over-helm-3.md — the Helm 4.2.3 adoption record, resolving STACK.md's one MEDIUM-confidence call"
  - "docs/adr/README.md — both Phase-2 prospective rows retired, Records table extended through 0007"
affects: []

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "ADR voice/structure follows 0000-template.md exactly, options lettered with inline verdicts, per 0005's established convention"

key-files:
  created:
    - docs/adr/0006-unmaintained-upstream-artifacts.md
    - docs/adr/0007-helm-4-over-helm-3.md
  modified:
    - docs/adr/README.md

key-decisions:
  - "ADR-0006 names all three unmaintained artifacts the Package Legitimacy Audit found (pgsty/minio, registry.k8s.io/ingress-nginx/controller:v1.15.1, quay.io/minio/mc), not MinIO alone — the ingress-nginx archival (2026-03-24) is new since STACK.md was written"
  - "ADR-0007 records the Helm 3.21.3 fallback as having no surviving trigger in this phase, since all five pinned charts verified installing cleanly under 4.2.3 in 02-RESEARCH.md"

requirements-completed: [INFRA-05]

# Metrics
duration: ~35min
completed: 2026-08-12
---

# Phase 2 Plan 5: Architecture Decision Records — unmaintained upstreams and Helm 4 Summary

**Wrote ADR-0006 (the three-artifact supply-chain risk acceptance: `pgsty/minio`, the archived `ingress-nginx` controller, `quay.io/minio/mc`) and ADR-0007 (Helm 4.2.3 adopted over the documented Helm 3.21.3 fallback, now that the compatibility gate has run), and retired both Phase-2 rows from `docs/adr/README.md`'s prospective-records table.**

## Performance

- **Duration:** ~35 minutes
- **Tasks:** 2/2 complete
- **Files modified:** 3 (2 created, 1 modified across both tasks)

## Accomplishments

- ADR-0006 names all three unmaintained upstream artifacts with dated evidence (MinIO archived 2026-04-25 / CE console removed May 2025; ingress-nginx archived read-only 2026-03-24 with successor InGate also retired; `mc` ~20 months stale but genuine pre-archival), six lettered options with research-grounded verdicts, and a Decision Outcome that names the two engineering seams (boto3 endpoint injection, annotation-free `Ingress` objects) that keep both migrations values changes rather than rewrites. Migration trigger names four concrete observable events, none of them "none".
- ADR-0007 records that the Helm 4.2.3 compatibility gate — all five pinned charts rendering and installing cleanly — has run and resolved STACK.md's one MEDIUM-confidence call, and documents the CLI contract differences (`--atomic` removed, `--wait`'s `hookOnly` default when the flag is omitted, server-side apply as default, `--force-replace`) that make a copied Helm-3 command line silently wrong rather than erroring, naming `scripts/helm-install.sh` as the single place the wait strategy is expressed.
- `docs/adr/README.md`'s Records table now runs 0001–0007 with no gaps and `0000` still unindexed; both Phase-2 prospective rows (MinIO fork, Helm 4) are gone, leaving only the Phase-5 Vault row.
- `make check` verified green after each task's commit.

## Task Commits

1. **Task 1: ADR-0006 — the three unmaintained upstream artifacts** - `ecfbfaf` (docs)
2. **Task 2: ADR-0007 — Helm 4.2.3 over the Helm 3.21.3 fallback** - `80b7f95` (docs)

## Files Created/Modified

- `docs/adr/0006-unmaintained-upstream-artifacts.md` — new ADR
- `docs/adr/0007-helm-4-over-helm-3.md` — new ADR
- `docs/adr/README.md` — Records table extended, both Phase-2 prospective rows removed (split across the two task commits so each commit's diff matches its own task)

## Decisions Made

- Split the single combined README.md diff (adding both rows, removing both prospective rows) into two task-scoped diffs by reverting and reapplying per task, so each commit's file set exactly matches its task's declared `<files>` — no decision content was affected, only commit granularity.

## Deviations from Plan

### Auto-fixed Issues

None — plan executed exactly as written. The `<human-check>` text in Task 1's `<verify>` block is guidance for the plan-checker/reviewer to confirm the accepted risk (T-02-21) reads correctly on inspection; this task type is `auto`, not `checkpoint:human-verify`, so no execution pause was required, and the ADR's own Consequences section states the T-02-21 disposition inline as instructed.

**Environment note (not a plan deviation):** at start of execution, this worktree's branch (`worktree-agent-a6b8a3222741fc6cc`) was found sitting one commit behind its declared base (`eafcb15`, which carries 02-01's live-verification fix). The mandatory `<worktree_branch_check>` `git merge-base` assertion caught this and the prescribed `git reset --hard eafcb15` corrected it before any file was read or edited. This is the standard worktree-setup safety check operating as designed, not a deviation from plan execution.

**Total deviations:** 0
**Impact on plan:** None. Plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None.

## Known Stubs

None. Both ADRs are complete, accepted records with every template heading filled and no placeholder content.

## Threat Flags

None. This plan's threat model item (T-02-SC, T-02-21, T-02-22) is the subject of the ADRs themselves, not new surface introduced by writing them.

## Next Phase Readiness

Both records exist, are indexed, and the Phase-2 prospective table is empty except for the Phase-5 Vault row (out of scope for this phase). `make check` passes. No blockers for remaining Phase 2 plans.

---
*Phase: 02-kind-cluster-core-infrastructure*
*Completed: 2026-08-12*

## Self-Check: PASSED

- `docs/adr/0006-unmaintained-upstream-artifacts.md` — FOUND
- `docs/adr/0007-helm-4-over-helm-3.md` — FOUND
- Commit `ecfbfaf` — FOUND in `git log`
- Commit `80b7f95` — FOUND in `git log`
