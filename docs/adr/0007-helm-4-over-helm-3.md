---
status: accepted
date: 2026-08-12
---

# ADR-0007: Helm 4.2.3 is adopted over the documented Helm 3.21.3 fallback

## Context and Problem Statement

`.planning/research/STACK.md` pinned Helm `4.2.3` with `3.21.3` documented as a fallback and
flagged the pairing as its single MEDIUM-confidence call in the whole stack: Helm 4 driving
Helm-3-era charts (all five of this platform's pinned charts predate Helm 4) was an untested
claim. `docs/adr/README.md`'s deliberately-deferred-records table held this record back for
exactly that reason — "Helm 4 rendering the Airflow chart 1.22.0 cleanly is an untested
MEDIUM-confidence claim with a documented fallback. Writing the record before the
compatibility gate runs would record a guess as a decision."

The gate has now run. `02-RESEARCH.md` § Summary: **Helm 4.2.3 rendered and installed all
five pinned charts cleanly** on a live kind cluster — Airflow `1.22.0`, MinIO `5.4.0`,
CloudNativePG operator `0.29.0`, CloudNativePG `cluster` `0.8.1`, ingress-nginx `4.15.1`.
`run-airflow-migrations` succeeded with image tag `3.3.0` against the chart's declared
`appVersion: 3.2.2`, migrating the full 71-table schema onto CNPG PostgreSQL 17.10, with all
four Airflow 3 workloads reaching `Ready` in 48 seconds.

Separately, Helm 3's final feature release is 2026-09-09 and its security support ends
February 2027 (`.claude/CLAUDE.md` § A). Starting a months-long project on Helm 3 today would
buy an immediate migration debt against a deadline five months out.

## Considered Options

* **A — Adopt Helm 4.2.3.** The compatibility gate is now evidence, not a guess: all five
  charts render and install. Buys five months of runway before Helm 3 loses security support,
  for free.
* **B — Stay on Helm 3.21.3 until a chart fails.** Defers the migration this project would
  eventually have to make anyway, and starts accumulating Helm-3-specific CLI usage
  (`--atomic`, `--force`, boolean `--wait`) that would all need rewriting at the point of
  migration — the exact debt this project's own standing convention ("pinned versions live in
  exactly one place, asserted at runtime") exists to avoid paying twice.
* **C — Pin both, select per chart.** Doubles the installed-tool surface for no chart that
  actually needs it — every one of the five charts installed cleanly under 4.2.3 in this
  phase's live verification. A second Helm binary is a second thing `make doctor` must assert
  and a second thing every contributor's environment must carry, bought against a compatibility
  problem that does not exist for any chart this platform currently uses.

## Decision Outcome

Chosen option: **A — Helm 4.2.3**, because the compatibility gate that this record was
deliberately withheld pending has now run and produced a positive result on every chart this
platform depends on, and because Helm 3's EOL clock makes deferral strictly worse, not
neutral.

The Helm 3.21.3 fallback documented in STACK.md **has no surviving trigger in this phase**
and should be re-evaluated only if a future chart fails to render or install under Helm 4 —
not adopted preemptively.

### Consequences

Helm 4 changes the CLI contract in ways that make a copied Helm-3 command line silently do
the wrong thing rather than fail loudly (`02-RESEARCH.md` Pitfall 7, verified against
`helm --help` output verbatim):

* `--atomic` **no longer exists.** Its nearest equivalent is `--rollback-on-failure`, and
  Helm auto-defaults `--wait` to `watcher` when `--rollback-on-failure` is set.
* `--wait` changed from a boolean to a **WaitStrategy**. Its default **when the flag is
  omitted entirely is `hookOnly`** — it does not wait for workloads to become ready at all.
  A `helm upgrade --install` invocation that a Helm-3 user would expect to block until the
  release is serving instead returns almost immediately with the workload still starting.
* **Server-side apply is the default** (`--server-side` defaults `true` on install, `"auto"`
  on upgrade). Field-manager conflicts against hand-edited live resources become possible;
  `--force-conflicts` is the escape hatch, and `--force` itself was renamed
  `--force-replace`.
* `--dry-run` takes a **string** (`none`/`client`/`server`), not a boolean.
* `helm status --show-resources` **no longer exists** as a flag; any script that parsed its
  output breaks outright rather than degrading.

This platform's own D-09 command line — `helm upgrade --install --wait` — would, taken
literally from Helm-3 habit, silently inherit the `hookOnly` default and stop waiting for
workloads. That is precisely the trap this record exists to name: `scripts/helm-install.sh`
is the **single place** in this repository the wait strategy is expressed, and it passes
`--wait=watcher` **explicitly**, with the omission behavior documented inline in the script's
own header comment. The measured consequence of getting this wrong, observed in this phase's
research, is an operator install returning in **1.0 s** with the operator not yet serving —
a install that reports success while nothing is actually ready.

* Good, because the platform adopts the CLI surface it will be on for years, once, rather
  than migrating mid-project against Helm 3's EOL.
* Good, because `scripts/helm-install.sh` is the only place this contract is expressed, so
  a future Helm CLI change is a one-file fix, not a repository-wide audit.
* Bad, because every contributor and every CI step must be on Helm 4 syntax from day one —
  a copied Helm-3 snippet from documentation, Stack Overflow or muscle memory will compile
  and run, but silently do the wrong thing rather than error.
* Neutral, because server-side apply's default change has produced no observed conflict in
  this phase — every resource in this repository is Helm-managed, never hand-edited — but the
  `--force-conflicts` escape hatch is now a documented fact rather than a surprise if that
  ever changes.

## Migration trigger

**A pinned chart failing to render or install under Helm 4, or an upstream chart explicitly
declaring a Helm-4 incompatibility.** Until either happens, the Helm 3.21.3 fallback stays
documented and unused.

## References

* `.planning/phases/02-kind-cluster-core-infrastructure/02-RESEARCH.md` § Summary — the
  five-chart compatibility result; § Pitfall 7 — the verbatim Helm 4 CLI table
* `.claude/CLAUDE.md` § A — the Helm row and the Helm 3 EOL dates (final feature release
  2026-09-09, security support to February 2027)
* `scripts/helm-install.sh` — the single place the wait strategy is expressed
* `docs/adr/README.md` — the deliberately-deferred-records table this record retires
