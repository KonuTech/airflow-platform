---
status: accepted
date: 2026-08-11
---

# ADR-0001: Architecture decisions are recorded as numbered MADR files under `docs/adr/`

## Context and Problem Statement

This project is specified by a 3,386-line `README.md`. That specification is fixed
and is not edited — it is the thing the build is measured against. But the build
already departs from it in several places, because parts of the 2026 ecosystem the
README assumes have moved: the package layout it suggests (§68, §75) puts CSV at the
root of the namespace, which contradicts §29/§95's requirement that non-CSV sources
be addable without redesign.

An undocumented departure from a fixed specification has no audit trail. The next
person to open `packages/` — quite possibly the same person, eight phases later —
finds a structure that contradicts the README, no record of why, and re-argues the
decision under whatever pressure that phase carries. Worse, the argument is re-opened
without the evidence that settled it the first time, because that evidence lived in a
research document nobody re-reads.

The decision log is therefore not documentation-for-its-own-sake. It is the **only**
record of where and why this repository diverges from its own specification.

## Considered Options

* **No decision log.** Rely on commit messages, code comments and the `.planning/`
  research corpus.
* **Nygard-format ADRs** (Context / Decision / Status / Consequences), the original
  four-section form.
* **MADR 4.0 minimal**, extended with a bespoke `Migration trigger` heading.
* **MADR 4.0 full**, including the `decision-makers` / `consulted` / `informed` RACI
  front matter.

## Decision Outcome

Chosen option: **MADR 4.0 minimal, extended with a `Migration trigger` heading**,
stored as numbered Markdown files in `docs/adr/`.

Rejected, with reasons:

* **No log.** Commit messages record *what* changed, not which alternatives were
  weighed and discarded. `.planning/research/` holds the evidence but is organised by
  research topic, not by decision, and is not part of the delivered repository. A
  reader of `packages/` has no path from the code to the reasoning.
* **Nygard.** Every Nygard record is a valid MADR record, so this is a subset rather
  than a rival. Its four sections have **nowhere to put the rejected options** — and
  for nearly every decision in this project the rejected options *are* the content.
  "We use MinIO" is not a decision; "the upstream is archived, here is the fork we
  pinned, here is the migration target if the fork dies" is.
* **MADR full.** The RACI fields (`decision-makers`, `consulted`, `informed`) describe
  a review process that does not exist on a single-author project. Empty ceremony
  fields train readers to skim the front matter, which is where `status` lives.

The `Migration trigger` heading is not part of MADR and is added deliberately. Several
decisions here are explicitly provisional — pinned to a community fork, or to a
pre-release major, or to a measurement that has headroom today. Recording the
*observable event* that would reverse a decision converts "we should revisit this
sometime" into a testable condition. `"None — this is permanent"` is a valid answer and
must be written out, so that a blank section always reads as an omission.

### Conventions this record establishes

| Convention | Rule |
|---|---|
| Location | `docs/adr/`. README §75 lists `docs/` without `adr/`; this is an addition, not a conflict. |
| Filename | `NNNN-kebab-case-title.md`, zero-padded to four digits. |
| Numbering | Monotonic, never reused, never renumbered. Gaps are permitted (a rejected draft keeps its number). |
| Title | A decision phrased as a claim, not a topic: "Two images, two dependency sets", not "Docker images". |
| Status | One of `proposed`, `accepted`, `rejected`, `deprecated`, `superseded by ADR-00NN`. |
| Superseding | **Never edit a decided record's decision.** Add a new record and set the old one's status to `superseded by ADR-00NN`. |
| Template | `docs/adr/0000-template.md`. `0000` is the template's permanent number and is never a real record. |
| Index | `docs/adr/README.md` carries the table of records and the list of deliberately deferred ones. |

### Consequences

* Good, because the departure from README §68/§75 is written down once, with its
  rejected alternatives, before any code depends on it — so Phase 10 inherits a
  decision rather than an argument at the moment `cdc/` and `scd/` need a home.
* Good, because `git log docs/adr/` is the tamper-evidence for the superseding rule:
  an in-place edit of a decided record's decision section shows up as a diff on a file
  whose stated convention forbids exactly that.
* Good, because `Migration trigger` gives every provisional decision a stated exit
  condition rather than an implicit hope.
* Bad, because it is a manual discipline. Nothing mechanically fails a build when a
  decision is taken without a record; only review catches that.
* Neutral, because no tooling is adopted. `adr-tools` is a shell helper for creating
  and linking files; with a template and an index it would add an unpinned dependency
  for negligible benefit. A record is created by copying `0000-template.md`.

## Migration trigger

**None foreseen.** The format would only be revisited if the project gained multiple
authors and a real review process, at which point MADR's full template — the RACI
fields this record rejects — becomes worth its cost. That is an additive change to the
template, not a reversal of this decision.

## References

* README §75 — the documentation set `docs/adr/` extends
* [adr.github.io/madr](https://adr.github.io/madr/) — MADR 4.0.0, minimal and full templates
* `.planning/phases/01-repository-toolchain-ci-skeleton/01-RESEARCH.md` § ADR Mechanics
* `.planning/research/ARCHITECTURE.md` — "Recommended Repository Structure" (`docs/  # §75 set + adr/`)
