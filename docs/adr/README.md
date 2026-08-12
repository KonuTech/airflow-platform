# Architecture Decision Records

This directory is the **only** record of where and why this repository departs from
`README.md`. The README is a fixed specification and is never edited; when the build
diverges from it, the divergence is recorded here with the options that were rejected
and the event that would reverse it.

An ADR records a decision that has been **taken**. It is not a proposal, a plan or a
preview. If the decision has not been made yet, there is nothing to record.

## Records

| # | Title | Status | Date |
|---|---|---|---|
| [0001](0001-record-architecture-decisions.md) | Architecture decisions are recorded as numbered MADR files under `docs/adr/` | accepted | 2026-08-11 |
| [0002](0002-dataplat-core-with-csv-processor-plugin.md) | `dataplat` is the source-agnostic core; `csv_processor` is a plugin that depends on it | accepted | 2026-08-11 |
| [0003](0003-uv-workspace-members-under-packages.md) | Workspace members live under `packages/`, with src-layout preserved inside each member | accepted | 2026-08-11 |
| [0004](0004-two-images-two-dependency-sets.md) | Two images with two dependency sets; the Airflow distribution is not a workspace member | accepted | 2026-08-11 |
| [0005](0005-fixture-corpus-generated-from-a-seed.md) | The CSV fixture corpus is generated from a seed and never committed; the digest file is the oracle | accepted | 2026-08-11 |
| [0006](0006-unmaintained-upstream-artifacts.md) | This platform knowingly runs three unmaintained upstream artifacts, each with a named migration target and a dated trigger | accepted | 2026-08-12 |
| [0007](0007-helm-4-over-helm-3.md) | Helm 4.2.3 is adopted over the documented Helm 3.21.3 fallback | accepted | 2026-08-12 |

Records 0002 through 0005 are the decisions Phase 1 actually takes. 0002 is the
headline departure from README §68/§75; 0003 and 0004 are its physical
consequences; 0005 is the phase's design decision about the fixture corpus. 0006
is Phase 2's supply-chain risk acceptance from the Package Legitimacy Audit; 0007
is the phase's Helm 4 compatibility gate result.

`0000-template.md` is the template. `0000` is its permanent number; it is never a
record and never appears in the table above.

## Format

**MADR 4.0 minimal**, extended with a bespoke `Migration trigger` heading. See
[ADR-0001](0001-record-architecture-decisions.md) for why.

MADR is a **superset of Nygard's** four-section form — every Nygard record is a valid
MADR record — and it adds the one section this project cannot do without:
`## Considered Options`. Nearly every decision here has the shape *"the obvious choice
is dead upstream; here is the alternative, and here is the escape hatch"*. Nygard has
nowhere to put the alternative, and the alternative is the content.

`## Migration trigger` is not part of MADR. It exists because several decisions in this
project are explicitly provisional, and it states the **observable event** that would
make us revisit one. `None — this is permanent` is a valid answer and must be written
out; a blank `Migration trigger` always means the author forgot, never that the
decision is permanent.

## Numbering

* Zero-padded four digits: `0001`, `0002`, … in a `NNNN-kebab-case-title.md` filename.
* **Monotonic and never reused.** The next record takes the next free number even if an
  earlier one was rejected or superseded.
* **Never renumbered.** Other records, code comments and commit messages cite these
  numbers; renumbering silently invalidates every citation.
* Gaps are permitted and are not an error.

The title is a decision phrased as a claim, not a topic — "Two images, two dependency
sets", not "Docker images". A reader scanning the table should learn what was decided
without opening the file.

## Superseding

**A decided record's decision is never edited.** Reversing a decision means writing a
new record and setting the old one's front matter to:

```yaml
status: superseded by ADR-00NN
```

The superseded record stays in place, unedited, with its original reasoning intact.
Its row in the table above keeps its status column updated to point forward.

This matters more here than in most projects: the README is fixed, so this log is the
sole history of the departures from it. A record edited in place to say something else
destroys exactly the evidence the log exists to preserve. `git log docs/adr/` is the
audit trail — an in-place edit of a decision section is visible there as a diff on a
file whose stated convention forbids it.

Corrections that do not change the decision — a typo, a broken link, a clarified
sentence in `Context` — are ordinary edits and need no ceremony.

## Deliberately deferred records

The absence of these is a decision, not an oversight. Each records something this
project *will* decide, in the phase where the decision is actually taken and where its
migration trigger becomes a meaningful, observable condition:

| Prospective record | Target phase | Why not now |
|---|---|---|
| Vault is BUSL-1.1 and IBM-owned; OpenBao is the API-compatible escape hatch | **Phase 5** | Nothing is deployed against Vault until Phase 5. The licence assessment is real but has no consequence to record yet. |

## Adding a record

1. Copy `0000-template.md` to `NNNN-your-decision-as-a-claim.md`, taking the next free
   number from the table above.
2. Fill in every heading. `Migration trigger` is never left blank.
3. Add the row to the table above.
4. Commit the record in the same commit as the change it justifies, so the reasoning
   and the code arrive together.
