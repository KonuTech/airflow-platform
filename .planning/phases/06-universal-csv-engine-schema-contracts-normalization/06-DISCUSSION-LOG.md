# Phase 6: Universal CSV Engine, Schema Contracts & Normalization - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-08-15
**Phase:** 6-universal-csv-engine-schema-contracts-normalization
**Areas discussed:** Schema evolution policy, Filename mask syntax, Locale/normalization profile scope, Historical schema resolution, Column contract shape, Compression/archive scope, Diagnostic code convention

---

## Schema evolution policy

### Q1 — Does a compatible new column's data reach the target table in Phase 6?

| Option | Description | Selected |
|--------|-------------|----------|
| Detect + record only | New column classified compatible, captured in `meta.schema_versions`, values not persisted until a human migration lands | ✓ |
| Auto-widen the target table | Phase 6 issues `ALTER TABLE ADD COLUMN` automatically | |
| Stash in a catch-all JSONB column | `_unmapped_fields jsonb` captures unrecognized-but-compatible values | |

**User's choice:** Detect + record only

### Q2 — What happens to a file with a breaking change, before Phase 8's quarantine engine exists?

| Option | Description | Selected |
|--------|-------------|----------|
| Whole file fails, nothing loads | Raises `IncompatibleSchemaError` before any row stages | ✓ |
| Best-effort load under the old contract | Attempt old column mapping anyway | |
| Classify + report only, no run-level consequence | Defers actual handling to Phase 8 | |

**User's choice:** Whole file fails, nothing loads

### Q3 — Where do the policy matrix default values live?

| Option | Description | Selected |
|--------|-------------|----------|
| Platform default in `configs/defaults.yaml` | Inherited via existing shallow-merge, dataset overrides only if needed | ✓ |
| Every dataset declares its own policy explicitly | No platform default | |

**User's choice:** Platform default in configs/defaults.yaml

### Q4 — Default classification for column removal and data-type retyping

| Option | Description | Selected |
|--------|-------------|----------|
| Both breaking (freeze) by default | Same treatment as a rename | ✓ |
| Removal compatible, retype breaking | Distinguishes "stopped arriving" from "lying about its shape" | |

**User's choice:** Both breaking (freeze) by default

### Q5 — Does a breaking file block sibling files in the same batch?

| Option | Description | Selected |
|--------|-------------|----------|
| No — isolated to that file's own task | Matches Phase 4's existing per-file Dynamic Task Mapping architecture | ✓ |
| Yes — a breaking file halts the whole batch | Requires new run-level gating logic | |

**User's choice:** No — isolated to that file's own task

### Q6 — How does a human find out about a compatible-schema-evolution proposal?

| Option | Description | Selected |
|--------|-------------|----------|
| SQL query only, no tooling | `meta.schema_versions` directly queryable | ✓ |
| A convenience CLI/make target surfaces it | Mirrors Phase 5's `make vault-audit-tail` | |

**User's choice:** SQL query only, no tooling

**Notes:** Combined, these six answers produce a coherent posture: only "new column appears" evolves (detect+record, no DDL); every other structural change (rename, removal, retype) freezes the whole file; defaults live platform-wide; failures stay scoped to one file's task; nothing new is built to surface proposals beyond SQL.

---

## Filename mask syntax

### Q1 — What syntax should the filename-parsing section use?

| Option | Description | Selected |
|--------|-------------|----------|
| strptime-style named tokens | `{dataset}_{country}_{business_date:%Y%m%d}_{seq:03d}.csv` | ✓ |
| Named-group regex | `(?P<dataset>\w+)_...` | |
| Hybrid: token shorthand + `regex:` escape hatch | Tokens for the common case, regex fallback | |

**User's choice:** strptime-style named tokens

### Q2 — What role does the extracted business_date facet play?

| Option | Description | Selected |
|--------|-------------|----------|
| Fallback when the data has no derivable date | Matches PITFALLS.md's literal priority order | ✓ |
| Cross-check / anomaly flag only | Data always wins; mismatch raises a quality warning | |
| Lineage/searchability metadata only | Never consulted by derivation logic | |

**User's choice:** Fallback when the data has no derivable date

### Q3 — What happens when a filename doesn't match its mask at all?

| Option | Description | Selected |
|--------|-------------|----------|
| Reject the file with a named diagnostic | Matches the phase goal's own wording | ✓ |
| Process it, leave unmatched facets null | More forgiving, loses the signal on a typo | |

**User's choice:** Reject the file with a named diagnostic

### Q4 — Given the real demo/E2E filenames have no mask structure at all, how should customers.yaml handle this?

| Option | Description | Selected |
|--------|-------------|----------|
| Filename masks are opt-in per dataset; customers.yaml doesn't declare one yet | No changes to working Phase 4 infrastructure | ✓ |
| Give customers.yaml a real mask, update demo/fixture filenames to match | Rewrites `ingest-demo.py` and E2E fixtures | |

**User's choice:** Filename masks are opt-in per dataset; customers.yaml doesn't declare one yet
**Notes:** Flagged during discussion: `scripts/ingest-demo.py` currently uploads files like `customers_small.csv-1755289200.csv` — no dataset/country/date/seq structure. This option avoids breaking that flow.

### Q5 — Should individual mask tokens be markable optional?

| Option | Description | Selected |
|--------|-------------|----------|
| Yes — bracket syntax marks optional facets | `[_{seq:03d}]`, matches CSV-01's "where present" wording | ✓ |
| No — every declared token is mandatory | Would need separate mask patterns per shape variant | |

**User's choice:** Yes — bracket syntax marks optional facets

---

## Locale/normalization profile scope

### Q1 — What granularity for the locale/normalization contract?

| Option | Description | Selected |
|--------|-------------|----------|
| One locale profile per dataset | Single block applies to every column | ✓ |
| Per-column overridable | Needed for mixed-locale files (e.g. USD + PLN columns) | |

**User's choice:** One locale profile per dataset

### Q2 — Named presets or explicit fields?

| Option | Description | Selected |
|--------|-------------|----------|
| Named presets (e.g. `locale: pl-PL`) | Small registry bundles separator/token defaults | |
| Explicit fields only, no presets | No registry to build or maintain | ✓ |

**User's choice:** Explicit fields only, no presets

### Q3 — What's the default NULL-token set?

| Option | Description | Selected |
|--------|-------------|----------|
| Empty string only; everything else is explicit opt-in | Zero ambiguity | ✓ |
| A small conservative platform default list | Empty string + literal `NULL` inherited by every dataset | |

**User's choice:** Empty string only; everything else is explicit opt-in

### Q4 — Fixed platform rule or per-dataset choice for Unicode normalization form?

| Option | Description | Selected |
|--------|-------------|----------|
| Always normalize to NFC — not configurable | No known use case needs anything but NFC | ✓ |
| Contract-configurable per dataset (NFC/NFD/none) | Adds a field for an unused choice | |

**User's choice:** Always normalize to NFC — not configurable

**Notes:** Confirmed as a natural consequence (not asked as a fresh question): `customers.yaml` needs no CSV-10 locale block today (no numeric columns) but will still need explicit `strptime` date-format declarations for `birth_date`/`event_ts`, per the already-locked "never `dateutil.parser.parse`" rule.

---

## Historical schema resolution

### Q1 — How does a file get matched to its historical schema version?

| Option | Description | Selected |
|--------|-------------|----------|
| Re-derive and hash-match against schema_versions history | Self-contained, works regardless of filename mask | ✓ |
| Filename's version facet is authoritative | Only works for datasets with a mask + version token | |
| Explicit backfill run parameter only | Doesn't cover the ordinary late-arriving-old-file case | |

**User's choice:** Re-derive and hash-match against schema_versions history

### Q2 — Build the general config_policy replay knob now, or just the specific mechanism?

| Option | Description | Selected |
|--------|-------------|----------|
| Just the specific hash-match mechanism | Matches ROADMAP's literal Phase 6 success criteria, nothing more | ✓ |
| Build the full config_policy knob now | Implements all 3 replay modes ahead of Phase 9's need | |

**User's choice:** Just the specific hash-match mechanism

---

## Column contract shape

### Q1 — How should the new `columns:` section relate to `deduplication.keys`?

| Option | Description | Selected |
|--------|-------------|----------|
| `columns:` is the source of truth; `deduplication.keys` stays, cross-checked by a validator | No rework of Phase 4's working dedup code | ✓ |
| `columns:` fully absorbs business-key marking; `deduplication.keys` removed | Touches already-working Phase 4 code | |

**User's choice:** columns: becomes the source of truth; deduplication.keys stays but must reference names declared there

### Q2 — How should column "semantics" be expressed?

| Option | Description | Selected |
|--------|-------------|----------|
| Free-text description field | Documentation value only | |
| Controlled tags (pii, sensitivity, role) | Machine-actionable, needs a vocabulary designed now | |
| Free-text description now; tags deferred | Satisfies SCHEMA-02 literally without inventing a vocabulary | ✓ |

**User's choice:** Free-text description now; tags deferred

### Q3 — Two distinct fields (required + nullable) or one?

| Option | Description | Selected |
|--------|-------------|----------|
| Two distinct fields: required + nullable | Matches SCHEMA-02's literal wording; genuinely different failure modes | ✓ |
| Collapse to one field (nullable only) | Simpler, doesn't literally give "required" its own field | |

**User's choice:** Two distinct fields: required (must appear) + nullable (value can be empty)

---

## Compression/archive scope

### Q1 — Does .zip need multi-member support?

| Option | Description | Selected |
|--------|-------------|----------|
| Exactly one CSV per archive, same as .gz | Consistent with existing corpus fixture 61 | ✓ (with addition) |
| Multi-member zip — all CSVs discovered as separate files | Closer to existing multi-part discovery grouping | |

**User's choice:** Option 1 (exactly one CSV per archive). **Free-text addition:** "However, python libraries should allow to read compressed files without decompression."
**Notes:** This surfaced a real architectural constraint not covered by the original options — decompression must happen in-stream (wrapping the compressed byte stream directly, e.g. `gzip.GzipFile`/`zipfile.ZipFile` over the existing boto3 `StreamingBody` adapter), never by decompressing to a temp file first. Captured as D-22, treated as a locked constraint per the user's explicit correction.

---

## Diagnostic code convention

### Q1 — Should every failure carry a stable diagnostic code, or is message + context enough?

| Option | Description | Selected |
|--------|-------------|----------|
| Stable diagnostic codes (a small catalog) | Formalizes the corpus's existing informal `quarantine_reason` pattern | ✓ |
| Message + context dict only, no formal catalog | "Named" satisfied by exception class name alone | |

**User's choice:** Stable diagnostic codes (a small catalog)

### Q2 — Should the catalog adopt the corpus's existing diagnostic strings?

| Option | Description | Selected |
|--------|-------------|----------|
| Yes — the corpus's existing strings ARE the catalog | Oracle and implementation can never quietly drift apart | ✓ |
| No — design a separate, independent code catalog | More deliberate naming, touches 69 committed fixtures | |

**User's choice:** Yes — the corpus's existing strings ARE the catalog

### Q3 — Should row-level and file/run-level diagnostic codes be one unified vocabulary?

| Option | Description | Selected |
|--------|-------------|----------|
| Yes — one shared code vocabulary for rows, files and runs | `RejectedRecord.error_type` and exception context codes draw from the same catalog | ✓ |
| No — keep row-level and file/run-level codes as separate systems | Two disconnected naming schemes | |

**User's choice:** Yes — one shared code vocabulary for rows, files and runs

---

## Claude's Discretion

- Exact type-token vocabulary for `columns:[].type` (small closed set implied by requirement text).
- Exact regex-anchoring semantics for filename masks (unexercised by customers today).
- Decompression dispatch mechanism (file extension vs. magic-byte sniffing).
- Whether a corrupted/truncated-archive corpus fixture is added proactively now or only once encountered.
- Exact module/location for the diagnostic-code catalog; one-to-one vs. many-to-one code-to-exception-class mapping.
- Whether a schema-authoring bootstrap CLI gets built (not required by any named requirement).
- Multi-row/hierarchical header handling (already locked by ROADMAP itself: detect + reject, no flattening).

## Deferred Ideas

- Auto-widening the target table via `ALTER TABLE ADD COLUMN` on a compatible change — considered and explicitly **rejected** (anti-feature per FEATURES.md), not merely deferred.
- A catch-all `_unmapped_fields jsonb` column — considered and explicitly **rejected** in favor of detect+record-only.
- Controlled-vocabulary column semantic tags (`pii`, `role`, etc.) — deferred until a real machine-actionable need appears.
- Per-column locale overrides (mixed decimal separators in one file) — deferred until a real dataset needs it.
- The general `config_policy` replay knob (`AS_OF_LOGICAL_DATE`/`LATEST`/`PINNED`) — deferred to whichever phase first needs human-selectable replay (likely Phase 9).
- Multi-member `.zip` archive support — deferred until a real feed delivers bundled archives.
- A convenience CLI/make target for pending schema-evolution proposals — deferred until SQL-only querying proves insufficient in practice.
- A schema-authoring bootstrap CLI (infer a draft contract from a sample file) — not committed; build only if cheap alongside the inference engine.
