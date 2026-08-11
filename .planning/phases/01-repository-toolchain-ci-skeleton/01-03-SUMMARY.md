---
phase: 01-repository-toolchain-ci-skeleton
plan: 03
subsystem: fixture-corpus
tags: [qual-08, determinism, corpus, yaml, sha256, policy-tests]
status: complete

requires:
  - 01-01 (Makefile gate chain, ruff/mypy config, stubbed fixtures targets)
provides:
  - tools/corpus/ — validated manifest model, deterministic generator, digest oracle, CLI
  - tests/fixtures/corpus.yaml — the committed specification (5 of 69 fixtures)
  - tests/fixtures/CORPUS.sha256 — the committed oracle, in standard sha256sum format
  - three policy tests enforcing determinism rules R1, R2 and R6
  - `make fixtures` / `make fixtures-verify`, with fixtures-verify inside `make check`
affects:
  - plans 01-06, 01-07 and 01-08, which author the remaining 64 fixtures against
    this framework without changing it
  - Phase 6 detector tests, which become a parametrised loop over `expect:` blocks
  - Phase 8 quarantine vocabulary, which is writable today via the permissive `expect:`

tech-stack:
  added:
    - stdlib only — yaml.safe_load, dataclasses, hashlib, random, gzip, unicodedata, codecs
  patterns:
    - Generated corpus, committed oracle — `generate` rewrites, `verify` only reads
    - Per-fixture derived random streams so one added fixture is one changed digest line
    - Architecture policy as pytest, with AST-based inspection so documentation is not
      mistaken for a violation

key-files:
  created:
    - tools/corpus/__init__.py
    - tools/corpus/manifest.py
    - tools/corpus/generators.py
    - tools/corpus/digests.py
    - tools/corpus/__main__.py
    - tests/fixtures/corpus.yaml
    - tests/fixtures/CORPUS.sha256
    - tests/unit/test_corpus_manifest.py
    - tests/policy/test_corpus_determinism.py
    - tests/policy/test_corpus_not_committed.py
    - tests/policy/test_generator_determinism_rules.py
  modified:
    - Makefile

decisions:
  - The manifest model is hand-written frozen dataclasses rather than Pydantic,
    because Pydantic is not in the dev dependency group and `pyproject.toml`/`uv.lock`
    are outside this plan's declared file scope. The semantic contract STACK.md asks
    for is unchanged (frozen, unknown keys forbidden outside `expect:`, safe_load
    only); hand-writing additionally lets every rejection name the fixture, which a
    positional path into a YAML list does not.
  - Digest listing names are repository-root-relative (`tests/fixtures/csv/01_simple.csv`),
    not the bare names 01-RESEARCH.md illustrates, so `sha256sum -c` works from the
    repository root as the plan's acceptance criterion requires.
  - `generate --fast --write-digests` is refused rather than merged. A partial oracle
    looks complete, which is the failure mode the oracle exists to prevent.
  - The large fixture is 11,000,000 rows (293 MB), not RESEARCH's 9,000,000 (241 MB),
    because RESEARCH's own worked entry violates the `approx_bytes > 2 * rlimit_as_bytes`
    invariant it specifies. Raising the row count preserves the *verified* 128 MiB limit.
  - The R2/R6 policy scan masks docstrings and comments and inspects the AST, rather
    than matching raw text, so the generator can name the helpers it must not use.
  - Intra-package imports under `tools/corpus/` are relative, because `tools/` is a
    namespace package and absolute self-imports make mypy resolve the tree twice.

requirements-completed: [QUAL-08]

coverage:
  - deliverable: "The corpus regenerates byte-identically from the recorded seed"
    human_judgment: false
    verification:
      - kind: command
        ref: "make fixtures && make fixtures-verify"
        status: pass
      - kind: command
        ref: "TZ=America/New_York LC_ALL=C.UTF-8 PYTHONHASHSEED=99 make fixtures-verify"
        status: pass
      - kind: test
        ref: "tests/policy/test_corpus_determinism.py#test_two_generations_in_one_process_agree"
        status: pass
  - deliverable: "The oracle is independently checkable and detects a single altered byte"
    human_judgment: false
    verification:
      - kind: command
        ref: "sha256sum -c tests/fixtures/CORPUS.sha256"
        status: pass
      - kind: command
        ref: "corrupt one hex char of a digest line -> fixtures-verify exits 1 naming the fixture"
        status: pass
      - kind: command
        ref: "alter one literal declaration in a scratch manifest -> verify exits 1 naming it"
        status: pass
  - deliverable: "A corrupt manifest fails loudly at load time"
    human_judgment: false
    verification:
      - kind: test
        ref: "tests/unit/test_corpus_manifest.py"
        status: pass
  - deliverable: "No corpus file is committed"
    human_judgment: false
    verification:
      - kind: test
        ref: "tests/policy/test_corpus_not_committed.py#test_no_generated_fixture_is_tracked_by_git"
        status: pass
  - deliverable: "Determinism rules R1, R2 and R6 cannot erode silently"
    human_judgment: false
    verification:
      - kind: test
        ref: "tests/policy/test_generator_determinism_rules.py"
        status: pass
      - kind: command
        ref: "inject rng.choice into tools/corpus/digests.py -> fails naming file:145 and R2"
        status: pass
  - deliverable: "The generator framework covers every kind the remaining 64 fixtures need"
    human_judgment: true
    rationale: >
      Four generator kinds and the large profile are exercised by the five seeded
      fixtures, but whether they suffice for all 69 is only provable when plans
      01-06 to 01-08 author the rest. The claim is structural, not yet observed.

metrics:
  duration: ~50 min
  completed: 2026-08-11
  tasks: 3
  commits: 4

actuals:
  tokens: 21600
  tasks: 3
  commits: 4
---

# Phase 1 Plan 03: Fixture Corpus Framework Summary

A deterministic corpus generator whose bytes are reproducible from a committed
seed under a foreign timezone, locale and hash seed, proven on every `make check`
against a `sha256sum`-format oracle that an independent tool can also verify.

## What was built

**Task 1 — the manifest is the specification** (`af1879f` RED, `b46b7a5` GREEN)

`tools/corpus/manifest.py` parses `tests/fixtures/corpus.yaml` with
`yaml.safe_load` into frozen dataclasses. The outer schema forbids unknown keys;
`expect:` accepts them, with the reason written on the model — Open Question 3's
adopted resolution, so the Phase 6 encoding-confidence floor and the Phase 8
quarantine vocabulary are writable today without a model migration.

Cross-field rules, each rejecting at load time rather than generating something
incoherent: a delimiter that collides with a column's decimal separator; a
large-profile fixture whose declared size is not more than twice its declared
address-space limit; duplicate fixture names; a wrapper naming a target not
declared earlier; unknown generator kinds, encodings, line terminators and
profiles; and a `literal_unicode` fixture whose rows declare different fields.

Ten unit tests, one per rejection case, each asserting on the error *message*.
Every message names both the offending key and the fixture it appeared in.

**Task 2 — deterministic generation and the committed oracle** (`926dd1d`)

`generators.py` implements R1–R10. Four kinds: `tabular` (streamed, so the
293 MB fixture is never materialised whole), `literal` (escaped bytes resolved
from the manifest), `literal_unicode` (explicit `unicodedata.normalize`), and
`wrapper` (gzip with `mtime=0`, `filename=""` and a pinned compression level).
Randomness is consumed only through `Random.random()`; integers and selections
are index arithmetic over it. A module-level comment states that the
reproducible PRNG is the correct one here and `secrets` is not.

`digests.py` emits standard `sha256sum` lines. `__main__.py` exposes
`generate` and `verify`; `verify` regenerates into a temporary directory and
reads only the manifest and the oracle.

`corpus.yaml` seeds five fixtures — `01_simple.csv`, `29_large_file.csv`,
`32_nul_bytes.csv`, `44_unicode_nfc_vs_nfd.csv`, `61_gzipped.csv.gz` — covering
every generator kind and the large profile, so no later plan has to change the
framework and re-baseline everything.

**Task 3 — policy tests that keep the rules from eroding** (`9e67164`)

`test_corpus_determinism.py` generates the whole corpus twice in one process and
compares the two digest maps to each other, never to the oracle: a failure there
can only mean the generator disagrees with itself, which the Makefile's
separate-process check cannot distinguish from a stale oracle.

`test_corpus_not_committed.py` asserts `git ls-files tests/fixtures/csv` is
empty — and, in the same file, that the manifest and the oracle *are* tracked,
because otherwise the first assertion passes for the wrong reason.

`test_generator_determinism_rules.py` enforces R2 and R6 over every module under
`tools/corpus/`, and R1 by its observable consequence.

## Verification performed

| Claim | Command | Result |
|---|---|---|
| Regenerates against the oracle | `make fixtures && make fixtures-verify` | `5 fixtures match` |
| Stable across environment | `TZ=America/New_York LC_ALL=C.UTF-8 PYTHONHASHSEED=99 make fixtures-verify` | pass, with `tests/fixtures/csv/` deleted first |
| Independently checkable | `sha256sum -c tests/fixtures/CORPUS.sha256` (repo root) | 5× `OK` |
| Oracle is stable | second `make fixtures` then `git diff --exit-code` | no diff |
| One corrupted hex char | edit a digest line, `make fixtures-verify` | exit 1, names `01_simple.csv` with both digests |
| One altered `literal` declaration | scratch manifest with `ab\x00cd` → `abXcd` | exit 1, names `32_nul_bytes.csv` — **and only that fixture**, which is R1 observed rather than asserted |
| Forbidden helper in a real module | append `rng.choice(...)` to `digests.py` | `tools/corpus/digests.py:145: 'choice' violates determinism rule R2`; reverted |
| `--fast` cannot write a partial oracle | `generate --fast --write-digests` | refused, oracle untouched |
| Large fixture is real | `stat -c%s tests/fixtures/csv/29_large_file.csv` | `293058633` (> 200 MB) |
| Corpus is not committed | `git ls-files tests/fixtures/csv` | empty |
| Gate | `make check` | green, 35.9 s (RESEARCH budget: < 90 s) |

Byte-level spot checks: `32_nul_bytes.csv` contains a real `\0` inside the
field; `44_unicode_nfc_vs_nfd.csv` contains `Wi ś niewski` as `c5 9b` (NFC) on
one row and `73 cc 81` (NFD) on the other, so the distinction the fixture exists
to test survived the round trip through YAML and git.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 3 — Blocking] Pydantic is not available, and adding it is out of scope**

- **Found during:** Task 1.
- **Issue:** STACK.md §F and the plan both describe the manifest model in
  Pydantic terms, but `pydantic` is not in the `dev` dependency group and this
  plan's `files_modified` does not include `pyproject.toml` or `uv.lock`.
  Adding it would also mean editing `uv.lock` — the highest-conflict file in a
  three-way parallel wave — and `uv lock` needs network.
- **Fix:** frozen `@dataclass(slots=True)` models with hand-written validation.
  Every property the requirement actually names is preserved: frozen, unknown
  keys forbidden at the outer level and permitted inside `expect:`,
  `yaml.safe_load` only. There is a genuine gain, not just parity: Pydantic's
  default message for an unknown key is a positional path (`fixtures.0.typo`),
  and the plan requires the message to name the *fixture*.
- **Files:** `tools/corpus/manifest.py`
- **Commit:** `b46b7a5`

**2. [Rule 3 — Blocking] mypy strict has no PyYAML stubs**

- **Found during:** Task 1, `make typecheck`.
- **Issue:** PyYAML 6.0.3 ships no `py.typed`, so `import yaml` fails mypy
  strict with `import-untyped`. The project-wide fix (`types-PyYAML`, or a
  `[[tool.mypy.overrides]]` entry) is in `pyproject.toml` — out of scope.
- **Fix:** a single narrowed `# type: ignore[import-untyped]` on the import,
  with a comment recording why it is scoped to one line and that it disappears
  the moment the stubs land. Everything `yaml` returns is re-validated before it
  reaches a typed model, so the untyped boundary is one line wide. `strict`
  includes `warn_unused_ignores`, so this fails loudly rather than lingering.
- **Files:** `tools/corpus/manifest.py`
- **Commit:** `b46b7a5`
- **Follow-up for a later plan:** add `types-PyYAML` to the `dev` group and
  delete the suppression.

**3. [Rule 1 — Bug] RESEARCH's large-fixture entry violates its own invariant**

- **Found during:** Task 2, writing `corpus.yaml`.
- **Issue:** 01-RESEARCH.md line 843 specifies `rows: 9000000` /
  `approx_bytes: 241000000` / `rlimit_as_bytes: 134217728`, and line 895
  specifies the invariant `approx_bytes > 2 × rlimit_as_bytes`. But
  2 × 134,217,728 = 268,435,456 > 241,000,000, so the worked entry fails the
  rule stated two paragraphs later. Transcribing it would have made the manifest
  unloadable.
- **Fix:** raised the fixture to 11,000,000 rows (measured 293,058,633 bytes)
  and kept `rlimit_as_bytes: 134217728`. Raising rows rather than lowering the
  limit is the safer lever: 128 MiB is the value RESEARCH *verified* leaves the
  interpreter its ~30–50 MB of footprint, whereas a lower limit is untested.
  `approx_bytes` is declared as the measured value, not a flattering round one.
- **Files:** `tests/fixtures/corpus.yaml`
- **Commit:** `926dd1d`
- **Cost:** generation is ~12 s rather than RESEARCH's projected ~4 s. The pure
  Python row loop, not the size, is the dominant term.

**4. [Rule 1 — Bug] Bare digest names are not checkable from the repository root**

- **Found during:** Task 2, running the plan's `sha256sum -c` acceptance check.
- **Issue:** RESEARCH's illustrated oracle uses bare file names, which resolve
  only when `sha256sum` is run from inside `tests/fixtures/csv/`. The plan's
  acceptance criterion requires it to succeed from the repository root. A
  listing that only works from one undocumented directory turns the independent
  second opinion into a trick you have to know.
- **Fix:** names are written repository-root-relative. `verify` applies the same
  prefix to its temporary-directory results, so the prefix is a rendering
  concern and the throwaway path never reaches the oracle.
- **Files:** `tools/corpus/digests.py`, `tools/corpus/__main__.py`
- **Commit:** `926dd1d`

**5. [Rule 3 — Blocking] mypy resolved `tools/corpus/` under two module names**

- **Found during:** Task 2, `make typecheck`.
- **Issue:** `tools/` has no `__init__.py`, so mypy names the package `corpus`
  when walking the tree but `tools.corpus` when following an absolute
  self-import — `Source file found twice under different module names`.
- **Fix:** intra-package imports are relative. The obvious alternative, adding
  `tools/__init__.py`, was rejected deliberately: plan 01-02 is creating
  `tools/security/` in a sibling worktree right now and would hit the identical
  error, so both plans would independently create the same file and produce an
  add/add merge conflict. Relative imports fix it inside this plan's own files.
- **Files:** `tools/corpus/generators.py`, `tools/corpus/__main__.py`
- **Commit:** `926dd1d`

**6. [Rule 1 — Bug] A textual R2 scan would have flagged its own documentation**

- **Found during:** Task 3.
- **Issue:** the plan specifies that the forbidden helper names "must not
  appear" in `tools/corpus/`. Taken literally that fails immediately:
  `generators.py`'s module docstring names `choice`, `shuffle`, `sample`,
  `randrange`, `randint`, `secrets` and `os.urandom` precisely in order to
  explain why none of them may be called. A scan that punishes the explanation
  teaches authors to delete it.
- **Fix:** the scan masks docstrings and comments (via `ast` line ranges and
  `tokenize` COMMENT tokens) and inspects the AST for attribute, name and import
  identifiers. Two nets, because AST alone misses `getattr(random, "choice")`
  and text alone misses nothing but flags prose. A test asserts the masking
  directly, so a future author cannot "fix" the scan by removing it.
- **Files:** `tests/policy/test_generator_determinism_rules.py`
- **Commit:** `9e67164`

**7. [Addition] The R1 probe is inserted at the head of the fixture list**

Not specified by the plan, which says "append a new fixture declaration". As
written that assertion would pass even with the bug it exists to catch: a shared
random stream only perturbs fixtures generated *after* the insertion point, so
appending at the end proves nothing. Of the five seeded fixtures only the two
`tabular` ones draw randomness at all, and one of those is skipped in fast mode,
so the probe is inserted *before* `01_simple.csv`. The reason is written in the
test's docstring, because the insertion point looks arbitrary otherwise.

**8. [Addition] `make fixtures FAST=1` and its oracle guard**

RESEARCH line 897 asks for a fast switch. The hazard is that
`generate --fast --write-digests` would silently write an oracle missing every
large-profile digest — a partial listing that looks complete. The CLI refuses
that combination outright, and the Makefile drops `--write-digests` when
`FAST=1` rather than passing a truncated listing.

### Verified corrections to the research

- **`32_nul_bytes.csv` expects quarantine, not a parser exception.** Carried
  from the plan and re-confirmed here: the generated file contains a real NUL
  inside a field, and its `expect:` block declares `parser_raises: false` plus
  `quarantine_reason: "nul-byte-in-text-field"`. FEATURES.md §3.4 says the
  stdlib reader raises; it does not on CPython 3.12.3.
- **R1 was observed, not merely asserted.** Altering `32_nul_bytes.csv`'s
  declaration changed exactly one digest and left `44` and `61` — both declared
  after it — untouched.

## Authentication gates

None. This plan touches no authenticated service and no network.

## Known stubs

None. Every code path this plan introduces is reachable and exercised.

Sixty-four of the sixty-nine fixtures are not yet authored. That is the plan's
stated scope, not a stub: the five seeded here exercise all four generator kinds
and the large profile, so plans 01-06, 01-07 and 01-08 add manifest entries
without touching the generator.

## Threat flags

None. No new network endpoint, auth path or schema at a trust boundary.

Mitigations assigned to this plan were applied and asserted:

| Threat | Mechanism | Asserted by |
|---|---|---|
| T-01-13 EoP via YAML deserialisation | `yaml.safe_load` only, into frozen models forbidding unknown keys | `test_yaml_object_construction_is_refused` and nine sibling tests |
| T-01-14 Tampering — corpus drifts from spec | committed oracle; `verify` regenerates to a temp dir and only reads the oracle | two negative tests, both observed failing then reverted |
| T-01-15 Repudiation — rules erode silently | AST + masked-source inspection for R2/R6; R1 by consequence | `test_generator_determinism_rules.py`, observed failing on an injected violation |
| T-01-18 Tampering — fixtures enter a build context | `.dockerignore` (01-01) plus git exclusion | `test_corpus_not_committed.py` |

T-01-16 (synthetic credential-shaped values) has no surface yet: none of the
five seeded fixtures contains a credential-shaped value. It becomes live when
plans 01-07/01-08 add them, against plan 01-02's scoped allowlist.

## Requirements addressed

| ID | Mechanism |
|---|---|
| QUAL-08 | `make fixtures-verify` inside `make check` regenerates the whole corpus and compares to `tests/fixtures/CORPUS.sha256`; observed failing on both a corrupted oracle line and an altered manifest declaration. Second signal: `test_corpus_not_committed.py`. Third: `test_corpus_determinism.py` for intra-process determinism. |

## Next

Ready for plans 01-06, 01-07 and 01-08 to author the remaining 64 fixtures. Two
things they should know: adding a fixture must change exactly one line of
`CORPUS.sha256` (if it changes more, R1 has been broken, and
`test_generator_determinism_rules.py` will say so), and every `tabular` column
needs an explicit `row_spec` entry — the model has no default, deliberately.

## Self-Check: PASSED

All 12 declared files verified present on disk. All 4 commits verified in
`git log`. Working tree clean at `9e67164`; `make check` green; no file
deletions in any commit; no file created or edited outside the plan's declared
`files_modified` scope; `STATE.md` and `ROADMAP.md` untouched.
