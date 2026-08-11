---
phase: 01-repository-toolchain-ci-skeleton
plan: 08
subsystem: fixture-corpus
tags: [qual-08, corpus, numerics, dates, timezones, dst, nulls, booleans, semantic-damage]
status: complete

requires:
  - 01-03 (the manifest model, the deterministic generator, the digest oracle, the R1/R2/R6 policy tests)
  - 01-06 (the six byte-level capabilities and the `output_names` definition of what a declaration emits)
  - 01-07 (the `expect:`-block test pattern and the single-quoted `content:` convention)
provides:
  - the final seventeen semantic-damage declarations; the corpus is CLOSED at 69 of 69 declared, 70 emitted files
  - tests/unit/test_corpus_semantic_fixtures.py — eight assertions the digest structurally cannot make, including the completeness check
  - the decision-pinning expectations: unrecoverable spreadsheet damage, undecidable date formats, DST gap/overlap classification, present-and-empty vs absent
  - ADR-0005 updated with the full-corpus scope decision and why the expectation sub-schema stays permissive
affects:
  - Phase 6 detector and normaliser work, which is now measured against a specification rather than against itself
  - Phase 8 quarantine vocabulary, extended here with six more reasons
  - Phase 10 SCD effective-dating, which inherits 55 and 56's timezone declarations

tech-stack:
  added:
    - none — declarations plus one test module; `tools/corpus/` is byte-identical to its 01-06 state
  patterns:
    - A pair of fixtures carrying the SAME quantities under opposite conventions, so each is the other's misreading
    - A clean control row inside every damaged fixture, so "detects the damage" and "rejects everything" differ
    - The test re-derives a fact from the source of truth (zoneinfo) when the manifest is what it exists to check
    - A narrow, named exemption list rather than a pattern, with an assertion that every exempted key is still in use

key-files:
  created:
    - tests/unit/test_corpus_semantic_fixtures.py
  modified:
    - tests/fixtures/corpus.yaml
    - tests/fixtures/CORPUS.sha256
    - docs/adr/0005-fixture-corpus-generated-from-a-seed.md

key-decisions:
  - 50 (scientific notation) declares `damage_is_unrecoverable` because digits are genuinely gone — six significant digits remain of fifteen, and expanding the exponent form yields a valid-looking identifier for a DIFFERENT record. 51 (leading zeros) deliberately does NOT claim unrecoverability: for a declared fixed-width column, left-padding is deterministic. Its rejection rests on a different and more precise argument — padding manufactures a digit that is not in the file, on an assumption that is usually right, which is what makes it dangerous to apply silently. It is a declared and RECORDED repair, never a parser default. Writing "unrecoverable" for both would have been the plan's literal wording and would have put a false statement into the specification.
  - 22/23 and 20/21 are authored as PAIRS carrying the same three dates and the same four quantities under opposite conventions. The pairing is the specification: it proves the rendering carries no evidence of its own format, which a single fixture cannot show.
  - 52 names both candidate formats even though neither may be chosen. "Detection declines" is a decision about a named candidate set, not a shrug, and Phase 6's detector has to try exactly those two.
  - 54 declares the epoch as "the analogue of a format for this encoding" rather than inventing a strftime pattern for an integer. The epoch is the parameter without which the characters do not denote a day.
  - The DST test re-derives its classifications from `zoneinfo` instead of comparing against the manifest, breaking this module's own rule. The manifest is exactly what that test exists to check, so trusting it would make the test circular.
  - Classification uses the UTC round-trip, not a cross-fold offset comparison. The obvious implementation (offsets differ between fold=0 and fold=1) is true for BOTH the gap and the overlap and therefore distinguishes neither — this was caught by running it before writing the declaration.
  - The float prohibition is scoped to data-valued expectations. `encoding_confidence_min: 1.0` (01-06) is a probability, not a value that must round-trip; the exemption is a two-key list with an assertion that both keys are still declared somewhere.

requirements-completed: [QUAL-08]

coverage:
  - id: D1
    description: "All sixty-nine fixtures named across README §73 and the research feature additions are declared, with no gaps and no duplicates"
    requirement: QUAL-08
    human_judgment: false
    verification:
      - kind: unit
        ref: "tests/unit/test_corpus_semantic_fixtures.py#test_the_corpus_is_complete_against_both_sources"
        status: pass
      - kind: other
        ref: "69 unique names in corpus.yaml, 70 lines in CORPUS.sha256"
        status: pass
  - id: D2
    description: "The unrecoverable-damage cases are declared as rejections with reasons, not as values to coerce"
    requirement: QUAL-08
    human_judgment: false
    verification:
      - kind: unit
        ref: "test_the_scientific_notation_fixture_really_lost_digits — 6 significant digits vs 15, expansion != original"
        status: pass
      - kind: unit
        ref: "test_the_leading_zero_fixture_differs_by_the_leading_zeros_alone — lstrip('0') equality on both damaged rows"
        status: pass
  - id: D3
    description: "The daylight-saving fixture declares a nonexistent local time and an ambiguous one against a named zone"
    requirement: QUAL-08
    human_judgment: false
    verification:
      - kind: unit
        ref: "test_the_daylight_saving_times_are_really_nonexistent_and_ambiguous — classifications re-derived from zoneinfo"
        status: pass
      - kind: other
        ref: "transitions located by search: 2026-03-29T01:00:00Z (01:59:59 -> 03:00:00), 2026-10-25T01:00:00Z (02:59:59 -> 02:00:00)"
        status: pass
  - id: D4
    description: "The empty-string and null cases are distinguishable by expectation"
    requirement: QUAL-08
    human_judgment: false
    verification:
      - kind: unit
        ref: "test_the_final_field_is_present_and_empty_not_absent — field counts {0:3,1:3,2:2,3:3}; row 1 ends with the delimiter, row 2 does not"
        status: pass
      - kind: unit
        ref: "test_the_null_and_boolean_tokens_are_matched_exactly_not_by_substring"
        status: pass
  - id: D5
    description: "No date fixture declares an inferred format; every one names the format it uses"
    requirement: QUAL-08
    human_judgment: false
    verification:
      - kind: other
        ref: "all seven temporal declarations scanned for a '%'-bearing format key; MISSING FORMATS: NONE"
        status: pass
      - kind: unit
        ref: "test_every_component_of_the_ambiguous_date_fixture_is_twelve_or_below — both readings parse, neither raises"
        status: pass
  - id: D6
    description: "No numeric expectation is written as a binary floating-point value"
    requirement: QUAL-08
    human_judgment: false
    verification:
      - kind: unit
        ref: "test_no_data_valued_expectation_is_a_binary_float — recursive scan over every expect: block"
        status: pass
  - id: D7
    description: "Adding these fixtures leaves every previously-committed digest line unchanged (R1)"
    requirement: QUAL-08
    human_judgment: false
    verification:
      - kind: other
        ref: "git diff --numstat CORPUS.sha256 — 7/0, 7/0, 3/0 after tasks 1, 2 and 3"
        status: pass
      - kind: unit
        ref: "tests/policy/test_generator_determinism_rules.py#test_adding_a_fixture_leaves_every_other_digest_unchanged"
        status: pass
  - id: D8
    description: "No semantic case required a generator change"
    requirement: QUAL-08
    human_judgment: false
    verification:
      - kind: other
        ref: "git diff --stat tools/corpus/ — empty after every task"
        status: pass
  - id: D9
    description: "The corpus is complete enough that Phase 6 implements against a specification rather than against itself"
    human_judgment: true
    rationale: >
      All 69 declarations exist and every `expect:` block states a declared
      meaning, so the structural claim holds today. Whether the corpus is
      SUFFICIENT — whether Phase 6 can build the detector and normaliser
      against these blocks without discovering a missing case — is only
      provable when Phase 6 runs. The expectations were written before any
      normaliser exists, which is the property that makes them a
      specification rather than a description, but sufficiency is a
      prediction until it is used.

metrics:
  duration: ~55 min (including a resume after an external quota interruption)
  completed: 2026-08-11
  tasks: 3
  commits: 3

actuals:
  tokens: 27400
  tasks: 3
  commits: 3
---

# Phase 1 Plan 08: Semantic Fixture Corpus Summary

The final seventeen fixtures — numeric, temporal, null and boolean damage —
closing the corpus at all sixty-nine names, with the decisions a normaliser
could get catastrophically wrong pinned before the normaliser exists.

## What was built

**Task 1 — numeric damage, including the two unrecoverable ones (7)** (`29f0907`)

`20_decimal_comma`, `21_decimal_point`, `50_excel_scientific_notation_ids`,
`51_excel_leading_zero_stripped`,
`57_negative_parentheses_and_trailing_minus`, `58_currency_and_percent`,
`59_numeric_null_sentinels`.

**50 is the most important declaration in the plan.** A fifteen-digit
identifier rendered `1.23457E+14` carries six significant digits; nine are
gone from the file. Expanding it yields `123457000000000` — measured, not
assumed — which is a valid-looking identifier for a *different* record. So the
declared outcome is a rejection with a reason, and both the original and the
rendered form are recorded so the loss is visible in the specification itself.
Row 3 is the same fifteen digits intact, so a reader cannot pass by rejecting
every long number.

**51 is where the plan's wording and the truth diverged slightly, and the
summary should say so.** The plan groups 50 and 51 as "unrecoverable". For a
declared fixed-width column, left-padding `1234` to `01234` is deterministic —
it *is* recoverable in principle. Writing `damage_is_unrecoverable: true` there
would have put a false statement into a document whose whole purpose is to be
believed. The declaration instead rests on a sharper argument:
`padding_would_manufacture_a_digit_the_file_does_not_contain`, and the repair
is available as a *declared and recorded* repair rather than a parser default.
Same outcome the plan asked for (a rejection with a reason), defensible reason.

The other four: 20/21 are a pair carrying the same four quantities under
`pl-PL` and `en-US` profiles; 57 pins that stripping non-numeric characters
from `(123.45)` and `123.45-` yields a **sign flip**, not a parse error; 58
pins that reading a grouping dot as a decimal point is wrong by a factor of a
thousand *and still a valid decimal*, so nothing raises; 59 declares sentinels
as a contract decision with a genuine `-1.50` alongside so the two are
distinguishable.

**Task 2 — temporal, including the DST edges (7)** (`4cf7612`)

`22_eu_dates`, `23_us_dates`, `52_date_ambiguous_dm_vs_md`,
`53_two_digit_year`, `54_excel_serial_dates`, `55_dst_gap_and_overlap`,
`56_mixed_timezone_offsets`.

22 and 23 carry **the same three ISO dates** under opposite conventions —
verified by parsing both with `strptime` before the declarations were written.
Row 2 of each (`01/02/2026` and `02/01/2026`, both 2026-02-01) is the other's
misreading. That pairing is what proves the rendering carries no evidence of
its own format, which no single fixture can show.

53 records a fact worth having in writing: under CPython's inherited POSIX
pivot the adjacent two-digit years `68` and `69` land in **2068 and 1969** —
ninety-nine years apart. Verified.

**55 needed the most care, and the naive approach was wrong.** The obvious way
to classify a local time is to compare UTC offsets across `fold=0` and
`fold=1`. Running it first showed that test returns true for *both* the gap and
the overlap — it identifies both cases and distinguishes neither. The correct
test is a UTC round-trip: a nonexistent time does not come back as itself
(02:30 returns as 03:30), an ambiguous one does but its two folds resolve to
different instants. Both the method and the reason it is not the obvious one
are written into the declaration and the test docstring.

The transition instants were located by search rather than transcribed:
Europe/Warsaw springs forward at `2026-03-29T01:00:00Z` (local 01:59:59 →
03:00:00) and falls back at `2026-10-25T01:00:00Z` (local 02:59:59 → 02:00:00).
This matches the research's recorded values.

**Task 3 — nulls, booleans, closure, assertions and the ADR (3)** (`71e6c4b`)

`24_null_values`, `60_boolean_localized`, `70_empty_last_field_vs_null`.

24's row 8 is `NULL Industries` — a company name, so a reader testing
`"NULL" in value` rather than equality destroys it. The declaration also
records that `NA` is Namibia's ISO 3166 code, so the token list is a contract
decision and a country dataset must not declare it absent.

70 is the subtlest in the corpus. Row 1 ends with the delimiter so `comment` is
present-and-empty; row 2 does not, so it is absent. The **field count is the
only evidence** of which, and the generated bytes confirm it: `{0:3, 1:3, 2:2,
3:3}`. Row 2 is a rejected record carrying the same reason as
`15_missing_columns`, not a padded row.

`tests/unit/test_corpus_semantic_fixtures.py` makes eight assertions. Seven
follow 01-07's rule of comparing against each fixture's own `expect:` block.
The DST test deliberately breaks that rule and re-derives its classifications
from `zoneinfo`, because the manifest is precisely what it exists to check —
comparing the manifest to itself would be circular.

## Verification performed

| Claim | Command / experiment | Result |
|---|---|---|
| Precondition (01-07 landed) | `make fixtures-verify` before any edit | `53 fixtures match` |
| Regeneration is byte-identical | `make fixtures && make fixtures-verify` per task | `60`, `67`, `70 fixtures match` |
| Oracle grows by addition only | `git diff --numstat CORPUS.sha256` | `7 0`, `7 0`, `3 0` |
| No generator change | `git diff --stat tools/corpus/` after each task | empty |
| Declaration counts | `yaml.safe_load` + unique-name assertion | 59, 66, 69 |
| Corpus is not committed | `git ls-files tests/fixtures/csv` | 0 files |
| Escape resolution is what it looks like | printed resolved `content` for all 17 | exact, incl. `€`, `zł`, `N/A` |
| Scientific-notation loss is real | `Decimal` round-trip on both damaged values | 6 vs 15 sig digits, expansion != original |
| Two-digit-year pivot | `strptime` on 26/68/69/99 | 2026, 2068, 1969, 1999 |
| Excel serials | date arithmetic, both epochs | 2025-08-11 vs 2029-08-12, 1462 days apart |
| Zero date is not a date | `strptime('0000-00-00','%Y-%m-%d')` | raises, as declared |
| DST transitions are real | binary search over `zoneinfo`, then second-precision confirmation | 01:00:00Z both, gap and overlap as declared |
| DST classification method | naive cross-fold offset comparison vs UTC round-trip | naive returns ambiguous for BOTH; round-trip distinguishes |
| 22/23 carry the same dates | `strptime` under both formats | identical ISO triples |
| 52 is genuinely undecidable | parsed from the GENERATED file | components (3,4) (1,2) (12,11) (5,6) — all ≤ 12 |
| Every temporal declaration names a format | scan for `%`-bearing keys | `MISSING FORMATS: NONE` |
| No data-valued float | recursive scan of every `expect:` block | none (2 confidence keys exempted, both in use) |
| 70's distinction is in the bytes | field counts + line-ending check | `{0:3,1:3,2:2,3:3}`; row 1 ends `,`, row 2 does not |
| Semantic tests are not vacuous | changed 55's declared zone to UTC | test failed `'unambiguous' == 'nonexistent'`; **`fixtures-verify` still passed** |
| Local gate | `make check` | exit 0 — 60 tests |
| CI gate end to end | `make ci` | exit 0 — 51 commits scanned, no leaks, SEC-11 self-test passed |

The probe is the one worth highlighting: with 55's zone changed, the digest
oracle still reported "70 fixtures match" while the test failed. An `expect:`
block is not part of the generated bytes, so a declaration can drift from its
fixture with a perfectly stable digest. That is exactly the gap this module
fills, and it is now observed rather than argued.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 1 — Bug] a declared field count in fixture 20 was asserted, not measured**

- **Found during:** Task 1, verifying content resolution.
- **Issue:** `20_decimal_comma`'s expectation claimed
  `field_count_if_comma_is_chosen: 6`. Parsing the generated bytes with a comma
  delimiter gives 1 field for the header and 2 for each data row. The *claim*
  (a comma delimiter splits every amount in half) was right; the number was
  invented rather than measured — the exact failure the corpus exists to prevent.
- **Fix:** replaced with `header_field_count_if_comma_is_chosen: 1`,
  `body_field_count_if_comma_is_chosen: 2`, and the literal split of row 1
  (`["1;1234", "56;wartosc standardowa"]`) so the damage is visible.
- **Files:** `tests/fixtures/corpus.yaml`
- **Commit:** `29f0907`

**2. [Rule 2 — Missing critical] two temporal declarations named no format**

- **Found during:** Task 2, running the acceptance check.
- **Issue:** Task 2 requires every one of the seven declarations to name an
  explicit format. `52` declared `detected_date_format: null` (correct — it
  must decline) and `54` declared only an epoch, so a mechanical scan found no
  format for either.
- **Fix:** 52 now declares `candidate_formats: ["%d/%m/%Y", "%m/%d/%Y"]` with
  `the_ambiguity_is_between_two_named_formats_not_an_open_question` — declining
  is a decision about a named candidate set, and Phase 6's detector must try
  exactly those two. 54 declares `serial_is_not_a_formatted_date`,
  `epoch_is_the_analogue_of_a_format_for_this_encoding` and the format of the
  converted value. Both are genuine improvements, not criterion-gaming.
- **Files:** `tests/fixtures/corpus.yaml`
- **Commit:** `4cf7612`

**3. [Rule 1 — Bug] the float prohibition was written over-broad and failed on 01-06's declarations**

- **Found during:** Task 3, first run of the new test module.
- **Issue:** `test_no_numeric_expectation_is_a_binary_float` flagged five
  pre-existing values — `encoding_confidence_min: 1.0` and
  `encoding_confidence_max: 0.99` from plans 01-06. The manifest was not wrong;
  the assertion was. A detector's confidence is a probability, not a value that
  must round-trip exactly, and rewriting it as a string would imply a precision
  claim nobody is making.
- **Fix:** scoped the check to data-valued expectations via a two-key exemption
  list, with a second assertion that every exempted key is still declared
  somewhere so the list cannot rot. Renamed to
  `test_no_data_valued_expectation_is_a_binary_float`. The control stays sharp:
  a float anywhere else still fails.
- **Files:** `tests/unit/test_corpus_semantic_fixtures.py`
- **Commit:** `71e6c4b`

**4. [Rule 3 — Blocking] five lint failures in the new test module**

- **Found during:** Task 3, `make check`.
- **Issue:** `UP017` (`datetime.timezone.utc` → `datetime.UTC`), two `PLC0207`
  (unbounded `str.split`), two `E501`, plus one `ruff format` disagreement.
- **Fix:** `ruff check --fix` for three, manual line breaks for the two E501s,
  `ruff format` for the last.
- **Files:** `tests/unit/test_corpus_semantic_fixtures.py`
- **Commit:** `71e6c4b`

### Deliberate departures from the plan's wording

**5. [Departure] `51` does not claim unrecoverability**

The plan groups 50 and 51 as the "unrecoverable" pair and the must-haves say
"the four unrecoverable-damage cases are declared as rejections". 50's damage
genuinely is unrecoverable — digits are gone. 51's is not, strictly: for a
declared fixed-width column, left-padding is deterministic and would restore
the original.

Writing `damage_is_unrecoverable: true` on 51 would have put a false statement
into the corpus, and a specification that is wrong in a checkable way is worse
than one that is silent. The declaration delivers what the plan actually wanted
— a rejection outcome with a reason, both values recorded — via a more precise
argument: padding manufactures a digit that is not in the file, on an
assumption that is usually right, which is exactly what makes silent
application dangerous. The repair is declared and recorded, never a default.

### Additions beyond the plan

**6. [Addition] the naive DST classifier was tried first and rejected in writing**

Comparing UTC offsets across `fold=0`/`fold=1` — the implementation most people
reach for — returns true for both the gap and the overlap. Rather than just
using the correct method, both the declaration
(`comparing_utc_offsets_across_folds_does_not_distinguish_the_two_cases`) and
the test docstring record why the obvious approach is wrong, so a future reader
does not "simplify" the round-trip back into the bug.

**7. [Addition] `expect:` blocks name six more Phase-8 quarantine reasons**

`scientific-notation-identifier-unrecoverable`,
`fixed-width-identifier-below-declared-width`,
`spreadsheet-serial-date-does-not-exist`, `nonexistent-local-time`,
`naive-timestamp-without-a-declared-zone` and `unmapped-boolean-token`, plus
the contract parameters `csv.locale_profile`, `csv.negative_style`,
`csv.null_sentinels`, `csv.date_format`, `csv.two_digit_year_pivot`,
`csv.spreadsheet_epoch`, `csv.timezone`, `csv.ambiguous_time_policy`,
`csv.naive_timestamp_zone`, `csv.null_tokens`, `csv.boolean_tokens` and
`csv.empty_string_is_null`. Writable today only because 01-03 kept `expect:`
permissive.

**8. [Addition] the completeness list is transcribed, not derived**

`EXPECTED_FIXTURES` is written out from README §73 and FEATURES.md §3.4 rather
than read from the manifest. Deriving it would make the test assert only that
the manifest contains what the manifest contains — it would pass with a fixture
missing, which is the one thing it exists to catch. (Note for future readers:
the second list runs 68 then 70; there is no fixture 69.)

---

**Total deviations:** 4 auto-fixed (2 bugs, 1 missing-critical, 1 blocking),
1 deliberate departure, 3 additions.
**Impact:** none on the deliverables. Every acceptance criterion in all three
tasks is met.

## Authentication gates

None. This plan touches no authenticated service. The one network operation
(`tools/security/install_gitleaks.sh`, run as part of `make ci`) is
unauthenticated and succeeded.

## Known stubs

None. All sixty-nine fixtures are declared, generated, byte-reproducible, and
either asserted structurally/semantically or covered by a declared `expect:`
block that Phase 6 will loop over.

The corpus is complete against both sources and **not closed** — README §73's
own wording is "grow the corpus as edge cases are discovered", and the manifest
header records that growth remains one entry plus one digest line.

## Threat flags

None. No new network endpoint, auth path or schema at a trust boundary.

Every mitigation this plan's threat register assigned was applied and observed:

| Threat | Mechanism | Evidence |
|---|---|---|
| T-01-38 Repudiation — an expectation that coerces unrecoverable data | 50 and 51 declare rejection outcomes with reasons and record both the original and the damaged value; `must_not_be_coerced_to_a_number`, `must_not_be_expanded_and_treated_as_the_original`, `padding_would_manufacture_a_digit_the_file_does_not_contain` | `test_the_scientific_notation_fixture_really_lost_digits` and `test_the_leading_zero_fixture_differs_by_the_leading_zeros_alone` prove the bytes carry the damage |
| T-01-39 Spoofing — a date parser guessing an ambiguous format | every date declaration names its format; 52's components are all ≤ 12 so the data cannot decide, and its expectation is that detection declines between two named candidates | `test_every_component_of_the_ambiguous_date_fixture_is_twelve_or_below`, verified against the generated file |
| T-01-40 Tampering — numeric fidelity lost to binary floating point | every data-valued expectation is a quoted decimal literal | `test_no_data_valued_expectation_is_a_binary_float` over all 69 blocks |
| T-01-41 Repudiation — corpus declared complete while a case is missing | completeness assertion against a transcribed list, failing on gap, duplicate or invention | `test_the_corpus_is_complete_against_both_sources` |
| T-01-42 Information disclosure — synthetic identifiers resembling real data | the corpus is synthetic by construction; no credential-shaped value was added, so the allowlist was not widened | `make gitleaks` over full history and working tree: no leaks; SEC-11 self-test passed with allowlists still path-scoped |

## Requirements addressed

| ID | Status | Mechanism |
|---|---|---|
| QUAL-08 | **complete** | The corpus exists at all 69 declarations / 70 emitted files, is generated from a seed rather than committed (`git ls-files tests/fixtures/csv` is empty), is byte-reproducible (`make fixtures-verify` inside `make check`), and carries the `expect:` blocks that make it a specification. This plan is the last of the four declaring QUAL-08 (01-03, 01-06, 01-07, 01-08), so the shared-ID gate that held it open across 01-06 and 01-07 now releases. `REQUIREMENTS.md` is deliberately untouched in worktree mode — the orchestrator owns that write. |

## Next

The corpus is the specification the CSV engine will be built against. Four
things Phase 6 should know:

1. **The `expect:` blocks are a parametrised test loop, not prose.** Every
   declaration states its outcome in the vocabulary Phase 6 and Phase 8 are
   about to define. Disagreement between a fixture's declared meaning and the
   detector's behaviour should be a test failure, not a judgement call.
2. **Four declarations pin decisions that are expensive to reverse.** 50 and 51
   forbid coercing destroyed identifiers, 52 forbids guessing an ambiguous
   date, 60 forbids defaulting an unmapped boolean. If an implementation finds
   one inconvenient, that is the conversation the fixture exists to force.
3. **The digest cannot see an `expect:` change.** This was observed, not
   assumed. Any new fixture whose declaration carries a checkable claim needs
   an assertion in one of the three test modules, or the claim is unguarded.
4. **`tools/corpus/` has not changed since 01-06.** Forty-eight fixtures were
   added across 01-07 and 01-08 with an empty generator diff. If Phase 6 needs
   a generator change, that is a genuine finding worth stating rather than a
   quiet edit — it means re-baselining digests.

## Self-Check: PASSED

The one created file and all three modified files verified present on disk. All
three commits (`29f0907`, `4cf7612`, `71e6c4b`) verified in `git log`. No file
deletions in any commit (`git diff --diff-filter=D` empty for each). Working
tree clean; `make check` and `make ci` both green at `71e6c4b`. Oracle at 70
lines; `git ls-files tests/fixtures/csv` returns 0 files. `STATE.md` and
`ROADMAP.md` untouched, as required in worktree mode.
