---
phase: 01-repository-toolchain-ci-skeleton
plan: 06
subsystem: fixture-corpus
tags: [qual-08, corpus, encodings, byte-order-mark, splicing, multipart, determinism]
status: complete

requires:
  - 01-03 (the manifest model, the deterministic generator, the digest oracle, the R1/R2/R6 policy tests)
  - 01-05 (the seven policy modules any new make target or test must now satisfy)
provides:
  - six byte-level generator capabilities — strict encoding, mark placement, raw byte splicing, per-record terminator cycles, parameterised field width, multipart splitting
  - sixteen new fixture declarations; the corpus is now 21 of 69 declared, 22 emitted files
  - tests/unit/test_corpus_byte_level_fixtures.py — the semantic assertions the digest oracle structurally cannot make
  - nineteen new manifest rejection tests, one per coherently-wrong declaration the new fields admit
affects:
  - plans 01-07 and 01-08, which now add declarations only — no generator change, so no re-baseline
  - Phase 6 detector tests, which loop over the expect: blocks authored here
  - Phase 8 quarantine vocabulary, extended here with undecodable-bytes and field-exceeds-max-field-bytes

tech-stack:
  added:
    - stdlib only — codecs incremental encoders, unicodedata, hashlib
  patterns:
    - A byte-level capability is a declarative manifest field with a validator, never a special case in the writer
    - Each capability is admitted only on the generator kinds whose precondition is checkable at load time
    - One definition of "what files does this declaration emit", shared by the generator, the oracle and the determinism test
    - A fixture pair straddling a limit, because one fixture cannot distinguish "the limit works" from "the feature is broken"

key-files:
  created:
    - tests/unit/test_corpus_byte_level_fixtures.py
  modified:
    - tools/corpus/manifest.py
    - tools/corpus/generators.py
    - tests/fixtures/corpus.yaml
    - tests/fixtures/CORPUS.sha256
    - tests/unit/test_corpus_manifest.py
    - tests/policy/test_corpus_determinism.py

key-decisions:
  - Each new capability is legal only on the generator kinds whose precondition is decidable at load time — mark placement on tabular/literal_unicode (record count is declared), splicing on literal (content is declared), splitting over tabular/literal_unicode targets. This is what lets every rejection be a ManifestError naming the fixture rather than a GeneratorError discovered halfway through a run.
  - Combining a byte-order mark with splices is refused outright. Both insert bytes at declared positions, and "is this offset measured before or after the mark?" is exactly the ambiguity a fixture must not carry. No fixture needs both.
  - generate_corpus now keys its digests by emitted path rather than fixture name, because a part set is one declaration and several files. Every oracle line still names a file `sha256sum -c` can open.
  - The byte-level test module generates the corpus into a temporary directory rather than reading tests/fixtures/csv/, so it passes on a clean checkout and cannot be confused with fixtures-verify's job.
  - 39_utf8_invalid_sequences declares no error policy. It pins only that a strict decoder raises, so §9's reader must choose strict/replace/surrogateescape in its contract rather than inheriting one by accident.
  - 28_large_fields and 67_row_exceeding_field_size_limit are authored as a pair either side of the stdlib 131 072-byte limit, and the tests read that limit from csv.field_size_limit() rather than restating it.

requirements-completed: []

coverage:
  - deliverable: "Every byte-level-hard fixture is generated, never committed, and reproduces byte-identically"
    human_judgment: false
    verification:
      - kind: command
        ref: "make fixtures && make fixtures-verify"
        status: pass
      - kind: command
        ref: "sha256sum -c tests/fixtures/CORPUS.sha256 (repo root, 22 lines)"
        status: pass
      - kind: command
        ref: "second `make fixtures` then diff against a snapshot of the oracle — byte-identical"
        status: pass
      - kind: test
        ref: "tests/policy/test_corpus_not_committed.py#test_no_generated_fixture_is_tracked_by_git"
        status: pass
  - deliverable: "The generator can express every byte-level capability the remaining fixtures need"
    human_judgment: true
    rationale: >
      All six capabilities are exercised by the sixteen fixtures authored here,
      so the claim is observed for this plan's scope. Whether it holds for the
      remaining forty-eight is only provable when 01-07 and 01-08 author them
      without touching tools/corpus/. Those are structural and semantic
      fixtures — plain text over the existing tabular and literal kinds — so
      the claim is well-founded, but it is a prediction until then.
  - deliverable: "A byte-order mark is emitted as raw bytes chosen by the declared encoding, not by a text-mode encoder guess"
    human_judgment: false
    verification:
      - kind: test
        ref: "tests/unit/test_corpus_byte_level_fixtures.py#test_the_mid_file_mark_is_at_a_non_zero_offset"
        status: pass
      - kind: test
        ref: "tests/unit/test_corpus_byte_level_fixtures.py#test_the_two_byte_encoding_without_a_mark_carries_none"
        status: pass
      - kind: command
        ref: "xxd 41_bom_mid_file.csv — EF BB BF at byte 23, after the header and first data record"
        status: pass
  - deliverable: "An invalid multibyte sequence fixture contains bytes that cannot round-trip through a strict decoder"
    human_judgment: false
    verification:
      - kind: test
        ref: "tests/unit/test_corpus_byte_level_fixtures.py#test_a_strict_decoder_rejects_the_invalid_sequence_fixture"
        status: pass
      - kind: command
        ref: "xxd 39_utf8_invalid_sequences.csv — C3 28 inside row 1's name field, 80 at the head of row 3"
        status: pass
  - deliverable: "Adding these fixtures leaves every previously-committed digest line unchanged"
    human_judgment: false
    verification:
      - kind: command
        ref: "git diff --numstat tests/fixtures/CORPUS.sha256 — 11/0 after task 2, 6/0 after task 3"
        status: pass
      - kind: test
        ref: "tests/policy/test_generator_determinism_rules.py#test_adding_a_fixture_leaves_every_other_digest_unchanged"
        status: pass
  - deliverable: "Each new manifest field rejects its nonsensical combinations, loudly and by name"
    human_judgment: false
    verification:
      - kind: test
        ref: "tests/unit/test_corpus_manifest.py — 19 new rejection tests, each asserting on the message"
        status: pass
  - deliverable: "The generated bytes mean what the manifest says they mean"
    human_judgment: false
    verification:
      - kind: test
        ref: "tests/unit/test_corpus_byte_level_fixtures.py — 8 tests"
        status: pass
      - kind: command
        ref: "probe: bom_after_record deleted from 41 -> test failed naming offset 0; reverted, tree clean"
        status: pass

metrics:
  duration: ~28 min
  completed: 2026-08-11
  tasks: 3
  commits: 3

actuals:
  tokens: 30787
  tasks: 3
  commits: 3
---

# Phase 1 Plan 06: Byte-Level Fixture Corpus Summary

Six byte-level generator capabilities — strict encoding, mark placement, raw
byte splicing, terminator cycles, parameterised field width and multipart
splitting — and the sixteen fixtures that need them, added without changing a
single previously-committed digest.

## What was built

**Task 1 — the capabilities, each a declarative field with a validator** (`e6f2265`)

Six additions to `tools/corpus/manifest.py` and `tools/corpus/generators.py`:

| Capability | Manifest field | What it makes expressible |
|---|---|---|
| Strict encoding | (policy, not a field) | An unrepresentable character stops generation and names itself |
| Mark placement | `bom_after_record` | A mark mid-stream, which is what a concatenated export looks like |
| Raw byte splicing | `splices: [{bytes, offset\|after_record}]` | Bytes no encoder would produce, without committing a binary |
| Terminator cycle | `line_terminators` | One file that genuinely mixes CRLF and LF |
| Field width | `row_spec` kind `repeat` | A field declared either side of the parser's limit |
| Part sets | generator `multipart`, `parts` | Numbered parts, header in the first only |

The design rule that made the validators exact: **each capability is legal only
on the generator kinds whose precondition is checkable at load time.** Mark
placement needs a declared record count, so it is allowed on `tabular` and
`literal_unicode` and refused elsewhere. Splicing needs the encoded content, so
it is allowed on `literal`. Splitting needs a record structure, so its target
must be one of the two record-structured kinds. Every rejection is therefore a
`ManifestError` naming the fixture, not a `GeneratorError` surfacing partway
through a run with half a corpus on disk.

Nineteen new rejection tests, each asserting on the message: a mark past the
last record, a mark position with no mark, a splice inside a multi-byte
character, a splice past the content, a splice with neither or both addressing
modes, splices out of order, a mark and a splice together, a cycle of one, a
cycle alongside a single terminator, an unknown terminator, a `repeat` column
with a two-character `char` or a zero length, a split below two parts, a split
into more parts than the target has records, a part set disagreeing with its
target's encoding or terminator, a split count on a single-file kind, and a
character the declared encoding cannot represent.

**Task 2 — the eleven encoding and Unicode fixtures** (`45c9f5d`)

`05_utf8_bom`, `06_windows1250`, `07_utf16`, `26_unicode`,
`27_polish_characters`, `39_utf8_invalid_sequences`, `40_utf16_no_bom`,
`41_bom_mid_file`, `42_zero_width_and_bidi`, `43_nbsp_thousands_separator`,
`68_utf8_bom_semicolon_pl_excel`.

The three that needed care behaved as the plan predicted:

- **41** places the mark after the header and the first data record — observed
  at byte 23. A reader that sniffs only the opening bytes will not see it; a
  reader that treats any mark anywhere as an encoding declaration will re-decode
  the whole file on the strength of three bytes in its middle.
- **40** is 07's encoding with the mark removed, and its `expect:` asserts
  `encoding_is_certain: false`. A detector returning 1.0 there claims
  determinism CSV-03 forbids.
- **39** carries a truncated two-byte lead (`C3 28`) inside row 1's `name` field
  and a lone continuation byte (`80`) at the head of row 3, exercising both
  addressing modes. Its expectations require the reader to *declare* an error
  policy and pin only that the default raises.

`42` and `43` carry characters that are invisible in a diff, so they are written
as `\u` escapes and their code points are named in `expect.code_points_present`.
The first draft pasted the literal characters; that was corrected before commit
(see deviations).

**Task 3 — terminators, field size, delivery shape, and the semantic assertions** (`b67cd55`)

`28_large_fields`, `30_crlf_lf_mixed`, `31_cr_only`, `62_multipart_split`,
`67_row_exceeding_field_size_limit`. Twenty-one declared, twenty-two emitted.

`tests/unit/test_corpus_byte_level_fixtures.py` makes the assertions a digest
cannot. This is the load-bearing test in the plan: a fixture whose declaration
and content silently disagree has a perfectly stable digest, and every Phase 6
test built on it inherits the mistake with nothing in the repository noticing.
Eight tests — mark offset, absence of a mark in the ambiguous twin, strict
decode refusal, terminator mix, missing line feed, both sides of the field
limit, header placement across the split, and a guard that every declaration
still has a generated file.

## Verification performed

| Claim | Command / experiment | Result |
|---|---|---|
| Precondition (01-03 landed) | `make fixtures-verify` before any edit | `5 fixtures match` |
| Capabilities disturb no seeded byte | `make fixtures && make fixtures-verify`, then `git diff --exit-code CORPUS.sha256` | no diff |
| Oracle grows by addition only | `git diff --numstat CORPUS.sha256` | `11 0` after task 2, `6 0` after task 3 |
| Regeneration is byte-identical | `make fixtures && make fixtures-verify` | `22 fixtures match` |
| Oracle is stable across two runs | snapshot, second `make fixtures`, `diff -q` | identical |
| Independently checkable | `sha256sum -c tests/fixtures/CORPUS.sha256` (repo root) | 22× `OK`, including both part files |
| Mark is mid-file | `xxd 41_bom_mid_file.csv` | `EF BB BF` at offset 23, two newlines before it |
| Ambiguous file has no mark | `xxd 40_utf16_no_bom.csv` | first bytes `69 00 64 00` |
| Invalid bytes are real | strict `bytes.decode("utf-8")` on 39 | `UnicodeDecodeError: invalid continuation byte` at 12 |
| cp1250 is genuinely cp1250 | decode 06 as cp1250, then as utf-8 | first succeeds, second raises |
| Bare CR file has no LF | `b"\n" not in payload` | holds; 6 `\r`, split on `\n` yields 1 |
| Oversized field trips the parser | `csv.reader` at the default limit | `_csv.Error: field larger than field limit` |
| Header is in the first part only | read both parts; concatenate | header in part-00000, part-00001 starts `000011,`, parts reconstruct the source exactly |
| Byte-level tests are not vacuous | deleted `bom_after_record: 2` from the real manifest | test failed naming offset 0; reverted, `git diff` empty |
| Corpus is not committed | `git ls-files tests/fixtures/csv` | empty (0 files) |
| Local gate | `make check` | exit 0 |
| CI gate end to end | `make ci` | exit 0 — 42 commits scanned, no leaks, SEC-11 self-test passed |

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 3 — Blocking] one declaration is no longer one file, and a policy test assumed it was**

- **Found during:** Task 1, designing the multipart capability.
- **Issue:** `tests/policy/test_corpus_determinism.py` ended with
  `assert list(digests_a) == [fixture.name for fixture in manifest.fixtures]`.
  A part set is one declaration and several files, so `generate_corpus` had to
  start keying digests by emitted path — which makes that assertion false.
  Keeping the 1:1 mapping instead would have meant either emitting one digest
  for a concatenation of the parts (so `sha256sum -c` could no longer open the
  names in the oracle) or declaring each part as its own fixture (so "multipart
  splitting" would not be a generator capability at all, and the plan's
  twenty-one-fixture count would not hold).
- **Fix:** added `output_names(fixture)` to `generators.py` as the single
  definition of what a declaration emits, and the test now expands the declared
  order through that same function. The assertion is **not** weakened: it still
  pins every emitted name in exact order, and now pins the part naming too.
- **Files:** `tools/corpus/generators.py`, `tests/policy/test_corpus_determinism.py`
- **Commit:** `e6f2265`

**2. [Rule 2 — Missing critical] the acceptance criteria require rejection tests the plan's file scope has nowhere to put**

- **Found during:** Task 1.
- **Issue:** Task 1's acceptance criteria require "one unit test per rejection",
  and its own `<verify>` block runs `pytest tests/unit/test_corpus_manifest.py`
  — but `files_modified` lists neither that file nor any other home for them.
  The alternative, putting manifest-rejection tests into
  `test_corpus_byte_level_fixtures.py`, would file load-time schema tests under
  a module whose stated purpose is reading generated bytes.
- **Fix:** the nineteen rejection tests were added to
  `tests/unit/test_corpus_manifest.py`, which is unambiguously where manifest
  rejections belong and is the file the task's own verify command exercises.
  One file beyond the declared scope, in the same subsystem, no conflict risk
  (this plan ran alone in its wave).
- **Files:** `tests/unit/test_corpus_manifest.py`
- **Commit:** `e6f2265`

**3. [Rule 1 — Bug] the invisible-character fixtures were first written with the characters pasted in**

- **Found during:** Task 2, immediately after writing 42 and 43.
- **Issue:** U+200B, U+200E and U+00A0 were pasted as literal characters while
  the accompanying comment claimed they were written as escapes. That is the
  precise failure both fixtures exist to warn about: a reviewer cannot see them,
  and an editor or git filter could substitute or strip one without any visible
  change to the manifest.
- **Fix:** all four occurrences replaced with their YAML escape spellings (`\u200B`, `\u200E`, `\u00A0`),
  verified to decode to the intended code points through
  `yaml.safe_load`, and asserted present in the generated bytes.
- **Files:** `tests/fixtures/corpus.yaml`
- **Commit:** `45c9f5d`

### Additions beyond the plan

**4. [Addition] the multipart target may not be a large-profile fixture**

Splitting reads the target whole. A part set over `29_large_file.csv` would
therefore materialise 293 MB in memory — breaking the one property that fixture
exists to prove, and failing outright under `FAST=1` where the target is
skipped. Refused at load time with a message that says why.

**5. [Addition] a sensitivity probe on the new test module**

Following 01-05's established rule that a control never observed failing is
indistinguishable from a disabled one: `bom_after_record: 2` was deleted from
the real manifest, the mark moved to offset zero, and
`test_the_mid_file_mark_is_at_a_non_zero_offset` failed naming the offset. The
manifest was reverted and the working tree confirmed clean.

**6. [Addition] `expect:` blocks state two Phase-8 quarantine reasons**

`undecodable-bytes` (39) and `field-exceeds-max-field-bytes` (67), plus the
contract parameter name `csv.max_field_bytes`. These are writable today only
because 01-03 kept `expect:` permissive; they are recorded here so Phase 8
inherits vocabulary rather than inventing it.

---

**Total deviations:** 3 auto-fixed (1 bug, 1 missing-critical, 1 blocking) and
3 additions. **Impact:** none on the deliverables. Every acceptance criterion
in all three tasks is met.

## Authentication gates

None. This plan touches no authenticated service. The one network operation
(`tools/security/install_gitleaks.sh`, run as part of `make ci`) is
unauthenticated and succeeded.

## Known stubs

None. Every code path introduced here is reachable and exercised by a test.

Forty-eight of the sixty-nine fixtures remain unauthored. That is the plan's
stated scope, not a stub: they are structural, semantic and header-hygiene
cases, all plain text over the existing `tabular` and `literal` kinds, so
01-07 and 01-08 add declarations without touching `tools/corpus/`.

## Threat flags

None. No new network endpoint, auth path or schema at a trust boundary.

Every mitigation this plan's threat register assigned was applied and observed:

| Threat | Mechanism | Evidence |
|---|---|---|
| T-01-29 Tampering — a capability re-baselines the whole corpus | per-fixture seed derivation; oracle diff required to be additions-only | `git diff --exit-code CORPUS.sha256` after task 1; `11 0` and `6 0` numstat after tasks 2 and 3 |
| T-01-30 Repudiation — bytes stop matching the declaration | `test_corpus_byte_level_fixtures.py` asserts mark offset, decodability, terminator mix, field limit, header placement | 8 tests, observed failing on a deliberate probe then reverted |
| T-01-31 Information disclosure — synthetic credential-shaped values | none of the sixteen fixtures contains one; the allowlist was not widened | `make gitleaks` over full history and working tree: no leaks; `make gitleaks-selftest` passed with allowlists still path-scoped |
| T-01-32 Tampering — nondeterministic multipart output | a part is a byte range with no archive header, so there is no timestamp or embedded filename to zero | two consecutive `make fixtures` runs produced a byte-identical oracle |
| T-01-33 DoS — oversized field aborting the parse (accepted) | declared outcome is a rejected record with a reason; the limit is a contract parameter | 67's `expect:` states `parser_aborts_whole_file: false` and names `csv.max_field_bytes`; the test observes the stdlib default raising |

## Requirements addressed

| ID | Status | Mechanism |
|---|---|---|
| QUAL-08 | **held open** | `make fixtures-verify` inside `make check` now regenerates 22 files across 21 declarations and compares to the oracle; the byte-identity claim was observed holding across a generator change, which is the property that makes the corpus reviewable. Not marked complete: 01-07 and 01-08 also declare QUAL-08 and have not run, so the shared-ID gate holds it open. `REQUIREMENTS.md` is deliberately untouched. |

## Next

Ready for 01-07 and 01-08 to author the remaining forty-eight fixtures as
declarations only. Four things they should know:

1. **Append, do not reorder.** Inserting a declaration between two others adds
   one line to `CORPUS.sha256`; moving an existing one rewrites the oracle and
   makes review impossible. `git diff --numstat` must show zero deletions.
2. **Every `tabular` column needs an explicit `row_spec` entry** — there is no
   default, deliberately. The kinds are now `zero_padded_int`, `pick`,
   `decimal` and `repeat`.
3. **`expect:` is permissive and is the important half.** Phase 6 loops over it.
4. **If you find yourself needing to change `tools/corpus/`, stop and say so** —
   that is the outcome this plan exists to prevent, and it is worth a deviation
   note rather than a quiet edit.

## Self-Check: PASSED

The one created file and all six modified files verified present on disk. All
three commits (`e6f2265`, `45c9f5d`, `b67cd55`) verified in `git log`. No file
deletions in any commit. Working tree clean; `make check` and `make ci` both
green at `b67cd55`. `STATE.md` and `ROADMAP.md` untouched, as required in
worktree mode.
