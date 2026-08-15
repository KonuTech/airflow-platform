"""``DIAGNOSTIC_CODES`` — the shared, stable diagnostic-code catalog (D-23/D-24/D-25).

Every detector/validation failure this phase adds carries a stable,
documented diagnostic code — not just a message string and a free-form
context dict (D-23). This is what makes the phase goal's literal wording
("fail with a **named** diagnostic") actually true.

The catalog is **built from the corpus's already-declared strings** (D-24):
``tests/fixtures/corpus.yaml``'s existing ``quarantine_reason``/
``quarantine_reason_row_N``/``quarantine_reasons`` values *are* the
corpus-derived half of this catalog, not a separately/independently designed
vocabulary — a raise site reuses the corpus's own words verbatim rather than
inventing a differently-cased or differently-worded equivalent. See each
code's citation below for exactly which fixture it comes from. The other
half is new-this-phase codes for situations the corpus does not literally
cover (e.g. filename masks, decompression, schema evolution) — pre-declared
here, before any Wave 2 plan has a raise site for them, so eleven
concurrently-running plans each import an existing constant instead of
racing to add one.

``tests/unit/test_diagnostics.py`` is D-24's drift guard: it asserts every
corpus-derived code here is a subset of what ``tests/fixtures/corpus.yaml``
actually declares, so the oracle (the corpus) and this catalog can never
quietly drift apart.

The vocabulary is **unified across row-level and file/run-level failures**
(D-25) — one shared catalog, not two parallel systems.
``dataplat.models.record.RejectedRecord.error_type`` (Phase 3, "a short,
stable, machine-readable reason code") is the row-level half; a new
``SourceError``/``SchemaError`` subclass's ``context["diagnostic_code"]``
(``dataplat.errors``) is the file/run-level half; both draw from the same
codes below.

**D-25 casing note:** every code added here — corpus-derived and new-this-
phase alike — is kebab-case, matching the corpus's own convention (e.g.
``"nul-byte-in-text-field"``). ``dataplat.pipeline.engine.RaggedRowGuard``'s
pre-existing ``error_type="RAGGED_ROW"`` (SCREAMING_SNAKE_CASE, predating
this catalog, Phase 3) is grandfathered as a documented exception — renaming
it would touch already-shipped, already-tested Phase 3 code for a
cosmetic-only change, and ``RAGGED_ROW``'s meaning already overlaps this
catalog's ``"field-count-below-header"``/``"field-count-above-header"``.
Nothing in this phase renames ``RAGGED_ROW`` or edits ``pipeline/engine.py``.
"""

from __future__ import annotations

from typing import Final

# Corpus-derived codes (D-24) — verbatim from tests/fixtures/corpus.yaml's own
# quarantine_reason/quarantine_reason_row_N/quarantine_reasons values. Each
# comment cites the fixture (`name:`) the code was first grep'd from
# (`grep -n "quarantine_reason" tests/fixtures/corpus.yaml`), so a human can
# audit the oracle-vs-catalog link without re-running the grep:
#
#   "nul-byte-in-text-field"                        -> 32_nul_bytes.csv (line 363)
#   "undecodable-bytes"                              -> 39_utf8_invalid_sequences.csv (line 396)
#   "field-exceeds-max-field-bytes"                  -> 67_row_exceeding_field_size_limit.csv
#                                                        (line 591)
#   "empty-file"                                     -> 18_empty.csv (line 1041)
#   "duplicate-header-names"                         -> 48_duplicate_header_names_case_variant.csv
#                                                        (line 1092), also 14_duplicate_columns.csv
#                                                        (line 1204)
#   "field-count-below-header"                       -> 15_missing_columns.csv (line 1226), also
#                                                        33_ragged_rows.csv (line 1334) and
#                                                        70_empty_last_field_vs_null.csv (line 2193)
#   "field-count-above-header"                       -> 16_extra_columns.csv (line 1249), also
#                                                        33_ragged_rows.csv (line 1334)
#   "unclosed-quote-at-eof"                          -> 34_unclosed_quote_eof.csv (line 1366)
#   "scientific-notation-identifier-unrecoverable"   -> 50_excel_scientific_notation_ids.csv
#                                                        (line 1614)
#   "fixed-width-identifier-below-declared-width"    -> 51_excel_leading_zero_stripped.csv
#                                                        (line 1659)
#   "spreadsheet-serial-date-does-not-exist"         -> 54_excel_serial_dates.csv (line 1972)
#   "nonexistent-local-time"                         -> 55_dst_gap_and_overlap.csv
#                                                        (quarantine_reason_row_1, line 2031)
#   "naive-timestamp-without-a-declared-zone"        -> 56_mixed_timezone_offsets.csv
#                                                        (quarantine_reason_row_3, line 2073)
#   "unmapped-boolean-token"                         -> 60_boolean_localized.csv (line 2142)
_CORPUS_DERIVED_CODES: Final[frozenset[str]] = frozenset(
    {
        "nul-byte-in-text-field",
        "undecodable-bytes",
        "field-exceeds-max-field-bytes",
        "empty-file",
        "duplicate-header-names",
        "field-count-below-header",
        "field-count-above-header",
        "unclosed-quote-at-eof",
        "scientific-notation-identifier-unrecoverable",
        "fixed-width-identifier-below-declared-width",
        "spreadsheet-serial-date-does-not-exist",
        "nonexistent-local-time",
        "naive-timestamp-without-a-declared-zone",
        "unmapped-boolean-token",
    },
)

# New this phase (D-25: same kebab-case convention) — pre-declared here so
# Wave 2's parallel plans each import an existing constant instead of racing
# to add one. No raise site exists yet for any of these; each gets its first
# real raise site in a specific Wave 2 plan of this same phase.
#
# "ambiguous-local-time-requires-a-declared-fold-policy" is the one
# exception to "pre-declared before any raise site exists": plan 06-09's
# own Task 2 added it (corpus fixture 55 row 2's
# outcome_row_2 == "requires-a-declared-fold-policy" -- the ten codes
# pre-seeded above do not cover the "ambiguous, no policy declared" case
# distinctly from "nonexistent" or "naive, no zone at all"), with its raise
# site landing in the same commit (dataplat.normalize.dates.DateNormalizer).
_NEW_THIS_PHASE_CODES: Final[frozenset[str]] = frozenset(
    {
        "filename-does-not-match-mask",
        "dialect-detection-declined",
        "multi-row-header-not-supported",
        "decompression-bomb-exceeded",
        "corrupted-archive",
        "multipart-group-incomplete",
        "multipart-group-too-large",
        "invalid-calendar-date",
        "invalid-numeric-value",
        "schema-column-disappeared",
        "schema-column-retyped",
        "schema-columns-reordered",
        "ambiguous-local-time-requires-a-declared-fold-policy",
    },
)

DIAGNOSTIC_CODES: Final[frozenset[str]] = _CORPUS_DERIVED_CODES | _NEW_THIS_PHASE_CODES
