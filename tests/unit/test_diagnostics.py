"""D-24's oracle-vs-catalog drift guard: the corpus and ``DIAGNOSTIC_CODES`` must never disagree.

``dataplat.diagnostics.DIAGNOSTIC_CODES``'s corpus-derived half is meant to be
*verbatim* from ``tests/fixtures/corpus.yaml``'s own ``quarantine_reason``/
``quarantine_reason_row_N``/``quarantine_reasons`` values (D-24) -- not a
separately/independently designed vocabulary. This test checks that claim in
the one direction testable before any Wave 2 raise site exists: every
corpus-derived code the catalog claims must actually appear somewhere in the
corpus's own declarations.

The 14 codes below are transcribed from ``06-CONTEXT.md`` D-24's own citation
list, the same way ``test_corpus_semantic_fixtures.py``'s
``README_SEVENTY_THREE`` transcribes its fixture-name list "from the two
sources rather than derived from the manifest" -- deriving this list from
``dataplat.diagnostics`` itself would make the test a tautology (it would
assert the module agrees with itself), and would stop catching the one
failure mode that matters: a corpus edit that silently renames or drops one
of these strings out from under the catalog.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from tools.corpus.manifest import load_manifest

from dataplat.diagnostics import DIAGNOSTIC_CODES

if TYPE_CHECKING:
    from tools.corpus.manifest import Manifest

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = REPO_ROOT / "tests" / "fixtures" / "corpus.yaml"

# Transcribed from 06-CONTEXT.md D-24 / diagnostics.py's own citation
# comments -- see this file's module docstring for why this is not derived
# from dataplat.diagnostics itself.
CORPUS_DERIVED_CODES = (
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
)


def _quarantine_reasons_declared_in(manifest: Manifest) -> set[str]:
    """Collect every value declared under a ``quarantine_reason*`` expect: key.

    Walks every fixture's ``expect`` mapping for keys starting with
    ``"quarantine_reason"`` -- this covers the singular ``quarantine_reason``
    and ``quarantine_reason_row_N`` string values, and the plural
    ``quarantine_reasons`` list value, all with the same prefix.
    """
    declared: set[str] = set()
    for fixture in manifest.fixtures:
        for key, value in fixture.expect.items():
            if not key.startswith("quarantine_reason"):
                continue
            if isinstance(value, str):
                declared.add(value)
            elif isinstance(value, list):
                declared.update(item for item in value if isinstance(item, str))
    return declared


def test_every_corpus_derived_code_is_still_in_the_catalog() -> None:
    """Sanity check: the catalog must not have silently dropped one of these."""
    missing_from_catalog = set(CORPUS_DERIVED_CODES) - DIAGNOSTIC_CODES

    assert not missing_from_catalog, (
        f"DIAGNOSTIC_CODES is missing corpus-derived code(s): {sorted(missing_from_catalog)}"
    )


def test_every_corpus_derived_code_is_still_declared_by_the_corpus() -> None:
    """D-24's actual drift guard: the corpus oracle must still declare every code."""
    manifest = load_manifest(MANIFEST_PATH)
    declared_in_corpus = _quarantine_reasons_declared_in(manifest)

    missing_from_corpus = set(CORPUS_DERIVED_CODES) - declared_in_corpus

    assert not missing_from_corpus, (
        f"corpus.yaml no longer declares corpus-derived code(s): {sorted(missing_from_corpus)}"
    )
