"""Unit tests for ``dataplat.scd.hashing.tracked_attribute_hash`` -- SCD-05.

Proves the deterministic normalized-hash change-detection recipe this
plan's ``recompute_version_chain`` (10-02 Task 2) dispatches Type-2
version boundaries from: same pipe-joined/None-to-empty-string convention
as ``dataplat.load.staging``'s own ``_record_hash`` precedent (staging.py
lines ~653-672), computed over values the caller has ALREADY NFC-normalized
(``dataplat.normalize.unicode.UnicodeNormalizer`` runs upstream in the real
pipeline) -- this function must never re-normalize, only hash.
"""

from __future__ import annotations

import hashlib
import unicodedata

from hypothesis import assume, given
from hypothesis import strategies as st

from dataplat.scd.hashing import tracked_attribute_hash


def test_returns_32_byte_sha256_digest_deterministically() -> None:
    """A real SHA-256 digest (32 bytes); identical inputs always hash identically."""
    first = tracked_attribute_hash("Anna Kowalski", "PL", hash_version=1)
    second = tracked_attribute_hash("Anna Kowalski", "PL", hash_version=1)

    assert len(first) == 32
    assert isinstance(first, bytes)
    assert first == second


def test_changing_either_value_changes_the_hash() -> None:
    """Changing EITHER name or country changes the hash; changing neither does not."""
    baseline = tracked_attribute_hash("Anna Kowalski", "PL", hash_version=1)
    changed_name = tracked_attribute_hash("Jan Kowalski", "PL", hash_version=1)
    changed_country = tracked_attribute_hash("Anna Kowalski", "DE", hash_version=1)
    unchanged = tracked_attribute_hash("Anna Kowalski", "PL", hash_version=1)

    assert changed_name != baseline
    assert changed_country != baseline
    assert unchanged == baseline


def test_none_value_does_not_raise() -> None:
    """A nullable Type-2 column may legitimately be ``None`` -- must not raise.

    Mirrors staging.py's own ``"" if field is None else str(field)`` convention.
    """
    result = tracked_attribute_hash(None, "PL", hash_version=1)

    assert isinstance(result, bytes)
    assert len(result) == 32


@given(
    a=st.tuples(st.one_of(st.none(), st.text()), st.one_of(st.none(), st.text())),
    b=st.tuples(st.one_of(st.none(), st.text()), st.one_of(st.none(), st.text())),
)
def test_hash_determinism_and_distinctness_property(
    a: tuple[str | None, str | None],
    b: tuple[str | None, str | None],
) -> None:
    """Equal inputs always hash equally; unequal (name, country) pairs (almost) never collide.

    ``None`` and ``""`` are DELIBERATELY the same normalized representation
    (matching staging.py's own ``"" if field is None else str(field)``
    convention) -- that specific pair is excluded from the distinctness
    check via ``assume()`` rather than treated as a collision bug.
    """
    normalized_a = tuple("" if v is None else v for v in a)
    normalized_b = tuple("" if v is None else v for v in b)
    assume(normalized_a != normalized_b)

    hash_a1 = tracked_attribute_hash(*a, hash_version=1)
    hash_a2 = tracked_attribute_hash(*a, hash_version=1)
    hash_b = tracked_attribute_hash(*b, hash_version=1)

    assert hash_a1 == hash_a2
    assert hash_a1 != hash_b


def test_already_normalized_nfc_and_nfd_strings_hash_identically() -> None:
    """Two already-NFC-normalized-by-caller strings hash the same -- no internal re-normalization.

    Both inputs handed to ``tracked_attribute_hash`` here are the SAME NFC
    form (proving the caller's own guarantee is sufficient) -- this function
    must not call ``unicodedata.normalize`` itself, only hash what it is
    given.
    """
    nfc_literal = "Wiśniewski"
    nfd_then_caller_normalizes_to_nfc = unicodedata.normalize(
        "NFC",
        unicodedata.normalize("NFD", "Wiśniewski"),
    )

    assert nfc_literal == nfd_then_caller_normalizes_to_nfc  # sanity: same NFC form

    hash_from_literal = tracked_attribute_hash(nfc_literal, "PL", hash_version=1)
    hash_from_caller_normalized = tracked_attribute_hash(
        nfd_then_caller_normalizes_to_nfc,
        "PL",
        hash_version=1,
    )

    assert hash_from_literal == hash_from_caller_normalized

    # And prove this function matches staging.py's own established recipe exactly.
    expected = hashlib.sha256("Wiśniewski|PL".encode()).digest()
    assert hash_from_literal == expected


def test_never_imports_or_calls_unicodedata_normalize() -> None:
    """Structural guard: hashing.py must not import/call ``unicodedata.normalize``.

    Checks actual import/call statements via the AST, not mere prose
    mentions of the word "unicodedata" in comments/docstrings explaining
    WHY the module doesn't do this (this docstring itself legitimately
    discusses the topic).
    """
    import ast

    import dataplat.scd.hashing as hashing_module

    source = hashing_module.__file__
    assert source is not None
    with open(source, encoding="utf-8") as fh:
        tree = ast.parse(fh.read(), filename=source)

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert not any(alias.name == "unicodedata" for alias in node.names)
        if isinstance(node, ast.ImportFrom):
            assert node.module != "unicodedata"
        if isinstance(node, ast.Attribute) and node.attr == "normalize":
            assert not (isinstance(node.value, ast.Name) and node.value.id == "unicodedata")
