"""QUAL-08: the corpus regenerates byte-identically inside a single process.

``make fixtures-verify`` already compares a fresh generation against the
committed oracle, but a mismatch there has two possible causes: the generator
became nondeterministic, or the oracle is simply stale. This test separates
them. It generates the whole corpus **twice in one process** into two
directories and compares the digest maps to each other, never to the oracle. A
failure here can only mean the generator does not agree with itself.

Marked ``slow`` because it materialises the ~293 MB large-profile fixture twice.
It stays in the gate regardless: ``make policy`` runs the whole directory, and
a determinism check that only runs when someone remembers to ask for it is not a
check.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from tools.corpus.generators import generate_corpus
from tools.corpus.manifest import load_manifest

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "corpus.yaml"


@pytest.mark.slow
def test_two_generations_in_one_process_agree() -> None:
    manifest = load_manifest(MANIFEST)

    with (
        tempfile.TemporaryDirectory(prefix="corpus-a-") as first,
        tempfile.TemporaryDirectory(prefix="corpus-b-") as second,
    ):
        digests_a = generate_corpus(manifest, Path(first))
        digests_b = generate_corpus(manifest, Path(second))

    assert digests_a, "the manifest declared no fixtures"
    differing = [
        f"{name}: {digests_a[name]} then {digests_b.get(name)}"
        for name in digests_a
        if digests_a.get(name) != digests_b.get(name)
    ]
    assert not differing, (
        "the generator does not agree with itself within a single process:\n" + "\n".join(differing)
    )

    # Declared order is part of the contract: it is what makes adding a fixture
    # a one-line diff in CORPUS.sha256 (R1) instead of a reshuffle.
    assert list(digests_a) == list(digests_b)
    assert list(digests_a) == [fixture.name for fixture in manifest.fixtures]
