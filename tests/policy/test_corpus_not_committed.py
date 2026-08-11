"""QUAL-08, second signal: no corpus file is ever committed.

The corpus is generated from a committed seed and a committed oracle. Committing
the bytes as well would bloat every build context, drown the secret scanner in
synthetic high-entropy values, and — worst — make it possible for the committed
bytes and the manifest to disagree, at which point neither is the specification.

This is the mechanical form of that decision. ``git ls-files`` is asked, not the
filesystem, because the question is "what is tracked", not "what exists": the
corpus is expected to exist locally after ``make fixtures``.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CORPUS_DIR = "tests/fixtures/csv"


def test_no_generated_fixture_is_tracked_by_git() -> None:
    result = subprocess.run(  # noqa: S603
        ["git", "ls-files", "--", CORPUS_DIR],  # noqa: S607
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    tracked = [line for line in result.stdout.splitlines() if line.strip()]
    assert not tracked, (
        f"{len(tracked)} generated fixture(s) are tracked by git:\n"
        + "\n".join(f"  {path}" for path in tracked)
        + f"\n\n{CORPUS_DIR}/ is generated, never committed. Remove them with "
        f"`git rm --cached` and check .gitignore."
    )


def test_the_manifest_and_the_oracle_are_tracked() -> None:
    # The negative assertion above is only meaningful if the two files that
    # replace the corpus are themselves committed. A .gitignore that swallowed
    # them would make the test above pass for entirely the wrong reason.
    result = subprocess.run(
        ["git", "ls-files", "--", "tests/fixtures/corpus.yaml", "tests/fixtures/CORPUS.sha256"],  # noqa: S607
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    tracked = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    assert tracked == {"tests/fixtures/corpus.yaml", "tests/fixtures/CORPUS.sha256"}, (
        f"the manifest and the oracle must both be tracked; git reports {sorted(tracked)}"
    )
