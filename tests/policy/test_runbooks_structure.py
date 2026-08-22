"""tests/policy/test_runbooks_structure.py -- OBS-06 structural proof for docs/runbooks/.

D-41 (11-CONTEXT.md): runbooks are one doc per README §89 scenario, never a single
consolidated file, each carrying exactly 5 `##` headings in a fixed order: Symptoms,
Diagnosis, Recovery, Reprocessing, Verification. Mirrors
`tests/policy/test_supply_chain_guards.py`'s reusable-checker-function style -- a
`missing_headings(text) -> list[str]` function both the real-file test and the
non-vacuity test call, even though this module walks markdown files rather than YAML.

**This module intentionally does NOT assert `len(list(docs/runbooks/*.md)) == 18`.**
Plan 11-13 (this plan) writes 15 of the 18 verified README §89 scenarios; the
remaining 3 (MinIO unavailable, PostgreSQL unavailable, Secret unavailable) are
deliberately deferred to plan 11-14, which trails plan 11-09's chaos tests by design
(D-41: "runbooks trail the chaos tests, they are not written speculatively ahead of
them"). A premature total-count assertion here would be a false negative between this
plan and that one -- plan 11-14, which completes the set, is the right place to add it.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNBOOKS_DIR = REPO_ROOT / "docs" / "runbooks"

REQUIRED_HEADINGS = ("Symptoms", "Diagnosis", "Recovery", "Reprocessing", "Verification")

# The 15 runbook files this plan (11-13) creates. Intentionally NOT a glob over
# docs/runbooks/*.md -- plan 11-14 adds 3 more files to that same directory, and a
# glob-based count here would silently start asserting structure on files this plan
# never wrote, which is a different (and premature) claim than this module makes.
THIS_PLAN_RUNBOOKS = (
    "airflow-unavailable.md",
    "vault-unavailable.md",
    "kubernetes-pod-stuck.md",
    "failed-backfill.md",
    "task-repeatedly-failing.md",
    "csv-malformed.md",
    "schema-changed.md",
    "duplicate-batch.md",
    "late-arriving-data.md",
    "cdc-failure.md",
    "scd-correction.md",
    "corrupted-file.md",
    "partial-database-load.md",
    "secret-rotation.md",
    "unauthorized-access.md",
)

_HEADING_RE = re.compile(r"^##\s+(\S+)", re.MULTILINE)


def missing_headings(text: str) -> list[str]:
    """Report structural problems in `text`'s `##` headings against `REQUIRED_HEADINGS`.

    A heading counts only as an exact `## <word>` line match -- `_HEADING_RE` anchors
    to line start and requires whitespace immediately after the two literal `#`
    characters, so a deeper `### Symptoms` subheading is never mistaken for a real
    section marker, and a mention of "Symptoms" inside prose is never counted either.

    Returns an empty list when `text` has exactly the 5 `REQUIRED_HEADINGS`, each
    appearing once, in order. Otherwise returns one message per distinct problem
    class found (missing, unexpected extra, or present-but-wrong-order/duplicated) --
    every problem class is reported, not just the first one hit, so a single run of
    `missing_headings` gives a complete diagnostic rather than requiring repeated
    fix-rerun cycles to discover the next issue.
    """
    found = _HEADING_RE.findall(text)
    problems: list[str] = []

    missing = [heading for heading in REQUIRED_HEADINGS if heading not in found]
    if missing:
        problems.append(f"missing heading(s): {missing}")

    extra = [heading for heading in found if heading not in REQUIRED_HEADINGS]
    if extra:
        problems.append(f"unexpected heading(s): {extra}")

    if not missing and not extra and tuple(found) != REQUIRED_HEADINGS:
        problems.append(f"headings present but out of order or duplicated: {found}")

    return problems


def test_every_runbook_created_so_far_has_the_five_required_headings() -> None:
    problems: list[str] = []
    for filename in THIS_PLAN_RUNBOOKS:
        path = RUNBOOKS_DIR / filename
        assert path.exists(), f"{filename} does not exist under docs/runbooks/"
        text = path.read_text(encoding="utf-8")
        found = missing_headings(text)
        if found:
            problems.append(f"{filename}: {found}")
    assert not problems, "runbook(s) with incorrect heading structure:\n" + "\n".join(problems)


def test_a_runbook_missing_a_heading_is_reported() -> None:
    """Non-vacuity: a synthetic string missing one heading is caught by the same checker."""
    complete = "\n".join(f"## {heading}\ncontent\n" for heading in REQUIRED_HEADINGS)
    assert missing_headings(complete) == []

    for omit in REQUIRED_HEADINGS:
        mutated = "\n".join(
            f"## {heading}\ncontent\n" for heading in REQUIRED_HEADINGS if heading != omit
        )
        problems = missing_headings(mutated)
        assert problems, (
            f"removing '## {omit}' from an otherwise-complete document was not reported"
        )
