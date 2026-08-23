"""tests/policy/test_runbooks_structure.py -- OBS-06 structural proof for docs/runbooks/.

D-41 (11-CONTEXT.md): runbooks are one doc per README §89 scenario, never a single
consolidated file, each carrying exactly 5 `##` headings in a fixed order: Symptoms,
Diagnosis, Recovery, Reprocessing, Verification. Mirrors
`tests/policy/test_supply_chain_guards.py`'s reusable-checker-function style -- a
`missing_headings(text) -> list[str]` function both the real-file test and the
non-vacuity test call, even though this module walks markdown files rather than YAML.

Plan 11-13 wrote 15 of the 18 verified README §89 scenarios. Plan 11-14 completes the
set with the 3 remaining chaos-trailing scenarios (MinIO unavailable, PostgreSQL
unavailable, Secret unavailable) and adds this module's own `len(...) == 18` completeness
assertion and the exact-named-set assertion below -- both deliberately withheld by plan
11-13 (see 11-13-SUMMARY.md) until the full set actually existed.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNBOOKS_DIR = REPO_ROOT / "docs" / "runbooks"

REQUIRED_HEADINGS = ("Symptoms", "Diagnosis", "Recovery", "Reprocessing", "Verification")

# The full, exact 18-item verified README §89 list (11-RESEARCH.md's own "README §84 vs
# §89 -- Verified Lists" section), transcribed directly, not a glob over docs/runbooks/*.md
# -- an explicit, hard-coded list means a future accidental rename or deletion fails
# loudly by name, not just by count.
ALL_RUNBOOKS = (
    "airflow-unavailable.md",
    "minio-unavailable.md",
    "postgresql-unavailable.md",
    "vault-unavailable.md",
    "kubernetes-pod-stuck.md",
    "csv-malformed.md",
    "schema-changed.md",
    "duplicate-batch.md",
    "failed-backfill.md",
    "late-arriving-data.md",
    "cdc-failure.md",
    "scd-correction.md",
    "corrupted-file.md",
    "task-repeatedly-failing.md",
    "partial-database-load.md",
    "secret-unavailable.md",
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


def test_every_runbook_has_the_five_required_headings() -> None:
    problems: list[str] = []
    for filename in ALL_RUNBOOKS:
        path = RUNBOOKS_DIR / filename
        assert path.exists(), f"{filename} does not exist under docs/runbooks/"
        text = path.read_text(encoding="utf-8")
        found = missing_headings(text)
        if found:
            problems.append(f"{filename}: {found}")
    assert not problems, "runbook(s) with incorrect heading structure:\n" + "\n".join(problems)


def test_docs_runbooks_contains_exactly_18_files() -> None:
    """OBS-06's completeness assertion, deliberately withheld by plan 11-13 until now.

    `docs/runbooks/` must contain exactly the 18 verified README §89 scenario files --
    no more (an accidental stray file), no fewer (a scenario silently never written).
    """
    actual = sorted(p.name for p in RUNBOOKS_DIR.glob("*.md"))
    assert len(actual) == 18, (
        f"docs/runbooks/ contains {len(actual)} .md files, expected exactly 18: {actual}"
    )


def test_the_full_verified_scenario_set_is_covered_by_filename() -> None:
    """Every one of the 18 expected filenames is present, no more, no fewer -- by name.

    A renamed or missing scenario file is reported by its exact name here, not merely by
    a count mismatch in `test_docs_runbooks_contains_exactly_18_files` above.
    """
    actual = {p.name for p in RUNBOOKS_DIR.glob("*.md")}
    expected = set(ALL_RUNBOOKS)

    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    assert not missing, f"expected runbook(s) missing from docs/runbooks/: {missing}"
    assert not extra, f"unexpected file(s) in docs/runbooks/ not in the verified §89 set: {extra}"


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
