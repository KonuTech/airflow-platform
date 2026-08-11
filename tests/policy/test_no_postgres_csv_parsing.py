"""LOAD-12: the ETL processor is the only component that parses CSV.

PostgreSQL must never parse raw input, because rows loaded by ``COPY ... FORMAT
csv`` bypass every structural, type and quality check the platform exists to
perform — letting PostgreSQL parse the CSV voids the entire product.

This test is deliberately live from Phase 1, before any loader exists, so the
constraint is never briefly true-by-accident and then violated.

Honest limitation, recorded rather than papered over: a regex over source cannot
detect a ``COPY`` statement assembled at runtime from string fragments or read
from a config file. This test raises the cost of the mistake and documents the
rule where a developer will meet it; it does not make the mistake impossible.
The mechanism that does is architectural — the staging table is all-``TEXT`` —
and belongs to Phase 4. Both should exist.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

SCANNED_SUFFIXES = frozenset({".py", ".sql", ".yaml", ".yml", ".j2", ".sh"})
EXCLUDED_DIRS = frozenset(
    {
        ".git",
        ".venv",
        ".planning",
        "docs",
        "tests/policy",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        "tests/fixtures/csv",
    },
)

# Assembled from fragments so this file does not match its own pattern.
_COPY = "CO" + "PY"
_FMT = "FOR" + "MAT"
FORBIDDEN = re.compile(rf"(?is)\b{_COPY}\b(?:(?!;).){{0,400}}?\b(?:{_FMT}\s+)?\bCSV\b")
# Also catches psql's backslash form and psycopg's copy_expert(...) legacy path.
FORBIDDEN_EXTRA = re.compile(r"(?i)\\\s*copy\b(?:(?!;).){0,400}?\bcsv\b")


def _candidate_files() -> list[Path]:
    out: list[Path] = []
    for path in REPO_ROOT.rglob("*"):
        if not path.is_file() or path.suffix not in SCANNED_SUFFIXES:
            continue
        rel = path.relative_to(REPO_ROOT).as_posix()
        if any(rel == d or rel.startswith(f"{d}/") for d in EXCLUDED_DIRS):
            continue
        out.append(path)
    return sorted(out)  # sorted: stable failure ordering


def test_postgres_never_parses_csv() -> None:
    violations: list[str] = []
    for path in _candidate_files():
        text = path.read_text(encoding="utf-8", errors="replace")
        for lineno, line_text in enumerate(text.splitlines(), start=1):
            if FORBIDDEN.search(line_text) or FORBIDDEN_EXTRA.search(line_text):
                rel = path.relative_to(REPO_ROOT).as_posix()
                violations.append(f"{rel}:{lineno}: {line_text.strip()[:120]}")

    assert not violations, (
        "LOAD-12 violation: PostgreSQL must never parse raw CSV.\n"
        "Use the processor to parse, then COPY the already-validated rows into an\n"
        "all-TEXT staging table using the default TEXT format.\n\n" + "\n".join(violations)
    )


def test_the_scan_actually_reaches_files() -> None:
    """A scanner that walks nothing passes for the wrong reason."""
    assert _candidate_files(), "policy scan found no candidate files — the walk is broken"
