"""ORCH-02/06: the DAG folder stays thin -- no CSV/DB business logic, ever.

Mirrors ``tests/policy/test_no_postgres_csv_parsing.py``'s skeleton, scoped
to ``airflow/dags/*.py`` only. Two independent checks: (1) neither DAG file
nor ``_common/kpo.py`` imports ``csv``/``psycopg``/``boto3``/``pydantic``
directly -- every one of those concerns is delegated to the ``csv-processor``
image via ``KubernetesPodOperator``'s ``cmds``/``arguments``; (2) no raw
SQL-shaped string literal appears anywhere in the DAG folder.

Honest limitation, same one ``test_no_postgres_csv_parsing.py`` already
records: a regex over source cannot catch an import or a SQL string
assembled at runtime from fragments. This test raises the cost of the
mistake; it does not make it impossible.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DAGS_DIR = REPO_ROOT / "airflow" / "dags"

# `_common/kpo.py` legitimately imports `kubernetes.client.models` -- pure
# Kubernetes-API-object construction, not business logic (04-07-PLAN.md Task
# 1) -- so it is exempt from the IMPORT check below, BY NAME, for that
# reason. It is deliberately NOT exempt from the SQL-string check: it should
# never contain one either, and a silent blanket exemption would hide a
# real regression just as easily as it hides a false positive.
_EXEMPT_FROM_IMPORT_CHECK = frozenset({"airflow/dags/_common/kpo.py"})

FORBIDDEN_IMPORTS = re.compile(r"^\s*(?:import|from)\s+(csv|psycopg|boto3|pydantic)\b")
FORBIDDEN_SQL = re.compile(r"(?i)(SELECT |INSERT INTO|UPDATE )")


def _candidate_files() -> list[Path]:
    return sorted(p for p in DAGS_DIR.rglob("*.py") if p.is_file())


def test_no_business_logic_imports() -> None:
    violations: list[str] = []
    for path in _candidate_files():
        rel = path.relative_to(REPO_ROOT).as_posix()
        if rel in _EXEMPT_FROM_IMPORT_CHECK:
            continue
        text = path.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.splitlines(), start=1):
            match = FORBIDDEN_IMPORTS.match(line)
            if match:
                marker = match.group(1)
                violations.append(f"{rel}:{lineno}: forbidden import of {marker!r}: {line.strip()}")

    assert not violations, (
        "ORCH-02/06 violation: the DAG folder must delegate CSV/DB concerns "
        "to the csv-processor image via KubernetesPodOperator, never import "
        "them directly.\n\n" + "\n".join(violations)
    )


def test_no_raw_sql_strings() -> None:
    violations: list[str] = []
    for path in _candidate_files():
        rel = path.relative_to(REPO_ROOT).as_posix()
        text = path.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.splitlines(), start=1):
            if FORBIDDEN_SQL.search(line):
                violations.append(f"{rel}:{lineno}: {line.strip()[:120]}")

    assert not violations, (
        "ORCH-02/06 violation: the DAG folder must never contain a raw "
        "SQL-shaped string literal -- every database write happens inside "
        "the csv-processor image.\n\n" + "\n".join(violations)
    )


def test_the_scan_actually_reaches_files() -> None:
    """A scanner that walks nothing passes for the wrong reason."""
    assert _candidate_files(), "policy scan found no candidate files -- the walk is broken"
