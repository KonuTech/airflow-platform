"""OBS-03: the console-write ban is repository-wide, with exactly three carve-outs.

`test_gates_actually_fail.py` proves the ban fires. This proves it still covers
what it is supposed to cover. The two are different failures: a rule can be
perfectly live and simultaneously be relaxed for half the tree.

The agreed carve-outs, and nothing else:

* `scripts/**` — operator scripts whose output IS the interface.
* `tools/corpus/__main__.py` — the corpus command-line entry point.
* `tests/e2e/conftest.py` — the CI failure-traceback streaming hook
  (`pytest_runtest_logreport`), whose printed output IS the interface: it
  exists solely to put each failure's traceback into the streamed CI job log
  the moment it is known, so a run cancelled at the job's
  ``timeout-minutes`` ceiling still carries the WHY, not just the WHICH
  (debug/ci-pipeline-ingestion-timeout ROUND 15, rider i -- ROUND 14's nine
  cancelled-run failures lost their tracebacks to exactly this gap).

01-RESEARCH.md Pitfall 2 is the reason this test exists in this shape. The
natural response to lint noise is to narrow the rule, and narrowing `T20` is the
one narrowing that silently voids a requirement rather than merely relaxing a
style. Adding a third path, or sinking the family into the blanket `ignore`
list, must both fail — and both are proved to fail below against scratch copies
of the configuration, so this module's own sensitivity is committed evidence
rather than something a reviewer has to take on trust.

A second, smaller hole is checked separately: an inline `noqa` naming a T20 code
would relax the ban at a line the configuration never mentions. Bare `# noqa`
with no codes is already rejected by ruff's own `PGH004`, which `select = ALL`
turns on, so this module asserts nothing disables PGH004 rather than duplicating
that check.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = REPO_ROOT / "pyproject.toml"

PRINT_CODES = ("T201", "T203")  # print, pprint
BLANKET_NOQA_GUARD = "PGH004"
ALLOWED_CARVE_OUTS = frozenset(
    {"scripts/**", "tools/corpus/__main__.py", "tests/e2e/conftest.py"},
)

SCANNED_SUFFIX = ".py"
EXCLUDED_DIRS = frozenset(
    {
        ".git",
        ".venv",
        ".planning",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        "tests/fixtures/csv",
    },
)

# Assembled from fragments so this module does not trip its own scan, and so
# ruff does not read the literal below as a directive applying to this line.
_NOQA = "no" + "qa"
NOQA_WITH_CODES = re.compile(rf"{_NOQA}\s*:\s*([A-Z]+[0-9]*(?:\s*,\s*[A-Z]+[0-9]*)*)")


def _disables(entry: str) -> bool:
    """True if a ruff ignore entry switches off the print family.

    Prefix semantics, because ruff accepts any prefix of a code: `T`, `T2` and
    `T20` all disable `T201`, and only comparing full codes would miss them.
    """
    return bool(entry) and any(code.startswith(entry) for code in PRINT_CODES)


def _blanket_ignores(lint: dict[str, Any]) -> list[str]:
    return [*lint.get("ignore", []), *lint.get("extend-ignore", [])]


def _per_file_ignores(lint: dict[str, Any]) -> dict[str, list[str]]:
    merged: dict[str, list[str]] = {}
    for key in ("per-file-ignores", "extend-per-file-ignores"):
        for glob, codes in (lint.get(key) or {}).items():
            merged.setdefault(glob, []).extend(codes)
    return merged


def print_ban_problems(config: dict[str, Any]) -> list[str]:
    """Report every way the parsed configuration weakens the console-write ban."""
    problems: list[str] = []
    lint = config.get("tool", {}).get("ruff", {}).get("lint", {})

    selected = [*lint.get("select", []), *lint.get("extend-select", [])]
    if not any(entry == "ALL" or _disables(entry) for entry in selected):
        problems.append(f"the print family is not selected at all: select={selected}")

    banned_globally = sorted(entry for entry in _blanket_ignores(lint) if _disables(entry))
    if banned_globally:
        problems.append(
            f"the print family is in the blanket ignore list ({banned_globally}); "
            "OBS-03 may be relaxed by path only",
        )

    if any(entry and BLANKET_NOQA_GUARD.startswith(entry) for entry in _blanket_ignores(lint)):
        problems.append(
            f"{BLANKET_NOQA_GUARD} is ignored, so a bare suppression comment could "
            "silently reopen the carve-out anywhere",
        )

    relaxed = {
        glob for glob, codes in _per_file_ignores(lint).items() if any(map(_disables, codes))
    }
    extra = sorted(relaxed - ALLOWED_CARVE_OUTS)
    if extra:
        problems.append(
            f"the console-write ban is relaxed for unapproved paths: {extra}; "
            f"only {sorted(ALLOWED_CARVE_OUTS)} are agreed",
        )
    return problems


def _config() -> dict[str, Any]:
    return tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))


def test_the_console_write_ban_is_repository_wide() -> None:
    problems = print_ban_problems(_config())
    assert not problems, "OBS-03 has been weakened:\n" + "\n".join(problems)


def test_widening_the_carve_out_is_reported() -> None:
    """Adding a third relaxed path must fail. Scratch copy; nothing on disk changes."""
    config = _config()
    config["tool"]["ruff"]["lint"]["per-file-ignores"]["packages/**"] = ["T20"]
    problems = print_ban_problems(config)
    assert any("unapproved paths" in p for p in problems), (
        f"a third print-ban carve-out was not reported: {problems}"
    )


def test_sinking_the_family_into_the_blanket_ignore_is_reported() -> None:
    """Moving T20 into the global ignore list must fail, for every prefix form."""
    for entry in ("T20", "T201", "T2", "T"):
        config = _config()
        config["tool"]["ruff"]["lint"]["ignore"].append(entry)
        problems = print_ban_problems(config)
        assert any("blanket ignore list" in p for p in problems), (
            f"a global ignore of {entry!r} was not reported: {problems}"
        )


def test_deselecting_the_family_is_reported() -> None:
    """A narrowed `select` would disable the rule without mentioning T20 at all."""
    config = _config()
    config["tool"]["ruff"]["lint"]["select"] = ["E", "F"]
    config["tool"]["ruff"]["lint"]["extend-select"] = ["D417"]
    problems = print_ban_problems(config)
    assert any("not selected" in p for p in problems), (
        f"deselecting the print family was not reported: {problems}"
    )


def test_ignoring_the_blanket_noqa_guard_is_reported() -> None:
    config = _config()
    config["tool"]["ruff"]["lint"]["ignore"].append(BLANKET_NOQA_GUARD)
    problems = print_ban_problems(config)
    assert any(BLANKET_NOQA_GUARD in p for p in problems), (
        f"ignoring {BLANKET_NOQA_GUARD} was not reported: {problems}"
    )


def _candidate_files() -> list[Path]:
    """Every Python file except this module, which necessarily names the codes."""
    out: list[Path] = []
    for path in REPO_ROOT.rglob(f"*{SCANNED_SUFFIX}"):
        if not path.is_file() or path == Path(__file__).resolve():
            continue
        rel = path.relative_to(REPO_ROOT).as_posix()
        if any(rel == d or rel.startswith(f"{d}/") for d in EXCLUDED_DIRS):
            continue
        out.append(path)
    return sorted(out)


def test_no_inline_suppression_relaxes_the_ban_off_the_agreed_paths() -> None:
    """A `noqa: T201` would relax OBS-03 at a line the configuration never names.

    Inside the two agreed carve-outs such a comment is redundant but harmless,
    so it is tolerated there and nowhere else.
    """
    violations: list[str] = []
    for path in _candidate_files():
        rel = path.relative_to(REPO_ROOT).as_posix()
        if rel.startswith("scripts/") or rel == "tools/corpus/__main__.py":
            continue
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            for match in NOQA_WITH_CODES.finditer(line):
                codes = [c.strip() for c in match.group(1).split(",")]
                if any(map(_disables, codes)):
                    violations.append(f"{rel}:{lineno}: {line.strip()[:100]}")

    assert not violations, (
        "OBS-03 is suppressed inline outside the agreed carve-outs. Widen the\n"
        "per-file-ignores in pyproject.toml instead, where this test can see it.\n\n"
        + "\n".join(violations)
    )


def test_the_scan_actually_reaches_files() -> None:
    """A walk that finds nothing passes for the wrong reason."""
    assert _candidate_files(), "the inline-suppression scan found no Python files"
