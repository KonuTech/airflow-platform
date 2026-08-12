"""WINDOWS #8, turned from a note into a gate (02-RESEARCH.md Open Question 5).

Phase 1 discovered that `make check` names test paths explicitly, so a new
test directory is silently uncollected until a target names it —
`tests/e2e/cluster/` (D-16) is exactly the kind of directory that defect
would swallow. This file decides one narrow, structural question: does any
target in `check`'s or `ci`'s full prerequisite closure name a path under
`tests/e2e/` in its own recipe? And separately: is `cluster-verify` the
*only* target in the whole Makefile that does?

**Honest limit.** This decides the PATH-NAMING question — whether
`tests/e2e/` is reachable from the offline gate by a recipe literally
mentioning it. It does not decide the general "the offline gate never
touches the network" claim: a target could reach the network through a tool
invocation that names no path at all (e.g. a bare `helm repo add`). That
broader claim is out of scope here; `tests/policy/test_ci_invokes_make_only.py`
covers the adjacent-but-different claim that CI only ever calls `make`.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
MAKEFILE = REPO_ROOT / "Makefile"

OFFLINE_GATES = ("check", "ci")

# A target line: `name: prereq1 prereq2  ## comment`. The negative lookahead
# on `=` excludes GNU Make's `NAME := value` / `NAME ?= value` / `NAME += value`
# immediate-assignment forms, which are lexically indistinguishable from a
# rule unless that one character is checked — `.PHONY`, `SHELL`, `UV_REQUIRED_
# VERSION` and every other variable in this Makefile use one of those forms.
_RULE_RE = re.compile(r"^([a-zA-Z][a-zA-Z0-9_.%-]*)\s*:(?!=)\s*(.*)$")


def _parse_makefile(text: str) -> dict[str, dict[str, Any]]:
    """Parse a GNU Makefile into {target: {"prereqs": [...], "recipe_lines": [...]}}.

    Deliberately narrow: this repository's Makefile has no line-continued
    prerequisite lists and no multi-target rules, so a general-purpose parser
    buys nothing a test suite should carry the weight of maintaining.
    """
    targets: dict[str, dict[str, Any]] = {}
    current: str | None = None
    for raw_line in text.splitlines():
        if raw_line.startswith("\t"):
            if current is None:
                continue
            # Comment lines inside a recipe are ignored: a comment EXPLAINING
            # why a target delegates to tests/e2e should not itself read as a
            # violation of the rule it is explaining (mirrors
            # test_ci_invokes_make_only.py's identical carve-out for YAML).
            stripped = raw_line[1:].strip().removeprefix("@").strip()
            if stripped and not stripped.startswith("#"):
                targets[current]["recipe_lines"].append(stripped)
            continue

        match = _RULE_RE.match(raw_line)
        if not match:
            current = None
            continue
        name, rest = match.group(1), match.group(2)
        prereqs = rest.split("##", 1)[0].split()
        targets[name] = {"prereqs": prereqs, "recipe_lines": []}
        current = name
    return targets


def _prerequisite_closure(targets: dict[str, dict[str, Any]], start: str) -> set[str]:
    seen: set[str] = set()
    stack = [start]
    while stack:
        name = stack.pop()
        if name in seen:
            continue
        seen.add(name)
        info = targets.get(name)
        if info is not None:
            stack.extend(info["prereqs"])
    return seen


def offline_gate_problems(targets: dict[str, dict[str, Any]]) -> list[str]:
    """Report every tests/e2e reference reachable from check's or ci's closure."""
    problems: list[str] = []
    for gate in OFFLINE_GATES:
        for name in sorted(_prerequisite_closure(targets, gate)):
            info = targets.get(name)
            if info is None:
                continue
            problems.extend(
                f"{gate} (via target '{name}'): {line!r}"
                for line in info["recipe_lines"]
                if "tests/e2e" in line
            )
    return problems


def targets_naming_tests_e2e(targets: dict[str, dict[str, Any]]) -> list[str]:
    return sorted(
        name
        for name, info in targets.items()
        if any("tests/e2e" in line for line in info["recipe_lines"])
    )


def _load_targets() -> dict[str, dict[str, Any]]:
    return _parse_makefile(MAKEFILE.read_text(encoding="utf-8"))


# 1. The paired false-positive control ---------------------------------------


def test_check_and_ci_never_reach_tests_e2e() -> None:
    problems = offline_gate_problems(_load_targets())
    assert not problems, (
        "`make check`/`make ci` must never be able to reach tests/e2e/ — it needs a\n"
        "live cluster, and check/ci are contractually offline and cluster-free.\n\n"
        + "\n".join(problems)
    )


def test_cluster_verify_is_the_only_target_naming_tests_e2e() -> None:
    names = targets_naming_tests_e2e(_load_targets())
    assert names == ["cluster-verify"], (
        f"expected exactly the 'cluster-verify' target to name tests/e2e; found {names}. "
        f"tests/e2e/cluster must be collected by exactly one target (D-16)."
    )


# 2. Non-vacuity by mutation on an in-memory copy of the Makefile text ------


def test_adding_tests_e2e_to_the_test_recipe_is_reported() -> None:
    text = MAKEFILE.read_text(encoding="utf-8")
    target_line = "$(RUN) pytest tests/unit tests/regression -q --cov --cov-report=term-missing"
    mutated_line = (
        "$(RUN) pytest tests/unit tests/regression tests/e2e/cluster "
        "-q --cov --cov-report=term-missing"
    )
    assert target_line in text, "fixture assumption broken: the `test` recipe line moved"
    mutated_text = text.replace(target_line, mutated_line, 1)
    assert mutated_text != text, "the scratch mutation did not apply — this test proves nothing"

    problems = offline_gate_problems(_parse_makefile(mutated_text))
    assert problems, "adding tests/e2e/cluster to the `test` recipe was not reported"
    assert any("test" in p for p in problems), (
        f"a violation was reported, but not attributed to the 'test' target: {problems}"
    )
