"""A pull request must run the FULL gate, not a subset of it (CICD-02).

`test_ci_invokes_make_only.py` proves the workflow does not duplicate commands.
That is only half of CICD-02: a workflow can delegate to make perfectly and
still run `make lint` alone, which is a subset that happens to be green.

This module asserts the other half — that the make targets the workflow invokes
between them cover every target in the CI gate's chain. The chain is computed by
parsing the Makefile's prerequisite graph, not by matching a hard-coded list of
target names, so adding a target to `check` automatically tightens this test.
If a new target is added to the gate and no workflow job runs it, this fails.

It also asserts the structural claim behind "superset": the CI target's chain
strictly contains the local target's chain. `make ci` that no longer implies
`make check` would mean a developer's local gate is not the gate CI runs.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
MAKEFILE = REPO_ROOT / "Makefile"
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"

LOCAL_TARGET = "check"
CI_TARGET = "ci"

# A rule line: `name: prereq prereq  ## comment`. `:=` is excluded so variable
# assignments (RUN := ...) are not mistaken for rules, and a leading dot is
# excluded so .PHONY is not mistaken for a target.
RULE = re.compile(r"^(?P<target>[A-Za-z][\w.-]*)\s*:(?!=)(?P<prereqs>[^#\n]*)", re.MULTILINE)
MAKE_CALL = re.compile(r"\bmake\s+(?P<target>[A-Za-z][\w.-]*)")


def parse_prerequisites(makefile_text: str) -> dict[str, list[str]]:
    """Map each target to its declared prerequisites."""
    graph: dict[str, list[str]] = {}
    for match in RULE.finditer(makefile_text):
        target = match.group("target")
        prereqs = match.group("prereqs").split()
        # A target may legitimately appear once; later duplicates would be a
        # Makefile bug, so keep the first and let make's own warning cover it.
        graph.setdefault(target, prereqs)
    return graph


def chain(graph: dict[str, list[str]], target: str) -> set[str]:
    """All targets reachable from `target`, including itself."""
    seen: set[str] = set()
    stack = [target]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        stack.extend(graph.get(current, []))
    return seen


def workflow_make_targets() -> set[str]:
    """Every `make <target>` invoked by any run step in any workflow."""
    targets: set[str] = set()
    for path in sorted(WORKFLOW_DIR.glob("*.y*ml")):
        workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
        for job in (workflow.get("jobs") or {}).values():
            for step in job.get("steps") or []:
                run = step.get("run") or ""
                targets.update(m.group("target") for m in MAKE_CALL.finditer(run))
    return targets


def uncovered_targets(graph: dict[str, list[str]], invoked: set[str]) -> set[str]:
    """Targets in the CI chain that the invoked make targets do not cover.

    An aggregate target — one that exists only to group prerequisites, like
    `ci` itself — counts as covered when every one of its prerequisites is
    covered. That matters because the workflow deliberately splits the gate
    across two jobs: the secret scan needs its own `fetch-depth: 0` checkout,
    so it runs `make gitleaks` separately rather than everything under a single
    `make ci`. Demanding a literal `make ci` step would forbid that split and
    would be exactly the hard-coded string match this module exists to avoid.
    What must hold is that no target in the chain goes unrun.
    """
    reached: set[str] = set()
    for target in invoked:
        reached |= chain(graph, target)

    resolving: set[str] = set()
    memo: dict[str, bool] = {}

    def covered(target: str) -> bool:
        if target in reached:
            return True
        if target in memo:
            return memo[target]
        if target in resolving:  # cyclic prerequisites: treat as uncovered
            return False
        resolving.add(target)
        prereqs = graph.get(target, [])
        result = bool(prereqs) and all(covered(p) for p in prereqs)
        resolving.discard(target)
        memo[target] = result
        return result

    return {t for t in chain(graph, CI_TARGET) if not covered(t)}


def _graph() -> dict[str, list[str]]:
    graph = parse_prerequisites(MAKEFILE.read_text(encoding="utf-8"))
    # Vacuity guard: a parser that silently returned {} would make every
    # assertion below trivially true.
    assert LOCAL_TARGET in graph, f"Makefile has no `{LOCAL_TARGET}` target — the gate moved"
    assert CI_TARGET in graph, f"Makefile has no `{CI_TARGET}` target — the gate moved"
    return graph


def test_the_ci_target_is_a_superset_of_the_local_target() -> None:
    graph = _graph()
    local = chain(graph, LOCAL_TARGET)
    continuous = chain(graph, CI_TARGET)
    assert local < continuous, (
        f"`make {CI_TARGET}` must strictly contain `make {LOCAL_TARGET}`, or the gate a\n"
        f"developer runs is not the gate CI runs.\n"
        f"  {LOCAL_TARGET}: {sorted(local)}\n  {CI_TARGET}: {sorted(continuous)}"
    )


def test_the_workflow_runs_the_whole_ci_chain() -> None:
    graph = _graph()
    invoked = workflow_make_targets()
    assert invoked, "no workflow step invokes a make target — the gate is not wired to CI"

    missing = uncovered_targets(graph, invoked)
    assert not missing, (
        "a pull request would run only part of the gate. These targets are in the\n"
        f"`{CI_TARGET}` chain but no workflow job reaches them: {sorted(missing)}\n"
        f"Workflow invokes: {sorted(invoked)}"
    )


def test_every_workflow_make_target_exists() -> None:
    """A typo'd target would fail the build loudly, but silently drop coverage
    if the step were also marked continue-on-error. Catch it here instead.
    """
    graph = _graph()
    unknown = sorted(t for t in workflow_make_targets() if t not in graph)
    assert not unknown, f"workflow invokes make targets the Makefile does not define: {unknown}"


def test_dropping_a_gate_target_from_ci_is_reported() -> None:
    """The coverage check must be sensitive to a shrinking workflow.

    Modelled on a mutated graph rather than an edited file: if the workflow only
    ran the subset reachable from `install`, the missing gate must be named.
    """
    graph = _graph()
    missing = uncovered_targets(graph, {"install"})
    assert LOCAL_TARGET in missing, (
        "a workflow that ran only `make install` was not reported as skipping the gate"
    )


def test_a_ci_target_that_skips_check_is_reported() -> None:
    """`ci` must not become a sibling of `check` instead of a superset."""
    graph = dict(_graph())
    graph[CI_TARGET] = ["gitleaks", "gitleaks-selftest"]  # scratch: no longer depends on check
    assert not (chain(graph, LOCAL_TARGET) < chain(graph, CI_TARGET)), (
        "a `ci` target that no longer implies `check` was not reported as a broken superset"
    )
