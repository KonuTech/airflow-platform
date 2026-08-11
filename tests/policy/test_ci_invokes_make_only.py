"""CI and local development must run the identical gate (CICD-02).

The Makefile is the single definition of the gate. If a workflow step invokes a
linter, type checker, test runner or import checker directly, there are two
definitions, and "green locally, red in CI" — or worse, the reverse — becomes
possible. A composite action would solve the CI-to-CI case; it does nothing for
the CI-to-local case, which is the one that actually bites here.

The scanner *installer* is deliberately not on the forbidden list. Downloading a
pinned binary is not a gate, and the CI job is expected to call it directly. The
scan itself must still go through `make gitleaks`, so the scanner *binary* path
is forbidden while the installer path is not.

Non-vacuity is committed rather than observed once: the predicate below is pure,
and `test_a_direct_tool_invocation_is_reported` feeds it a mutated copy of the
real workflow and asserts the violation is caught. Nothing on disk is edited.
"""

from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"

# The four gate tools, plus the scanner binary. `lint-imports` is the
# import-linter entry point; `tools/bin/gitleaks` is the scanner itself, as
# opposed to tools/security/install_gitleaks.sh which merely fetches it.
DIRECT_TOOLS = re.compile(r"\b(ruff|mypy|pytest|lint-imports)\b|tools/bin/gitleaks")


def _workflow_paths() -> list[Path]:
    return sorted(p for p in WORKFLOW_DIR.glob("*.y*ml"))


def _load(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _run_steps(workflow: dict[str, Any]) -> list[tuple[str, str]]:
    """Yield every (job_id, run_block) pair in a parsed workflow."""
    out: list[tuple[str, str]] = []
    for job_id, job in (workflow.get("jobs") or {}).items():
        for step in job.get("steps") or []:
            run = step.get("run")
            if run:
                out.append((job_id, run))
    return out


def direct_tool_invocations(workflow: dict[str, Any], label: str = "") -> list[str]:
    """Return one message per run line that invokes a gate tool directly.

    Comment lines inside a run block are ignored: a comment explaining *why* the
    step delegates to make should not read as a violation of the rule it is
    explaining.
    """
    offenders: list[str] = []
    for job_id, run in _run_steps(workflow):
        for line in run.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if DIRECT_TOOLS.search(stripped):
                offenders.append(f"{label}{job_id}: {stripped}")
    return offenders


def test_no_workflow_invokes_a_gate_directly() -> None:
    offenders: list[str] = []
    for path in _workflow_paths():
        offenders += direct_tool_invocations(_load(path), label=f"{path.name} ")

    assert not offenders, (
        "CI must invoke every gate through `make`, never directly, so the local\n"
        "and CI definitions cannot drift. Add the check to the Makefile instead.\n\n"
        + "\n".join(offenders)
    )


def test_the_scan_actually_reaches_run_steps() -> None:
    """A walk that inspects nothing passes for the wrong reason."""
    paths = _workflow_paths()
    assert paths, "no workflow files found — CICD-02 is unenforced"
    steps = [step for path in paths for step in _run_steps(_load(path))]
    assert steps, "no run steps found in any workflow — the walk is broken"
    # The gate is expected to be reached through make; if no step calls make at
    # all, this test is guarding an empty claim.
    assert any("make " in run for _, run in steps), "no workflow step invokes make"


def test_a_direct_tool_invocation_is_reported() -> None:
    """The predicate must catch a duplicated command, not just look at one."""
    workflow = _load(WORKFLOW_DIR / "ci.yml")
    for injected in ("uv run ruff check .", "pytest tests/unit -q", "./tools/bin/gitleaks dir ."):
        mutated = copy.deepcopy(workflow)
        job = next(iter(mutated["jobs"].values()))
        job["steps"].append({"run": injected})
        assert direct_tool_invocations(mutated), (
            f"a workflow step running `{injected}` was not reported as a direct invocation"
        )


def test_the_installer_step_is_not_treated_as_a_gate() -> None:
    """Fetching the pinned binary is not running a gate, and must stay allowed.

    Without this the obvious "ban anything mentioning gitleaks" rule would fire
    on the installer, and the natural fix would be to weaken the pattern until
    it also stopped catching a direct scan.
    """
    workflow = _load(WORKFLOW_DIR / "ci.yml")
    mutated = copy.deepcopy(workflow)
    job = next(iter(mutated["jobs"].values()))
    job["steps"] = [{"run": "tools/security/install_gitleaks.sh"}, {"run": "make gitleaks"}]
    mutated["jobs"] = {"only": job}
    assert not direct_tool_invocations(mutated), (
        "the installer or `make gitleaks` was misreported as a direct gate invocation"
    )
