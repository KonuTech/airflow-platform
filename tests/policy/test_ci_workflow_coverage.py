"""Coverage reporting and the dagtest CI job are actually wired, not just described (CICD-05).

Two proof-over-prose claims this plan (11-06) makes, each getting its own
non-vacuity check so a later edit that silently drops the wiring is caught
rather than trusted from a docstring:

1. The `check` job reports coverage (a `$GITHUB_STEP_SUMMARY` write plus an
   `actions/upload-artifact` step for `htmlcov/`) with NO `fail_under`
   threshold anywhere in `[tool.coverage.report]` -- D-23's explicit
   "report without gating" contract. Parsed from the real TOML table, not a
   substring `grep`, so a comment merely *mentioning* `fail_under` (as
   pyproject.toml's own "No fail_under: ..." rationale comment does) is
   never mistaken for the directive itself.
2. `ci.yml` defines a `dagtest` job that actually invokes `make test-dagtest`
   -- closing the pre-existing gap this plan's own objective names (tests/
   dagtest existed since Phase 8 but was never wired into continuous CI).

Mirrors `test_ci_calls_make_ci.py`/`test_ci_invokes_make_only.py`'s own
established shape: parse the real files, assert the real claim, then feed a
mutated copy through the same predicate to prove the check is sensitive to
regression, not merely descriptive of the current state.
"""

from __future__ import annotations

import copy
import tomllib
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
PYPROJECT = REPO_ROOT / "pyproject.toml"


def _load_workflow() -> dict[str, Any]:
    return yaml.safe_load(CI_WORKFLOW.read_text(encoding="utf-8"))


def _run_steps(job: dict[str, Any]) -> list[str]:
    return [step.get("run", "") for step in job.get("steps") or [] if step.get("run")]


def coverage_report_findings(workflow: dict[str, Any]) -> list[str]:
    """Return a list of missing-coverage-reporting complaints; empty means compliant."""
    jobs = workflow.get("jobs") or {}
    check_job = jobs.get("check")
    if check_job is None:
        return ["ci.yml has no `check` job at all"]

    runs = "\n".join(_run_steps(check_job))
    findings: list[str] = []
    if "GITHUB_STEP_SUMMARY" not in runs:
        findings.append("no `check` job step writes to $GITHUB_STEP_SUMMARY")
    if "coverage report" not in runs and "coverage html" not in runs:
        findings.append("no `check` job step runs `coverage report`/`coverage html`")

    uses_upload_artifact = any(
        "upload-artifact" in (step.get("uses") or "") for step in check_job.get("steps") or []
    )
    if not uses_upload_artifact:
        findings.append("no `check` job step uses actions/upload-artifact")
    else:
        artifact_paths = [
            (step.get("with") or {}).get("path", "")
            for step in check_job.get("steps") or []
            if "upload-artifact" in (step.get("uses") or "")
        ]
        if not any("htmlcov" in p for p in artifact_paths):
            findings.append("the upload-artifact step does not reference an htmlcov/ path")
    return findings


def dagtest_job_findings(workflow: dict[str, Any]) -> list[str]:
    """Return a list of missing-dagtest-job complaints; empty means compliant."""
    jobs = workflow.get("jobs") or {}
    dagtest_job = jobs.get("dagtest")
    if dagtest_job is None:
        return ["ci.yml has no `dagtest` job"]
    runs = "\n".join(_run_steps(dagtest_job))
    if "make test-dagtest" not in runs:
        return ["the `dagtest` job does not invoke `make test-dagtest`"]
    return []


def test_the_check_job_reports_coverage_via_step_summary_and_artifact() -> None:
    findings = coverage_report_findings(_load_workflow())
    assert not findings, "\n".join(findings)


def test_no_fail_under_threshold_is_declared() -> None:
    """D-23: report coverage, never gate a build on a numeric percentage.

    Parses the real `[tool.coverage.report]` table (not a `grep`), so the
    module's own explanatory comment ("No fail_under: ...") can say the word
    without tripping this assertion -- only an actual `fail_under = ...` key
    in the parsed table counts.
    """
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    coverage_report = data.get("tool", {}).get("coverage", {}).get("report", {})
    assert "fail_under" not in coverage_report, (
        "pyproject.toml's [tool.coverage.report] declares fail_under -- CICD-05/D-23 "
        "requires coverage reporting with no numeric gate"
    )


def test_the_dagtest_job_exists_and_calls_make_test_dagtest() -> None:
    findings = dagtest_job_findings(_load_workflow())
    assert not findings, "\n".join(findings)


def test_dropping_the_coverage_steps_is_reported() -> None:
    """Non-vacuity: a `check` job with no coverage steps must be flagged."""
    workflow = _load_workflow()
    mutated = copy.deepcopy(workflow)
    mutated["jobs"]["check"]["steps"] = [{"run": "make check"}]
    assert coverage_report_findings(mutated), (
        "a `check` job with no coverage-reporting steps was not reported as missing them"
    )


def test_dropping_the_dagtest_job_is_reported() -> None:
    """Non-vacuity: a workflow with no `dagtest` job must be flagged."""
    workflow = _load_workflow()
    mutated = copy.deepcopy(workflow)
    del mutated["jobs"]["dagtest"]
    assert dagtest_job_findings(mutated), (
        "a workflow missing the `dagtest` job was not reported as missing it"
    )


def test_a_dagtest_job_that_never_calls_make_test_dagtest_is_reported() -> None:
    """Non-vacuity: a `dagtest` job present in name only must still be flagged."""
    workflow = _load_workflow()
    mutated = copy.deepcopy(workflow)
    mutated["jobs"]["dagtest"]["steps"] = [{"run": "echo hello"}]
    assert dagtest_job_findings(mutated), (
        "a `dagtest` job that never invokes `make test-dagtest` was not reported"
    )
