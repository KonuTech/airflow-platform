"""Structural, non-vacuous proof that `publish.yml` really does what plan
11-01's `must_haves` claim — not merely that a step with a plausible name
exists somewhere in the file.

Mirrors `test_supply_chain_guards.py`'s idiom exactly: a module-level YAML
loader, small pure `*_problems()` functions that return a list of strings
(empty means "no problem found"), and thin `test_*` functions that assert
`not problems(...)` against the real file. The mutation tests
(`test_removing_the_trivy_severity_gate_is_reported`,
`test_removing_the_cosign_sign_step_is_reported`) deep-copy the parsed
workflow, break one property on the copy, and assert the SAME checker
function now reports it — proving the checker actually discriminates a
broken workflow from a correct one, rather than passing regardless of what
the file contains.

Everything here reads the *parsed* workflow rather than matching raw text
(the `${{ github.sha }}`/`${{ steps.build.outputs.digest }}` checks compare
against the interpolated `with:`/`run:` values, not line-by-line grep), so a
restructured step — different step order, `with:` reformatted — still gets
checked correctly.
"""

from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "publish.yml"

import yaml  # noqa: E402  (import after path constants for readability)

FULL_SHA_PIN = re.compile(r"[^@]+@[0-9a-f]{40}")
GITHUB_SHA_REF = "${{ github.sha }}"
DIGEST_REF = "steps.build.outputs.digest"
BUILD_ACTION = "docker/build-push-action"
TRIVY_ACTION = "aquasecurity/trivy-action"
JOB_ID = "publish-csv-processor"


def _publish_workflow() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))


def _steps(workflow: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Flatten every (job_id, step) pair across the whole workflow."""
    out: list[tuple[str, dict[str, Any]]] = []
    for job_id, job in (workflow.get("jobs") or {}).items():
        out.extend((job_id, step) for step in job.get("steps") or [])
    return out


def _find_step(workflow: dict[str, Any], uses_prefix: str) -> dict[str, Any] | None:
    for _job_id, step in _steps(workflow):
        if str(step.get("uses", "")).startswith(uses_prefix):
            return step
    return None


def sha_pin_problems(workflow: dict[str, Any]) -> list[str]:
    """Every `uses:` reference must be pinned by a full 40-char commit SHA.

    A tag (`@v4`, `@main`) is mutable and can be silently repointed at
    malicious code after the fact; a commit SHA cannot.
    """
    problems: list[str] = []
    for job_id, step in _steps(workflow):
        uses = step.get("uses")
        if uses is None:
            continue
        if not FULL_SHA_PIN.fullmatch(uses):
            problems.append(f"{job_id}: {uses!r} is not pinned by a full 40-char commit SHA")
    return problems


def permission_scope_problems(workflow: dict[str, Any]) -> list[str]:
    """The workflow-level floor stays `contents: read`; only the publish job
    widens it, and only to exactly the two scopes GHCR push + cosign OIDC
    signing need.
    """
    problems: list[str] = []
    top_level = workflow.get("permissions") or {}
    if top_level != {"contents": "read"}:
        problems.append(
            f"workflow-level permissions are {top_level!r}, not exactly "
            "{'contents': 'read'} — a wider floor defeats least privilege"
        )
    job = (workflow.get("jobs") or {}).get(JOB_ID) or {}
    job_permissions = job.get("permissions") or {}
    problems.extend(
        f"{JOB_ID}: job permissions {job_permissions!r} do not grant '{scope}: write'"
        for scope in ("packages", "id-token")
        if job_permissions.get(scope) != "write"
    )
    return problems


def build_tag_problems(workflow: dict[str, Any]) -> list[str]:
    """The published image must be tagged by the triggering commit's git SHA,
    never `:latest` — INFRA-08's replayability precedent, generalized here.
    """
    step = _find_step(workflow, BUILD_ACTION)
    if step is None:
        return [f"no {BUILD_ACTION} step found"]
    tags = str((step.get("with") or {}).get("tags", ""))
    problems: list[str] = []
    if GITHUB_SHA_REF not in tags:
        problems.append(f"build step tags {tags!r} do not contain {GITHUB_SHA_REF!r}")
    if "latest" in tags:
        problems.append(f"build step tags {tags!r} contain the substring 'latest'")
    return problems


def sbom_provenance_problems(workflow: dict[str, Any]) -> list[str]:
    """D-13: the build itself must produce an SBOM and a provenance
    attestation — not a separate, easy-to-forget manual step.
    """
    step = _find_step(workflow, BUILD_ACTION)
    if step is None:
        return [f"no {BUILD_ACTION} step found"]
    with_block = step.get("with") or {}
    problems: list[str] = []
    if not with_block.get("provenance"):
        problems.append(f"build step 'provenance' is not truthy: {with_block.get('provenance')!r}")
    if not with_block.get("sbom"):
        problems.append(f"build step 'sbom' is not truthy: {with_block.get('sbom')!r}")
    return problems


def cosign_sign_problems(workflow: dict[str, Any]) -> list[str]:
    """A `cosign sign` step must exist and must sign the image by DIGEST, not
    by tag — a tag can be repointed after signing, a digest cannot.
    """
    problems: list[str] = []
    found = False
    for job_id, step in _steps(workflow):
        run = step.get("run") or ""
        if "cosign sign" not in run:
            continue
        found = True
        if DIGEST_REF not in run:
            problems.append(
                f"{job_id}: `cosign sign` step does not reference {DIGEST_REF!r} — "
                "signing a bare tag is not bound to immutable content"
            )
    if not found:
        problems.append("no step runs `cosign sign`")
    return problems


def cosign_experimental_problems(workflow: dict[str, Any]) -> list[str]:
    """`COSIGN_EXPERIMENTAL` has been vestigial since cosign v2 (keyless is
    already the default) — its presence is a stale-tutorial regression, not
    a harmless leftover.
    """
    target = "COSIGN_EXPERIMENTAL"
    problems: list[str] = []
    for job_id, step in _steps(workflow):
        env = step.get("env") or {}
        if target in env:
            problems.append(f"{job_id}: sets {target} in env: — vestigial since cosign v2")
        run = step.get("run") or ""
        if target in run:
            problems.append(f"{job_id}: references {target} in a run: body")
    return problems


def trivy_gate_problems(workflow: dict[str, Any]) -> list[str]:
    """A trivy scan must gate on HIGH and CRITICAL findings with exit-code 1
    — a scan that only reports, without failing the build, proves nothing
    (CICD-08).
    """
    problems: list[str] = []
    found = False
    for job_id, step in _steps(workflow):
        if not str(step.get("uses", "")).startswith(TRIVY_ACTION):
            continue
        found = True
        with_block = step.get("with") or {}
        severity = str(with_block.get("severity", ""))
        if "HIGH" not in severity or "CRITICAL" not in severity:
            problems.append(
                f"{job_id}: trivy severity {severity!r} does not gate on both HIGH and CRITICAL"
            )
        exit_code = with_block.get("exit-code")
        if str(exit_code) != "1":
            problems.append(f"{job_id}: trivy exit-code {exit_code!r} is not '1'")
    if not found:
        problems.append(f"no {TRIVY_ACTION} step found")
    return problems


# ===========================================================================
# Positive-case tests: every property holds against the real, committed file.
# ===========================================================================


def test_publish_job_pins_every_action_by_full_sha() -> None:
    problems = sha_pin_problems(_publish_workflow())
    assert not problems, "\n".join(problems)


def test_the_job_has_least_privilege_permissions_with_write_scoped_to_the_job() -> None:
    problems = permission_scope_problems(_publish_workflow())
    assert not problems, "\n".join(problems)


def test_the_build_step_tags_the_image_by_git_sha_never_latest() -> None:
    problems = build_tag_problems(_publish_workflow())
    assert not problems, "\n".join(problems)


def test_the_build_step_requests_sbom_and_provenance() -> None:
    problems = sbom_provenance_problems(_publish_workflow())
    assert not problems, "\n".join(problems)


def test_a_cosign_sign_step_targets_the_built_image_by_digest() -> None:
    problems = cosign_sign_problems(_publish_workflow())
    assert not problems, "\n".join(problems)


def test_cosign_experimental_is_never_set() -> None:
    problems = cosign_experimental_problems(_publish_workflow())
    assert not problems, "\n".join(problems)


def test_a_trivy_scan_gates_on_high_and_critical_with_exit_code_1() -> None:
    problems = trivy_gate_problems(_publish_workflow())
    assert not problems, "\n".join(problems)


# ===========================================================================
# Non-vacuity / mutation tests: scratch copies of the real workflow are
# broken on purpose, and the SAME checker above must now report a problem.
# Nothing on disk is ever edited.
# ===========================================================================


def test_removing_the_trivy_severity_gate_is_reported() -> None:
    workflow = _publish_workflow()

    downgraded = copy.deepcopy(workflow)
    mutated_severity = False
    for job in downgraded["jobs"].values():
        for step in job.get("steps") or []:
            if str(step.get("uses", "")).startswith(TRIVY_ACTION):
                step["with"]["severity"] = "LOW"
                mutated_severity = True
    assert mutated_severity, (
        "scratch mutation target (trivy step) not found — this test proves nothing"
    )
    assert trivy_gate_problems(downgraded), (
        "downgrading the trivy severity gate from HIGH,CRITICAL to LOW was not reported"
    )

    dropped_exit_code = copy.deepcopy(workflow)
    mutated_exit_code = False
    for job in dropped_exit_code["jobs"].values():
        for step in job.get("steps") or []:
            if str(step.get("uses", "")).startswith(TRIVY_ACTION):
                step["with"].pop("exit-code", None)
                mutated_exit_code = True
    assert mutated_exit_code, (
        "scratch mutation target (trivy step) not found — this test proves nothing"
    )
    assert trivy_gate_problems(dropped_exit_code), (
        "dropping the trivy exit-code gate was not reported"
    )


def test_removing_the_cosign_sign_step_is_reported() -> None:
    workflow = _publish_workflow()

    mutated = copy.deepcopy(workflow)
    for job in mutated["jobs"].values():
        job["steps"] = [
            step
            for step in (job.get("steps") or [])
            if "cosign sign" not in (step.get("run") or "")
        ]
    assert mutated != workflow, "the scratch mutation did not apply — this test proves nothing"
    assert cosign_sign_problems(mutated), "removing the `cosign sign` step was not reported"
