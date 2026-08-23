"""Structural, non-vacuous proof that `publish.yml`/`ghcr-cleanup.yml` really
do what plan 11-01's and 11-02's `must_haves` claim — not merely that a step
with a plausible name exists somewhere in the file.

Mirrors `test_supply_chain_guards.py`'s idiom exactly: a module-level YAML
loader, small pure `*_problems()` functions that return a list of strings
(empty means "no problem found"), and thin `test_*` functions that assert
`not problems(...)` against the real file. The mutation tests
(`test_removing_the_trivy_severity_gate_is_reported`,
`test_removing_the_cosign_sign_step_is_reported`, and plan 11-02's own
additions below) deep-copy the parsed workflow, break one property on the
copy, and assert the SAME checker function now reports it — proving the
checker actually discriminates a broken workflow from a correct one, rather
than passing regardless of what the file contains.

Everything here reads the *parsed* workflow rather than matching raw text
(the `${{ github.sha }}`/`${{ steps.build.outputs.digest }}` checks compare
against the interpolated `with:`/`run:` values, not line-by-line grep), so a
restructured step — different step order, `with:` reformatted — still gets
checked correctly.

Plan 11-02 generalized `publish.yml`'s single `publish-csv-processor` job
into a 3-image matrix job named `publish` (JOB_ID below), added a
`pull_request` trigger (D-09) with a computed `pr-<number>`/git-SHA tag
output, a fork-PR guard (Pitfall 6), a `release`-triggered `retag-release`
job (D-03), and a companion `ghcr-cleanup.yml` (D-11) — the checkers and
tests below were extended in place, not replaced, for exactly that reason.
"""

from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "publish.yml"
CLEANUP_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "ghcr-cleanup.yml"

import yaml  # noqa: E402  (import after path constants for readability)

FULL_SHA_PIN = re.compile(r"[^@]+@[0-9a-f]{40}")
GITHUB_SHA_REF = "${{ github.sha }}"
DIGEST_REF = "steps.build.outputs.digest"
BUILD_ACTION = "docker/build-push-action"
TRIVY_ACTION = "aquasecurity/trivy-action"
DELETE_PACKAGE_VERSIONS_ACTION = "actions/delete-package-versions"
# Plan 11-01's original single-image job was `publish-csv-processor`; plan
# 11-02 Task 1 restructured it into a 3-image matrix job renamed `publish`.
JOB_ID = "publish"
RETAG_JOB_ID = "retag-release"
EXPECTED_IMAGES = frozenset({"csv-processor", "dbt", "airflow"})
IMAGETOOLS_CREATE = "buildx imagetools create"
RELEASE_TAG_REF = "github.event.release.tag_name"
PR_NUMBER_REF = "github.event.pull_request.number"
FORK_GUARD_LEFT = "github.event.pull_request.head.repo.full_name"
FORK_GUARD_RIGHT = "github.repository"
PUSH_ONLY_GATE = re.compile(r"^\s*github\.event_name\s*==\s*['\"]push['\"]\s*$")


def _publish_workflow() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))


def _cleanup_workflow() -> dict[str, Any]:
    return yaml.safe_load(CLEANUP_WORKFLOW_PATH.read_text(encoding="utf-8"))


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


def _on_block(workflow: dict[str, Any]) -> dict[str, Any]:
    """PyYAML's `safe_load` follows YAML 1.1 boolean resolution, under which
    the bare key `on:` parses to the Python boolean `True`, not the string
    `"on"` — a well-known GitHub-Actions-YAML gotcha. `workflow["on"]` would
    silently return nothing and every trigger check built on it would be a
    false positive (reporting a real, correct workflow as broken). Look up
    both spellings; whichever one PyYAML actually produced wins.
    """
    return workflow.get("on") or workflow.get(True) or {}


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
    """The published image must be tagged by the triggering commit's git SHA
    (on push) or a computed `pr-<number>` (on pull_request) — never a bare
    `:latest` — INFRA-08's replayability precedent, generalized to D-09's
    two-trigger tag scheme. Plan 11-01's original check compared `tags:`
    against the literal `${{ github.sha }}` string; plan 11-02 replaced the
    hardcoded tag with a computed `steps.tag.outputs.value` step output, so
    this checker now accepts either form rather than only the pre-11-02
    literal.
    """
    step = _find_step(workflow, BUILD_ACTION)
    if step is None:
        return [f"no {BUILD_ACTION} step found"]
    tags = str((step.get("with") or {}).get("tags", ""))
    problems: list[str] = []
    if "steps.tag.outputs" not in tags and GITHUB_SHA_REF not in tags:
        problems.append(
            f"build step tags {tags!r} reference neither a computed tag step "
            f"output (steps.tag.outputs...) nor the literal {GITHUB_SHA_REF!r}"
        )
    if "latest" in tags:
        problems.append(f"build step tags {tags!r} contain the substring 'latest'")
    return problems


def matrix_problems(workflow: dict[str, Any]) -> list[str]:
    """D-01: the `publish` job's matrix must build exactly the three images
    this platform ships — csv-processor, dbt, airflow — each pointed at a
    Dockerfile that actually exists on disk, not a typo'd path that would
    silently never build.
    """
    problems: list[str] = []
    job = (workflow.get("jobs") or {}).get(JOB_ID) or {}
    include = ((job.get("strategy") or {}).get("matrix") or {}).get("include")
    if not include:
        return [f"{JOB_ID}: no strategy.matrix.include found"]
    seen_images: dict[str, str] = {}
    for entry in include:
        image = entry.get("image")
        dockerfile = entry.get("dockerfile")
        if not image or not dockerfile:
            problems.append(f"{JOB_ID}: matrix entry {entry!r} is missing image/dockerfile")
            continue
        seen_images[image] = dockerfile
        if not (REPO_ROOT / dockerfile).is_file():
            problems.append(
                f"{JOB_ID}: matrix entry {image!r} points at a missing file {dockerfile!r}"
            )
    if set(seen_images) != EXPECTED_IMAGES:
        problems.append(
            f"{JOB_ID}: matrix images {sorted(seen_images)} != expected {sorted(EXPECTED_IMAGES)}"
        )
    return problems


def pr_tag_problems(workflow: dict[str, Any]) -> list[str]:
    """D-09: the tag-computation logic must branch on the triggering
    pull_request's own number and produce a value literally prefixed `pr-`
    — a PR smoke test (plan 11-04) that pulled `pr-` with no number, or a
    hardcoded PR number, would silently test the wrong image.
    """
    for _job_id, step in _steps(workflow):
        run = step.get("run") or ""
        if PR_NUMBER_REF in run and "pr-" in run:
            return []
    return [
        f"no step's run: body both references {PR_NUMBER_REF!r} and produces a 'pr-' prefixed value"
    ]


def _condition_excludes_same_repo_pr(condition: str) -> bool:
    """True if `condition` is a push-only gate that would exclude a
    legitimate (non-fork) pull_request run — i.e. it mentions
    `github.event_name == 'push'` but carries no pull_request allowance at
    all. A condition that ALSO allows same-repo PRs (the fork guard's own
    `... || github.event.pull_request.head.repo.full_name ==
    github.repository` shape) is fine; a bare push-only condition is not.
    """
    if not condition:
        return False
    return bool(PUSH_ONLY_GATE.match(condition)) or (
        "github.event_name == 'push'" in condition and "pull_request" not in condition
    )


def pr_parity_problems(workflow: dict[str, Any]) -> list[str]:
    """D-10 / Pitfall 5: the cosign-sign and trivy-scan steps must run for
    `pr-<number>` images exactly like merge-tagged images — never gated on
    `github.event_name == 'push'` alone. An unsigned/unscanned PR image is
    denied by plan 11-03's own Kyverno policy for what looks like an
    unrelated pod-scheduling failure.
    """
    problems: list[str] = []
    found_cosign = False
    found_trivy = False
    for job_id, step in _steps(workflow):
        run = step.get("run") or ""
        is_cosign_sign = "cosign sign" in run
        is_trivy = str(step.get("uses", "")).startswith(TRIVY_ACTION)
        if not (is_cosign_sign or is_trivy):
            continue
        if is_cosign_sign:
            found_cosign = True
        if is_trivy:
            found_trivy = True
        condition = str(step.get("if", ""))
        if _condition_excludes_same_repo_pr(condition):
            problems.append(
                f"{job_id}: step is gated by a push-only condition ({condition!r}) "
                "that excludes legitimate pull_request runs"
            )
    if not found_cosign:
        problems.append("no step runs `cosign sign`")
    if not found_trivy:
        problems.append(f"no {TRIVY_ACTION} step found")
    return problems


def fork_guard_problems(workflow: dict[str, Any]) -> list[str]:
    """Pitfall 6: some job- or step-level `if:` must compare the triggering
    pull_request's head repository against this repository — otherwise a
    fork PR's build/sign/scan/push steps would attempt to run with a
    GitHub-enforced read-only token and fail confusingly instead of being
    skipped cleanly.
    """
    job = (workflow.get("jobs") or {}).get(JOB_ID) or {}
    job_condition = str(job.get("if", ""))
    if FORK_GUARD_LEFT in job_condition and FORK_GUARD_RIGHT in job_condition:
        return []
    for _job_id, step in _steps(workflow):
        condition = str(step.get("if", ""))
        if FORK_GUARD_LEFT in condition and FORK_GUARD_RIGHT in condition:
            return []
    return [
        f"no job- or step-level if: references both {FORK_GUARD_LEFT!r} and {FORK_GUARD_RIGHT!r}"
    ]


def release_retag_problems(workflow: dict[str, Any]) -> list[str]:
    """D-03: a GitHub Release must retag the already-published SHA image
    with the release's own semver tag via `docker buildx imagetools
    create` — a manifest copy, never a second build — referencing both the
    release's own tag name and the git SHA it is retagging from, in a
    dedicated job scoped to the `release` event only.
    """
    if RETAG_JOB_ID not in (workflow.get("jobs") or {}):
        return [f"no {RETAG_JOB_ID!r} job found in jobs:"]
    for _job_id, step in _steps(workflow):
        run = step.get("run") or ""
        if IMAGETOOLS_CREATE not in run:
            continue
        problems: list[str] = []
        if RELEASE_TAG_REF not in run:
            problems.append(f"`{IMAGETOOLS_CREATE}` step does not reference {RELEASE_TAG_REF!r}")
        if "github.sha" not in run:
            problems.append(f"`{IMAGETOOLS_CREATE}` step does not reference github.sha")
        return problems
    return [f"no step runs `{IMAGETOOLS_CREATE}`"]


def lowercase_owner_problems(workflow: dict[str, Any]) -> list[str]:
    """GHCR/OCI repository names must be all-lowercase; this repository's
    owner login (`KonuTech`) is not. Both of plan 11-01's real push-to-main
    runs failed at the build-push-action step with Docker's own `invalid
    tag ... repository name must be lowercase` error before this was fixed
    — every `ghcr.io/...` reference must route through a lowercasing step
    output, never interpolate `github.repository_owner` directly.
    """
    problems: list[str] = []
    for job_id, step in _steps(workflow):
        run = step.get("run") or ""
        if "ghcr.io/${{ github.repository_owner }}" in run:
            problems.append(
                f"{job_id}: {run.strip()!r} interpolates github.repository_owner directly"
            )
        with_block = step.get("with") or {}
        for key, value in with_block.items():
            if "ghcr.io/${{ github.repository_owner }}" in str(value):
                problems.append(
                    f"{job_id}: with.{key} {value!r} interpolates github.repository_owner directly"
                )
    return problems


def cleanup_trigger_problems(workflow: dict[str, Any]) -> list[str]:
    """D-11: `ghcr-cleanup.yml` must trigger on `pull_request: types:
    [closed]` — the only event that fires on both a merged AND a
    non-merged (abandoned) PR close, matching D-11's own "merged or not"
    wording.
    """
    pr_trigger = _on_block(workflow).get("pull_request") or {}
    types = pr_trigger.get("types") or []
    if "closed" not in types:
        return [f"on.pull_request.types {types!r} does not include 'closed'"]
    return []


def cleanup_action_problems(workflow: dict[str, Any]) -> list[str]:
    """D-11: the cleanup job must reference
    `actions/delete-package-versions` and select this PR's own `pr-<number>`
    tag — not a hardcoded number, not every version indiscriminately.
    """
    problems: list[str] = []
    found_action = False
    found_pr_pattern = False
    for _job_id, step in _steps(workflow):
        if str(step.get("uses", "")).startswith(DELETE_PACKAGE_VERSIONS_ACTION):
            found_action = True
        run = step.get("run") or ""
        if "pr-" in run and PR_NUMBER_REF in run:
            found_pr_pattern = True
    if not found_action:
        problems.append(f"no step uses {DELETE_PACKAGE_VERSIONS_ACTION}")
    if not found_pr_pattern:
        problems.append(f"no step's run: body selects a 'pr-'+{PR_NUMBER_REF!r} pattern")
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


def test_all_three_images_are_matrixed() -> None:
    problems = matrix_problems(_publish_workflow())
    assert not problems, "\n".join(problems)


def test_pull_request_builds_get_a_pr_number_tag() -> None:
    problems = pr_tag_problems(_publish_workflow())
    assert not problems, "\n".join(problems)


def test_pr_tagged_images_are_signed_and_scanned_identically_to_merge_images() -> None:
    problems = pr_parity_problems(_publish_workflow())
    assert not problems, "\n".join(problems)


def test_the_fork_guard_is_present() -> None:
    problems = fork_guard_problems(_publish_workflow())
    assert not problems, "\n".join(problems)


def test_release_created_retags_without_rebuilding() -> None:
    problems = release_retag_problems(_publish_workflow())
    assert not problems, "\n".join(problems)


def test_ghcr_image_owner_is_always_lowercased_before_use() -> None:
    problems = lowercase_owner_problems(_publish_workflow())
    assert not problems, "\n".join(problems)
    # ghcr-cleanup.yml constructs its own ghcr.io-adjacent references too
    # (the ${{ steps.owner.outputs.value }} pattern) — same regression class.
    problems = lowercase_owner_problems(_cleanup_workflow())
    assert not problems, "\n".join(problems)


def test_ghcr_cleanup_workflow_exists_and_triggers_on_pull_request_closed() -> None:
    problems = cleanup_trigger_problems(_cleanup_workflow())
    assert not problems, "\n".join(problems)


def test_ghcr_cleanup_deletes_by_resolved_pr_number_version_id() -> None:
    problems = cleanup_action_problems(_cleanup_workflow())
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


def test_gating_cosign_or_trivy_to_push_only_is_reported() -> None:
    """Non-vacuity for `pr_parity_problems`: a workflow that scoped
    cosign-sign/trivy-scan to `github.event_name == 'push'` alone (silently
    excluding legitimate same-repo pull_request runs) must be caught, not
    waved through — this is exactly Pitfall 5's failure mode (a PR image
    that is pushed but never signed/scanned, then denied by plan 11-03's
    Kyverno policy for what looks like an unrelated scheduling error).
    """
    workflow = _publish_workflow()

    mutated = copy.deepcopy(workflow)
    mutated_count = 0
    for job in mutated["jobs"].values():
        for step in job.get("steps") or []:
            run = step.get("run") or ""
            is_cosign_sign = "cosign sign" in run
            is_trivy = str(step.get("uses", "")).startswith(TRIVY_ACTION)
            if is_cosign_sign or is_trivy:
                step["if"] = "github.event_name == 'push'"
                mutated_count += 1
    assert mutated_count >= 2, (
        "scratch mutation target (cosign sign + trivy steps) not found — this test proves nothing"
    )
    assert pr_parity_problems(mutated), (
        "narrowing cosign sign / trivy scan to a push-only if: was not reported"
    )


def test_dropping_a_matrix_image_is_reported() -> None:
    """Non-vacuity for `matrix_problems`: dropping one of the three required
    images from the matrix must be caught, not silently accepted as "2
    images is still a matrix".
    """
    workflow = _publish_workflow()

    mutated = copy.deepcopy(workflow)
    include = mutated["jobs"][JOB_ID]["strategy"]["matrix"]["include"]
    assert len(include) == 3, "scratch mutation precondition failed — matrix is not 3 entries"
    include.pop()
    assert matrix_problems(mutated), "dropping a matrix image entry was not reported"


def test_reintroducing_a_raw_repository_owner_reference_is_reported() -> None:
    """Non-vacuity for `lowercase_owner_problems`: this is the exact live-
    confirmed regression — two real push-to-main runs (32614234666,
    32619730503) failed with Docker's `repository name must be lowercase`
    before this fix. The checker must catch a raw, unlowercased
    `github.repository_owner` reappearing in a ghcr.io image reference in
    the future, not merely happen to pass against today's already-fixed
    file.
    """
    workflow = _publish_workflow()

    mutated = copy.deepcopy(workflow)
    regressed = False
    for job in mutated["jobs"].values():
        for step in job.get("steps") or []:
            if step.get("id") == "build":
                with_block = step.setdefault("with", {})
                if "tags" in with_block:
                    with_block["tags"] = "ghcr.io/${{ github.repository_owner }}/x:y"
                    regressed = True
    assert regressed, (
        "scratch mutation target (build step tags) not found — this test proves nothing"
    )
    assert lowercase_owner_problems(mutated), (
        "reintroducing a raw github.repository_owner reference in a ghcr.io tag was not reported"
    )
