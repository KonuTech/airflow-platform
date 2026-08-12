"""Guards on the supply chain that keeps credentials out of this repository.

Three unrelated things are asserted here because they defend one claim — that
the secret scan is real and cannot be quietly neutered:

* SEC-02: the scan sees the whole history, not one commit (below).
* T-01-09 / CR-02: the scanner binary is verified BEFORE it is extracted.
* CR-03: its trust anchor is pinned in this repository, and the installed
  binary is never executed to decide whether to trust it.
* CR-01: `make install` cannot rewrite a stale `uv.lock` out from under
  `lock-check`.

The last three arrived as fixes for bugs found during Phase 1 review and
verification, and each carries `@pytest.mark.regression` so
`pytest -m regression` is an honest inventory of what this project has promised
not to reintroduce.

## Image-pin agreement (extended for this phase)

A fourth, unrelated thing is asserted here for the same reason as the first
three: an image a values file selects is itself part of the supply chain, and
`helm/versions.env` is this repository's single declared source for it
(§77, INFRA-08 precedent — the same "pinned versions live in exactly one
place" rule `tests/policy/test_pinned_tool_versions_agree.py` already
enforces for tool binaries and chart versions, generalised here to container
image tags). Two properties, following that module's load-bearing-source
model exactly:

* every image tag a values file selects agrees with `helm/versions.env`'s
  own pin (`MINIO_IMAGE_TAG`, `AIRFLOW_IMAGE_TAG`) — a reader per source,
  each perturbed in turn to prove none is silently ignored;
* no values file selects a mutable tag (`latest`, `main`, `master`, `edge`,
  `nightly`, or an empty string) for ANY image field, not only the two
  sources above — a chart pinned by version can still be handed a floating
  image tag, which is precisely the gap `imagePullPolicy: IfNotPresent`
  makes silent (02-RESEARCH.md PITFALLS A5).

SEC-02: the secret scan must see the whole history, not one commit.

This is not a style rule. `actions/checkout` defaults to `fetch-depth: 1`, and
`gitleaks git --log-opts="--all"` over a depth-1 checkout examines a single
commit and reports "no leaks found" — a green build that proves nothing.
01-RESEARCH.md verified the failure mode directly: a depth-1 checkout logs
`1 commits scanned`. The job would keep passing, faster and faster, while the
claim it exists to support quietly became false.

That claim is the one that has to be true before this repository can be made
public: once it is, any credential in any reachable commit is world-readable and
rotation is the only remedy. A scan that silently narrowed to HEAD would let a
credential committed months earlier survive the audit.

Everything here reads the *parsed* workflow rather than matching text, so a
restructured step — a different job name, a different step order, `with:`
written inline — still gets checked.

Two properties are asserted together because either alone is insufficient:

* the scanning job checks out full history (`fetch-depth: 0`), and
* the scan itself is the all-refs form (`--log-opts="--all"`).

Full depth with a HEAD-only scan, or an all-refs scan over a shallow clone,
both reduce to scanning almost nothing.
"""

from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"
MAKEFILE = REPO_ROOT / "Makefile"

HISTORY_SCAN_TARGET = "gitleaks"
CHECKOUT_ACTION = "actions/checkout"
FULL_HISTORY = 0
INSTALLER = "tools/security/install_gitleaks.sh"


def _workflows() -> dict[str, dict[str, Any]]:
    return {
        path.name: yaml.safe_load(path.read_text(encoding="utf-8"))
        for path in sorted(WORKFLOW_DIR.glob("*.y*ml"))
    }


def _jobs_running_the_history_scan(workflow: dict[str, Any]) -> dict[str, Any]:
    """Jobs with a run step invoking the full-history scan target."""
    found = {}
    for job_id, job in (workflow.get("jobs") or {}).items():
        for step in job.get("steps") or []:
            run = step.get("run") or ""
            if re.search(rf"\bmake\s+{HISTORY_SCAN_TARGET}\b", run):
                found[job_id] = job
                break
    return found


def shallow_checkout_problems(workflow: dict[str, Any], label: str = "") -> list[str]:
    """Report any scanning job that does not check out the full history."""
    problems: list[str] = []
    for job_id, job in _jobs_running_the_history_scan(workflow).items():
        checkouts = [
            step
            for step in (job.get("steps") or [])
            if str(step.get("uses", "")).startswith(CHECKOUT_ACTION)
        ]
        if not checkouts:
            problems.append(f"{label}{job_id}: runs the secret scan without a checkout step")
            continue
        for step in checkouts:
            depth = (step.get("with") or {}).get("fetch-depth")
            if depth != FULL_HISTORY:
                problems.append(
                    f"{label}{job_id}: checkout uses fetch-depth={depth!r}, not "
                    f"{FULL_HISTORY} — the history scan would examine one commit "
                    "and report clean",
                )
    return problems


def test_the_secret_scan_job_checks_out_full_history() -> None:
    problems: list[str] = []
    for name, workflow in _workflows().items():
        problems += shallow_checkout_problems(workflow, label=f"{name} ")
    assert not problems, "SEC-02 would pass without proving anything:\n" + "\n".join(problems)


def test_a_scanning_job_exists_at_all() -> None:
    """A workflow with no scan job would satisfy every assertion above vacuously."""
    scanning = {
        name: sorted(_jobs_running_the_history_scan(workflow))
        for name, workflow in _workflows().items()
    }
    assert any(scanning.values()), (
        f"no workflow job runs `make {HISTORY_SCAN_TARGET}` — SEC-02 is unenforced: {scanning}"
    )


def test_the_scan_target_reads_every_ref() -> None:
    """Full depth is pointless if the scan only looks at the current branch."""
    text = MAKEFILE.read_text(encoding="utf-8")
    target = re.search(rf"^{HISTORY_SCAN_TARGET}:.*?(?=^\S|\Z)", text, re.MULTILINE | re.DOTALL)
    assert target, f"Makefile no longer defines a `{HISTORY_SCAN_TARGET}` target"
    body = target.group(0)
    missing = [flag for flag in ("--log-opts", "--all") if flag not in body]
    assert not missing, (
        f"the scan no longer covers every ref (missing {missing}); a full-depth\n"
        "checkout buys nothing without it:\n" + body
    )


def test_removing_full_depth_is_reported() -> None:
    """Scratch copies of the real workflow; nothing on disk is edited."""
    workflow = yaml.safe_load((WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8"))

    dropped = copy.deepcopy(workflow)
    for job in dropped["jobs"].values():
        for step in job.get("steps") or []:
            if str(step.get("uses", "")).startswith(CHECKOUT_ACTION):
                step.pop("with", None)
    assert shallow_checkout_problems(dropped), (
        "a scanning job whose checkout lost `fetch-depth: 0` was not reported"
    )

    shallow = copy.deepcopy(workflow)
    for job in shallow["jobs"].values():
        for step in job.get("steps") or []:
            if str(step.get("uses", "")).startswith(CHECKOUT_ACTION):
                step["with"] = {"fetch-depth": 1}
    assert shallow_checkout_problems(shallow), (
        "a scanning job pinned to a depth-1 checkout was not reported"
    )


@pytest.mark.regression
def test_the_installer_verifies_before_it_extracts() -> None:
    """T-01-09: the scanner binary is fetched over the network, then executed.

    A tampered binary would report clean forever and nobody would notice, so the
    published digest is checked BEFORE the archive is opened. Verifying after
    extraction would still fail the build, but only after writing an attacker's
    file to disk — and that reordering is a plausible, innocent-looking edit.

    Honest limit, recorded rather than implied: this asserts the ordering of the
    two steps in the script, not the behaviour of a corrupted download. The
    fail-closed path itself was observed by hand in plan 01-02 (a PATH-shimmed
    curl corrupting the tarball: exit 1, nothing extracted) and still has no
    committed behavioural coverage.

    Comments are stripped before the search, and that is load-bearing rather
    than tidiness. A plain `text.find("sha256sum -c")` matched the PROSE above
    the verification, which sits above `tar -xzf` unconditionally — so the
    assertion held no matter where the real verification lived, and the guard
    was vacuous. Moving the genuine check below the extraction was observed
    passing this test before this fix. Search executable lines only.
    """
    text = (REPO_ROOT / INSTALLER).read_text(encoding="utf-8")

    # Blank out comment bodies while preserving byte offsets, so the indices
    # below still correspond to positions in the real file.
    executable = "\n".join(
        line.split("#", 1)[0] if line.lstrip().startswith("#") else line
        for line in text.splitlines()
    )

    verify = executable.find("sha256sum -c")
    extract = executable.find("tar -xzf")
    assert verify != -1, (
        f"{INSTALLER} no longer verifies a checksum in executable code "
        "(a mention inside a comment does not count)"
    )
    assert extract != -1, f"{INSTALLER} no longer extracts an archive"
    assert verify < extract, (
        f"{INSTALLER} extracts the archive before verifying its checksum — the "
        "download must fail closed, with nothing written on mismatch"
    )


@pytest.mark.regression
def test_make_install_refuses_a_stale_lockfile() -> None:
    """CR-01: `make install` must not be able to rewrite `uv.lock`.

    The bug: `install:` ran a bare `uv sync`, which *updates* a stale lockfile
    rather than failing on it. CI runs `make install` before `make check`, and
    `check` depends on `lock-check` (`uv lock --check`) — so by the time the
    staleness gate ran, `uv sync` had already refreshed the very file it
    inspects. Observed end to end: with a stale lock `uv lock --check` exited
    1, `uv sync` rewrote the lock, and the same check then exited 0. A pull
    request could change a dependency without regenerating the lock and still
    go green, on a dependency resolution nobody reviewed.

    `--locked` makes `uv sync` fail instead of resolving, so the lockfile
    reaching `lock-check` is the one that was committed.

    This asserts the flag rather than the behaviour, deliberately: reproducing
    the behaviour needs a network resolve and a mutated `pyproject.toml`, which
    does not belong in the offline `make check` path (ROADMAP criterion 4). The
    flag is the whole fix, and dropping it is the plausible regression.
    """
    text = MAKEFILE.read_text(encoding="utf-8")

    body = re.search(r"^install:.*?(?=^\S)", text, re.MULTILINE | re.DOTALL)
    assert body, "Makefile no longer defines an `install:` target"

    # Executable lines only — a `--locked` mentioned in the rationale comment
    # above the recipe must not satisfy this. That exact comment-vs-code
    # confusion is what made the T-01-09 ordering guard vacuous (CR-02).
    recipe = "\n".join(
        line for line in body.group(0).splitlines() if not line.lstrip().startswith("#")
    )

    assert "uv" in recipe, (
        "`install:` no longer invokes uv — re-check this guard against the new recipe"
    )
    assert "sync" in recipe, (
        "`install:` no longer runs `uv sync` — re-check this guard against the new recipe"
    )
    assert "--locked" in recipe, (
        "`make install` runs `uv sync` WITHOUT `--locked`, so a stale uv.lock is "
        "silently rewritten before `lock-check` inspects it, and an unreviewed "
        "dependency resolution passes CI (CR-01)"
    )


@pytest.mark.regression
def test_the_installer_trusts_only_an_in_repo_digest() -> None:
    """CR-03: the scanner's trust anchor must not come from the download origin.

    Two defects, one fix. First, the release's `checksums.txt` was fetched from
    the same URL prefix as the tarball it describes, so whoever could alter one
    could alter the other — the digest caught corruption in transit but not
    substitution at the source, which is the threat T-01-09 names. Second, an
    already-present binary in the gitignored `tools/bin/` was EXECUTED to read
    its version, so a once-planted binary was trusted forever and never
    re-verified. Observed: a shim reporting `8.30.1` and `no leaks found` was
    accepted by the old idempotent path; it is now replaced by the real binary.

    Both are asserted structurally, because the behaviour needs a network
    download that `make check` must not perform (ROADMAP criterion 4). The
    behavioural proof was run by hand at fix time: a deliberately wrong pin
    exited 1 with `tools/bin/` left empty.
    """
    text = (REPO_ROOT / INSTALLER).read_text(encoding="utf-8")
    executable = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))

    assert re.search(r"^PINNED_SHA256_\w+=", executable, re.MULTILINE), (
        f"{INSTALLER} no longer pins any SHA-256 in-repo — the digest would come "
        "from the same origin as the artifact it validates (CR-03)"
    )

    # The verified-against file must be BUILT from the pin, not filtered out of
    # the downloaded checksums. `grep ... "${checksums}" > expected.sha256` was
    # the origin-vouches-for-itself construction this replaced.
    assert 'echo "${pinned}  ${tarball}" > expected.sha256' in executable, (
        f"{INSTALLER} no longer builds its verification input from the in-repo "
        "pin; a digest taken from the download cannot authenticate the download"
    )

    # The idempotent fast path must not run the binary to decide whether to
    # trust it. Executing an unverified artifact IS the vulnerability.
    assert '"${dest}" version' not in executable, (
        f"{INSTALLER} executes the installed binary to decide whether to trust "
        "it — a planted binary that lies about its version is then trusted "
        "forever and never re-verified (CR-03)"
    )


# ===========================================================================
# Image-pin agreement (see module docstring) — helm/versions.env is the one
# declared source; every values file selecting an image tag must agree.
# ===========================================================================

VERSIONS_ENV = REPO_ROOT / "helm" / "versions.env"
VALUES_LOCAL_DIR = REPO_ROOT / "helm" / "values" / "local"
VALUES_CI_DIR = REPO_ROOT / "helm" / "values" / "ci"

MUTABLE_TAG_VALUES = frozenset({"", "latest", "main", "master", "edge", "nightly"})


def _versions_env_variable(name: str) -> str:
    match = re.search(rf"^{name}=(.+)$", VERSIONS_ENV.read_text(encoding="utf-8"), re.MULTILINE)
    assert match, f"helm/versions.env no longer defines {name}"
    return match.group(1).strip()


def _values_field(path: Path, *keys: str) -> str:
    node: Any = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    for key in keys:
        if not isinstance(node, dict) or key not in node:
            return ""
        node = node[key]
    return str(node)


def minio_image_readings() -> dict[str, str]:
    return {
        "helm/versions.env": _versions_env_variable("MINIO_IMAGE_TAG"),
        "helm/values/local/minio.yaml": _values_field(
            VALUES_LOCAL_DIR / "minio.yaml",
            "image",
            "tag",
        ),
        "helm/values/ci/minio.yaml": _values_field(
            VALUES_CI_DIR / "minio.yaml",
            "image",
            "tag",
        ),
    }


def airflow_image_readings() -> dict[str, str]:
    return {
        "helm/versions.env": _versions_env_variable("AIRFLOW_IMAGE_TAG"),
        "helm/values/local/airflow.yaml": _values_field(
            VALUES_LOCAL_DIR / "airflow.yaml",
            "defaultAirflowTag",
        ),
        "helm/values/ci/airflow.yaml": _values_field(
            VALUES_CI_DIR / "airflow.yaml",
            "defaultAirflowTag",
        ),
    }


IMAGE_TAG_READINGS: dict[str, Any] = {
    "minio": minio_image_readings,
    "airflow": airflow_image_readings,
}


def image_tag_disagreements(image: str, readings: dict[str, str]) -> list[str]:
    """Mirrors test_pinned_tool_versions_agree.py's disagreements() exactly."""
    problems: list[str] = []
    blank = sorted(source for source, tag in readings.items() if not tag)
    if blank:
        problems.append(f"{image}: no tag could be read from {blank}")
    distinct = {tag for tag in readings.values() if tag}
    if len(distinct) > 1:
        rendered = ", ".join(f"{src}={tag!r}" for src, tag in sorted(readings.items()))
        problems.append(f"{image}: tags disagree -> {rendered}")
    return problems


def test_every_image_tag_agrees_with_versions_env() -> None:
    problems: list[str] = []
    for image, reader in IMAGE_TAG_READINGS.items():
        problems += image_tag_disagreements(image, reader())
    assert not problems, (
        "an image tag has drifted from helm/versions.env, the single "
        "declared source (§77, INFRA-08 precedent):\n" + "\n".join(problems)
    )


def test_every_image_tag_source_is_load_bearing() -> None:
    """Perturbing any single source must produce a disagreement (§77)."""
    for image, reader in IMAGE_TAG_READINGS.items():
        readings = reader()
        assert len(readings) >= 2, f"{image}: only one source, nothing to compare"
        for source in readings:
            mutated = dict(readings)
            mutated[source] = "0.0.0-scratch"
            assert image_tag_disagreements(image, mutated), (
                f"changing {source} alone for {image} was not reported as drift"
            )


def _flatten_for_image_scan(value: Any, prefix: str = "") -> dict[str, Any]:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, child in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            out.update(_flatten_for_image_scan(child, path))
        return out
    return {prefix: value}


def mutable_tag_problems(doc: dict[str, Any], label: str) -> list[str]:
    """Report any `tag`-shaped leaf holding a mutable value, anywhere in `doc`.

    Broader than the two named sources above on purpose: a chart pinned by
    VERSION can still be handed a floating IMAGE tag, and
    `imagePullPolicy: IfNotPresent` makes that silent (02-RESEARCH.md
    PITFALLS A5) rather than loud.
    """
    problems: list[str] = []
    for path, value in _flatten_for_image_scan(doc).items():
        leaf = path.rsplit(".", 1)[-1]
        if leaf.lower() != "tag" and leaf != "defaultAirflowTag":
            continue
        if isinstance(value, str) and value.strip().lower() in MUTABLE_TAG_VALUES:
            problems.append(f"{label}: {path} selects a mutable tag ({value!r})")
    return problems


def _all_values_paths() -> list[Path]:
    paths: list[Path] = []
    for directory in (VALUES_LOCAL_DIR, VALUES_CI_DIR):
        paths.extend(sorted(directory.glob("*.yaml")))
    return paths


def test_no_values_file_selects_a_mutable_image_tag() -> None:
    problems: list[str] = []
    for path in _all_values_paths():
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        problems += mutable_tag_problems(doc, str(path.relative_to(REPO_ROOT)))
    assert not problems, "a mutable image tag was selected:\n" + "\n".join(problems)


def test_replacing_a_tag_with_a_mutable_one_is_reported() -> None:
    """Non-vacuity: perturbing a real values file's tag on an in-memory copy."""
    path = VALUES_LOCAL_DIR / "minio.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    mutated = copy.deepcopy(doc)
    mutated["image"]["tag"] = "latest"
    assert mutated != doc, "the scratch mutation did not apply — this test proves nothing"
    assert mutable_tag_problems(mutated, "scratch"), (
        "replacing helm/values/local/minio.yaml's image tag with 'latest' was not reported"
    )


def test_a_pinned_tag_is_not_reported() -> None:
    """False-positive control: the real, pinned tags produce no messages."""
    doc = yaml.safe_load((VALUES_LOCAL_DIR / "minio.yaml").read_text(encoding="utf-8")) or {}
    assert not mutable_tag_problems(doc, "scratch")
