"""SEC-02: the secret scan must see the whole history, not one commit.

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
