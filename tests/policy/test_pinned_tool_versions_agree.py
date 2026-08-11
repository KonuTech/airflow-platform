"""A gate tool's version is pinned in several files; they must not drift apart.

This is the cheap test that prevents a whole class of "the gate got weaker and
nobody noticed". Three examples of what it catches:

* `ruff` bumped in `pyproject.toml` but not in `.pre-commit-config.yaml`, so the
  local hook keeps auto-fixing under the old ruleset and CI starts failing on
  code that looked clean at commit time.
* `GITLEAKS_VERSION` bumped in the workflow but not in the installer, so CI
  downloads one scanner and a developer machine downloads another. Plan 01-02
  recorded this duplication explicitly and handed the assertion here.
* A pin loosened from `==` to `>=`, which quietly turns a reproducible gate into
  whatever resolved most recently.

The comparison is over exact equality of every reading, so it fails when EITHER
side changes alone — the property the plan asks for. `test_every_source_is_load_
bearing` proves that bidirectionality mechanically: it perturbs each reading in
turn and requires a disagreement to be reported for every one of them, so no
source can be silently ignored by the comparison.

CI installs from `uv.lock` (`make install` → `uv sync`), so the lockfile is the
version the workflow actually installs and is compared too. `make lock-check`
catches a stale lock; this catches a lock that is fresh but disagrees with the
pin a human reads.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = REPO_ROOT / "pyproject.toml"
UV_LOCK = REPO_ROOT / "uv.lock"
PRE_COMMIT = REPO_ROOT / ".pre-commit-config.yaml"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
MAKEFILE = REPO_ROOT / "Makefile"
INSTALLER = REPO_ROOT / "tools" / "security" / "install_gitleaks.sh"

EXACTLY_PINNED = ("ruff", "mypy")


def disagreements(tool: str, readings: dict[str, str]) -> list[str]:
    """Report a message unless every source names the same non-empty version."""
    problems: list[str] = []
    blank = sorted(source for source, version in readings.items() if not version)
    if blank:
        problems.append(f"{tool}: no version could be read from {blank}")
    distinct = {version for version in readings.values() if version}
    if len(distinct) > 1:
        rendered = ", ".join(f"{src}={ver!r}" for src, ver in sorted(readings.items()))
        problems.append(f"{tool}: versions disagree -> {rendered}")
    return problems


def _dev_group_pins() -> dict[str, str]:
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    pins: dict[str, str] = {}
    for entry in data["dependency-groups"]["dev"]:
        match = re.fullmatch(r"\s*([A-Za-z0-9_.-]+)\s*==\s*([^\s;]+)\s*", entry)
        if match:
            pins[match.group(1).lower()] = match.group(2)
    return pins


def _dev_group_specifiers() -> dict[str, str]:
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    specs: dict[str, str] = {}
    for entry in data["dependency-groups"]["dev"]:
        match = re.match(r"\s*([A-Za-z0-9_.-]+)\s*(.*)", entry)
        if match:
            specs[match.group(1).lower()] = match.group(2).strip()
    return specs


def _locked_versions() -> dict[str, str]:
    data = tomllib.loads(UV_LOCK.read_text(encoding="utf-8"))
    return {p["name"].lower(): p["version"] for p in data.get("package", [])}


def _pre_commit_revs() -> dict[str, str]:
    data = yaml.safe_load(PRE_COMMIT.read_text(encoding="utf-8"))
    revs: dict[str, str] = {}
    for repo in data.get("repos", []):
        url = str(repo.get("repo", ""))
        rev = str(repo.get("rev", ""))
        revs[url] = rev.removeprefix("v")
    return revs


def _pre_commit_rev_for(fragment: str) -> str:
    matches = [rev for url, rev in _pre_commit_revs().items() if fragment in url]
    assert len(matches) == 1, f"expected exactly one pre-commit repo matching {fragment!r}"
    return matches[0]


def _workflow_env() -> dict[str, str]:
    data = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    return {k: str(v) for k, v in (data.get("env") or {}).items()}


def _makefile_variable(name: str) -> str:
    match = re.search(rf"^{name}\s*:?=\s*(\S+)", MAKEFILE.read_text(encoding="utf-8"), re.MULTILINE)
    assert match, f"Makefile no longer defines {name}"
    return match.group(1)


def _installer_default_version() -> str:
    text = INSTALLER.read_text(encoding="utf-8")
    match = re.search(r'GITLEAKS_VERSION="\$\{GITLEAKS_VERSION:-([^}"]+)\}"', text)
    assert match, "install_gitleaks.sh no longer carries a default GITLEAKS_VERSION"
    return match.group(1)


def ruff_readings() -> dict[str, str]:
    return {
        "pyproject dev group": _dev_group_pins().get("ruff", ""),
        "uv.lock": _locked_versions().get("ruff", ""),
        ".pre-commit-config.yaml": _pre_commit_rev_for("ruff-pre-commit"),
    }


def mypy_readings() -> dict[str, str]:
    return {
        "pyproject dev group": _dev_group_pins().get("mypy", ""),
        "uv.lock": _locked_versions().get("mypy", ""),
    }


def gitleaks_readings() -> dict[str, str]:
    return {
        ".github/workflows/ci.yml": _workflow_env().get("GITLEAKS_VERSION", ""),
        "tools/security/install_gitleaks.sh": _installer_default_version(),
        ".pre-commit-config.yaml": _pre_commit_rev_for("gitleaks"),
    }


def uv_readings() -> dict[str, str]:
    return {
        "Makefile": _makefile_variable("UV_REQUIRED_VERSION"),
        ".github/workflows/ci.yml": _workflow_env().get("UV_VERSION", ""),
    }


ALL_READINGS = {
    "ruff": ruff_readings,
    "mypy": mypy_readings,
    "gitleaks": gitleaks_readings,
    "uv": uv_readings,
}


def test_every_pinned_tool_version_agrees_across_files() -> None:
    problems: list[str] = []
    for tool, reader in ALL_READINGS.items():
        problems += disagreements(tool, reader())
    assert not problems, (
        "pinned tool versions have drifted. Every file naming a gate tool's\n"
        "version must name the same one.\n\n" + "\n".join(problems)
    )


def test_the_gate_tools_are_pinned_exactly() -> None:
    """A range specifier turns a reproducible gate into a moving target."""
    specs = _dev_group_specifiers()
    loose = sorted(
        f"{tool}{specs.get(tool, ' (absent)')}"
        for tool in EXACTLY_PINNED
        if not specs.get(tool, "").startswith("==")
    )
    assert not loose, "these are gates and must be pinned with `==`, not a range: " + str(loose)


def test_the_makefile_scanner_target_defers_to_the_pinned_installer() -> None:
    """The Makefile must not become a fourth place that names a scanner version.

    `make gitleaks` expects whatever the installer pins. Asserting the target
    calls the installer is what makes "the version the Makefile target expects"
    a well-defined thing rather than a second copy to keep in step.
    """
    text = MAKEFILE.read_text(encoding="utf-8")
    target = re.search(r"^gitleaks:.*?(?=^\S|\Z)", text, re.MULTILINE | re.DOTALL)
    assert target, "Makefile no longer defines a `gitleaks` target"
    body = target.group(0)
    assert "tools/security/install_gitleaks.sh" in body, (
        "`make gitleaks` no longer invokes the pinned installer:\n" + body
    )
    assert not re.search(r"\d+\.\d+\.\d+", body), (
        "the Makefile's gitleaks target now names a version literal; it must\n"
        "defer to the installer instead:\n" + body
    )


def test_every_source_is_load_bearing() -> None:
    """Perturbing any single source must produce a disagreement.

    This is what makes the claim "it fails when either side changes alone" a
    fact rather than an intention: a comparison that quietly ignored one of its
    inputs would pass every other test in this module.
    """
    for tool, reader in ALL_READINGS.items():
        readings = reader()
        assert len(readings) >= 2, f"{tool}: only one source, nothing to compare"
        for source in readings:
            mutated = dict(readings)
            mutated[source] = "0.0.0-scratch"
            assert disagreements(tool, mutated), (
                f"changing {source} alone for {tool} was not reported as drift"
            )
