"""SEC-10, stated in the form that is actually decidable.

**The general form of SEC-10 is undecidable, and pretending otherwise would be
the most dangerous thing this file could do.** "No CI job ever echoes a secret
value" cannot be checked by inspection: a future step running
`curl -H "Authorization: Bearer $TOKEN" -v` leaks the credential through verbose
output that matches no pattern, and a tool that prints its own configuration on
error can do the same without any suspicious syntax appearing in the workflow.

What *is* decidable, and what this phase actually asserts, is a **stronger
structural claim**: this workflow references no repository secret at all. If no
secret is ever interpolated, no step can echo one. That claim is checkable, it
is true today, and it stops being true the moment someone adds the first
`secrets.*` reference.

`ALLOWED_SECRETS` is therefore empty, deliberately. It is expected to grow in
Phase 11, when publishing images introduces a registry credential. **Adding a
name to it does not merely permit that secret — it invalidates the structural
claim above and obliges a re-audit of SEC-10 in its general form**, because from
that point on "no job echoes a secret" becomes a review-time judgement rather
than something this test can decide. Whoever adds the first entry owes that
re-audit; they cannot inherit this one.

Three assertions, matching SEC-10's three decidable parts:

1. every repository-secret reference is in `ALLOWED_SECRETS` (empty in Phase 1);
2. every scanner invocation carries `--redact`, so a finding never reaches a log
   with its value intact (SEC-10b);
3. no run block contains an environment-dumping construct (SEC-10c).

A fourth is asserted because the first would otherwise overstate its own
strength: `GITHUB_TOKEN` is injected into every workflow whether or not anything
references `secrets.*`, so "this workflow holds no credential" is only true
while its permissions stay read-only.
"""

from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"
MAKEFILE = REPO_ROOT / "Makefile"

# Empty on purpose. Read the module docstring before adding anything.
ALLOWED_SECRETS: frozenset[str] = frozenset()

SECRET_REFERENCE = re.compile(r"secrets\.([A-Za-z_][A-Za-z0-9_]*)")

# Constructs that dump the environment — and therefore any secret in it — into
# the job log. Assembled as a table so a failure names the offending construct.
ENV_DUMPS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("shell tracing (set -x)", re.compile(r"(?:^|[;&|]\s*)set\s+(?:-\w*x|-o\s+xtrace)")),
    ("bare environment print", re.compile(r"(?:^|[;&|]\s*)env\s*(?:$|[;&|>])")),
    ("printenv", re.compile(r"(?:^|[;&|]\s*)printenv\b")),
    ("declare -p", re.compile(r"(?:^|[;&|]\s*)declare\s+-p\b")),
    ("export -p", re.compile(r"(?:^|[;&|]\s*)export\s+-p\b")),
)

REDACT_FLAG = "--redact"
SCANNER_INVOCATION = re.compile(r"^\s*\.?/?tools/bin/gitleaks\s+(?P<args>.*)$", re.MULTILINE)

READ_ONLY = {"contents": "read"}


def _workflow_paths() -> list[Path]:
    return sorted(p for p in WORKFLOW_DIR.glob("*.y*ml"))


def _run_blocks(workflow: dict[str, Any]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for job_id, job in (workflow.get("jobs") or {}).items():
        for step in job.get("steps") or []:
            run = step.get("run")
            if run:
                out.append((job_id, run))
    return out


def secret_reference_problems(text: str, label: str = "") -> list[str]:
    """Report every repository-secret reference outside the allowed set."""
    problems: list[str] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        for match in SECRET_REFERENCE.finditer(line):
            name = match.group(1)
            if name not in ALLOWED_SECRETS:
                problems.append(f"{label}line {lineno}: references secrets.{name}")
    return problems


def env_dump_problems(workflow: dict[str, Any], label: str = "") -> list[str]:
    """Report every run block containing an environment-dumping construct."""
    problems: list[str] = []
    for job_id, run in _run_blocks(workflow):
        for lineno, line in enumerate(run.splitlines(), start=1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            for description, pattern in ENV_DUMPS:
                if pattern.search(stripped):
                    problems.append(f"{label}{job_id} run line {lineno}: {description}: {stripped}")
    return problems


def unredacted_scanner_invocations(makefile_text: str) -> list[str]:
    """Report every scanner invocation that could print a finding in the clear."""
    return [
        match.group(0).strip()
        for match in SCANNER_INVOCATION.finditer(makefile_text)
        if REDACT_FLAG not in match.group("args")
    ]


def permission_problems(workflow: dict[str, Any], label: str = "") -> list[str]:
    """Report a workflow whose token is not read-only, or a job that widens it."""
    problems: list[str] = []
    top = workflow.get("permissions")
    if top != READ_ONLY:
        problems.append(f"{label}workflow permissions are {top!r}, expected {READ_ONLY!r}")
    for job_id, job in (workflow.get("jobs") or {}).items():
        if "permissions" in job and job["permissions"] != READ_ONLY:
            problems.append(f"{label}{job_id} widens permissions to {job['permissions']!r}")
    return problems


# 1. The empty secret set --------------------------------------------------


def test_no_workflow_references_a_repository_secret() -> None:
    problems: list[str] = []
    for path in _workflow_paths():
        problems += secret_reference_problems(path.read_text(encoding="utf-8"), f"{path.name} ")
    assert not problems, (
        "This phase's SEC-10 claim is structural: the workflow references no\n"
        "repository secret at all. A new reference invalidates that claim and\n"
        "obliges a re-audit of SEC-10 in its general form — see this module's\n"
        "docstring before adding a name to ALLOWED_SECRETS.\n\n" + "\n".join(problems)
    )


def test_a_new_secret_reference_is_reported() -> None:
    text = (WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8")
    injected = "env:\n      TOKEN: ${{ secrets.NPM_TOKEN }}"
    mutated = text.replace("runs-on: ubuntu-latest", injected, 1)
    assert mutated != text, "the scratch mutation did not apply — this test proves nothing"
    assert secret_reference_problems(mutated), "a new secrets.* reference was not reported"


# 2. Redaction -------------------------------------------------------------


def test_every_scanner_invocation_redacts() -> None:
    text = MAKEFILE.read_text(encoding="utf-8")
    invocations = SCANNER_INVOCATION.findall(text)
    assert invocations, "no scanner invocation found in the Makefile — SEC-10b is unenforced"
    unredacted = unredacted_scanner_invocations(text)
    assert not unredacted, (
        f"a finding would reach the job log with its value intact. Add {REDACT_FLAG}:\n"
        + "\n".join(unredacted)
    )


def test_dropping_redaction_is_reported() -> None:
    text = MAKEFILE.read_text(encoding="utf-8").replace(f" {REDACT_FLAG}", "", 1)
    assert unredacted_scanner_invocations(text), (
        f"a scanner invocation without {REDACT_FLAG} was not reported"
    )


# 3. No environment dumping ------------------------------------------------


def test_no_run_block_dumps_the_environment() -> None:
    problems: list[str] = []
    for path in _workflow_paths():
        workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
        problems += env_dump_problems(workflow, label=f"{path.name} ")
    assert not problems, "a run block would print the environment into the job log:\n" + str(
        problems,
    )


def test_an_environment_dump_is_reported() -> None:
    """Every construct in the table must actually be caught."""
    workflow = yaml.safe_load((WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8"))
    for injected in ("set -x", "set -o xtrace", "env", "printenv", "declare -p", "export -p"):
        mutated = copy.deepcopy(workflow)
        job = next(iter(mutated["jobs"].values()))
        job["steps"].append({"run": f"{injected}\nmake check"})
        assert env_dump_problems(mutated), f"a run block containing `{injected}` was not reported"


def test_ordinary_commands_are_not_mistaken_for_dumps() -> None:
    """A pattern that fires on `make install` would be turned off, not fixed."""
    workflow = yaml.safe_load((WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8"))
    mutated = copy.deepcopy(workflow)
    job = next(iter(mutated["jobs"].values()))
    job["steps"] = [
        {"run": "make install"},
        {"run": "env_file=.env.example make check"},
        {"run": "set -euo pipefail\nmake ci"},
    ]
    mutated["jobs"] = {"only": job}
    assert not env_dump_problems(mutated), "an ordinary command was misreported as an env dump"


# 4. The token that is always present --------------------------------------


def test_the_workflow_token_stays_read_only() -> None:
    """`GITHUB_TOKEN` is injected whether or not anything references secrets.*.

    Without this, "the workflow holds no credential" would be an overstatement:
    a job with `permissions: write-all` holds a very capable one.
    """
    problems: list[str] = []
    for path in _workflow_paths():
        workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
        problems += permission_problems(workflow, label=f"{path.name} ")
    assert not problems, "the workflow token is not least-privilege:\n" + "\n".join(problems)


def test_a_widened_permission_is_reported() -> None:
    workflow = yaml.safe_load((WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8"))
    mutated = copy.deepcopy(workflow)
    next(iter(mutated["jobs"].values()))["permissions"] = {"contents": "write"}
    assert permission_problems(mutated), "a job widening permissions was not reported"
