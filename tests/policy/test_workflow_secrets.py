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

`ALLOWED_SECRETS` was empty through Phase 1-10, deliberately. Phase 11 plan
11-01 is the anticipated trigger this comment named in advance: `publish.yml`
introduces the platform's first registry credential, `secrets.GITHUB_TOKEN`,
consumed by `docker/login-action`'s `password:` input to authenticate to
`ghcr.io`. **Adding a name to `ALLOWED_SECRETS` does not merely permit that
secret — it invalidates the structural claim above and obliges a re-audit of
SEC-10 in its general form**, because from that point on "no job echoes a
secret" becomes a review-time judgement rather than something this test can
decide alone. That re-audit, performed when `GITHUB_TOKEN` was added:

* `docker/login-action` is the only step reading `secrets.GITHUB_TOKEN`; it is
  passed as a typed `with.password` input, never interpolated into a `run:`
  shell body — the action's own contract is to authenticate `docker login`
  without echoing the credential to its own log output.
* No `run:` step anywhere in `publish.yml` references `GITHUB_TOKEN`,
  `secrets.*`, or any construct that could print it (`env_dump_problems`
  below independently checks the whole workflow for exactly that class of
  leak, over every step, not only ones referencing a secret).
* The token is the ephemeral, job-scoped one GitHub Actions injects and
  auto-revokes at job end — never a long-lived PAT — and its own scope is
  the minimum this job needs (`packages: write`, `id-token: write`; see
  `ALLOWED_PERMISSION_WIDENING` below).

A second name added later still obliges its own fresh re-audit; this one
does not grandfather anything beyond `GITHUB_TOKEN` in `publish.yml`.

## Phase 11 plan 11-02: same secret, three more jobs — a widening of
## `ALLOWED_PERMISSION_WIDENING`, not of `ALLOWED_SECRETS`

Plan 11-02 restructured `publish.yml`'s single `publish-csv-processor` job
into a 3-image matrix job renamed `publish`, added a `retag-release` job
(D-03, `release`-triggered manifest retagging), and added a wholly new
workflow file, `ghcr-cleanup.yml` (D-11, PR-close package cleanup). None of
this introduces a new secret NAME — every job below still reads only
`secrets.GITHUB_TOKEN`, already audited above — but three (workflow, job)
permission-widening pins replace/join the one plan 11-01 added:

* `("publish.yml", "publish")` replaces the old `("publish.yml",
  "publish-csv-processor")` entry (same job, renamed by the matrix
  restructure) — same exact permission set as before
  (`packages: write`, `id-token: write`, plus `contents: read`).
* `("publish.yml", "retag-release")` — `contents: read` +
  `packages: write` only. No `id-token: write`: this job never signs
  anything, it only copies an already-signed manifest reference to a new
  tag via `docker buildx imagetools create`, authenticated through the
  same `docker/login-action` `with.password` typed input as `publish`
  (never interpolated into a `run:` body).
* `("ghcr-cleanup.yml", "cleanup")` — `packages: write` only (no
  `contents` at all: this job never checks out repository code, only
  calls the GitHub Packages REST API via `gh api` and the
  `actions/delete-package-versions` action). Its one `run:` step that
  touches `secrets.GITHUB_TOKEN` does so via a typed `env: GH_TOKEN: ...`
  block (`gh`'s own documented CI authentication pattern), not
  string-interpolated into the shell body — the same non-echoing shape
  already audited for `docker/login-action` above.

Three assertions, matching SEC-10's three decidable parts:

1. every repository-secret reference is in `ALLOWED_SECRETS` (empty through
   Phase 10; `GITHUB_TOKEN` only, from Phase 11 plan 11-01);
2. every scanner invocation carries `--redact`, so a finding never reaches a log
   with its value intact (SEC-10b);
3. no run block contains an environment-dumping construct (SEC-10c).

A fourth is asserted because the first would otherwise overstate its own
strength: `GITHUB_TOKEN` is injected into every workflow whether or not anything
references `secrets.*`, so "this workflow holds no credential" is only true
while its permissions stay read-only. `ALLOWED_PERMISSION_WIDENING` is this
assertion's own equivalent of `ALLOWED_SECRETS`: empty through Phase 10, and
from Phase 11 plan 11-01 holding exactly ONE (workflow, job) pin — with the
EXACT permission set that job may hold, not a blanket exemption — for the same
reason `publish.yml` needed `GITHUB_TOKEN` allowed above (D-13's cosign
keyless signing needs `id-token: write`; pushing to GHCR needs
`packages: write`).

## D-14 widening: a second scanned surface, additive only

Everything above is the Phase 1 workflow-scoped claim. `ALLOWED_SECRETS` and
`ALLOWED_PERMISSION_WIDENING` stay Phase-1-empty AS FAR AS D-14 IS CONCERNED —
D-14 (Phase 2) adds no CI secret and widens no job's permissions, and neither
set changed for D-14's sake. Phase 11 plan 11-01 is a wholly separate,
later widening of both sets, for a wholly separate reason (D-13's image
publishing) — the paragraphs above this section document that widening on
its own terms; nothing below this line touches either set.

D-14 needs a DIFFERENT claim over a DIFFERENT surface: no credential literal
may appear anywhere under `helm/`, `kubernetes/`, `kind/` or `scripts/`.
MinIO's and Airflow's credentials are generated during `cluster-up` and live
only in the cluster (02-CONTEXT.md D-14); every committed file references one
by Secret NAME — `existingSecret`, `existingSecretKey`, `metadataSecretName`,
`fernetKeySecretName`, `webserverSecretKeySecretName`, `apiSecretKeySecretName`
— never by value. Two decidable structural checks back that claim:

1. **A forbidden literal-holding key carries a value.** `rootPassword`,
   `fernetKey` and `webserverSecretKeySecretName`'s un-suffixed sibling
   `webserverSecretKey` are the exact keys 02-RESEARCH.md names (Project
   Constraints table, Anti-Patterns) as the ones a chart accepts a literal
   value for where this repository has a reference form instead. Checked by
   EXACT leaf-key equality after flattening parsed YAML — not substring
   matching — so `fernetKeySecretName` (the permitted reference form) is
   never mistaken for `fernetKey` (the forbidden literal form) merely
   because one contains the other as a prefix.
2. **A committed `kind: Secret` document carries a `data:`/`stringData:`
   block.** Every Secret in this platform is generated at `cluster-up`
   (`scripts/minio-credentials.sh`, `scripts/airflow-metadata-secret.sh`) and
   applied via `kubectl apply -f -` on stdin — never written to a file this
   repository commits. A committed Secret manifest carrying real data is
   therefore always a violation, full stop.

`scripts/` is YAML-free, so the same two checks run there as a plain-text
regex over `key: value` / `key=value` assignments instead of a parsed-YAML
walk — forward-looking, since nothing in the current tree matches it (every
credential in every script is either a Secret NAME or the output of
`_random_hex`/a `kubectl get` read, never a literal).

## Phase 11 plan 11-04: two more names — a second, independent re-audit

`e2e-smoke.yml` (D-21) conditionally logs into Docker Hub to raise the
anonymous pull-rate limit for the upstream chart images its own ephemeral
cluster-up pulls, adding `secrets.DOCKERHUB_USERNAME` and
`secrets.DOCKERHUB_TOKEN` — the platform's first NON-GitHub-issued
credential referenced by any workflow. This obliges its own fresh re-audit,
independent of the `GITHUB_TOKEN` one above:

* Both are read exactly once, via `docker/login-action`'s typed
  `with.username`/`with.password` inputs — never interpolated into a `run:`
  shell body (the same non-echoing shape already established for
  `GITHUB_TOKEN`). `env_dump_problems` independently checks the whole
  workflow for any environment-dumping construct regardless of which secret
  it might expose.
* The login step itself is gated `if: ${{ secrets.DOCKERHUB_USERNAME != ''
  }}` — never a hard requirement. An unconfigured secret degrades to
  anonymous Docker Hub pulls, it does not fail the job (this plan's own
  `user_setup` note).
* Neither secret widens any job's `permissions:` — `e2e-smoke.yml`'s `smoke`
  job declares no `permissions:` key at all (inherits the workflow-level
  `contents: read` floor unchanged), unlike `publish.yml`'s `GITHUB_TOKEN`
  use, which needed `packages: write`/`id-token: write`. No
  `ALLOWED_PERMISSION_WIDENING` entry is needed for this workflow.
* Both are read-only Docker Hub credentials (an access token, never a
  password with write scope) — scoped to authentication for `docker pull`
  only, nothing this job ever pushes to Docker Hub.
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

# Read the module docstring before adding anything. GITHUB_TOKEN is the
# entry Phase 11 plan 11-01 added, for publish.yml's docker/login-action GHCR
# auth. DOCKERHUB_USERNAME/DOCKERHUB_TOKEN are the two Phase 11 plan 11-04
# added, for e2e-smoke.yml's own optional docker/login-action Docker Hub
# auth (D-21) — each addition's own re-audit is written into the docstring
# above.
ALLOWED_SECRETS: frozenset[str] = frozenset(
    {"GITHUB_TOKEN", "DOCKERHUB_USERNAME", "DOCKERHUB_TOKEN"},
)

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

# Read the module docstring before adding anything. Each key is an EXACT
# (workflow file name, job id) pair; each value is the EXACT permission dict
# that job is allowed to hold — not "any widening is fine once allowlisted".
# A future edit that adds a THIRD scope to an already-exempted job is still
# reported, because the comparison below is equality against this pinned
# dict, not mere presence of the key.
ALLOWED_PERMISSION_WIDENING: dict[tuple[str, str], dict[str, str]] = {
    # Phase 11 plan 11-01, renamed by plan 11-02's 3-image matrix restructure
    # (publish-csv-processor -> publish); same exact permission set.
    ("publish.yml", "publish"): {
        "contents": "read",
        "packages": "write",  # push the built image to ghcr.io
        "id-token": "write",  # cosign keyless GitHub-OIDC signing (D-13)
    },
    # Phase 11 plan 11-02 (D-03): retags an already-published, already-signed
    # SHA image with a release's semver tag — no new signing, so no
    # id-token: write.
    ("publish.yml", "retag-release"): {
        "contents": "read",
        "packages": "write",  # docker buildx imagetools create -> ghcr.io
    },
    # Phase 11 plan 11-02 (D-11): deletes a closed PR's pr-<number> GHCR
    # package versions. Never checks out repository code, so no
    # contents: read at all.
    ("ghcr-cleanup.yml", "cleanup"): {
        "packages": "write",  # list + delete package versions via gh api
    },
    # Phase 11 plan 11-05 (D-22): the merge-triggered full E2E suite files or
    # updates a tracked GitHub issue on failure, since main already has the
    # failing commit by definition. `issues: write` is scoped to exactly
    # that step (`if: failure()`) via `gh issue create`/`gh issue comment`,
    # both job-level here (no narrower per-step permissions block exists in
    # the Actions permission model).
    ("e2e-full.yml", "e2e-full"): {
        "contents": "read",
        "issues": "write",  # D-22 gh issue create/comment on failure
    },
    # Phase 11 plan 11-05 (D-22), same reasoning as e2e-full.yml above,
    # applied to the wholly separate chaos suite's own failure-notification
    # step (distinct title prefix, same mechanism).
    ("e2e-chaos.yml", "chaos"): {
        "contents": "read",
        "issues": "write",  # D-22 gh issue create/comment on failure
    },
}


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


def permission_problems(
    workflow: dict[str, Any],
    label: str = "",
    workflow_name: str = "",
) -> list[str]:
    """Report a workflow whose token is not read-only, or a job that widens it
    beyond its exact `ALLOWED_PERMISSION_WIDENING` pin (if any).
    """
    problems: list[str] = []
    top = workflow.get("permissions")
    if top != READ_ONLY:
        problems.append(f"{label}workflow permissions are {top!r}, expected {READ_ONLY!r}")
    for job_id, job in (workflow.get("jobs") or {}).items():
        job_permissions = job.get("permissions")
        if job_permissions is None or job_permissions == READ_ONLY:
            continue
        allowed = ALLOWED_PERMISSION_WIDENING.get((workflow_name, job_id))
        if job_permissions != allowed:
            problems.append(f"{label}{job_id} widens permissions to {job_permissions!r}")
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
    a job with `permissions: write-all` holds a very capable one. Widening is
    permitted only for the exact (workflow, job) pins in
    `ALLOWED_PERMISSION_WIDENING`, each holding its own EXACT permission set.
    """
    problems: list[str] = []
    for path in _workflow_paths():
        workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
        problems += permission_problems(workflow, label=f"{path.name} ", workflow_name=path.name)
    assert not problems, "the workflow token is not least-privilege:\n" + "\n".join(problems)


def test_a_widened_permission_is_reported() -> None:
    """A job outside `ALLOWED_PERMISSION_WIDENING` must still be caught."""
    workflow = yaml.safe_load((WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8"))
    mutated = copy.deepcopy(workflow)
    next(iter(mutated["jobs"].values()))["permissions"] = {"contents": "write"}
    assert permission_problems(mutated, workflow_name="ci.yml"), (
        "a job widening permissions was not reported"
    )


def test_widening_the_allowlisted_job_beyond_its_pinned_scopes_is_reported() -> None:
    """The allowlist pins an EXACT set — it is not a rubber stamp for the
    whole job. Adding a THIRD scope to an already-exempted job must still be
    reported. Exercised against all three plan-11-02 (workflow, job) pins,
    not just `publish`, so a future job-specific regression in any one of
    them is still caught.
    """
    for workflow_name, job_id in (
        ("publish.yml", "publish"),
        ("publish.yml", "retag-release"),
        ("ghcr-cleanup.yml", "cleanup"),
    ):
        workflow = yaml.safe_load((WORKFLOW_DIR / workflow_name).read_text(encoding="utf-8"))
        mutated = copy.deepcopy(workflow)
        job = mutated["jobs"][job_id]
        job["permissions"]["actions"] = "write"
        assert permission_problems(mutated, workflow_name=workflow_name), (
            f"{workflow_name} {job_id}: adding an extra permission scope beyond the "
            "allowlisted job's pinned set was not reported"
        )


# ===========================================================================
# D-14: the widened, whole-infrastructure-tree scanned surface (additive —
# see module docstring). Nothing above this line is touched by D-14.
# ===========================================================================

INFRA_YAML_DIRS = (REPO_ROOT / "helm", REPO_ROOT / "kubernetes", REPO_ROOT / "kind")
INFRA_SCRIPT_DIR = REPO_ROOT / "scripts"

# The exact keys 02-RESEARCH.md names as accepting a literal credential VALUE
# where this repository has a Secret-NAME reference alternative. Adding an
# entry here is a structural claim — verify the corresponding *SecretName /
# existingSecret* reference form actually exists in the pinned chart before
# widening this set.
FORBIDDEN_LITERAL_KEYS: frozenset[str] = frozenset(
    {"rootPassword", "fernetKey", "webserverSecretKey"},
)

SECRET_DATA_FIELDS: tuple[str, ...] = ("data", "stringData")


def _flatten_keys(value: Any, prefix: str = "") -> dict[str, Any]:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, child in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            out.update(_flatten_keys(child, path))
        return out
    return {prefix: value}


def forbidden_literal_key_problems(doc: dict[str, Any], label: str) -> list[str]:
    """Report every leaf whose KEY exactly matches a forbidden literal-holding name."""
    problems: list[str] = []
    for path, value in _flatten_keys(doc).items():
        leaf = path.rsplit(".", 1)[-1]
        if leaf in FORBIDDEN_LITERAL_KEYS and isinstance(value, str) and value.strip():
            problems.append(
                f"{label}: {path} holds a literal value — use the "
                "*SecretName / existingSecret reference form instead (D-14)",
            )
    return problems


def inline_secret_data_problems(doc: dict[str, Any], label: str) -> list[str]:
    """Report a committed `kind: Secret` document carrying real data."""
    if doc.get("kind") != "Secret":
        return []
    return [
        f"{label}: kind: Secret carries a committed `{field}:` block — "
        "every Secret in this platform is generated at cluster-up "
        "(D-14), never committed"
        for field in SECRET_DATA_FIELDS
        if doc.get(field)
    ]


_SCRIPT_LITERAL_KEY_ASSIGNMENT = re.compile(
    r"\b(?P<key>rootPassword|fernetKey|webserverSecretKey)\s*[:=]\s*"
    r"(?P<value>[\"']?[^\s\"'$`][^\n]*)",
)


def script_literal_key_problems(text: str, label: str) -> list[str]:
    """The scripts/ analogue of forbidden_literal_key_problems, over raw text.

    scripts/ is not YAML, so this is a plain-text regex rather than a parsed
    flatten — forward-looking: nothing in the current tree matches it.
    """
    problems: list[str] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        problems.extend(
            f"{label}:{lineno}: {match.group('key')} assigned a literal "
            "value — use a Secret NAME reference instead (D-14)"
            for match in _SCRIPT_LITERAL_KEY_ASSIGNMENT.finditer(stripped)
        )
    return problems


def _infra_yaml_paths() -> list[Path]:
    paths: list[Path] = []
    for directory in INFRA_YAML_DIRS:
        if directory.is_dir():
            paths.extend(sorted(directory.rglob("*.yaml")))
            paths.extend(sorted(directory.rglob("*.yml")))
    return paths


def infrastructure_credential_problems() -> list[str]:
    problems: list[str] = []
    for path in _infra_yaml_paths():
        label = str(path.relative_to(REPO_ROOT))
        try:
            docs = [d for d in yaml.safe_load_all(path.read_text(encoding="utf-8")) if d]
        except yaml.YAMLError:
            continue
        for doc in docs:
            if not isinstance(doc, dict):
                continue
            problems += forbidden_literal_key_problems(doc, label)
            problems += inline_secret_data_problems(doc, label)
    if INFRA_SCRIPT_DIR.is_dir():
        for path in sorted(INFRA_SCRIPT_DIR.rglob("*.sh")):
            problems += script_literal_key_problems(
                path.read_text(encoding="utf-8"),
                str(path.relative_to(REPO_ROOT)),
            )
    return problems


def test_no_infrastructure_file_holds_a_credential_literal() -> None:
    problems = infrastructure_credential_problems()
    assert not problems, (
        "D-14: every credential must be generated at cluster-up and "
        "referenced by Secret name only:\n" + "\n".join(problems)
    )


def test_a_literal_credential_key_is_reported() -> None:
    """Non-vacuity: injecting a forbidden key into an in-memory copy of a real file."""
    path = REPO_ROOT / "helm" / "values" / "local" / "minio.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    mutated = copy.deepcopy(doc)
    mutated["rootPassword"] = "hunter2"
    assert mutated != doc, "the scratch mutation did not apply — this test proves nothing"
    assert forbidden_literal_key_problems(mutated, "scratch"), (
        "an injected literal `rootPassword` was not reported"
    )


def test_secret_reference_keys_are_not_reported() -> None:
    """False-positive control: the permitted *SecretName / existingSecret forms."""
    doc = {
        "fernetKeySecretName": "airflow-fernet-key",
        "webserverSecretKeySecretName": "airflow-api-secret-key",
        "apiSecretKeySecretName": "airflow-api-secret-key",
        "existingSecret": "minio-root",
        "existingSecretKey": "secretKey",
        "metadataSecretName": "airflow-metadata",
    }
    assert not forbidden_literal_key_problems(doc, "scratch"), (
        "a permitted Secret-reference key was mistaken for a literal-holding one"
    )


def test_a_committed_secret_data_block_is_reported() -> None:
    doc = {"kind": "Secret", "metadata": {"name": "x"}, "data": {"password": "aHVudGVyMg=="}}
    assert inline_secret_data_problems(doc, "scratch"), (
        "a committed Secret with a data: block was not reported"
    )


def test_a_namespace_document_is_not_reported() -> None:
    """False-positive control: kubernetes/namespaces.yaml's own Namespace kind."""
    doc = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "data"}}
    assert not inline_secret_data_problems(doc, "scratch")


def test_a_literal_credential_in_a_script_is_reported() -> None:
    text = 'fernetKey="a-hardcoded-value-not-derived-from-anything"\n'
    assert script_literal_key_problems(text, "scratch"), (
        "a hardcoded fernetKey= assignment in a script was not reported"
    )


def test_a_generated_credential_in_a_script_is_not_reported() -> None:
    """False-positive control mirroring scripts/minio-credentials.sh's real shape."""
    text = '"rootPassword=$(_random_hex 32)"\n'
    assert not script_literal_key_problems(text, "scratch"), (
        "a command-substitution-derived value was mistaken for a literal"
    )


def test_the_allowed_secrets_set_is_unchanged_by_d14() -> None:
    """D-14 must not touch SEC-10's claim — see the module docstring.

    `ALLOWED_SECRETS` is pinned to exactly the set Phase 11 plans 11-01 and
    11-04 introduced (`GITHUB_TOKEN` for `publish.yml`'s GHCR auth;
    `DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN` for `e2e-smoke.yml`'s optional
    Docker Hub auth, D-21) — not empty (that was true only through Phase 10)
    and not anything wider than those three, deliberate, re-audited entries.
    D-14 (Phase 2) contributed nothing to this set either way.
    """
    baseline = frozenset({"GITHUB_TOKEN", "DOCKERHUB_USERNAME", "DOCKERHUB_TOKEN"})
    assert baseline == ALLOWED_SECRETS, (
        "ALLOWED_SECRETS no longer matches the Phase 11 plan 11-01/11-04 "
        "baseline (exactly {'GITHUB_TOKEN', 'DOCKERHUB_USERNAME', "
        "'DOCKERHUB_TOKEN'}) — see this module's docstring before widening "
        "it further; D-14 itself adds no CI secret"
    )
