"""SEC-01's permanent structural guard: csv-processor-db, csv-processor-s3

and airflow-minio-connection never appear as a Secret-creation target again.

D-01 (`.planning/phases/05-vault-secrets-workload-identity/05-CONTEXT.md`):
once a credential is served from Vault, its old Kubernetes Secret is deleted
or emptied, never left in place as an unused fallback. Plans 05-02 and 05-03
carried out that migration for all three of this phase's Secrets --
`csv-processor-db`/`csv-processor-s3` (05-02) and `airflow-minio-connection`
(05-03) -- and `scripts/etl-secrets.sh`, the ONLY script that ever created
any of them, was deleted outright once the third and final migration
completed (05-03-SUMMARY.md). SEC-01's claim ("Vault is the only source of
runtime credentials") was TRUE at that moment; this module is what keeps it
true going forward -- a later phase reintroducing one of these three names
as a Secret-creation target (a `kubectl ... secret ...` construction in a
script, or a `secretKeyRef.name`/`existingSecret` value in a Helm values
file) is exactly the regression this guard exists to catch.

This is a SEPARATE module from `test_workflow_secrets.py`'s own D-14
section, not an edit to it (05-05-PLAN.md's own Interfaces section): D-14
asks "does any committed file hold a credential LITERAL"; this module asks
"does any committed file still TARGET one of these three specific,
already-migrated Secret NAMES for creation" -- a different predicate, over
a scanning surface that also includes `helm/values/*/airflow.yaml`'s
`secretKeyRef.name` fields, which D-14's own checks do not target (D-14
only flags `FORBIDDEN_LITERAL_KEYS`' literal VALUES, never a
Secret-reference NAME).
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from collections.abc import Iterator

REPO_ROOT = Path(__file__).resolve().parents[2]
INFRA_SCRIPT_DIR = REPO_ROOT / "scripts"
HELM_VALUES_DIRS = (REPO_ROOT / "helm" / "values" / "local", REPO_ROOT / "helm" / "values" / "ci")

# D-01's three migration targets, all now fully Vault-served:
#   csv-processor-db / csv-processor-s3   -- plan 05-02 (etl namespace)
#   airflow-minio-connection              -- plan 05-03 (airflow namespace)
STALE_SECRET_NAMES: frozenset[str] = frozenset(
    {"csv-processor-db", "csv-processor-s3", "airflow-minio-connection"},
)


def _iter_leaves(value: Any, prefix: str = "") -> Iterator[tuple[str, Any]]:
    """Recursively walk a parsed YAML value, yielding every (dotted_path, leaf_value) pair.

    Extends `test_workflow_secrets.py`'s own `_flatten_keys` (which recurses
    into dicts only) with LIST recursion, deliberately: a Kubernetes/Helm
    `env:` block is always a list of dicts, and `secretKeyRef.name` lives
    two levels inside each list element. This is not a hypothetical shape --
    it is the EXACT one `git show 851e7e5` removed for all three
    `AIRFLOW_CONN_MINIO_DEFAULT` `secretKeyRef` blocks this plan's own
    predecessor retired (`triggerer.env[0].valueFrom.secretKeyRef.name`,
    `scheduler.env[0]...`, `workers.kubernetes.env[0]...`). A dict-only walk
    would never see a re-introduced env-list `secretKeyRef` block, silently
    defeating this guard's own purpose -- so list elements are indexed
    (`env[0]`) and recursed into exactly like dict values.

    Args:
        value: The (sub)value to walk -- a dict, a list, or a scalar leaf.
        prefix: The dotted path accumulated so far.

    Yields:
        `(path, leaf_value)` for every non-dict, non-list value reachable
        from `value`.
    """
    if isinstance(value, dict):
        for key, child in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            yield from _iter_leaves(child, path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _iter_leaves(child, f"{prefix}[{index}]")
    else:
        yield prefix, value


def _is_secret_creation_target_key(path: str) -> bool:
    """True if `path`'s leaf is `secretKeyRef.name` (any depth) or an exact `existingSecret`."""
    segments = path.split(".")
    leaf = segments[-1]
    parent = segments[-2] if len(segments) >= 2 else None
    return leaf == "existingSecret" or (leaf == "name" and parent == "secretKeyRef")


def _values_stale_secret_problems(doc: dict[str, Any], label: str) -> list[str]:
    """Report every `secretKeyRef.name` / `existingSecret` leaf naming a stale Secret."""
    problems: list[str] = []
    for path, value in _iter_leaves(doc):
        if (
            isinstance(value, str)
            and value in STALE_SECRET_NAMES
            and _is_secret_creation_target_key(path)
        ):
            problems.append(f"{label}: {path} targets stale Secret {value!r} (D-01)")
    return problems


def _script_stale_secret_problems(text: str, label: str) -> list[str]:
    """Report a stale Secret name appearing on a non-comment, non-blank script line.

    A simple substring search restricted to non-comment lines -- mirrors
    `test_workflow_secrets.py`'s own `script_literal_key_problems` comment-
    stripping discipline. `scripts/etl-secrets.sh` -- the plan's own
    originally-cited file, and the ONLY script that ever created these
    three Secrets -- was deleted outright in plan 05-03 once the third and
    final D-01 migration completed (05-03-SUMMARY.md); there is currently
    no live creation-target line anywhere under `scripts/**/*.sh`. This
    scan is forward-looking: a regression guard against the pattern
    reappearing, not a check against a file that still exists on disk.
    """
    problems: list[str] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        problems.extend(
            f"{label}:{lineno}: references stale Secret {name!r} (D-01)"
            for name in STALE_SECRET_NAMES
            if name in stripped
        )
    return problems


def _helm_values_yaml_paths() -> list[Path]:
    paths: list[Path] = []
    for directory in HELM_VALUES_DIRS:
        if directory.is_dir():
            paths.extend(sorted(directory.rglob("*.yaml")))
            paths.extend(sorted(directory.rglob("*.yml")))
    return paths


def stale_secret_problems() -> list[str]:
    """Report every remaining Secret-creation-target reference to a D-01 migrated Secret."""
    problems: list[str] = []

    if INFRA_SCRIPT_DIR.is_dir():
        for path in sorted(INFRA_SCRIPT_DIR.rglob("*.sh")):
            problems += _script_stale_secret_problems(
                path.read_text(encoding="utf-8"),
                str(path.relative_to(REPO_ROOT)),
            )

    for path in _helm_values_yaml_paths():
        label = str(path.relative_to(REPO_ROOT))
        try:
            docs = [d for d in yaml.safe_load_all(path.read_text(encoding="utf-8")) if d]
        except yaml.YAMLError:
            continue
        for doc in docs:
            if isinstance(doc, dict):
                problems += _values_stale_secret_problems(doc, label)

    return problems


# 1. The permanent guard -----------------------------------------------------


def test_no_stale_secret_name_appears_as_a_creation_target() -> None:
    """D-01's permanent regression guard: 05-05-PLAN.md Task 1.

    `csv-processor-db`, `csv-processor-s3` and `airflow-minio-connection`
    must never again appear as a Secret-creation target -- neither in a
    `scripts/**/*.sh` script nor as a `secretKeyRef.name`/`existingSecret`
    value under `helm/values/local/` or `helm/values/ci/`. All three were
    live creation targets before this phase (Phase 4) and were migrated to
    Vault by plans 05-02/05-03; this test is what keeps SEC-01's "Vault is
    the only source of runtime credentials" claim true beyond this phase.
    """
    problems = stale_secret_problems()
    assert not problems, (
        "D-01: a migrated credential's old Kubernetes Secret must never "
        "reappear as a creation target once its Vault-backed replacement "
        "is live (05-05-PLAN.md Task 1):\n" + "\n".join(problems)
    )


# 2. Non-vacuity --------------------------------------------------------------

_SCRIPT_FOR_NON_VACUITY_PROOF = INFRA_SCRIPT_DIR / "stages" / "75-etl.sh"


def test_a_stale_secret_name_in_a_script_is_reported() -> None:
    """Non-vacuity: injecting a stale name into an in-memory copy of a real script.

    `scripts/etl-secrets.sh` -- the plan's own originally-cited file -- was
    deleted in plan 05-03 (see this module's docstring), so this mutates
    `scripts/stages/75-etl.sh` instead: a real, currently-committed script
    under the exact same scanned `scripts/**/*.sh` surface, whose own
    header comment already documents (in prose only, never as a creation
    target) that it used to hand off to the now-deleted script.
    """
    path = _SCRIPT_FOR_NON_VACUITY_PROOF
    text = path.read_text(encoding="utf-8")
    injected = 'kubectl create secret generic csv-processor-db --from-literal=dsn="${DSN}"\n'
    mutated = text + injected
    assert mutated != text, "the scratch mutation did not apply -- this test proves nothing"
    label = str(path.relative_to(REPO_ROOT))
    assert _script_stale_secret_problems(mutated, label), (
        "an injected `csv-processor-db` creation-target line was not reported"
    )


def test_a_stale_secret_name_in_helm_values_is_reported() -> None:
    """Non-vacuity: injecting a stale name as a secretKeyRef.name into an in-memory copy.

    Mirrors the EXACT shape `git show 851e7e5` removed: a `secretKeyRef`
    nested two levels inside an `env:` list, not a bare top-level key --
    the shape `_iter_leaves`'s list recursion exists to still catch.
    """
    path = REPO_ROOT / "helm" / "values" / "local" / "airflow.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    mutated = copy.deepcopy(doc)
    mutated.setdefault("triggerer", {})["env"] = [
        {
            "name": "AIRFLOW_CONN_MINIO_DEFAULT",
            "valueFrom": {
                "secretKeyRef": {
                    "name": "airflow-minio-connection",
                    "key": "AIRFLOW_CONN_MINIO_DEFAULT",
                },
            },
        },
    ]
    assert mutated != doc, "the scratch mutation did not apply -- this test proves nothing"
    assert _values_stale_secret_problems(mutated, "scratch"), (
        "an injected secretKeyRef.name: airflow-minio-connection (inside an env: list) "
        "was not reported"
    )


def test_a_non_stale_name_is_not_reported() -> None:
    """False-positive control: `csv-processor` alone or `minio-app` must never be reported.

    Only the three FULL stale names are -- a merely-similar or
    substring-overlapping name must not trip either scan.
    """
    doc = {
        "someBlock": {"secretKeyRef": {"name": "csv-processor"}},
        "otherBlock": {"existingSecret": "minio-app"},
    }
    assert not _values_stale_secret_problems(doc, "scratch"), (
        "a non-stale, merely-similar Secret name was mistaken for a full stale name"
    )
    text = "kubectl get secret csv-processor -n etl\nkubectl get secret minio-app -n data\n"
    assert not _script_stale_secret_problems(text, "scratch"), (
        "a non-stale, merely-similar Secret name in a script was mistaken for a full stale name"
    )
