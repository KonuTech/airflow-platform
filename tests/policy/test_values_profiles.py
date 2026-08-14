"""D-06's divergence-axis rule: the two values profiles differ on four named
axes, each carrying a written argument, and on nothing else.

**Honest limit.** This compares the two committed profiles structurally, by
dotted leaf path, and therefore decides which KEYS differ between
`helm/values/local/` and `helm/values/ci/` — not whether the differing
VALUES themselves are individually correct. A CI resource request that is
merely too small for the workload to function is outside this test's scope;
`tests/policy/test_manifest_resources.py` is what proves the rendered totals
are non-zero and within budget.

Every additional divergence axis is a bug class that appears only in CI, nine
phases downstream of where it was introduced (02-CONTEXT.md D-06) — the two
profiles must be identical in every dimension except the ones this file
explicitly names and argues for.
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
LOCAL_DIR = REPO_ROOT / "helm" / "values" / "local"
CI_DIR = REPO_ROOT / "helm" / "values" / "ci"

_MISSING = object()


def _flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    """Map every leaf (scalar or list) to its dotted path.

    Lists are treated as leaves, not recursed into: every list in this
    repository's values files (bucket definitions, IAM statements,
    tolerations, hosts) is compared as a whole rather than element-by-element,
    which is exactly how a reviewer reads a diff of one of these files.
    """
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, child in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            out.update(_flatten(child, path))
        return out
    return {prefix: value}


def _load(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _stems(directory: Path) -> set[str]:
    return {p.stem for p in directory.glob("*.yaml")}


# ---------------------------------------------------------------------------
# The permitted-axis table — D-06's three named axes plus the argued fourth
# ---------------------------------------------------------------------------


def _is_replica_count(path: str) -> bool:
    return path.rsplit(".", maxsplit=1)[-1] in {"replicaCount", "replicas"}


def _is_resource_sizing(path: str) -> bool:
    segments = path.split(".")
    # Case-insensitive on the "storage.size" suffix (plan 05-01): CNPG's own
    # `cluster.storage.size` is lowercase, but the Vault chart's PVC knobs
    # are `server.dataStorage.size`/`server.auditStorage.size` — camelCase
    # compound keys ending in "Storage", not a bare "storage" segment. Both
    # are the same permitted class this axis's own written argument already
    # names ("every PVC `storage.size` may differ in magnitude without
    # differing in shape") — matching only the lowercase spelling was an
    # incomplete implementation of that argument, not a narrower axis.
    return "resources" in segments or path.lower().endswith("storage.size")


def _is_monitoring_enablement(path: str) -> bool:
    segments = path.split(".")
    return "metrics" in segments or "monitoring" in segments


def _is_executor(path: str) -> bool:
    return path == "executor"


# (name, predicate, argument) — every entry MUST carry a non-empty argument
# (D-06: "any fourth axis needs an argument"), enforced by
# test_every_permitted_axis_carries_an_argument below rather than left to
# review convention.
PermittedAxis = tuple[str, Callable[[str], bool], str]

PERMITTED_AXES: tuple[PermittedAxis, ...] = (
    (
        "replica counts",
        _is_replica_count,
        (
            "CI runs single-node kind with one replica of everything the chart "
            "starts at >1 locally; no chart in this phase currently sets a "
            "differing replicaCount, but the axis is permitted structurally "
            "per D-06 so a future one is not a review surprise."
        ),
    ),
    (
        "resource sizing",
        _is_resource_sizing,
        (
            "The CI profile is deliberately smaller because it shares a 4 CPU / "
            "16 GB GitHub-hosted runner with every other pod the CI job renders "
            "(D-06/D-12) — every `resources` key and every PVC `storage.size` "
            "may differ in magnitude without differing in shape."
        ),
    ),
    (
        "monitoring enablement",
        _is_monitoring_enablement,
        (
            "Phase 7 owns kube-prometheus-stack; it is not deployed by this "
            "phase's CI job, so a chart's own metrics/monitoring toggle "
            "(ingress-nginx's controller.metrics.enabled, CNPG's "
            "monitoring.podMonitorEnabled) may be disabled in CI even where "
            "local leaves it on."
        ),
    ),
    (
        "executor",
        _is_executor,
        (
            "The argued fourth axis (helm/values/local/airflow.yaml's header "
            "comment): the CI runner is 4 CPU / 16 GB, KubernetesExecutor costs "
            "two pods and two multi-gigabyte image pulls per task, and "
            "values-ci.yaml exists precisely because the full local stack does "
            "not fit that runner — so CI uses LocalExecutor instead."
        ),
    ),
)


def unclassified_differences(local: dict[str, Any], ci: dict[str, Any]) -> list[str]:
    """Every leaf path that differs between `local` and `ci` and matches no permitted axis."""
    flat_local = _flatten(local)
    flat_ci = _flatten(ci)
    problems: list[str] = []
    for path in sorted(set(flat_local) | set(flat_ci)):
        local_value = flat_local.get(path, _MISSING)
        ci_value = flat_ci.get(path, _MISSING)
        if local_value == ci_value:
            continue
        if any(predicate(path) for _, predicate, _ in PERMITTED_AXES):
            continue
        problems.append(f"{path}: local={local_value!r} ci={ci_value!r}")
    return problems


def missing_pairs(local_names: set[str], ci_names: set[str]) -> list[str]:
    problems: list[str] = [
        f"{name}: local values file with no ci counterpart"
        for name in sorted(local_names - ci_names)
    ]
    problems.extend(
        f"{name}: ci values file with no local counterpart"
        for name in sorted(ci_names - local_names)
    )
    return problems


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_both_profiles_exist_for_every_component() -> None:
    problems = missing_pairs(_stems(LOCAL_DIR), _stems(CI_DIR))
    assert not problems, (
        "INFRA-10's 'from the first infrastructure commit' guarantee has "
        "drifted:\n" + "\n".join(problems)
    )


def test_a_missing_ci_counterpart_is_reported() -> None:
    """Non-vacuity: deleting one CI values file must fail the pairing check.

    Modelled on an in-memory mutated set rather than an actual file deletion
    — nothing on disk is touched.
    """
    local_names = _stems(LOCAL_DIR)
    ci_names = _stems(CI_DIR)
    assert ci_names, "fixture assumption broken: no ci values files found at all"
    mutated_ci = set(ci_names)
    mutated_ci.discard(next(iter(mutated_ci)))
    assert mutated_ci != ci_names, "the scratch mutation did not apply"
    assert missing_pairs(local_names, mutated_ci), (
        "removing one name from the ci side was not reported"
    )


def test_profiles_diverge_only_on_permitted_axes() -> None:
    components = sorted(_stems(LOCAL_DIR) & _stems(CI_DIR))
    assert components, "no component has both a local and a ci values file — nothing was compared"
    problems: list[str] = []
    for component in components:
        local = _load(LOCAL_DIR / f"{component}.yaml")
        ci = _load(CI_DIR / f"{component}.yaml")
        problems.extend(
            f"{component}.yaml: {message}" for message in unclassified_differences(local, ci)
        )
    assert not problems, (
        "these leaf paths differ between the two profiles on an axis this "
        "table does not name — either the difference is a bug or D-06's "
        "table needs a new, argued entry:\n" + "\n".join(problems)
    )


def test_a_fifth_axis_is_reported() -> None:
    """Non-vacuity: an unrelated leaf-path difference must be reported."""
    local = _load(LOCAL_DIR / "cnpg-airflow.yaml")
    ci = copy.deepcopy(_load(CI_DIR / "cnpg-airflow.yaml"))
    # `existingSecretKey`-shaped unrelated key present and identical in both
    # real files today: mutate a leaf that has nothing to do with replicas,
    # resources, monitoring, or the executor.
    ci["cluster"]["initdb"]["owner"] = "a-completely-different-owner"
    assert ci != _load(CI_DIR / "cnpg-airflow.yaml"), "the scratch mutation did not apply"
    problems = unclassified_differences(local, ci)
    assert problems, "an unrelated leaf-path difference was not reported"


def test_a_permitted_axis_is_not_reported() -> None:
    """False-positive control: a difference on a replica count must NOT be reported.

    A rule that fires on the permitted divergences would be turned off rather
    than fixed, which defeats the whole point of naming them.
    """
    local = copy.deepcopy(_load(LOCAL_DIR / "ingress-nginx.yaml"))
    ci = copy.deepcopy(_load(CI_DIR / "ingress-nginx.yaml"))
    local["controller"]["replicaCount"] = 3
    ci["controller"]["replicaCount"] = 1
    assert local["controller"]["replicaCount"] != ci["controller"]["replicaCount"], (
        "the scratch mutation did not apply"
    )
    assert not unclassified_differences(local, ci), (
        "a replicaCount difference — a permitted axis — was reported"
    )


def test_every_permitted_axis_carries_an_argument() -> None:
    assert len(PERMITTED_AXES) == 4, (
        f"expected exactly four permitted axes (D-06's three plus the argued "
        f"fourth), found {len(PERMITTED_AXES)}"
    )
    for name, _predicate, argument in PERMITTED_AXES:
        has_argument = bool(argument and argument.strip())
        assert has_argument, (
            f"the {name!r} axis carries no written argument — D-06 requires "
            "one for every entry, not only the fourth"
        )
