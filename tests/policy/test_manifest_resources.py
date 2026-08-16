"""D-12's two sizing claims, held by a test rather than asserted in prose.

**Honest limit.** This sums container *requests* over the *rendered* manifests
for a fixed values profile. It therefore bounds what the Kubernetes scheduler
will be ASKED for, not what the workloads will actually consume at runtime —
and it bounds the two Helm values profiles committed today, not every values
file that could ever be written. It also special-cases the CloudNativePG
`Cluster` custom resource: without that special case the two heaviest
workloads in this platform (both PostgreSQL clusters) contribute nothing to
the sum, which is exactly the bug 02-RESEARCH.md's own prototype produced
(Pitfall 6: a naive walker summed 0.350 cores across four charts with both
databases invisible).

## Why this module is split across the offline/manifests boundary

Two tests here — `test_ci_profile_fits_runner` and
`test_every_container_is_sized` — read `build/manifests/{local,ci}/`, the
gitignored output of `make manifests`. They carry `@pytest.mark.manifests`
(registered in plan 02-01) so `make policy` deselects them (`-m "not
manifests"`) and `make manifest-policy` selects them (`-m manifests`) AFTER
its `manifests` prerequisite has rendered the input they read — a Make
prerequisite EDGE, not a position in a list, which is what still orders the
render ahead of the read under `make -j`.

The quantity parser's own unit assertions, the container-walker's
unrecognised-kind guard, and `test_this_module_runs_after_the_render` need no
rendered input at all — they stay UNMARKED so they keep running in the
offline `make check` path, mirroring
`tests/policy/test_manifest_validation_fails_closed.py`'s docstring
convention.

## The anti-vacuity switch

A skip-when-absent module living in `tests/policy/` is collected by `make
policy`... except this one isn't (see above) — but a developer invoking
`pytest tests/policy -m manifests` directly, without `make manifest-policy`
around it, would see the two rendered-output tests silently skip if
`build/manifests/` is absent. `REQUIRE_RENDERED_MANIFESTS=1` (set by `make
manifest-policy`'s own recipe) turns that skip into a hard failure naming the
missing directory instead — so the one path that is actually a *gate* can
never report green by skipping the very inputs it exists to sum.
"""

from __future__ import annotations

import copy
import os
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from tests.policy.test_ci_calls_make_ci import chain, parse_prerequisites

REPO_ROOT = Path(__file__).resolve().parents[2]
MAKEFILE = REPO_ROOT / "Makefile"
LOCAL_BUILD_DIR = REPO_ROOT / "build" / "manifests" / "local"
CI_BUILD_DIR = REPO_ROOT / "build" / "manifests" / "ci"

# ---------------------------------------------------------------------------
# A Kubernetes quantity parser covering every shape a real chart default uses
# ---------------------------------------------------------------------------
# 02-RESEARCH.md § Don't Hand-Roll: "2", "500m", "90Mi" and "1e3" all appear
# in real chart defaults in this repository's own pinned charts, and a parser
# that merely strips a trailing "m" silently under-counts every binary-SI or
# exponent-form quantity it meets.

SUFFIX: dict[str, float] = {
    "": 1.0,
    "m": 1e-3,
    "k": 1e3,
    "M": 1e6,
    "G": 1e9,
    "T": 1e12,
    "P": 1e15,
    "E": 1e18,
    "Ki": 2**10,
    "Mi": 2**20,
    "Gi": 2**30,
    "Ti": 2**40,
    "Pi": 2**50,
    "Ei": 2**60,
}
# Longest suffix first (Ki before K, which does not exist as a k8s suffix at
# all — only lowercase "k" is decimal-SI kilo) so the regex cannot swallow
# "Ki" as a bare "" match followed by garbage.
_SUFFIX_ALTERNATION = "|".join(
    re.escape(suffix) for suffix in sorted((s for s in SUFFIX if s), key=len, reverse=True)
)
QUANTITY = re.compile(
    rf"^(?P<num>[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)(?P<suf>{_SUFFIX_ALTERNATION})?$",
)


def parse_quantity(value: Any) -> float:
    """Parse a Kubernetes resource quantity into a float in base units (cores, bytes)."""
    if value is None:
        return 0.0
    text = str(value).strip()
    if not text:
        return 0.0
    match = QUANTITY.fullmatch(text)
    if not match:
        message = f"cannot parse Kubernetes quantity {value!r}"
        raise ValueError(message)
    return float(match.group("num")) * SUFFIX[match.group("suf") or ""]


# ---------------------------------------------------------------------------
# The container walker
# ---------------------------------------------------------------------------

POD_TEMPLATE_KINDS = frozenset({"Deployment", "StatefulSet", "DaemonSet", "Job", "ReplicaSet"})

# Kinds that legitimately carry no container — reject-listing every Kubernetes
# kind that is not one of these (and not a Pod-template kind, CronJob, Pod, or
# the CNPG `Cluster` / kube-prometheus-stack `Prometheus`/`Alertmanager`
# special cases below) is the fail-closed half of Pitfall 6: a workload kind
# this repository's charts start emitting tomorrow and this set has never
# seen must be a named failure, not a silent zero.
NO_CONTAINER_KINDS = frozenset(
    {
        "Cluster",  # special-cased separately — see cluster_requests() below
        # plan 07-07: kube-prometheus-stack's own CRD kinds. `PrometheusRule`
        # and `ServiceMonitor` genuinely never carry a container (pure
        # config objects the Operator reconciles). `Prometheus`/
        # `Alertmanager` DO have a real, non-container-shaped resource
        # footprint (the Operator turns `spec.resources`/`spec.replicas`
        # into an actual StatefulSet's Pod template at runtime, invisible to
        # `helm template`) — special-cased in `custom_resource_requests()`
        # below, the same Pitfall-6-avoiding treatment `cluster_requests()`
        # already gives CNPG's `Cluster` kind, so listing them here alone
        # would silently zero them out of every budget sum instead.
        "PrometheusRule",
        "ServiceMonitor",
        "Prometheus",  # special-cased separately — see custom_resource_requests() below
        "Alertmanager",  # special-cased separately — see custom_resource_requests() below
        "APIService",
        "ClusterRole",
        "ClusterRoleBinding",
        "ConfigMap",
        "CustomResourceDefinition",
        "Endpoints",
        "EndpointSlice",
        "Ingress",
        "IngressClass",
        "MutatingWebhookConfiguration",
        "Namespace",
        "NetworkPolicy",
        "PersistentVolumeClaim",
        "PodDisruptionBudget",
        "PriorityClass",
        "Role",
        "RoleBinding",
        "Secret",
        "Service",
        "ServiceAccount",
        "StorageClass",
        "ValidatingWebhookConfiguration",
    },
)

# plan 07-07: kube-prometheus-stack CRD kinds whose `spec.replicas` field
# multiplies `spec.resources.requests` the same way CNPG's `Cluster.spec.
# instances` does (cluster_requests() below) — a real Prometheus/Alertmanager
# StatefulSet is created by the Operator at runtime with exactly this many
# replicas, each requesting `spec.resources.requests`.
CUSTOM_RESOURCE_REPLICA_FIELD: dict[str, str] = {
    "Prometheus": "replicas",
    "Alertmanager": "replicas",
}


def is_test_hook(doc: dict[str, Any]) -> bool:
    """Helm test-hook resources (02-RESEARCH.md Pitfall 9) are not deployed."""
    annotations = (doc.get("metadata") or {}).get("annotations") or {}
    return "test" in annotations.get("helm.sh/hook", "")


def containers(doc: dict[str, Any]) -> list[dict[str, Any]]:
    """Every init and ordinary container in a rendered document's Pod template.

    Raises ValueError naming the kind for anything not in POD_TEMPLATE_KINDS,
    CronJob, Pod, or NO_CONTAINER_KINDS — Pitfall 6's fail-closed rule: a
    budget test that quietly ignores an unrecognised workload is worse than no
    test at all.
    """
    kind = doc.get("kind")
    if kind in POD_TEMPLATE_KINDS:
        spec = doc["spec"]["template"]["spec"]
    elif kind == "CronJob":
        spec = doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    elif kind == "Pod":
        spec = doc["spec"]
    elif kind in NO_CONTAINER_KINDS:
        return []
    else:
        message = (
            f"unrecognised kind {kind!r} — add it to POD_TEMPLATE_KINDS, "
            "NO_CONTAINER_KINDS, or special-case it (like Cluster) before "
            "trusting this budget; a silent zero is worse than a failure"
        )
        raise ValueError(message)
    return list(spec.get("initContainers") or []) + list(spec.get("containers") or [])


def cluster_requests(doc: dict[str, Any]) -> tuple[float, float]:
    """CNPG `Cluster` CRs are not Pod templates (Pitfall 6).

    Requests live at `spec.resources.requests` and must be multiplied by
    `spec.instances` — a 3-instance Cluster with 250m/512Mi per instance asks
    the scheduler for 750m/1536Mi in aggregate, not 250m/512Mi.
    """
    spec = doc.get("spec") or {}
    instances = spec.get("instances", 1)
    requests = ((spec.get("resources") or {}).get("requests")) or {}
    cpu = parse_quantity(requests.get("cpu")) * instances
    memory = parse_quantity(requests.get("memory")) * instances
    return cpu, memory


def custom_resource_requests(doc: dict[str, Any]) -> tuple[float, float]:
    """kube-prometheus-stack `Prometheus`/`Alertmanager` CRs are not Pod templates (Pitfall 6).

    Same shape as `cluster_requests()` above, generalised to whichever
    `CUSTOM_RESOURCE_REPLICA_FIELD` entry names this doc's own replica-count
    field (`spec.replicas`, not CNPG's `spec.instances`): the Operator turns
    `spec.resources.requests` times `spec.replicas` into an actual
    StatefulSet at runtime, invisible to `helm template` — a silent zero
    here would repeat exactly the bug this project's own Pitfall 6 already
    names for CNPG.
    """
    kind = doc.get("kind", "")
    replica_field = CUSTOM_RESOURCE_REPLICA_FIELD[kind]
    spec = doc.get("spec") or {}
    replicas = spec.get(replica_field, 1)
    requests = ((spec.get("resources") or {}).get("requests")) or {}
    cpu = parse_quantity(requests.get("cpu")) * replicas
    memory = parse_quantity(requests.get("memory")) * replicas
    return cpu, memory


def _unwrap_lists(doc: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten a `kind: List` meta-document into its own real `items`, recursively.

    plan 07-07: kube-prometheus-stack's own `additionalServiceMonitors`
    template (`templates/prometheus/servicemonitors.yaml`) wraps its
    variable-length `{{- range }}` output in one `apiVersion: v1, kind:
    List, items: [...]` meta-document rather than emitting one `---`-
    separated document per item — verified via `helm template` this
    session. A `List` is not itself a workload and must never be treated as
    a single zero-container document (that would be exactly Pitfall 6's
    "silent zero" failure mode if a future List ever wraps something with a
    real container) — each of its `items` is unwrapped and walked as its
    own independent document instead, the same way it would be if the
    chart had used `---` separators. Recursive: nothing in Kubernetes
    forbids a `List` of `List`s, however unlikely.
    """
    if doc.get("kind") != "List":
        return [doc]
    flattened: list[dict[str, Any]] = []
    for item in doc.get("items") or []:
        flattened.extend(_unwrap_lists(item))
    return flattened


def load_documents(paths: list[Path]) -> list[tuple[str, dict[str, Any]]]:
    """(label, doc) pairs for every non-null, non-test-hook document across `paths`."""
    out: list[tuple[str, dict[str, Any]]] = []
    for path in paths:
        label = str(path.relative_to(REPO_ROOT))
        for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")):
            # `if not doc` is load-bearing: a `---` separator followed only by
            # comments parses to None, not {} (02-RESEARCH.md skeleton).
            if not doc or is_test_hook(doc):
                continue
            for unwrapped in _unwrap_lists(doc):
                if not unwrapped or is_test_hook(unwrapped):
                    continue
                out.append((label, unwrapped))
    return out


def request_totals(labeled_docs: list[tuple[str, dict[str, Any]]]) -> tuple[float, float]:
    """Sum CPU cores and memory bytes requested across every container, Cluster and CR."""
    cpu_total = 0.0
    memory_total = 0.0
    for _, doc in labeled_docs:
        if doc.get("kind") == "Cluster":
            cpu, memory = cluster_requests(doc)
            cpu_total += cpu
            memory_total += memory
            continue
        if doc.get("kind") in CUSTOM_RESOURCE_REPLICA_FIELD:
            cpu, memory = custom_resource_requests(doc)
            cpu_total += cpu
            memory_total += memory
            continue
        for container in containers(doc):
            requests = (container.get("resources") or {}).get("requests") or {}
            cpu_total += parse_quantity(requests.get("cpu"))
            memory_total += parse_quantity(requests.get("memory"))
    return cpu_total, memory_total


_SIZING_CHECKS: tuple[tuple[str, str, str], ...] = (
    ("requests", "cpu", "a CPU request"),
    ("requests", "memory", "a memory request"),
    ("limits", "cpu", "a CPU limit"),
    ("limits", "memory", "a memory limit"),
)


def unsized_containers(labeled_docs: list[tuple[str, dict[str, Any]]]) -> list[str]:
    """`<file>: <kind>/<name>: <container>: missing <field>` for every unsized container.

    An unrequested-and-unlimited container is QoS BestEffort and is evicted
    first — exactly when its data is needed to explain the incident
    (02-RESEARCH.md Pitfall 5, generalised past the Airflow chart to every
    chart in this phase).
    """
    problems: list[str] = []
    for label, doc in labeled_docs:
        if doc.get("kind") == "Cluster":
            continue  # sized separately by cluster_requests(); not a container
        if doc.get("kind") in CUSTOM_RESOURCE_REPLICA_FIELD:
            continue  # sized separately by custom_resource_requests(); not a container
        kind = doc.get("kind", "<unknown>")
        name = (doc.get("metadata") or {}).get("name", "<unnamed>")
        for container in containers(doc):
            resources = container.get("resources") or {}
            container_name = container.get("name", "<unnamed>")
            for section, field, description in _SIZING_CHECKS:
                bucket = resources.get(section) or {}
                if field not in bucket:
                    problems.append(
                        f"{label}: {kind}/{name}: {container_name}: missing {description}",
                    )
    return problems


# ---------------------------------------------------------------------------
# The CI runner budget (D-12 / test_ci_profile_fits_runner)
# ---------------------------------------------------------------------------
# GitHub-hosted "ubuntu-latest" standard runners: 4 vCPU / 16 GB RAM (CLAUDE.md
# "CI runner sizing" constraint; ROADMAP success criterion 5).
CI_CPU_BUDGET_CORES = 4.0
CI_MEMORY_BUDGET_BYTES = 16 * 2**30

# The runner also hosts the OS, the kind control-plane's own system pods,
# kube-proxy, CNI, and the Actions runner agent itself — none of which are
# rendered by this repository's own charts and therefore never appear in the
# sum above. A flat 20% headroom keeps the budget honest about what it
# bounds: what THIS REPOSITORY's workloads ask the scheduler for, not the
# whole machine.
CI_HEADROOM_FRACTION = 0.20
EFFECTIVE_CI_CPU_BUDGET = CI_CPU_BUDGET_CORES * (1 - CI_HEADROOM_FRACTION)
EFFECTIVE_CI_MEMORY_BUDGET = CI_MEMORY_BUDGET_BYTES * (1 - CI_HEADROOM_FRACTION)


def _require_rendered(build_dir: Path) -> None:
    """Skip (interactive) or fail (gated) when `build_dir` has not been rendered.

    See the module docstring's "anti-vacuity switch" section. Mirrors
    `test_manifest_validation_fails_closed.py`'s identical pattern.
    """
    if build_dir.is_dir() and any(build_dir.glob("*.yaml")):
        return
    if os.environ.get("REQUIRE_RENDERED_MANIFESTS"):
        pytest.fail(
            f"{build_dir} has no rendered manifests, and "
            "REQUIRE_RENDERED_MANIFESTS=1 forbids silently skipping this "
            "input. Run `make manifests` first (or `make manifest-policy`, "
            "which orders the render ahead of this test).",
        )
    pytest.skip(f"{build_dir} not rendered — run `make manifests` first")


@pytest.mark.manifests
def test_ci_profile_fits_runner() -> None:
    _require_rendered(CI_BUILD_DIR)
    docs = load_documents(sorted(CI_BUILD_DIR.glob("*.yaml")))
    assert docs, f"{CI_BUILD_DIR} rendered no documents — nothing to sum"

    cluster_docs = [doc for _, doc in docs if doc.get("kind") == "Cluster"]
    assert len(cluster_docs) >= 2, (
        f"expected both CNPG Cluster CRs (Airflow metadata + analytical) in "
        f"the rendered CI profile, found {len(cluster_docs)} — Pitfall 6 "
        "would make the sum below meaningless"
    )
    for doc in cluster_docs:
        cpu, memory = cluster_requests(doc)
        name = (doc.get("metadata") or {}).get("name", "<unnamed>")
        zero_contribution_message = (
            f"Cluster {name} contributed zero to the sizing sum — Pitfall 6's "
            "exact failure mode: a CNPG Cluster's requests are invisible to a "
            "walker that only reads Pod templates"
        )
        assert cpu > 0, zero_contribution_message
        assert memory > 0, zero_contribution_message

    cpu_total, memory_total = request_totals(docs)
    assert cpu_total > 0, "the CI profile's summed CPU total is zero"
    assert memory_total > 0, "the CI profile's summed memory total is zero"

    assert cpu_total <= EFFECTIVE_CI_CPU_BUDGET, (
        f"the CI profile requests {cpu_total:.3f} cores, over the "
        f"{EFFECTIVE_CI_CPU_BUDGET:.3f}-core effective budget "
        f"({CI_CPU_BUDGET_CORES} cores less {CI_HEADROOM_FRACTION:.0%} "
        "headroom) — trim a values file under helm/values/ci/"
    )
    assert memory_total <= EFFECTIVE_CI_MEMORY_BUDGET, (
        f"the CI profile requests {memory_total / 2**20:.0f}Mi, over the "
        f"{EFFECTIVE_CI_MEMORY_BUDGET / 2**20:.0f}Mi effective budget "
        f"({CI_MEMORY_BUDGET_BYTES / 2**30:.0f}Gi less "
        f"{CI_HEADROOM_FRACTION:.0%} headroom) — trim a values file under "
        "helm/values/ci/"
    )


@pytest.mark.manifests
def test_inflating_a_request_past_budget_is_reported() -> None:
    """Non-vacuity: the budget assertion must actually fire on a real overage."""
    _require_rendered(CI_BUILD_DIR)
    docs = load_documents(sorted(CI_BUILD_DIR.glob("*.yaml")))
    mutated = copy.deepcopy(docs)
    target_label, target_doc = next((label, doc) for label, doc in mutated if containers(doc))
    target_container = containers(target_doc)[0]
    before = copy.deepcopy(target_container)
    target_container.setdefault("resources", {}).setdefault("requests", {})["cpu"] = "1000"
    assert target_container != before, (
        "the scratch mutation did not apply — this test proves nothing"
    )

    cpu_total, _ = request_totals(mutated)
    assert cpu_total > EFFECTIVE_CI_CPU_BUDGET, (
        f"inflating {target_label}'s first container's CPU request to 1000 "
        "cores did not push the total over budget — the sum is not reading "
        "the mutation"
    )


@pytest.mark.manifests
def test_every_container_is_sized() -> None:
    """D-12 test 2: every container in BOTH profiles carries requests and limits.

    The real rendered output producing no messages here is this test's own
    false-positive control, paired with the mutation-based non-vacuity proof
    below.
    """
    for build_dir in (LOCAL_BUILD_DIR, CI_BUILD_DIR):
        _require_rendered(build_dir)
    docs = load_documents(
        sorted(LOCAL_BUILD_DIR.glob("*.yaml")) + sorted(CI_BUILD_DIR.glob("*.yaml")),
    )
    problems = unsized_containers(docs)
    assert not problems, "unsized containers found:\n" + "\n".join(problems)


@pytest.mark.manifests
def test_stripping_resources_is_reported() -> None:
    """Non-vacuity: a container with no `resources` block must be reported."""
    _require_rendered(CI_BUILD_DIR)
    docs = load_documents(sorted(CI_BUILD_DIR.glob("*.yaml")))
    mutated = copy.deepcopy(docs)
    target_label, target_doc = next((label, doc) for label, doc in mutated if containers(doc))
    target_container = containers(target_doc)[0]
    before = copy.deepcopy(target_container)
    target_container.pop("resources", None)
    assert target_container != before, (
        "the scratch mutation did not apply — this test proves nothing"
    )

    problems = unsized_containers(mutated)
    assert problems, f"stripping {target_label}'s first container's resources was not reported"


@pytest.mark.manifests
def test_an_unrecognised_cluster_scoped_kind_is_reported() -> None:
    """Non-vacuity for Pitfall 6's own fail-closed rule.

    Injecting a brand-new custom-resource kind into an in-memory document must
    fail loudly, naming the kind — not silently contribute a zero the way the
    02-RESEARCH.md prototype did before this fix.
    """
    _require_rendered(CI_BUILD_DIR)
    docs = load_documents(sorted(CI_BUILD_DIR.glob("*.yaml")))
    mutated = copy.deepcopy(docs)
    _, target_doc = mutated[0]
    before_kind = target_doc.get("kind")
    target_doc["kind"] = "TotallyUnrecognisedWorkloadKind"
    assert target_doc.get("kind") != before_kind, "the scratch mutation did not apply"

    with pytest.raises(ValueError, match="TotallyUnrecognisedWorkloadKind"):
        containers(target_doc)


# ---------------------------------------------------------------------------
# The quantity parser — pure, unmarked, offline (no rendered input needed)
# ---------------------------------------------------------------------------


def test_quantity_parser_round_trips_known_shapes() -> None:
    cases: dict[str, float] = {
        "2": 2.0,
        "0": 0.0,
        "0.5": 0.5,
        "500m": 0.5,
        "100m": 0.1,
        "90Mi": 90 * 2**20,
        "1Gi": 2**30,
        "2Gi": 2 * 2**30,
        "1e3": 1000.0,
        "1.5e2": 150.0,
        "1k": 1000.0,
    }
    for text, expected in cases.items():
        assert parse_quantity(text) == pytest.approx(expected), text


def test_quantity_parser_handles_none_and_absence() -> None:
    assert parse_quantity(None) == 0.0
    assert parse_quantity("") == 0.0


def test_quantity_parser_rejects_garbage() -> None:
    with pytest.raises(ValueError, match="cannot parse"):
        parse_quantity("not-a-quantity")


def test_an_unrecognised_pod_less_kind_is_reported_by_the_walker() -> None:
    """A pure unit-level pairing of the mutation test above — no rendered input."""
    fake_doc = {"kind": "SomeFutureCustomResource", "metadata": {"name": "x"}, "spec": {}}
    with pytest.raises(ValueError, match="SomeFutureCustomResource"):
        containers(fake_doc)


def test_known_non_container_kinds_are_not_reported() -> None:
    """False-positive control: legitimate container-less kinds must return []."""
    for kind in sorted(NO_CONTAINER_KINDS - {"Cluster"}):
        assert containers({"kind": kind, "metadata": {"name": "x"}}) == []


# ---------------------------------------------------------------------------
# The render-before-read ordering itself (unmarked — a static Makefile read)
# ---------------------------------------------------------------------------


def render_ordering_problems(graph: dict[str, list[str]]) -> list[str]:
    problems: list[str] = []
    if "manifests" not in graph.get("manifest-policy", []):
        problems.append("`manifest-policy` no longer declares `manifests` as a prerequisite")
    if "manifest-policy" not in chain(graph, "ci"):
        problems.append("`manifest-policy` is no longer reachable from `ci`")
    return problems


def test_this_module_runs_after_the_render() -> None:
    text = MAKEFILE.read_text(encoding="utf-8")
    graph = parse_prerequisites(text)
    problems = render_ordering_problems(graph)
    assert not problems, "\n".join(problems)

    policy_target = re.search(r"^policy:.*?(?=^\S|\Z)", text, re.MULTILINE | re.DOTALL)
    assert policy_target, "Makefile no longer defines a `policy` target"
    assert "not manifests" in policy_target.group(0), (
        "`policy` no longer deselects the manifests marker — this module's "
        "rendered-output tests would be collected by the offline gate"
    )


def test_removing_the_render_prerequisite_is_reported() -> None:
    parsed = parse_prerequisites(MAKEFILE.read_text(encoding="utf-8"))
    graph = {target: list(prereqs) for target, prereqs in parsed.items()}
    before = list(graph.get("manifest-policy", []))
    graph["manifest-policy"] = [p for p in graph.get("manifest-policy", []) if p != "manifests"]
    assert graph["manifest-policy"] != before, "the scratch mutation did not apply"
    assert render_ordering_problems(graph), (
        "removing the `manifest-policy: manifests` prerequisite edge was not reported"
    )
