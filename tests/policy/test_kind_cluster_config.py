"""INFRA-01 / INFRA-09, stated in the form that is actually decidable.

This file proves that `kind/cluster.yaml`'s DECLARATIONS are present and
internally consistent: 3 node entries with one control-plane and two
workers, every node pinning the digest-pinned image from
`helm/versions.env`, every node carrying a `KubeletConfiguration` patch that
sets `systemReserved`, `kubeReserved`, `evictionHard` and `maxPods`, both
`extraMounts` on every node with a host path that never starts with `/mnt/`,
`extraPortMappings` for 80 and 443 on the node labelled `ingress-ready`, and
a top-level `containerdConfigPatches` setting containerd's `config_path`.

**It does not prove the reservation NUMBERS are right for any given host.**
`tests/e2e/cluster/test_node_capacity.py` is what proves that, against the
live node — this file cannot see whether 3 CPU/6GiB per node is enough for
the workloads later phases run, or whether the summed allocatable across
nodes actually fits the host it runs on. Those are live-cluster facts, not
YAML facts.
"""

from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CLUSTER_YAML = REPO_ROOT / "kind" / "cluster.yaml"
VERSIONS_ENV = REPO_ROOT / "helm" / "versions.env"

REQUIRED_KUBELET_KEYS = ("systemReserved", "kubeReserved", "evictionHard", "maxPods")


def _versions_env_variable(name: str) -> str:
    text = VERSIONS_ENV.read_text(encoding="utf-8")
    for line in text.splitlines():
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].strip()
    msg = f"helm/versions.env does not define {name}"
    raise AssertionError(msg)


def _kubelet_config(node: dict[str, Any]) -> dict[str, Any] | None:
    for patch in node.get("kubeadmConfigPatches") or []:
        if "KubeletConfiguration" in patch:
            parsed: dict[str, Any] = yaml.safe_load(patch)
            return parsed
    return None


def _node_labels(node: dict[str, Any]) -> str:
    for patch in node.get("kubeadmConfigPatches") or []:
        if "InitConfiguration" in patch:
            parsed = yaml.safe_load(patch)
            labels: str = (
                parsed.get("nodeRegistration", {})
                .get("kubeletExtraArgs", {})
                .get(
                    "node-labels",
                    "",
                )
            )
            return labels
    return ""


def _node_count_problems(nodes: list[dict[str, Any]]) -> list[str]:
    problems: list[str] = []
    control_planes = [n for n in nodes if n.get("role") == "control-plane"]
    workers = [n for n in nodes if n.get("role") == "worker"]
    if len(nodes) != 3:
        problems.append(f"expected 3 node entries, found {len(nodes)}")
    if len(control_planes) != 1:
        problems.append(f"expected exactly 1 control-plane node, found {len(control_planes)}")
    if len(workers) != 2:
        problems.append(f"expected exactly 2 worker nodes, found {len(workers)}")
    return problems


def _node_problems(label: str, node: dict[str, Any], pinned_image: str) -> list[str]:
    """Every declaration a single node entry must carry."""
    problems: list[str] = []

    if node.get("image") != pinned_image:
        problems.append(
            f"{label}: image {node.get('image')!r} does not match the pinned "
            f"KIND_NODE_IMAGE in helm/versions.env ({pinned_image!r})",
        )

    kubelet = _kubelet_config(node)
    if kubelet is None:
        problems.append(f"{label}: no KubeletConfiguration patch found")
    else:
        problems.extend(
            f"{label}: KubeletConfiguration is missing {key!r}"
            for key in REQUIRED_KUBELET_KEYS
            if key not in kubelet
        )

    mounts = node.get("extraMounts") or []
    if len(mounts) != 2:
        problems.append(f"{label}: expected 2 extraMounts, found {len(mounts)}")
    problems.extend(
        f"{label}: extraMount hostPath {mount.get('hostPath')!r} is under /mnt/ "
        f"(9p mount penalty; forbidden)"
        for mount in mounts
        if str(mount.get("hostPath", "")).startswith("/mnt/")
    )

    return problems


def _ingress_ready_problems(nodes: list[dict[str, Any]]) -> list[str]:
    """Exactly the ingress-ready node must carry extraPortMappings for 80 and 443."""
    for node in nodes:
        if "ingress-ready=true" not in _node_labels(node):
            continue
        ports = [pm.get("hostPort") for pm in (node.get("extraPortMappings") or [])]
        if 80 not in ports or 443 not in ports:
            message = (
                f"the ingress-ready node must carry extraPortMappings for both 80 and "
                f"443, found {ports}"
            )
            return [message]
        return []
    return ["no node carries the 'ingress-ready=true' label"]


def kind_cluster_config_problems(doc: dict[str, Any]) -> list[str]:
    """Return every INFRA-01/INFRA-09 declaration missing from a parsed kind/cluster.yaml."""
    nodes = doc.get("nodes") or []
    pinned_image = _versions_env_variable("KIND_NODE_IMAGE")

    problems = _node_count_problems(nodes)
    for i, node in enumerate(nodes):
        label = f"node[{i}] ({node.get('role', '?')})"
        problems.extend(_node_problems(label, node, pinned_image))
    problems.extend(_ingress_ready_problems(nodes))

    containerd_patches = doc.get("containerdConfigPatches") or []
    if not any("config_path" in patch for patch in containerd_patches):
        problems.append("no top-level containerdConfigPatches sets containerd's config_path")

    return problems


def _load_cluster_yaml() -> dict[str, Any]:
    doc: dict[str, Any] = yaml.safe_load(CLUSTER_YAML.read_text(encoding="utf-8"))
    return doc


# 1. The paired false-positive control -------------------------------------


def test_the_real_cluster_yaml_has_no_problems() -> None:
    doc = _load_cluster_yaml()
    problems = kind_cluster_config_problems(doc)
    assert not problems, "\n".join(problems)


# 2. Non-vacuity by mutation on an in-memory copy ---------------------------


def test_deleting_a_node_is_reported() -> None:
    doc = copy.deepcopy(_load_cluster_yaml())
    before = len(doc["nodes"])
    del doc["nodes"][-1]
    assert len(doc["nodes"]) == before - 1, "the scratch mutation did not apply"
    assert kind_cluster_config_problems(doc), "a deleted node was not reported"


def test_deleting_an_extra_mount_is_reported() -> None:
    doc = copy.deepcopy(_load_cluster_yaml())
    before = len(doc["nodes"][0]["extraMounts"])
    del doc["nodes"][0]["extraMounts"][-1]
    assert len(doc["nodes"][0]["extraMounts"]) == before - 1, "the scratch mutation did not apply"
    assert kind_cluster_config_problems(doc), "a deleted extraMount was not reported"


def test_deleting_the_kubelet_configuration_patch_is_reported() -> None:
    doc = copy.deepcopy(_load_cluster_yaml())
    patches = doc["nodes"][0]["kubeadmConfigPatches"]
    before = len(patches)
    doc["nodes"][0]["kubeadmConfigPatches"] = [
        p for p in patches if "KubeletConfiguration" not in p
    ]
    assert len(doc["nodes"][0]["kubeadmConfigPatches"]) == before - 1, (
        "the scratch mutation did not apply"
    )
    assert kind_cluster_config_problems(doc), (
        "a deleted KubeletConfiguration patch was not reported"
    )


def test_deleting_the_ingress_ready_label_is_reported() -> None:
    doc = copy.deepcopy(_load_cluster_yaml())
    patches = doc["nodes"][0]["kubeadmConfigPatches"]
    doc["nodes"][0]["kubeadmConfigPatches"] = [
        re.sub(r"ingress-ready=true", "role=nothing", p) for p in patches
    ]
    assert doc["nodes"][0]["kubeadmConfigPatches"] != patches, "the scratch mutation did not apply"
    assert kind_cluster_config_problems(doc), (
        "removing the sole ingress-ready label was not reported"
    )


def test_deleting_the_containerd_config_patch_is_reported() -> None:
    doc = copy.deepcopy(_load_cluster_yaml())
    before = doc.get("containerdConfigPatches")
    doc["containerdConfigPatches"] = []
    assert before, "fixture assumption broken: no containerdConfigPatches to delete"
    assert kind_cluster_config_problems(doc), (
        "a deleted top-level containerdConfigPatches was not reported"
    )
