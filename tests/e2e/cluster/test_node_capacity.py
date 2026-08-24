"""tests/e2e/cluster/test_node_capacity.py — INFRA-09 proved on the live node.

Honest limit: this proves the node stops LYING about capacity on THIS host —
that its kubelet reservations are internally consistent with what
`status.allocatable` actually reports, and that three nodes do not together
claim more CPU/memory than the host running them actually has
(02-RESEARCH.md Pitfall 2, the whole point of this module). It does not
prove the chosen split between `systemReserved` and `kubeReserved` is
optimal for every future workload this platform runs (RESEARCH Assumptions
Log A1/A5) — only that the arithmetic the split produces is honest.

`tests/policy/test_kind_cluster_config.py` proves the *declarations* are
present in `kind/cluster.yaml`; this module proves the *live cluster*
actually behaves the way those declarations claim.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import yaml

if TYPE_CHECKING:
    from collections.abc import Callable

pytestmark = pytest.mark.cluster

REPO_ROOT = Path(__file__).resolve().parents[3]
CLUSTER_YAML = REPO_ROOT / "kind" / "cluster.yaml"

# The D-12 policy-test skeleton's quantity parser (02-RESEARCH.md § Code
# Examples), reused verbatim: Kubernetes resource quantities appear in both
# forms ("2", "500m") and binary/decimal suffix forms ("9Gi", "500Mi").
_QUANTITY_SUFFIXES: dict[str, float] = {
    "": 1,
    "m": 0.001,
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
_QUANTITY_RE = re.compile(
    r"^(?P<num>[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)(?P<suf>Ki|Mi|Gi|Ti|Pi|Ei|m|[kKMGTPE]|)$",
)


def _parse_quantity(value: str) -> float:
    """Parse a Kubernetes resource quantity string into a float base unit."""
    match = _QUANTITY_RE.match(str(value).strip())
    assert match, f"unparseable Kubernetes quantity: {value!r}"
    return float(match.group("num")) * _QUANTITY_SUFFIXES[match.group("suf")]


def _node_reservations() -> list[dict[str, float]]:
    """Read every node's systemReserved+kubeReserved+evictionHard.memory.available.

    Memory allocatable subtracts all three (02-RESEARCH.md Pitfall 2's
    measured arithmetic: `capacity - systemReserved - kubeReserved -
    evictionHard.memory.available`); CPU allocatable subtracts only the first
    two (kind ships no CPU eviction threshold).
    """
    doc = yaml.safe_load(CLUSTER_YAML.read_text(encoding="utf-8"))
    reservations: list[dict[str, float]] = []
    for node in doc["nodes"]:
        cpu_total = 0.0
        mem_total = 0.0
        for patch in node.get("kubeadmConfigPatches", []):
            if "KubeletConfiguration" not in patch:
                continue
            parsed = yaml.safe_load(patch)
            for key in ("systemReserved", "kubeReserved"):
                block = parsed.get(key) or {}
                cpu_total += _parse_quantity(str(block.get("cpu", "0")))
                mem_total += _parse_quantity(str(block.get("memory", "0")))
            eviction = parsed.get("evictionHard") or {}
            mem_total += _parse_quantity(str(eviction.get("memory.available", "0")))
        reservations.append({"cpu": cpu_total, "memory": mem_total})
    return reservations


def _host_cpu_count() -> int:
    return os.cpu_count() or 0


def _host_memory_bytes() -> float:
    """Read MemTotal from /proc/meminfo — the same source scripts/doctor.sh reads."""
    text = Path("/proc/meminfo").read_text(encoding="utf-8")
    match = re.search(r"^MemTotal:\s+(\d+)\s*kB", text, re.MULTILINE)
    assert match, "could not read MemTotal from /proc/meminfo"
    return float(match.group(1)) * 1024


@pytest.mark.multi_node
def test_exactly_three_nodes(kubectl_json: Callable[..., Any]) -> None:
    nodes = kubectl_json("get", "nodes")["items"]
    names = sorted(n["metadata"]["name"] for n in nodes)
    assert len(nodes) == 3, f"expected 3 nodes, found {len(nodes)}: {names}"


@pytest.mark.multi_node
def test_every_node_allocatable_is_positive_and_within_its_declared_ceiling(
    kubectl_json: Callable[..., Any],
) -> None:
    nodes = sorted(kubectl_json("get", "nodes")["items"], key=lambda n: n["metadata"]["name"])
    reservations = _node_reservations()
    assert len(reservations) == len(nodes), (
        f"kind/cluster.yaml declares {len(reservations)} node(s); the live cluster "
        f"reports {len(nodes)} — they must match for this test to mean anything"
    )

    problems: list[str] = []
    for node, reservation in zip(nodes, reservations, strict=True):
        name = node["metadata"]["name"]
        capacity = node["status"]["capacity"]
        allocatable = node["status"]["allocatable"]
        cap_cpu = _parse_quantity(capacity["cpu"])
        cap_mem = _parse_quantity(capacity["memory"])
        alloc_cpu = _parse_quantity(allocatable["cpu"])
        alloc_mem = _parse_quantity(allocatable["memory"])
        ceiling_cpu = cap_cpu - reservation["cpu"]
        ceiling_mem = cap_mem - reservation["memory"]

        if not (0 < alloc_cpu <= ceiling_cpu):
            problems.append(
                f"{name}: allocatable cpu {alloc_cpu} not in (0, {ceiling_cpu}] "
                f"(capacity {cap_cpu}, systemReserved+kubeReserved {reservation['cpu']})",
            )
        if not (0 < alloc_mem <= ceiling_mem):
            problems.append(
                f"{name}: allocatable memory {alloc_mem} not in (0, {ceiling_mem}] "
                f"(capacity {cap_mem}, reservations+evictionHard {reservation['memory']})",
            )
    assert not problems, "\n".join(problems)


def test_summed_allocatable_does_not_exceed_host_capacity(
    kubectl_json: Callable[..., Any],
) -> None:
    """The whole point (02-RESEARCH.md Pitfall 2).

    Three kind nodes on one host are not three separate machines — each
    node's `status.capacity` reports the FULL host, because containers on
    one host are not statically partitioned. If every node's reservations
    are sized independently without accounting for the other nodes sharing
    the same underlying resources, the scheduler ends up believing it has
    far more CPU/memory than the host actually has, and arbitration under
    real pressure falls to the host OOM killer instead of Kubernetes
    eviction. This is the live-cluster proof that the fair-share arithmetic
    in kind/cluster.yaml actually holds on THIS host, not merely on paper.
    """
    nodes = kubectl_json("get", "nodes")["items"]
    total_alloc_cpu = sum(_parse_quantity(n["status"]["allocatable"]["cpu"]) for n in nodes)
    total_alloc_mem = sum(_parse_quantity(n["status"]["allocatable"]["memory"]) for n in nodes)
    host_cpu = _host_cpu_count()
    host_mem = _host_memory_bytes()

    assert total_alloc_cpu <= host_cpu, (
        f"{len(nodes)} nodes advertise {total_alloc_cpu} allocatable CPU cores combined — "
        f"more than this host's real {host_cpu}. The scheduler is being lied to."
    )
    assert total_alloc_mem <= host_mem, (
        f"{len(nodes)} nodes advertise {total_alloc_mem} allocatable memory bytes combined — "
        f"more than this host's real {host_mem}. The scheduler is being lied to."
    )
