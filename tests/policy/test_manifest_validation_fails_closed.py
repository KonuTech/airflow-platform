"""CICD-07's manifest gate, proven non-vacuous.

**Honest limit.** This proves that `kubeconform`, configured exactly as
`scripts/render-manifests.sh` configures it, rejects one known-invalid
CloudNativePG `Cluster` document (02-RESEARCH.md Pitfall 3: `spec.postgresql:
null`, rendered by `templates/cluster.yaml` whenever a values file sets
nothing under `cluster.postgresql`) and accepts its valid twin. It does not
prove the configuration catches every class of invalid manifest — only that
the gate, as built, is live rather than decorative.

## Why this module lives outside the offline gate

This module shells out to the real `kubeconform` binary in the gitignored
`tools/bin/` and reads `helm/schemas/cnpg/`, both network-installed /
network-derived artifacts. A module in `tests/policy/` collected by `make
policy` would break the offline contract this phase's own prohibitions
declare: a fresh clone running `uv sync && make check` with no network would
error on a missing binary. The existing precedent is `gitleaks` and
`gitleaks-selftest` — Makefile targets in `ci`, not pytest modules in the
offline gate. This module follows it via `pytestmark = pytest.mark.manifests`
(registered by plan 02-01): `make policy` deselects it (`-m "not manifests"`),
`make manifest-policy` selects it (`-m manifests`, after its `manifests`
prerequisite has rendered `build/manifests/`).

## The anti-vacuity switch

`REQUIRE_RENDERED_MANIFESTS=1` (set by `make manifest-policy`'s recipe) turns
`test_the_rendered_cluster_manifests_validate`'s otherwise-silent skip — when
`build/manifests/` has not been rendered yet — into a hard failure. Without
the switch, a bare `uv run --frozen pytest tests/policy -m manifests -q`
(no prior `make manifests`) skips that one test rather than reporting nothing
useful; `make manifest-policy` is defined never to hit that path at all,
because `manifests` is its Make prerequisite.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.manifests

REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLES = Path(__file__).resolve().parent / "badmanifests"
KUBECONFORM = REPO_ROOT / "tools" / "bin" / "kubeconform"
CNPG_SCHEMA_LOCATION = str(
    REPO_ROOT / "helm" / "schemas" / "cnpg" / "{{.ResourceKind}}_{{.ResourceAPIVersion}}.json",
)

INVALID_SAMPLE = SAMPLES / "cluster_null_postgresql.yaml"
VALID_SAMPLE = SAMPLES / "good_cluster_null_postgresql.yaml"

# The exact JSON-pointer path kubeconform names in Pitfall 3's rejection
# (02-RESEARCH.md, re-verified live against kubeconform 0.8.0 this session).
EXPECTED_INVALID_JSON_POINTER = "/spec/postgresql"
MISSING_SCHEMA_MESSAGE = "could not find schema for Cluster"


def _kubeconform(*args: str) -> subprocess.CompletedProcess[str]:
    """Run the real, pinned kubeconform binary and hand back the real result.

    Args:
        args: Arguments to pass to kubeconform, after its own binary path.

    Returns:
        The completed subprocess — never raises on a non-zero exit, since a
        non-zero exit is the signal under test in half of this module's cases.
    """
    if not KUBECONFORM.exists():
        pytest.fail(
            f"{KUBECONFORM} not found — run `tools/k8s/install_kubeconform.sh` "
            "(or `make manifests`) first",
        )
    return subprocess.run(  # noqa: S603
        [str(KUBECONFORM), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


# --------------------------------------------------------------------------
# The negative case and its positive control
# --------------------------------------------------------------------------


def test_the_invalid_sample_is_rejected() -> None:
    proc = _kubeconform(
        "-strict",
        "-schema-location",
        "default",
        "-schema-location",
        CNPG_SCHEMA_LOCATION,
        str(INVALID_SAMPLE),
    )
    assert proc.returncode != 0, (
        f"kubeconform accepted a Cluster with spec.postgresql: null:\n{proc.stdout}"
    )
    assert EXPECTED_INVALID_JSON_POINTER in proc.stdout, (
        f"kubeconform failed the sample but not at {EXPECTED_INVALID_JSON_POINTER}:\n{proc.stdout}"
    )


def test_the_valid_twin_is_accepted() -> None:
    proc = _kubeconform(
        "-strict",
        "-schema-location",
        "default",
        "-schema-location",
        CNPG_SCHEMA_LOCATION,
        str(VALID_SAMPLE),
    )
    assert proc.returncode == 0, (
        f"kubeconform rejected the valid twin under the identical configuration:\n"
        f"{proc.stdout}\n{proc.stderr}"
    )


# --------------------------------------------------------------------------
# The vendored CRD schema, proven load-bearing rather than decorative
# --------------------------------------------------------------------------


def test_the_crd_schema_is_load_bearing() -> None:
    """Deleting helm/schemas/cnpg/ must break the build, not silently validate nothing.

    Runs the identical valid sample twice: once with no CNPG schema location
    at all (must fail — kubeconform has no built-in schema for a
    CustomResourceDefinition-defined kind like `Cluster`) and once with the
    vendored location supplied (must pass). The pair is what keeps someone
    from deleting the schema directory and leaving a gate that reports
    success while validating nothing for it.
    """
    without_schema = _kubeconform(
        "-strict",
        "-schema-location",
        "default",
        str(VALID_SAMPLE),
    )
    assert without_schema.returncode != 0, (
        "kubeconform validated a CNPG Cluster CR with no vendored schema "
        f"location supplied — the schema is not load-bearing:\n{without_schema.stdout}"
    )
    assert MISSING_SCHEMA_MESSAGE in without_schema.stdout, (
        f"expected a missing-schema error, got:\n{without_schema.stdout}"
    )

    with_schema = _kubeconform(
        "-strict",
        "-schema-location",
        "default",
        "-schema-location",
        CNPG_SCHEMA_LOCATION,
        str(VALID_SAMPLE),
    )
    assert with_schema.returncode == 0, (
        f"kubeconform still rejected the valid sample with the schema location "
        f"supplied:\n{with_schema.stdout}"
    )


# --------------------------------------------------------------------------
# The real rendered manifests — not just the synthetic sample — validate too
# --------------------------------------------------------------------------


def test_the_rendered_cluster_manifests_validate() -> None:
    """The real, rendered CNPG Cluster CRs validate — not just the synthetic sample.

    This is the one test in this module that depends on `build/manifests/`
    already existing, and therefore the natural home of the
    `REQUIRE_RENDERED_MANIFESTS` anti-vacuity switch documented in the module
    docstring: `make manifest-policy` declares `manifests` as a prerequisite,
    so under the real gate this directory always exists and this test never
    skips.
    """
    build_dir = REPO_ROOT / "build" / "manifests"
    rendered = sorted(build_dir.glob("*/cnpg-a*.yaml")) if build_dir.is_dir() else []

    if not rendered:
        if os.environ.get("REQUIRE_RENDERED_MANIFESTS"):
            pytest.fail(
                f"{build_dir} has no rendered CNPG Cluster manifests, and "
                "REQUIRE_RENDERED_MANIFESTS=1 forbids silently skipping this "
                "input. Run `make manifests` first (or `make manifest-policy`, "
                "which orders the render ahead of this test).",
            )
        pytest.skip(f"{build_dir} not rendered — run `make manifests` first")

    proc = _kubeconform(
        "-strict",
        "-schema-location",
        "default",
        "-schema-location",
        CNPG_SCHEMA_LOCATION,
        "-skip",
        "CustomResourceDefinition",
        *(str(path) for path in rendered),
    )
    assert proc.returncode == 0, (
        f"a real rendered CNPG Cluster manifest failed validation:\n{proc.stdout}"
    )


# --------------------------------------------------------------------------
# The bad samples must not poison the gate they exist to prove
# --------------------------------------------------------------------------


def test_the_main_gate_does_not_choke_on_the_bad_samples() -> None:
    """A broken manifest sitting in tests/policy/badmanifests/ must never break `make manifests`.

    Mirrors test_gates_actually_fail.py::test_the_main_gate_does_not_lint_the_bad_samples:
    invoked as a plain subprocess with `MAKEFLAGS` cleared, so a parent
    `make -j` does not leak its jobserver into the child. `render-manifests.sh`
    only ever reads `helm/values/` and writes `build/manifests/`; it has no
    reason to see `tests/policy/badmanifests/` at all, and this proves that
    structural fact rather than assuming it.
    """
    env = dict(os.environ)
    env.pop("MAKEFLAGS", None)
    proc = subprocess.run(
        ["make", "manifests"],  # noqa: S607 — "make" is deliberately resolved via PATH
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert proc.returncode == 0, (
        f"`make manifests` is red with tests/policy/badmanifests/ present:\n"
        f"{proc.stdout}\n{proc.stderr}"
    )
    # A target that ran nothing would also exit 0. `make` echoes its recipe,
    # so requiring the tool name in the transcript keeps this from passing
    # vacuously if the target is ever gutted.
    assert "kubeconform" in proc.stdout, (
        f"`make manifests` exited 0 without invoking kubeconform:\n{proc.stdout}"
    )
