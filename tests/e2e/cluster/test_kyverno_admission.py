"""tests/e2e/cluster/test_kyverno_admission.py -- D-18 live proof.

Mirrors the SEC-12 positive+negative pattern (tests/e2e/vault/test_
positive_auth.py / test_negative_auth.py): a real, live admission decision,
not a unit test of the CEL expression in isolation. Both tests apply an
actual Pod manifest against the real cluster (never `--dry-run`) and observe
the real admission outcome.

Positive case: a Pod referencing a real, cosign-signed, this-pipeline-
published `csv-processor` image (git-SHA-tagged by `.github/workflows/
publish.yml` on merge to `main`) is admitted -- the Pod object exists
afterward. The exact tag is resolved LIVE from GHCR (the newest
`ghcr.io/<owner>/csv-processor` package version whose tag is a full 40-hex
git SHA, i.e. a real merge publish, not a `pr-<N>` or a `sha256-...`
attestation/signature entry) rather than hardcoded: a frozen commit SHA
baked into this file would eventually reference an image `ghcr-cleanup.yml`
never touches (only `pr-<N>` tags are cleaned up) but that could in
principle be retagged/pruned by a future registry-hygiene change -- resolving
live is what actually matches this suite's own "prove it now, against the
real system" standard, and is exactly what a merge-triggered CI run of this
same file would naturally exercise anyway (D-19: the full e2e suite runs on
merge to `main`, always against the just-published image for that commit).

Negative case: a Pod referencing `docker.io/library/hello-world:latest` --
public, real, deliberately never signed by this pipeline, and not on
`kubernetes/kyverno-policy.yaml`'s D-16 exception list -- is DENIED at
admission. Mirrors test_minio_buckets.py's "prove it wasn't deleted anyway"
discipline (lines 84-128): the assertion doesn't stop at "an error was
raised" -- it also confirms the Pod object was never created, and inspects
the denial reason text to confirm this was really Kyverno's admission
webhook (not, say, a generic image-pull or scheduling failure that would
happen to also leave no Pod... except a scheduling/pull failure WOULD still
create the Pod object, just leave it Pending/ErrImagePull -- so the
"object does not exist" assertion alone already rules that out; the reason-
text check is the belt to that assertion's braces, catching a future
regression that silently disables the policy while some OTHER, unrelated
admission control still blocks pod creation for a different reason).
"""

from __future__ import annotations

import json
import shutil
import subprocess
import uuid
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

pytestmark = pytest.mark.cluster

NEGATIVE_IMAGE = "docker.io/library/hello-world:latest"
POLICY_NAME = "require-signed-images"
SCRATCH_NAMESPACE = "etl"


def _repo_owner_lowercase() -> str:
    """The GHCR owner segment: `git remote get-url origin`, lowercased (D-04).

    Never hardcoded -- GHCR/OCI repository names reject mixed case
    (plan 11-01's own live-confirmed finding), and re-deriving this from the
    actual remote (rather than assuming `konutech`) keeps this test correct
    under a fork or a repository rename.
    """
    proc = subprocess.run(
        ["git", "remote", "get-url", "origin"],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    url = proc.stdout.strip()
    # Handles both `git@github.com:Owner/repo.git` and
    # `https://github.com/Owner/repo.git` remote URL shapes.
    tail = url.split("github.com", 1)[-1].lstrip(":/")
    owner = tail.split("/", 1)[0]
    return owner.lower()


def _newest_merge_tagged_csv_processor_image() -> str:
    """Resolve the newest real, merge-published csv-processor image in GHCR.

    Filters to tags shaped like a full 40-hex-character git SHA -- excludes
    `pr-<N>` tags (cleaned up on PR close, `ghcr-cleanup.yml`, D-11) and the
    `sha256-<digest>` cosign signature/attestation entries the same package
    also carries (plan 11-02's own live GHCR query found both shapes
    present). Skips the test (not a hard failure) if `gh` is unavailable or
    no such tag exists yet -- this is a live-discovery precondition, the
    same class of "skip cleanly, name the reason" pattern
    tests/e2e/cluster/conftest.py's `_require_cluster` already establishes.
    """
    gh_bin = shutil.which("gh")
    if gh_bin is None:
        pytest.skip("gh not found on PATH — cannot resolve a live GHCR image reference")

    owner = _repo_owner_lowercase()
    proc = subprocess.run(  # noqa: S603
        [
            gh_bin,
            "api",
            f"/users/{owner}/packages/container/csv-processor/versions",
            "--paginate",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if proc.returncode != 0:
        pytest.skip(
            f"could not query GHCR for csv-processor versions (gh exited "
            f"{proc.returncode}): {proc.stderr.strip()}",
        )

    versions = json.loads(proc.stdout)

    def _sha_tags(version: dict[str, Any]) -> list[tuple[str, str]]:
        tags = (version.get("metadata") or {}).get("container", {}).get("tags") or []
        return [
            (version.get("created_at", ""), tag)
            for tag in tags
            if len(tag) == 40 and all(c in "0123456789abcdef" for c in tag)
        ]

    candidates = [pair for version in versions for pair in _sha_tags(version)]

    if not candidates:
        pytest.skip(
            "no merge-tagged (40-hex git SHA) csv-processor image found in GHCR — "
            "run this after a commit has been published via .github/workflows/publish.yml",
        )

    candidates.sort(key=lambda pair: pair[0], reverse=True)
    _created_at, newest_tag = candidates[0]
    return f"ghcr.io/{owner}/csv-processor:{newest_tag}"


def _apply_pod(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    *,
    name: str,
    image: str,
) -> subprocess.CompletedProcess[str]:
    """Create a bare, single-container Pod via `kubectl run` (never --dry-run).

    `--restart=Never` creates exactly one Pod object, not a Deployment --
    admission is a per-Pod-object decision, and the assertion this test cares
    about (the Pod object exists, or does not) only makes sense against a
    single, directly-named object.
    """
    return kubectl(
        "run",
        name,
        f"--image={image}",
        "-n",
        SCRATCH_NAMESPACE,
        "--restart=Never",
        "--command",
        "--",
        "sleep",
        "3600",
    )


def _pod_exists(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    *,
    name: str,
) -> bool:
    proc = kubectl("get", "pod", name, "-n", SCRATCH_NAMESPACE)
    return proc.returncode == 0


def test_a_signed_project_image_is_admitted(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    """A real, cosign-signed, this-pipeline-published image is admitted."""
    image = _newest_merge_tagged_csv_processor_image()
    pod_name = f"kyverno-admission-positive-{uuid.uuid4().hex[:10]}"

    try:
        proc = _apply_pod(kubectl, name=pod_name, image=image)
        assert proc.returncode == 0, (
            f"expected a signed image ({image}) to be admitted, but `kubectl run` "
            f"failed (exit {proc.returncode}):\n{proc.stderr}"
        )
        assert _pod_exists(kubectl, name=pod_name), (
            f"kubectl run reported success but pod/{pod_name} does not exist "
            f"in namespace {SCRATCH_NAMESPACE}"
        )
    finally:
        # --grace-period=0 --force: this is a throwaway `sleep 3600` test
        # pod with nothing to flush on shutdown — a graceful 30s termination
        # wait would otherwise race the kubectl fixture's own subprocess
        # timeout for no benefit.
        kubectl(
            "delete",
            "pod",
            pod_name,
            "-n",
            SCRATCH_NAMESPACE,
            "--ignore-not-found",
            "--grace-period=0",
            "--force",
        )


def test_an_unsigned_public_image_is_denied(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    """An unsigned, non-exempt public image is denied at admission.

    No `finally`/cleanup block: a correctly-working policy denies the
    request before the Pod object is ever created, so there is nothing to
    clean up. If cleanup were ever needed here, that would itself be a sign
    this test is passing for the wrong reason.
    """
    pod_name = f"kyverno-admission-negative-{uuid.uuid4().hex[:10]}"

    proc = _apply_pod(kubectl, name=pod_name, image=NEGATIVE_IMAGE)

    assert proc.returncode != 0, (
        f"expected an unsigned, non-exempt image ({NEGATIVE_IMAGE}) to be denied, "
        f"but `kubectl run` succeeded"
    )
    assert not _pod_exists(kubectl, name=pod_name), (
        f"kubectl run failed as expected, but pod/{pod_name} exists anyway in "
        f"namespace {SCRATCH_NAMESPACE} — the denial did not actually prevent "
        "the object from being created"
    )
    # The reason-text check: a future regression that silently disables (or
    # misconfigures) this policy, while some OTHER, unrelated admission
    # control happens to also reject this pod for a different reason, must
    # still fail THIS assertion rather than pass for the wrong reason.
    assert "admission webhook" in proc.stderr, (
        f"expected the denial to come from an admission webhook, got:\n{proc.stderr}"
    )
    assert POLICY_NAME in proc.stderr, (
        f"expected the denial to name policy {POLICY_NAME!r} specifically, got:\n{proc.stderr}"
    )
