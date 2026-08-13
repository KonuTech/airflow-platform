"""Integration proof of ROADMAP success criterion 3: a real `docker build`
followed by `docker run ... --version` against the actual
`docker/csv-processor/Dockerfile` -- mechanically proven, never a manual,
eyeballed step (03-VALIDATION.md's Manual-Only Verifications section names
this test explicitly as the reason that section is empty).

Lives in `tests/integration/` (the `cluster` dependency group, `make
test-integration`), never in `tests/unit/`: a cold Docker build is the
single slowest operation in this phase's test suite (03-RESEARCH.md
Environment Availability), and this test's whole purpose is proving a real
build+run works end to end -- it runs per-wave and at the phase gate, not on
every task commit (03-VALIDATION.md's Sampling Rate).

`ENTRYPOINT ["dataplat"]` is already set on the built image (plan 03-07 Task
2's Dockerfile), so the container is run as `docker run --rm <image>
--version`, not `docker run --rm <image> dataplat --version` -- the latter
would pass "dataplat" as a SECOND argument on top of the entrypoint, which
click resolves as an (unknown) subcommand of the `dataplat` group and fails
with `NoSuchCommand`. Observed directly while proving this test by hand.
"""

from __future__ import annotations

import subprocess
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = REPO_ROOT / "docker" / "csv-processor" / "Dockerfile"

# Generous, real wall-clock budgets: python:3.12-slim-bookworm-based layers can
# take real time to pull and build on a cold Docker layer cache. Docker's own
# layer caching (the dependency-only layer, see the Dockerfile) makes a
# REPEAT run of this test fast on the same host -- most of this budget is
# only ever spent once.
BUILD_TIMEOUT_SECONDS = 300
RUN_TIMEOUT_SECONDS = 30

# Phase 1's version.py sentinel for "no installed distribution found" --
# see dataplat/version.py UNKNOWN_VERSION. Seeing this string back from the
# running container would mean dataplat is merely importable from an
# uninstalled source tree, not genuinely installed.
UNKNOWN_VERSION_SENTINEL = "0.0.0+unknown"


def test_image_builds_and_prints_its_version() -> None:
    """Build the real Dockerfile, run it, and assert `--version` prints a
    genuine installed version -- never `:latest`, never the unknown sentinel.
    """
    image = f"csv-processor:test-{uuid.uuid4().hex[:12]}"
    assert ":latest" not in image

    build = subprocess.run(  # noqa: S603 -- fixed argv, no shell, no user input
        [  # noqa: S607 -- "docker" is resolved from PATH deliberately, not a shell injection risk
            "docker",
            "build",
            "--build-arg",
            "GIT_SHA=test-fixed-sha",
            "-t",
            image,
            "-f",
            str(DOCKERFILE),
            str(REPO_ROOT),
        ],
        capture_output=True,
        text=True,
        timeout=BUILD_TIMEOUT_SECONDS,
        check=False,
    )
    try:
        assert build.returncode == 0, (
            f"docker build failed (exit {build.returncode}):\n"
            f"stdout:\n{build.stdout}\nstderr:\n{build.stderr}"
        )

        run = subprocess.run(  # noqa: S603 -- fixed argv, no shell, no user input
            ["docker", "run", "--rm", image, "--version"],  # noqa: S607 -- resolved from PATH
            capture_output=True,
            text=True,
            timeout=RUN_TIMEOUT_SECONDS,
            check=False,
        )
        assert run.returncode == 0, (
            f"docker run failed (exit {run.returncode}):\n"
            f"stdout:\n{run.stdout}\nstderr:\n{run.stderr}"
        )

        stdout = run.stdout.strip()
        assert stdout, "docker run <image> --version produced no stdout"
        assert UNKNOWN_VERSION_SENTINEL not in stdout, (
            f"the image reports the Phase-1 UNKNOWN_VERSION sentinel -- dataplat is not "
            f"genuinely installed in this image: {stdout!r}"
        )
    finally:
        # Always attempt cleanup, build failure or not, so a failed run does
        # not leave a half-built tag behind either.
        subprocess.run(  # noqa: S603 -- fixed argv, no shell, no user input
            ["docker", "rmi", "-f", image],  # noqa: S607 -- resolved from PATH
            capture_output=True,
            text=True,
            check=False,
        )
