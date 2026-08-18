"""Integration proof for the dbt image (08.1-02's success criterion): a real
`docker build` followed by `docker run` against the actual
`docker/dbt/Dockerfile` -- mirrors `tests/integration/test_docker_image.py`'s
structure exactly (same REPO_ROOT/timeout constants, same build-then-run-
then-cleanup shape).

Lives in `tests/integration/` (the `cluster` dependency group, `make
test-integration`), never in `tests/unit/`: a cold Docker build is real
wall-clock time, and this test's whole purpose is proving a real build+run
works end to end -- it runs per-wave and at the phase gate, not on every
task commit, same reasoning as `test_docker_image.py`.

`ENTRYPOINT ["python", "/app/resolve_secrets.py"]` is already set on the
built image (this plan's Dockerfile), so a plain `docker run --rm <image>
--version` would try to resolve Vault credentials before ever reaching
`dbt --version` -- there is no live Vault in this test's environment.
`test_image_builds_and_prints_its_version` therefore bypasses the resolver
entirely for this ONE assertion via `docker run --entrypoint dbt <image>
--version`, since `--version` never needs a DB connection (08.1-02-PLAN.md's
own call-out).
"""

from __future__ import annotations

import subprocess
import uuid
from pathlib import Path

import pytest

# Matches tests/integration/test_publish_transaction_wiring.py's/
# test_metrics_otlp.py's own `pytestmark = pytest.mark.integration` idiom --
# needed for `pytest tests/integration/test_dbt_docker_image.py -m
# integration` to actually select these tests (a plain `pytest
# tests/integration -q`, what `make test-integration` runs, collects them
# either way).
pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = REPO_ROOT / "docker" / "dbt" / "Dockerfile"

# Generous, real wall-clock budgets: python:3.12-slim-bookworm-based layers
# can take real time to pull and build on a cold Docker layer cache. Docker's
# own layer caching makes a REPEAT run of this test fast on the same host --
# most of this budget is only ever spent once.
BUILD_TIMEOUT_SECONDS = 300
RUN_TIMEOUT_SECONDS = 30

DBT_VERSION = "1.12.2"


def _build_image() -> str:
    image = f"dbt:test-{uuid.uuid4().hex[:12]}"
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
    assert build.returncode == 0, (
        f"docker build failed (exit {build.returncode}):\nstdout:\n{build.stdout}\nstderr:\n{build.stderr}"
    )
    return image


def _remove_image(image: str) -> None:
    subprocess.run(  # noqa: S603 -- fixed argv, no shell, no user input
        ["docker", "rmi", "-f", image],  # noqa: S607 -- resolved from PATH
        capture_output=True,
        text=True,
        check=False,
    )


def test_image_builds_and_prints_its_version() -> None:
    """Build the real Dockerfile, run `dbt --version` bypassing the resolver
    entrypoint, and assert a genuine dbt-core 1.12.2 version string --
    never `:latest`.
    """
    image = _build_image()
    try:
        run = subprocess.run(  # noqa: S603 -- fixed argv, no shell, no user input
            [  # noqa: S607 -- "docker" resolved from PATH
                "docker",
                "run",
                "--rm",
                "--entrypoint",
                "dbt",
                image,
                "--version",
            ],
            capture_output=True,
            text=True,
            timeout=RUN_TIMEOUT_SECONDS,
            check=False,
        )
        assert run.returncode == 0, (
            f"docker run failed (exit {run.returncode}):\nstdout:\n{run.stdout}\nstderr:\n{run.stderr}"
        )

        stdout = run.stdout.strip()
        assert stdout, "docker run --entrypoint dbt <image> --version produced no stdout"
        assert DBT_VERSION in stdout, (
            f"the image's dbt-core version does not contain {DBT_VERSION!r}: {stdout!r}"
        )
    finally:
        _remove_image(image)


def test_image_contains_no_dataplat_or_airflow() -> None:
    """ADR-0004's two-image discipline extended to three (08.1-RESEARCH.md's
    Anti-Patterns table): neither `dataplat` nor `apache-airflow` is
    importable inside this image -- the dbt image's own version of
    `test_docker_image.py`'s UNKNOWN_VERSION_SENTINEL assertion, adapted for
    a non-`dataplat` image where the meaningful negative assertion is
    package absence instead.
    """
    image = _build_image()
    try:
        for module in ("dataplat", "airflow"):
            run = subprocess.run(  # noqa: S603 -- fixed argv, no shell, no user input
                [  # noqa: S607 -- "docker" resolved from PATH
                    "docker",
                    "run",
                    "--rm",
                    "--entrypoint",
                    "python",
                    image,
                    "-c",
                    f"import {module}",
                ],
                capture_output=True,
                text=True,
                timeout=RUN_TIMEOUT_SECONDS,
                check=False,
            )
            assert run.returncode != 0, (
                f"expected `import {module}` to fail inside the dbt image, but it succeeded:\n"
                f"stdout:\n{run.stdout}\nstderr:\n{run.stderr}"
            )
            assert "ModuleNotFoundError" in run.stderr, (
                f"expected a ModuleNotFoundError for `import {module}`, got:\nstderr:\n{run.stderr}"
            )
    finally:
        _remove_image(image)
