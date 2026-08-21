"""tests/e2e/cluster/test_doctor_live_mount_detection.py

`scripts/doctor-live.sh` (`make doctor-live` / `make doctor-live-check`) is
the self-heal counterpart to `scripts/doctor.sh`: `doctor` is a PRE-flight
that runs before `cluster-up` and has nothing to say about a cluster that is
already running. Three documented incidents on this host share one
downstream symptom — a Docker Desktop/WSL2-level VM restart breaks the DAGs
`hostPath` bind mount on every kind node, falling back to an empty read-only
tmpfs, which silently freezes Airflow scheduling cluster-wide with zero
exceptions logged (`.planning/debug/resolved/dagrun-scheduler-stall.md`,
`.planning/debug/docker-desktop-wsl2-vm-restart.md`). This module proves
`doctor-live.sh` actually detects that exact tmpfs-fallback state and
self-heals it, not just that it runs without error.

Honest limit: `test_doctor_live_passes_on_the_real_host` proves detection
reports "healthy" against a real, currently-working cluster. The broken/
repair cases below use a fake `docker` binary (same override-point pattern
`tests/policy/test_doctor_fails_closed.py` uses for `KIND=`/`HELM=`) rather
than actually breaking the live cluster's mount — deliberately forcing a
real tmpfs fallback is not something this suite can safely trigger (it would
require tearing down the Docker Desktop/WSL2 VM the developer is using), so
the classification and self-heal logic is proven correct by construction
against canned `mount` output instead of by reproducing the real incident
end-to-end.

This module lives in tests/e2e/cluster/ (not tests/policy/) and inherits
that directory's `_require_cluster` autouse skip, mirroring `cluster-verify`'s
own "needs a live cluster" placement precedent even though the fake-docker
cases below do not themselves touch the live cluster — `doctor-live.sh` is a
live-cluster tool by definition, so its tests belong with the others that
need `make cluster-up` to have already run.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from textwrap import dedent

import pytest

pytestmark = pytest.mark.cluster

REPO_ROOT = Path(__file__).resolve().parents[3]

_FAKE_DOCKER_TEMPLATE = """\
#!/usr/bin/env bash
# Fake docker for doctor-live.sh tests. Reports one running container
# ("{container}") whose /mnt/dags mount starts broken (tmpfs) and flips to
# healthy (ext4) once "docker restart" has been invoked -- so the same fake
# binary proves both detection and the self-heal repair path.
set -euo pipefail
STATE_FILE="{state_file}"
case "$1" in
  ps)
    echo "{container}"
    ;;
  exec)
    if [ -f "$STATE_FILE" ]; then
      echo "/dev/sde on /mnt/dags type ext4 (ro,relatime,discard,errors=remount-ro,data=ordered)"
    else
      echo "none on /mnt/dags type tmpfs (ro,relatime)"
    fi
    ;;
  restart)
    touch "$STATE_FILE"
    ;;
  *)
    echo "fake docker: unhandled subcommand $1" >&2
    exit 1
    ;;
esac
"""


def _write_fake_docker(tmp_path: Path, container: str) -> tuple[Path, Path]:
    """Write a fake `docker` executable; returns (docker_path, state_file)."""
    state_file = tmp_path / "restarted.marker"
    docker_path = tmp_path / "docker"
    docker_path.write_text(
        _FAKE_DOCKER_TEMPLATE.format(container=container, state_file=state_file),
        encoding="utf-8",
    )
    docker_path.chmod(0o755)
    return docker_path, state_file


_MAKE = shutil.which("make") or "make"


def _run_make(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603  # deliberately invoking the project toolchain
        [_MAKE, *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )


def test_doctor_live_passes_on_the_real_host() -> None:
    """Positive control: the real, currently-running cluster reports healthy."""
    proc = _run_make("doctor-live-check")
    assert proc.returncode == 0, (
        f"`make doctor-live-check` failed against the live cluster:\n{proc.stdout}\n{proc.stderr}"
    )
    for node in ("control-plane", "worker", "worker2"):
        assert f"{node}: healthy" in proc.stdout or f"-{node}: healthy" in proc.stdout, (
            f"expected node '{node}' reported healthy:\n{proc.stdout}"
        )


def test_doctor_live_detects_broken_tmpfs_mount(tmp_path: Path) -> None:
    """Detection-only mode reports the tmpfs fallback and exits non-zero, no restart issued."""
    docker_path, state_file = _write_fake_docker(tmp_path, "airflow-platform-control-plane")
    proc = _run_make("doctor-live-check", f"DOCKER={docker_path}")
    assert proc.returncode != 0, (
        f"expected a non-zero exit when /mnt/dags is tmpfs:\n{proc.stdout}\n{proc.stderr}"
    )
    assert "tmpfs" in proc.stderr, f"broken state not reported as tmpfs fallback:\n{proc.stderr}"
    assert not state_file.exists(), "DOCTOR_LIVE_REPAIR=false must never invoke 'docker restart'"


def test_doctor_live_self_heals_broken_mount(tmp_path: Path) -> None:
    """Repair mode (the default) restarts the affected node and re-verifies healthy."""
    docker_path, state_file = _write_fake_docker(tmp_path, "airflow-platform-control-plane")
    proc = _run_make("doctor-live", f"DOCKER={docker_path}")
    assert proc.returncode == 0, (
        f"expected doctor-live to self-heal and exit 0:\n{proc.stdout}\n{proc.stderr}"
    )
    assert state_file.exists(), "expected 'docker restart' to have been invoked as the repair"
    assert "repaired" in proc.stderr.lower() or "repaired" in proc.stdout.lower(), (
        f"expected the repair to be reported:\n{proc.stdout}\n{proc.stderr}"
    )


def test_doctor_live_reports_unknown_mount_output_without_crashing(tmp_path: Path) -> None:
    """Unrecognized `mount` output (docker exec failing) is advisory, never a false 'healthy'."""
    docker_path = tmp_path / "docker"
    docker_path.write_text(
        dedent(
            """\
            #!/usr/bin/env bash
            case "$1" in
              ps) echo "airflow-platform-control-plane" ;;
              exec) exit 1 ;;
              restart) exit 0 ;;
              *) exit 1 ;;
            esac
            """,
        ),
        encoding="utf-8",
    )
    docker_path.chmod(0o755)
    proc = _run_make("doctor-live-check", f"DOCKER={docker_path}")
    assert "control-plane: healthy" not in proc.stdout, (
        f"a failed docker exec must never be classified as healthy:\n{proc.stdout}"
    )
    assert proc.returncode == 0, (
        "an unreadable mount state is advisory (unknown), not a hard failure — it should not "
        f"fail the whole run on its own:\n{proc.stdout}\n{proc.stderr}"
    )
    assert "could not read" in proc.stderr or "ADVISORY" in proc.stderr, (
        f"expected an advisory for unreadable mount state:\n{proc.stderr}"
    )
