"""D-10, observed rejecting every failure class it claims to block.

A configured preflight that has never been seen to fail is indistinguishable
from a disabled one — the same argument `tests/policy/test_gates_actually_
fail.py` makes about the linters, applied to `scripts/doctor.sh`.

**Honest limit.** This file proves that `make doctor` rejects each failure
class named in D-10 (below its own documented floor, in the exact env var it
claims to read) and that the real, unmodified host also passes. It does not
prove D-10's failure-class list is complete, and it does not prove any single
remediation command it prints is correct on every operating system — only
that a failure is reported, non-vacuously, for each threshold this repository
commits to enforcing.

Each threshold is exercised twice, mirroring `test_gates_actually_fail.py`'s
pairing:

* a **negative** case — `make doctor` with the threshold env var set to an
  unsatisfiable value, which must exit non-zero and name the failing check in
  its stderr;
* the shared **positive control** (`test_doctor_passes_on_the_real_host`) —
  the same command with no overrides, against the real host, exiting 0. A
  preflight that rejects everything is exactly as broken as one that rejects
  nothing, and only the pair distinguishes them.

The missing-tool case follows the same shape as the Makefile's own `uv-guard`
`UV=` override: pointing `KIND` at a path that does not exist must be reported
distinguishably from a wrong-but-present version (`found 'none'` vs. a real
version string) — never conflated into a single generic failure.

`test_doctor_echoes_its_own_checks` is the transcript assertion: `make doctor`
exiting 0 without having actually run its checks would otherwise pass
vacuously, the same trap `test_the_main_gate_does_not_lint_the_bad_samples`
guards against for `ruff`/`mypy`.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _run(cmd: list[str], env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    """Run a real command from the repository root and hand back the real result.

    No `check=True`: a non-zero exit is the signal under test in every
    negative case here, and swallowing it into an exception would turn a
    broken gate into a passing test.
    """
    return subprocess.run(  # noqa: S603  # deliberately invoking the project toolchain
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


# One (env var, unsatisfiable value, substring expected in stderr) triple per
# threshold D-10 names. The substring is the check's own identity in its
# failure message — asserting only the exit code would also pass if `make`
# itself failed to find the target.
THRESHOLD_CASES: tuple[tuple[str, str, str], ...] = (
    ("DOCTOR_MIN_INOTIFY_WATCHES", "99999999", "max_user_watches"),
    ("DOCTOR_MIN_INOTIFY_INSTANCES", "99999999", "max_user_instances"),
    ("DOCTOR_MIN_FREE_GB", "999999", "free disk"),
    ("DOCTOR_MIN_CPUS", "999999", "host CPU count"),
    ("DOCTOR_MIN_MEM_GB", "999999", "host memory"),
)


def test_doctor_passes_on_the_real_host() -> None:
    """The positive control every negative case below is paired against."""
    proc = _run(["make", "doctor"])
    assert proc.returncode == 0, (
        f"`make doctor` failed on a host that should pass every check:\n"
        f"{proc.stdout}\n{proc.stderr}"
    )


def test_doctor_rejects_each_threshold_it_claims_to_block() -> None:
    problems: list[str] = []
    for var, bad_value, expected_substring in THRESHOLD_CASES:
        proc = _run(["make", "doctor", f"{var}={bad_value}"])
        if proc.returncode == 0:
            problems.append(f"{var}={bad_value}: `make doctor` exited 0 — expected a failure")
            continue
        if expected_substring not in proc.stderr:
            problems.append(
                f"{var}={bad_value}: exited non-zero but stderr never named "
                f"'{expected_substring}':\n{proc.stderr}"
            )
    assert not problems, "\n".join(problems)


def test_doctor_reports_a_missing_tool_distinguishably_from_a_wrong_version() -> None:
    """Overriding KIND at a nonexistent path — mirrors uv-guard's `UV=` override."""
    proc = _run(["make", "doctor", "KIND=/nonexistent-kind-binary-for-this-test"])
    assert proc.returncode != 0, (
        f"`make doctor KIND=/nonexistent...` exited 0 — a missing tool was not caught:\n"
        f"{proc.stdout}\n{proc.stderr}"
    )
    assert "'none'" in proc.stderr, (
        f"a missing kind binary was not reported as 'none' (the found-vs-required "
        f"distinction uv-guard's own message makes):\n{proc.stderr}"
    )
    assert "KIND=/nonexistent-kind-binary-for-this-test" in proc.stderr, (
        f"the failure did not name which KIND path was checked:\n{proc.stderr}"
    )


def test_doctor_echoes_its_own_checks() -> None:
    """`make doctor` exiting 0 without having run anything would pass vacuously.

    `make` echoes its recipe before running it — the same trick
    `test_the_main_gate_does_not_lint_the_bad_samples` uses for `ruff`/`mypy` —
    so requiring `scripts/doctor.sh` in the transcript keeps a gutted target
    from passing silently.
    """
    proc = _run(["make", "doctor"])
    assert proc.returncode == 0, f"positive control failed:\n{proc.stdout}\n{proc.stderr}"
    assert "scripts/doctor.sh" in proc.stdout, (
        f"`make doctor` exited 0 without invoking scripts/doctor.sh:\n{proc.stdout}"
    )
    assert "doctor: all checks passed." in proc.stdout, (
        f"`make doctor` exited 0 without doctor.sh's own success message:\n{proc.stdout}"
    )
