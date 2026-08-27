"""Shared pytest hooks for every ``tests/e2e/*`` suite.

One hook only: stream each failure's traceback the MOMENT it is known,
instead of buffering it for pytest's end-of-session summary.

Why (debug/ci-pipeline-ingestion-timeout ROUND 15, rider i): CI's e2e-full
job runs under a hard ``timeout-minutes`` ceiling, and a run cancelled at
that ceiling loses pytest's terminal summary entirely -- ROUNDs 11-13 left
ZERO per-test output on cancellation (fixed by the ``-v`` rider, ROUND 14),
and ROUND 14's surviving ``-v`` lines then carried the WHICH but not the
WHY: 9 tests showed ``FAILED`` with their tracebacks lost to the
cancellation (the ROUND 14 sweep failure's assert is still unknown for
exactly this reason). Printing ``rep.longreprtext`` from
``pytest_runtest_logreport`` makes every already-decided failure's full
traceback part of the streamed job log, so even a cancelled run carries the
evidence.

Scoped to ``tests/e2e/`` deliberately: local unit/integration runs finish
and print their summaries normally, and doubling their failure output would
be pure noise. The chaos/vault/slice/cluster/observability suites all sit
under this conftest automatically.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    """Stream a failed test's traceback immediately (setup, call, or teardown).

    Args:
        report: The phase report pytest just produced. Only failed reports
            with a rendered ``longrepr`` print anything; passes/skips are
            untouched, so the ``-v`` per-test lines stay the primary
            progress record.
    """
    if not report.failed:
        return
    text = report.longreprtext
    if not text:
        return
    # Plain prints, deliberately: this hook's output IS the interface (the
    # streamed CI job log) -- OBS-03 carve-out 3, per-file-ignores in
    # pyproject.toml + tests/policy/test_print_ban_scope.py's allowlist.
    header = f" {report.nodeid} failed during {report.when} "
    print(f"\n{header:=^100}", flush=True)
    print(text, flush=True)
    print("=" * 100, flush=True)
