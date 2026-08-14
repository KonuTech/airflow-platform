"""Unit tests for ``csv_processor.cli`` -- WR-01's Receipt-on-every-exit-path fix.

Covers: ``ingest()`` writes a `status="FAILED"` `Receipt` to the XCom path for
ANY exception raised inside its try body, not only `DataPlatformError` --
regression-proving 04-REVIEW.md's WR-01 finding, where a raw, unwrapped
exception (e.g. `psycopg.errors.DataError`, a network error, `MemoryError`)
previously propagated with no Receipt ever written, contradicting `ingest()`'s
own documented "every exit path" contract. Also proves the pre-existing
`except DataPlatformError:` path is unaffected by the new `except Exception:`
clause, since Python evaluates except clauses in the order they are written.

``dataplat.cli.main()`` (not `CliRunner.invoke()`) is what actually dispatches
into `csv_processor.cli.ingest` through the real `dataplat.plugins` entry
point -- the same `main()`-based invocation style
`tests/unit/test_cli_error_handling.py` already established for exercising
`dataplat`'s own CLI boundary.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from csv_processor import cli as csv_processor_cli
from dataplat.cli import main
from dataplat.errors import ConfigurationError

if TYPE_CHECKING:
    from pathlib import Path

_ASSIGNMENT_URI = "s3://metadata/assignments/customers/999.json"


def _raise_runtime_error() -> None:
    """Stand-in for an unwrapped, non-DataPlatformError failure.

    Represents the real-world case WR-01 names explicitly: an unwrapped
    `psycopg.errors.DataError` from a publish-time cast failure, a network
    error, or `MemoryError` -- none of which are `DataPlatformError`
    subclasses.
    """
    msg = "simulated psycopg.errors.DataError-style failure, not a DataPlatformError"
    raise RuntimeError(msg)


def _raise_configuration_error() -> None:
    """Stand-in for the pre-existing, already-covered `DataPlatformError` path."""
    msg = "bad config"
    raise ConfigurationError(msg, context={"detail": "WR-01 regression test"})


def test_ingest_writes_a_failed_receipt_for_a_non_dataplatformerror_exception(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """WR-01: a raw, non-DataPlatformError exception still results in a written
    FAILED Receipt, even though the exception itself still propagates out of
    `ingest()` (and out of `main()`, since `main()`'s own boundary only
    catches `DataPlatformError`) so Airflow still observes the task fail via
    the pod's non-zero exit code.
    """
    xcom_path = tmp_path / "xcom" / "return.json"
    monkeypatch.setenv("DATAPLAT_XCOM_PATH", str(xcom_path))
    monkeypatch.setattr(csv_processor_cli, "_build_common", _raise_runtime_error)

    with pytest.raises(RuntimeError, match="not a DataPlatformError"):
        main(["ingest", "--assignment", _ASSIGNMENT_URI])

    assert xcom_path.exists()
    payload = json.loads(xcom_path.read_text(encoding="utf-8"))
    assert payload["status"] == "FAILED"
    assert payload["run_id"] == -1


def test_ingest_dataplatformerror_path_is_unaffected_by_the_new_except_clause(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The pre-existing `except DataPlatformError:` branch still produces the
    identical FAILED Receipt shape it always has -- proving the new
    `except Exception:` clause (WR-01) never intercepts a `DataPlatformError`,
    since except clauses are evaluated in the order they are written and
    `DataPlatformError` is listed first.
    """
    xcom_path = tmp_path / "xcom" / "return.json"
    monkeypatch.setenv("DATAPLAT_XCOM_PATH", str(xcom_path))
    monkeypatch.setattr(csv_processor_cli, "_build_common", _raise_configuration_error)

    exit_code = main(["ingest", "--assignment", _ASSIGNMENT_URI])

    assert exit_code == 1
    assert xcom_path.exists()
    payload = json.loads(xcom_path.read_text(encoding="utf-8"))
    assert payload["status"] == "FAILED"
    assert payload["run_id"] == -1
