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


def test_build_common_resolves_vault_literals_held_inside_env_vars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression test, plan 05-03: three of `_build_common()`'s four env vars
    (`DATAPLAT_DB_DSN`, `DATAPLAT_S3_ACCESS_KEY`, `DATAPLAT_S3_SECRET_KEY`) each hold a
    SECOND opaque `vault://` reference as their own value (plan 05-02's `kpo.py` wiring)
    -- `_build_common()` must resolve them two `resolve_secret()` calls deep, not hand
    the raw, unresolved `vault://` string straight to `create_pool()`/`S3ObjectStore`.
    Found live: every real KPO pod failed with `missing "=" after "vault://etl/
    analytics-db#dsn" in connection info string` because the un-double-resolved DSN
    reached psycopg as if it were already a real DSN. `DATAPLAT_S3_ENDPOINT_URL` is the
    control case -- it holds a plain, non-secret literal directly and must resolve
    through exactly ONE `resolve_secret()` call, not two.
    """
    monkeypatch.setenv("DATAPLAT_DB_DSN", "vault://etl/analytics-db#dsn")
    monkeypatch.setenv("DATAPLAT_S3_ENDPOINT_URL", "http://minio.data.svc.cluster.local:9000")
    monkeypatch.setenv("DATAPLAT_S3_ACCESS_KEY", "vault://etl/minio#access_key")
    monkeypatch.setenv("DATAPLAT_S3_SECRET_KEY", "vault://etl/minio#secret_key")

    # Maps EVERY ref this fake ever expects to see resolved -- a ref this test does
    # not anticipate (e.g. a real vault:// string reaching create_pool unresolved,
    # which is exactly the bug) raises KeyError, failing the test loudly rather than
    # silently passing an unresolved reference through.
    resolved = {
        "env://DATAPLAT_DB_DSN": "vault://etl/analytics-db#dsn",
        "vault://etl/analytics-db#dsn": "postgresql://real-user:real-pass@host/db",
        "env://DATAPLAT_S3_ENDPOINT_URL": "http://minio.data.svc.cluster.local:9000",
        "env://DATAPLAT_S3_ACCESS_KEY": "vault://etl/minio#access_key",
        "vault://etl/minio#access_key": "real-access-key",
        "env://DATAPLAT_S3_SECRET_KEY": "vault://etl/minio#secret_key",
        "vault://etl/minio#secret_key": "real-secret-key",
    }

    def _fake_resolve_secret(ref: str) -> str:
        return resolved[ref]

    monkeypatch.setattr(csv_processor_cli, "resolve_secret", _fake_resolve_secret)

    captured: dict[str, object] = {}

    class _FakePool:
        def open(self, *, wait: bool = True) -> None:
            captured["opened"] = wait

    def _fake_create_pool(dsn: str) -> _FakePool:
        captured["dsn"] = dsn
        return _FakePool()

    def _fake_metadata_repository(pool: object) -> object:
        captured["metadata_pool"] = pool
        return object()

    def _fake_s3_object_store(*, endpoint_url: str, access_key: str, secret_key: str) -> object:
        captured["endpoint_url"] = endpoint_url
        captured["access_key"] = access_key
        captured["secret_key"] = secret_key
        return object()

    monkeypatch.setattr(csv_processor_cli, "create_pool", _fake_create_pool)
    monkeypatch.setattr(csv_processor_cli, "PostgresMetadataRepository", _fake_metadata_repository)
    monkeypatch.setattr(csv_processor_cli, "S3ObjectStore", _fake_s3_object_store)

    csv_processor_cli._build_common()  # noqa: SLF001 -- exercising the exact private function this test regression-covers

    assert captured["dsn"] == "postgresql://real-user:real-pass@host/db"
    assert captured["opened"] is True
    assert captured["endpoint_url"] == "http://minio.data.svc.cluster.local:9000"
    assert captured["access_key"] == "real-access-key"
    assert captured["secret_key"] == "real-secret-key"  # noqa: S105 -- a test double's literal, not a real credential


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
