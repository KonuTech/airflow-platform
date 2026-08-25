"""Unit tests for `scripts/vault-bootstrap.py` -- SEC-13 gap closure (plan 05-06).

Regression guard for CR-01 (`_ensure_etl_secrets`/`_ensure_airflow_secrets`
no longer depend on the three Kubernetes Secrets plans 05-02/05-03 already
deleted -- `csv-processor-db`, `csv-processor-s3`, `airflow-minio-connection`)
and CR-02 (`_ensure_policy` re-applies a policy whose live body has drifted,
not only one that is entirely absent).

Fully offline: `hvac.Client` is a `MagicMock` passed directly as the
`client` parameter -- the exact shape every `_ensure_*` function in the
module under test already accepts (mirroring `test_secrets_resolver.py`'s
own `MagicMock` style). `subprocess.run` is monkeypatched on the
dynamically-loaded module's own `subprocess` reference -- since the module
under test does a plain `import subprocess`, that reference IS the real,
global `subprocess` module, and `monkeypatch` reverts the patch after every
test. No live cluster, no `-m cluster` marker, no network call, no real
`kubectl`/`psql` invocation anywhere in this file.

`scripts/vault-bootstrap.py`'s filename is hyphenated and it lives outside
any package, so a normal `import` statement cannot reach it -- there is no
existing precedent for unit-testing a `scripts/*.py` file in this repo, so
`_load_vault_bootstrap`'s `importlib.util.spec_from_file_location`/
`module_from_spec`/`exec_module` boilerplate is new.
"""

from __future__ import annotations

import base64
import importlib.util
import subprocess
from pathlib import Path
from unittest.mock import MagicMock
from urllib.parse import quote

import hvac.exceptions
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT_PATH = REPO_ROOT / "scripts" / "vault-bootstrap.py"
_KUBECTL_CONTEXT = "kind-test-cluster"


def _load_vault_bootstrap():
    spec = importlib.util.spec_from_file_location("vault_bootstrap", _SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def vault_bootstrap():
    """A fresh import of `scripts/vault-bootstrap.py` for every test."""
    return _load_vault_bootstrap()


def _completed(*, stdout="", returncode=0, stderr=""):
    """Build a `subprocess.CompletedProcess` double -- no real process ever runs."""
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr=stderr)


# -- _ensure_etl_secrets: etl/analytics-db -----------------------------------


def test_ensure_etl_secrets_generates_password_and_writes_dsn_when_analytics_db_absent(
    monkeypatch: pytest.MonkeyPatch,
    vault_bootstrap,
) -> None:
    """CR-01: `etl/analytics-db` absent -- regenerates `etl_app`'s password via
    `kubectl exec` + `ALTER ROLE` (peer/local trust, never a network
    connection) and writes the assembled DSN straight to Vault KV.
    """
    client = MagicMock()

    def _read_secret_version(**kwargs):
        if kwargs["path"] == "analytics-db":
            raise hvac.exceptions.InvalidPath
        return {"data": {"data": {"access_key": "etl-app", "secret_key": "unused"}}}

    client.secrets.kv.v2.read_secret_version.side_effect = _read_secret_version

    mock_run = MagicMock(
        side_effect=[
            _completed(stdout="analytics-db-1"),  # kubectl get cluster ... currentPrimary
            _completed(stdout=""),  # kubectl exec ... psql
        ],
    )
    monkeypatch.setattr(vault_bootstrap.subprocess, "run", mock_run)

    vault_bootstrap._ensure_etl_secrets(client, _KUBECTL_CONTEXT)  # noqa: SLF001 -- exercising the private function this test covers

    assert mock_run.call_count == 2

    cluster_argv = mock_run.call_args_list[0].args[0]
    assert cluster_argv[1:] == [
        "--context",
        _KUBECTL_CONTEXT,
        "get",
        "cluster",
        "-n",
        "data",
        "analytics-db",
        "-o",
        "jsonpath={.status.currentPrimary}",
    ]

    exec_call = mock_run.call_args_list[1]
    exec_argv = exec_call.args[0]
    assert exec_argv[1:] == [
        "--context",
        _KUBECTL_CONTEXT,
        "exec",
        "-i",
        "-n",
        "data",
        "analytics-db-1",
        "--",
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        "postgres",
        "-d",
        "analytics",
    ]
    sql = exec_call.kwargs["input"]
    assert "etl_app" in sql
    assert "PASSWORD" in sql
    # The SQL is the ONLY carrier of the raw password -- extract it from the
    # exact shape `_ensure_etl_secrets` writes it in, never from argv (it is
    # never present there).
    prefix = "ALTER ROLE etl_app WITH PASSWORD '"
    assert sql.startswith(prefix)
    assert sql.endswith("';")
    raw_password = sql[len(prefix) : -2]
    assert raw_password  # non-empty
    assert all(char in "0123456789abcdef" for char in raw_password)

    client.secrets.kv.v2.create_or_update_secret.assert_called_once_with(
        mount_point="etl",
        path="analytics-db",
        secret={
            "dsn": (
                f"postgresql://etl_app:{quote(raw_password, safe='')}"
                "@analytics-db-rw.data:5432/analytics"
            ),
        },
    )


def test_ensure_etl_secrets_skips_analytics_db_when_already_present(
    monkeypatch: pytest.MonkeyPatch,
    vault_bootstrap,
) -> None:
    """CR-01 non-regression: an already-present `etl/analytics-db` secret is
    never rotated on a later idempotent bootstrap run -- no `kubectl`/`psql`
    call, no Vault write.
    """
    client = MagicMock()
    client.secrets.kv.v2.read_secret_version.return_value = {
        "data": {"data": {"dsn": "postgresql://etl_app:x@analytics-db-rw.data:5432/analytics"}},
    }

    mock_run = MagicMock()
    monkeypatch.setattr(vault_bootstrap.subprocess, "run", mock_run)

    vault_bootstrap._ensure_etl_secrets(client, _KUBECTL_CONTEXT)  # noqa: SLF001 -- exercising the private function this test covers

    mock_run.assert_not_called()
    client.secrets.kv.v2.create_or_update_secret.assert_not_called()


# -- _ensure_etl_secrets: etl/minio -------------------------------------------


def test_ensure_etl_secrets_reads_minio_app_secret_when_minio_absent(
    monkeypatch: pytest.MonkeyPatch,
    vault_bootstrap,
) -> None:
    """CR-01: `etl/minio` is sourced from the live `data/minio-app` Secret's
    `secretKey` field -- never the deleted `csv-processor-s3` Secret, and
    never the old field name `secret_key` (the two Secrets use different
    key casing).
    """
    client = MagicMock()

    def _read_secret_version(**kwargs):
        if kwargs["path"] == "minio":
            raise hvac.exceptions.InvalidPath
        return {
            "data": {"data": {"dsn": "postgresql://etl_app:x@analytics-db-rw.data:5432/analytics"}}
        }

    client.secrets.kv.v2.read_secret_version.side_effect = _read_secret_version

    encoded_secret = base64.b64encode(b"live-minio-app-secret").decode("ascii")
    mock_run = MagicMock(return_value=_completed(stdout=encoded_secret))
    monkeypatch.setattr(vault_bootstrap.subprocess, "run", mock_run)

    vault_bootstrap._ensure_etl_secrets(client, _KUBECTL_CONTEXT)  # noqa: SLF001 -- exercising the private function this test covers

    mock_run.assert_called_once()
    argv = mock_run.call_args.args[0]
    assert argv[1:] == [
        "--context",
        _KUBECTL_CONTEXT,
        "get",
        "secret",
        "-n",
        "data",
        "minio-app",
        "-o",
        "jsonpath={.data.secretKey}",
    ]

    client.secrets.kv.v2.create_or_update_secret.assert_called_once_with(
        mount_point="etl",
        path="minio",
        secret={"access_key": "etl-app", "secret_key": "live-minio-app-secret"},
    )


# -- _ensure_airflow_secrets ---------------------------------------------------


_ANALYTICS_DSN = "postgresql://etl_app:unit-test-password@analytics-db-rw.data:5432/analytics"


def test_ensure_airflow_secrets_assembles_conn_uri_when_absent(
    monkeypatch: pytest.MonkeyPatch,
    vault_bootstrap,
) -> None:
    """CR-01: `airflow/connections/minio_default` is assembled from the same
    live `data/minio-app` Secret `_ensure_etl_secrets` reads for
    `etl/minio` -- never the deleted `airflow-minio-connection` Secret.
    (i2, debug/ci-pipeline-ingestion-timeout ROUND 9):
    `airflow/connections/analytics_db_default` is copied verbatim from the
    `etl/analytics-db` `dsn` field `_ensure_etl_secrets` wrote earlier in
    the same bootstrap invocation -- never assembled independently.
    """
    client = MagicMock()

    def _read_secret_version(*, mount_point: str, path: str):
        # Both airflow/connections/* guard reads miss (fresh Vault); the
        # etl/analytics-db recovery read hits -- (h) always runs first.
        if mount_point == "etl" and path == "analytics-db":
            return {"data": {"data": {"dsn": _ANALYTICS_DSN}}}
        raise hvac.exceptions.InvalidPath

    client.secrets.kv.v2.read_secret_version.side_effect = _read_secret_version

    encoded_secret = base64.b64encode(b"live-minio-app-secret").decode("ascii")
    mock_run = MagicMock(return_value=_completed(stdout=encoded_secret))
    monkeypatch.setattr(vault_bootstrap.subprocess, "run", mock_run)

    vault_bootstrap._ensure_airflow_secrets(client, _KUBECTL_CONTEXT)  # noqa: SLF001 -- exercising the private function this test covers

    mock_run.assert_called_once()
    argv = mock_run.call_args.args[0]
    assert argv[1:] == [
        "--context",
        _KUBECTL_CONTEXT,
        "get",
        "secret",
        "-n",
        "data",
        "minio-app",
        "-o",
        "jsonpath={.data.secretKey}",
    ]

    writes = client.secrets.kv.v2.create_or_update_secret.call_args_list
    assert len(writes) == 2, "expected minio_default AND analytics_db_default writes"

    minio_kwargs = writes[0].kwargs
    assert minio_kwargs["mount_point"] == "airflow"
    assert minio_kwargs["path"] == "connections/minio_default"
    conn_uri = minio_kwargs["secret"]["conn_uri"]
    assert conn_uri.startswith("aws://etl-app:")
    assert (
        "endpoint_url=http%3A%2F%2Fminio.data.svc.cluster.local%3A9000&region_name=us-east-1"
        in conn_uri
    )

    analytics_kwargs = writes[1].kwargs
    assert analytics_kwargs["mount_point"] == "airflow"
    assert analytics_kwargs["path"] == "connections/analytics_db_default"
    assert analytics_kwargs["secret"] == {"conn_uri": _ANALYTICS_DSN}


def test_ensure_airflow_secrets_skips_analytics_db_connection_when_already_present(
    vault_bootstrap,
) -> None:
    """(i2)'s guard: an already-present `analytics_db_default` (e.g. the
    long-lived local cluster's own hand-written repair, see
    `test_backfill_2year_sweep.py` module docstring finding 4) is never
    overwritten -- and no `etl/analytics-db` recovery read even happens.
    """
    client = MagicMock()
    client.secrets.kv.v2.read_secret_version.return_value = {
        "data": {"data": {"conn_uri": "already-present"}}
    }

    vault_bootstrap._ensure_airflow_secrets(client, _KUBECTL_CONTEXT)  # noqa: SLF001 -- exercising the private function this test covers

    client.secrets.kv.v2.create_or_update_secret.assert_not_called()
    read_paths = {
        call.kwargs["path"] for call in client.secrets.kv.v2.read_secret_version.call_args_list
    }
    assert read_paths == {"connections/minio_default", "connections/analytics_db_default"}


# -- _ensure_policy: CR-02 policy-drift correction -----------------------------


def test_ensure_policy_reapplies_when_body_drifted(vault_bootstrap) -> None:
    """CR-02: a policy already present, but whose live body no longer
    matches the target HCL, is re-applied on the next idempotent bootstrap
    run -- not silently left drifted forever.
    """
    client = MagicMock()
    client.sys.list_policies.return_value = {"data": {"policies": ["csv-processor"]}}
    client.sys.read_policy.return_value = {
        "data": {"rules": 'path "old/path" { capabilities = ["read"] }\n'},
    }

    target_hcl = 'path "etl/data/analytics-db" { capabilities = ["read"] }\n'

    vault_bootstrap._ensure_policy(client, "csv-processor", target_hcl)  # noqa: SLF001 -- exercising the private function this test covers

    client.sys.create_or_update_policy.assert_called_once_with(
        name="csv-processor",
        policy=target_hcl,
    )


def test_ensure_policy_skips_when_body_matches(vault_bootstrap) -> None:
    """CR-02 non-regression: a policy whose live body already matches the
    target HCL (modulo surrounding whitespace) performs zero writes.
    """
    client = MagicMock()
    client.sys.list_policies.return_value = {"data": {"policies": ["csv-processor"]}}
    target_hcl = 'path "etl/data/analytics-db" { capabilities = ["read"] }\n'
    # Live body differs only in trailing whitespace -- must still compare
    # equal after the `.strip()` both sides go through.
    client.sys.read_policy.return_value = {"data": {"rules": target_hcl.strip() + "\n\n"}}

    vault_bootstrap._ensure_policy(client, "csv-processor", target_hcl)  # noqa: SLF001 -- exercising the private function this test covers

    client.sys.create_or_update_policy.assert_not_called()


# -- _ensure_dbt_secret ---------------------------------------------------------


def test_ensure_dbt_secret_generates_password_and_writes_five_fields_when_dbt_db_absent(
    monkeypatch: pytest.MonkeyPatch,
    vault_bootstrap,
) -> None:
    """`etl/dbt-db` absent -- regenerates `dbt_app`'s password via the same
    `kubectl exec` + `ALTER ROLE` mechanism `_ensure_etl_secrets` uses for
    `etl_app`, then writes FIVE discrete credential fields (never one `dsn`
    string) straight to Vault KV.
    """
    client = MagicMock()
    client.secrets.kv.v2.read_secret_version.side_effect = hvac.exceptions.InvalidPath

    mock_run = MagicMock(
        side_effect=[
            _completed(stdout="analytics-db-1"),  # kubectl get cluster ... currentPrimary
            _completed(stdout=""),  # kubectl exec ... psql
        ],
    )
    monkeypatch.setattr(vault_bootstrap.subprocess, "run", mock_run)

    vault_bootstrap._ensure_dbt_secret(client, _KUBECTL_CONTEXT)  # noqa: SLF001 -- exercising the private function this test covers

    assert mock_run.call_count == 2

    exec_call = mock_run.call_args_list[1]
    exec_argv = exec_call.args[0]
    assert exec_argv[1:] == [
        "--context",
        _KUBECTL_CONTEXT,
        "exec",
        "-i",
        "-n",
        "data",
        "analytics-db-1",
        "--",
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        "postgres",
        "-d",
        "analytics",
    ]
    sql = exec_call.kwargs["input"]
    prefix = "ALTER ROLE dbt_app WITH PASSWORD '"
    assert sql.startswith(prefix)
    assert sql.endswith("';")
    raw_password = sql[len(prefix) : -2]
    assert raw_password  # non-empty
    assert len(raw_password) == 64  # secrets.token_hex(32)'s own output shape
    assert all(char in "0123456789abcdef" for char in raw_password)

    client.secrets.kv.v2.create_or_update_secret.assert_called_once_with(
        mount_point="etl",
        path="dbt-db",
        secret={
            "host": "analytics-db-rw.data",
            "port": "5432",
            "user": "dbt_app",
            "password": raw_password,
            "dbname": "analytics",
        },
    )
    written_secret = client.secrets.kv.v2.create_or_update_secret.call_args.kwargs["secret"]
    assert set(written_secret.keys()) == {"host", "port", "user", "password", "dbname"}


def test_ensure_dbt_secret_skips_when_dbt_db_already_present(
    monkeypatch: pytest.MonkeyPatch,
    vault_bootstrap,
) -> None:
    """An already-present `etl/dbt-db` secret is never rotated on a later
    idempotent bootstrap run -- no `kubectl`/`psql` call, no Vault write.
    """
    client = MagicMock()
    client.secrets.kv.v2.read_secret_version.return_value = {
        "data": {
            "data": {
                "host": "analytics-db-rw.data",
                "port": "5432",
                "user": "dbt_app",
                "password": "x",
                "dbname": "analytics",
            },
        },
    }

    mock_run = MagicMock()
    monkeypatch.setattr(vault_bootstrap.subprocess, "run", mock_run)

    vault_bootstrap._ensure_dbt_secret(client, _KUBECTL_CONTEXT)  # noqa: SLF001 -- exercising the private function this test covers

    mock_run.assert_not_called()
    client.secrets.kv.v2.create_or_update_secret.assert_not_called()


# -- non-vacuity ----------------------------------------------------------------


def test_module_no_longer_defines_deleted_secret_name_constants(vault_bootstrap) -> None:
    """Non-vacuity: the dead constants naming the three deleted Secrets were
    actually removed from the module, not merely shadowed by unreachable
    code that still references them.
    """
    assert not hasattr(vault_bootstrap, "_DB_SECRET_NAME")
    assert not hasattr(vault_bootstrap, "_S3_SECRET_NAME")
    assert not hasattr(vault_bootstrap, "_AIRFLOW_MINIO_SECRET_NAME")
