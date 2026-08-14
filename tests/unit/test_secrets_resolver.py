"""Unit tests for ``dataplat.secrets.resolver`` -- SEC-15.

Covers the ``env://``, ``file://`` and ``vault://`` resolution paths, and the
fail-closed behavior on every other scheme (including a schemeless/malformed
reference). ``vault://``'s own malformed-reference and Vault-failure cases
are covered here against a MOCKED ``hvac.Client`` (patched via
``resolver._vault_client``, this repository's established
module-level-callable-patching convention -- see
``tests/unit/test_csv_processor_cli.py``) -- no live cluster needed for this
module. The end-to-end pairing with the logging redaction processor is
proven separately in ``test_logging_redaction.py``; the live Vault
authentication boundary itself is proven separately in
``tests/e2e/vault/test_positive_auth.py``/``test_negative_auth.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import hvac.exceptions
import pytest

from dataplat.errors import SecretResolutionError
from dataplat.secrets import resolver
from dataplat.secrets.resolver import resolve_secret

if TYPE_CHECKING:
    from pathlib import Path


def test_env_scheme_returns_the_set_environment_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOME_TEST_VAR", "s3cr3t-value")

    assert resolve_secret("env://SOME_TEST_VAR") == "s3cr3t-value"


def test_env_scheme_raises_when_the_variable_is_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MISSING_VAR", raising=False)

    with pytest.raises(SecretResolutionError):
        resolve_secret("env://MISSING_VAR")


def test_file_scheme_returns_the_stripped_file_contents(tmp_path: Path) -> None:
    secret_file = tmp_path / "secret.txt"
    secret_file.write_text("s3cr3t\n", encoding="utf-8")

    assert resolve_secret(f"file://{secret_file}") == "s3cr3t"


def test_file_scheme_raises_when_the_path_does_not_exist(tmp_path: Path) -> None:
    missing = tmp_path / "no" / "such" / "path"

    with pytest.raises(SecretResolutionError):
        resolve_secret(f"file://{missing}")


def test_vault_scheme_returns_the_field_value_from_a_mocked_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = MagicMock()
    fake_client.secrets.kv.v2.read_secret_version.return_value = {
        "data": {
            "data": {"dsn": "postgresql://etl_app:s3cr3t@analytics-db-rw.data:5432/analytics"},
        },
    }
    monkeypatch.setattr(resolver, "_vault_client", lambda: fake_client)

    assert resolve_secret("vault://etl/analytics-db#dsn") == (
        "postgresql://etl_app:s3cr3t@analytics-db-rw.data:5432/analytics"
    )
    fake_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
        mount_point="etl",
        path="analytics-db",
    )


def test_vault_scheme_reads_a_different_mount_path_and_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = MagicMock()
    fake_client.secrets.kv.v2.read_secret_version.return_value = {
        "data": {"data": {"access_key": "etl-app", "secret_key": "another-s3cr3t"}},
    }
    monkeypatch.setattr(resolver, "_vault_client", lambda: fake_client)

    assert resolve_secret("vault://etl/minio#secret_key") == "another-s3cr3t"
    fake_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
        mount_point="etl",
        path="minio",
    )


def test_vault_scheme_without_a_field_fragment_raises_before_any_client_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A malformed vault:// ref must fail closed WITHOUT ever authenticating."""
    fake_client = MagicMock()
    monkeypatch.setattr(resolver, "_vault_client", lambda: fake_client)

    with pytest.raises(SecretResolutionError, match=r"scheme://mount/path#field"):
        resolve_secret("vault://etl/analytics-db")

    fake_client.secrets.kv.v2.read_secret_version.assert_not_called()


def test_vault_scheme_wraps_a_vault_error_rather_than_letting_it_escape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = MagicMock()
    fake_client.secrets.kv.v2.read_secret_version.side_effect = hvac.exceptions.VaultError(
        "permission denied",
    )
    monkeypatch.setattr(resolver, "_vault_client", lambda: fake_client)

    with pytest.raises(SecretResolutionError):
        resolve_secret("vault://etl/analytics-db#dsn")


def test_vault_scheme_wraps_a_missing_field_as_secret_resolution_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = MagicMock()
    fake_client.secrets.kv.v2.read_secret_version.return_value = {"data": {"data": {"dsn": "x"}}}
    monkeypatch.setattr(resolver, "_vault_client", lambda: fake_client)

    with pytest.raises(SecretResolutionError):
        resolve_secret("vault://etl/analytics-db#not_a_real_field")


def test_unparseable_reference_raises_secret_resolution_error() -> None:
    """A malformed/schemeless reference must fail closed with the domain
    exception, not crash with an unrelated exception type.
    """
    with pytest.raises(SecretResolutionError):
        resolve_secret("not-a-uri-at-all")


def test_resolver_never_returns_the_raw_unresolved_reference_string() -> None:
    """The literal reference string itself must never come back as if it
    were a resolved value -- every unsupported path must raise instead.
    """
    ref = "ftp://not-supported"
    with pytest.raises(SecretResolutionError) as exc_info:
        resolve_secret(ref)

    assert str(exc_info.value) != ref
