"""Unit tests for ``dataplat.secrets.resolver`` — SEC-15.

Covers the ``env://`` and ``file://`` resolution paths, and the fail-closed
behavior on every other scheme (including ``vault://``, Phase 5's, and a
schemeless/malformed reference). The end-to-end pairing with the logging
redaction processor is proven separately in ``test_logging_redaction.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from dataplat.errors import SecretResolutionError
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


def test_vault_scheme_fails_closed_rather_than_passing_through() -> None:
    """SEC-15's central claim: the scheme most likely to be added carelessly
    later (Phase 5's ``vault://``) is rejected today, not silently accepted.
    """
    with pytest.raises(SecretResolutionError):
        resolve_secret("vault://kv/data/etl/db")


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
