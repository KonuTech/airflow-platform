"""tests/e2e/vault/test_negative_auth.py -- SEC-12 live proof.

The `default` ServiceAccount in namespace `etl` is denied a Kubernetes-auth
login against the `csv-processor` Vault role: Vault's role config binds
`bound_service_account_names=["csv-processor"]`, so a mismatched identity's
login attempt itself must fail closed -- no client token is ever issued, so
there is nothing to even attempt a KV read with afterward.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import hvac
import hvac.exceptions
import pytest

if TYPE_CHECKING:
    import subprocess
    from collections.abc import Callable

pytestmark = pytest.mark.cluster


def _kubectl_create_token(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    *,
    service_account: str,
    namespace: str,
) -> str:
    """Obtain a fresh projected token for `service_account` in `namespace`."""
    proc = kubectl("create", "token", service_account, "-n", namespace)
    assert proc.returncode == 0, (
        f"kubectl create token {service_account} -n {namespace} failed "
        f"(exit {proc.returncode}):\n{proc.stderr}"
    )
    return proc.stdout.strip()


def test_default_service_account_is_denied_the_csv_processor_role(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    vault_addr: str,
) -> None:
    """SEC-12: `default`'s login itself fails -- no token is ever issued, so
    there is nothing to attempt a KV read with. A second, state-based
    assertion (`client.token is None`) confirms this from a different angle
    than the raised exception alone: `login()` only ever assigns
    `client.token` AFTER a successful auth response, so a client that raised
    during login can never have picked one up -- even a hypothetical future
    regression that swallowed the exception without re-raising would still
    be caught here, because nothing before this assertion could have set it.
    """
    default_jwt = _kubectl_create_token(kubectl, service_account="default", namespace="etl")

    client = hvac.Client(url=vault_addr)
    with pytest.raises(hvac.exceptions.VaultError):
        client.auth.kubernetes.login(role="csv-processor", jwt=default_jwt)

    assert client.token is None, "a client token was issued despite the denied login"


def test_airflow_scheduler_service_account_is_also_denied(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    vault_addr: str,
) -> None:
    """Stretch case (05-CONTEXT.md Claude's-Discretion note, not a
    requirement -- ROADMAP SC2 names only `default`): a ServiceAccount from
    an entirely different namespace is denied the SAME way `default` is --
    the boundary is the exact bound identity match, not merely "is not
    named default".
    """
    scheduler_jwt = _kubectl_create_token(
        kubectl,
        service_account="airflow-scheduler",
        namespace="airflow",
    )

    client = hvac.Client(url=vault_addr)
    with pytest.raises(hvac.exceptions.VaultError):
        client.auth.kubernetes.login(role="csv-processor", jwt=scheduler_jwt)

    assert client.token is None, "a client token was issued despite the denied login"


def test_default_service_account_is_denied_the_dbt_role(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    vault_addr: str,
) -> None:
    """08.1-13/T-08.1-32: `default`'s login against the `dbt` role fails closed the same
    way it does against `csv-processor` above -- Vault's role config binds
    `bound_service_account_names=["dbt"]`, so a mismatched identity's login attempt
    itself must fail, no client token ever issued.
    """
    default_jwt = _kubectl_create_token(kubectl, service_account="default", namespace="etl")

    client = hvac.Client(url=vault_addr)
    with pytest.raises(hvac.exceptions.VaultError):
        client.auth.kubernetes.login(role="dbt", jwt=default_jwt)

    assert client.token is None, "a client token was issued despite the denied login"


def test_csv_processor_is_denied_the_dbt_role(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    vault_addr: str,
) -> None:
    """08.1-13/T-08.1-32: a REAL two-way least-privilege proof, not only "some other SA
    is denied" -- the `csv-processor` ServiceAccount (a real, bootstrapped Vault
    identity with its own role) is denied the `dbt` role, and (the sibling test in
    test_positive_auth.py / the mirror below) `dbt` never gets `csv-processor`'s own
    paths either. Vault's role config binds `bound_service_account_names=["dbt"]`
    only -- `csv-processor` is a real identity, not a placeholder like `default`, so
    this proves the boundary holds even against another genuinely-provisioned
    workload identity, not just an unprivileged one.
    """
    csv_processor_jwt = _kubectl_create_token(
        kubectl,
        service_account="csv-processor",
        namespace="etl",
    )

    client = hvac.Client(url=vault_addr)
    with pytest.raises(hvac.exceptions.VaultError):
        client.auth.kubernetes.login(role="dbt", jwt=csv_processor_jwt)

    assert client.token is None, "a client token was issued despite the denied login"


def test_dbt_service_account_is_denied_the_csv_processor_role(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    vault_addr: str,
) -> None:
    """08.1-13/T-08.1-32: the other direction of the same two-way proof -- `dbt` is
    denied the `csv-processor` role, completing the mutual boundary
    `test_csv_processor_is_denied_the_dbt_role` above only proves one side of.
    """
    dbt_jwt = _kubectl_create_token(kubectl, service_account="dbt", namespace="etl")

    client = hvac.Client(url=vault_addr)
    with pytest.raises(hvac.exceptions.VaultError):
        client.auth.kubernetes.login(role="csv-processor", jwt=dbt_jwt)

    assert client.token is None, "a client token was issued despite the denied login"
