"""Standalone Vault credential resolver for the dbt image's entrypoint.

Duplicated from `packages/dataplat/src/dataplat/secrets/resolver.py`'s
`_vault_client()`/`vault://` handling, NEVER imported from it -- this image
deliberately contains no `dataplat` package (ADR-0004's two-image discipline
extended to three; see `docker/dbt/Dockerfile`'s header comment). This exact
duplication-not-import discipline mirrors `airflow/dags/_common/
integrity_gate.py`'s own established precedent for `_reject_file`: "the SQL
shape below is duplicated from get_or_create_dataset/create_file, not
imported from it."

`VAULT_ADDR`/`VAULT_K8S_ROLE` are the same two env vars `kpo.py` already sets
platform-wide for every pod that needs Vault. This script resolves five
`vault://etl/dbt-db#{field}` references -- the KV path `etl/dbt-db` is
created by plan 08.1-03 -- into `DBT_PG_HOST`/`DBT_PG_PORT`/`DBT_PG_USER`/
`DBT_PG_PASSWORD`/`DBT_PG_DBNAME` process env vars, which `dbt`'s own
`profiles.yml` reads via `env_var()` calls. Those calls must already be
populated before `dbt` itself starts, which is why this script is the
image's `ENTRYPOINT`, not `dbt` directly.

A resolved secret value is NEVER logged or printed anywhere in this module --
matching `dataplat.secrets.resolver`'s own no-log discipline (SEC-08's audit
trail lives in Vault's own audit log, not in pod stdout).
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import hvac
import hvac.exceptions

# OBS-03: no `print()` anywhere outside scripts/**/tools/corpus/__main__.py
# (tests/policy/test_print_ban_scope.py) -- stdlib `logging`, matching
# airflow/dags/csv_ingest_customers.py's/csv_ingest_orders.py's own
# `logging.getLogger(__name__)` pattern for non-`dataplat` modules that
# can't use `dataplat.observability`'s structlog configuration.
log = logging.getLogger(__name__)

_VAULT_MOUNT_POINT = "etl"
_VAULT_SECRET_PATH = "dbt-db"  # noqa: S105 -- a Vault KV path segment, not a credential value

# field -> env var this script writes it to, for dbt's profiles.yml env_var()
# calls to read.
_FIELD_TO_ENV_VAR = {
    "host": "DBT_PG_HOST",
    "port": "DBT_PG_PORT",
    "user": "DBT_PG_USER",
    "password": "DBT_PG_PASSWORD",
    "dbname": "DBT_PG_DBNAME",
}


def _vault_client() -> hvac.Client:
    """Authenticate to Vault via Kubernetes auth using the pod's own SA token.

    Mirrors `dataplat.secrets.resolver._vault_client()` exactly (same two env
    vars, same token path, same login call) but is a standalone function --
    this script never imports `dataplat`.
    """
    client = hvac.Client(url=os.environ["VAULT_ADDR"])
    token_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
    client.auth.kubernetes.login(
        role=os.environ["VAULT_K8S_ROLE"],
        jwt=token_path.read_text(encoding="utf-8"),
    )
    return client


def _resolve_dbt_db_credentials() -> None:
    """Resolve all five `vault://etl/dbt-db#{field}` fields into DBT_PG_* env vars.

    Hardcodes the literal Vault KV path `etl/dbt-db`, matching how `kpo.py`
    hardcodes `vault://etl/analytics-db#dsn` for the csv-processor image --
    this is a fixed, single-purpose credential lookup, not a general-purpose
    resolver.
    """
    client = _vault_client()
    secret = client.secrets.kv.v2.read_secret_version(
        mount_point=_VAULT_MOUNT_POINT,
        path=_VAULT_SECRET_PATH,
    )
    data = secret["data"]["data"]
    for field, env_var in _FIELD_TO_ENV_VAR.items():
        os.environ[env_var] = str(data[field])


def main() -> None:
    """Populate DBT_PG_* env vars from Vault, then exec into `dbt build`.

    `os.execvp`, not `subprocess.run`: the resolver process is REPLACED by
    `dbt`, so PID 1 becomes `dbt` itself (proper signal handling, matching a
    standard entrypoint-wrapper pattern) rather than staying a Python parent
    process babysitting a `dbt` child.
    """
    try:
        _resolve_dbt_db_credentials()
    except (hvac.exceptions.VaultError, KeyError, OSError) as exc:
        # Never include a resolved secret value in this message -- only the
        # ref shape and the exception, matching
        # dataplat.secrets.resolver.resolve_secret's own no-secret-in-error
        # discipline.
        log.exception(
            "resolve_secrets: failed to resolve vault://%s/%s#<field>",
            _VAULT_MOUNT_POINT,
            _VAULT_SECRET_PATH,
        )
        raise SystemExit(1) from exc

    os.execvp(  # noqa: S606 -- fixed argv, no shell, no user input; PID-1 replacement is the point
        "dbt",  # noqa: S607 -- resolved from PATH deliberately, matching csv-processor's own entrypoint convention
        ["dbt", "build", "--project-dir", "/app/dbt", "--profiles-dir", "/app/dbt", *sys.argv[1:]],
    )


if __name__ == "__main__":
    main()
