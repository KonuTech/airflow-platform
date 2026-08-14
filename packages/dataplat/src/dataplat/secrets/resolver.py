"""SecretsResolver -- ``env://``, ``file://`` and ``vault://``; fails closed on everything else.

The caller never learns which backend actually served a secret (SEC-15): a
config or call site holds an opaque secret reference string, e.g.
``env://DB_PASSWORD`` or ``vault://etl/analytics-db#dsn``, and
``resolve_secret()`` is the only place that interprets the scheme. Any
unrecognized scheme, and any malformed or schemeless reference, raises
rather than silently passing the raw reference through. That fail-closed
behavior is SEC-15's entire point, and it is what makes the
Kubernetes-Secrets-to-Vault swap in Phase 5 touch only this module's
internals, never a call site (D3).
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlsplit

import hvac
import hvac.exceptions

from dataplat.errors import SecretResolutionError

# Lazily authenticated, cached for the lifetime of the process (module-level
# singleton). A KPO pod calls resolve_secret("vault://...") multiple times
# per run (csv_processor.cli._build_common() resolves three references in a
# row) -- authenticating once here, not once per call, avoids tripling Vault
# load and audit-log noise for one legitimate credential need.
_client: hvac.Client | None = None


def _vault_client() -> hvac.Client:
    """Return a cached, authenticated ``hvac.Client``, authenticating once per process.

    ``VAULT_ADDR``/``VAULT_K8S_ROLE`` are read via plain ``os.environ[...]``
    -- non-secret configuration, not credentials, the same way
    ``DATAPLAT_S3_ENDPOINT_URL`` is already a plain value in ``kpo.py``
    rather than a ``resolve_secret()``-mediated reference. The pod's own
    projected ServiceAccount token is read from its default, always-present
    path -- no explicit volume mount needed.

    Returns:
        The cached (or newly authenticated) ``hvac.Client``.
    """
    global _client  # noqa: PLW0603 -- the documented lazy-singleton pattern this module exists to provide
    if _client is None:
        client = hvac.Client(url=os.environ["VAULT_ADDR"])
        token_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
        client.auth.kubernetes.login(
            role=os.environ["VAULT_K8S_ROLE"],
            jwt=token_path.read_text(encoding="utf-8"),
        )
        _client = client
    return _client


def resolve_secret(ref: str) -> str:
    """Resolve an opaque secret reference to its value.

    Args:
        ref: An opaque secret reference, e.g. ``"env://DB_PASSWORD"`` or
            ``"vault://etl/analytics-db#dsn"``.

    Returns:
        The resolved secret value.

    Raises:
        SecretResolutionError: ``ref``'s scheme is not ``env://``,
            ``file://`` or ``vault://`` — including any malformed or
            schemeless reference — the named environment variable is unset,
            the referenced file cannot be read, the ``vault://`` reference
            is malformed (not ``scheme://mount/path#field``), or the Vault
            read itself fails. A raw, unresolved reference string is never
            returned from any code path; every unsupported case raises
            instead.
    """
    parsed = urlsplit(ref)
    if parsed.scheme == "env":
        value = os.environ.get(parsed.netloc or parsed.path.lstrip("/"))
        if value is None:
            msg = f"environment variable not set for ref {ref!r}"
            raise SecretResolutionError(msg, context={"ref": ref})
        return value
    if parsed.scheme == "file":
        try:
            return Path(parsed.path).read_text(encoding="utf-8").strip()
        except OSError as exc:
            msg = f"cannot read secret file for ref {ref!r}: {exc}"
            raise SecretResolutionError(msg, context={"ref": ref}) from exc
    if parsed.scheme == "vault":
        mount_point = parsed.netloc
        path = parsed.path.lstrip("/")
        field = parsed.fragment
        if not (mount_point and path and field):
            msg = f"malformed vault:// ref (need scheme://mount/path#field): {ref!r}"
            raise SecretResolutionError(msg, context={"ref": ref})
        try:
            secret = _vault_client().secrets.kv.v2.read_secret_version(
                mount_point=mount_point,
                path=path,
            )
            return str(secret["data"]["data"][field])
        except hvac.exceptions.VaultError as exc:
            msg = f"vault read failed for ref {ref!r}: {exc}"
            raise SecretResolutionError(msg, context={"ref": ref}) from exc
        except KeyError as exc:
            msg = f"vault secret at {mount_point}/{path} has no field {field!r}"
            raise SecretResolutionError(msg, context={"ref": ref}) from exc
    msg = f"unsupported secret ref scheme {parsed.scheme!r} in {ref!r}"
    raise SecretResolutionError(msg, context={"ref": ref})
