"""SecretsResolver — ``env://`` and ``file://`` today; fails closed on everything else.

The caller never learns which backend actually served a secret (SEC-15): a
config or call site holds an opaque secret reference string, e.g.
``env://DB_PASSWORD`` or ``file:///vault/secrets/analytical-db``, and
``resolve_secret()`` is the only place that interprets the scheme.
``vault://`` is real — it is Phase 5's, not this phase's — and any
unrecognized scheme (including ``vault://`` before Phase 5 implements it, and
any malformed or schemeless reference) raises rather than silently passing
the raw reference through. That fail-closed behavior is SEC-15's entire
point, and it is what makes the Kubernetes-Secrets-to-Vault swap in Phase 5
touch only this module's internals, never a call site (D3).
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlsplit

from dataplat.errors import SecretResolutionError


def resolve_secret(ref: str) -> str:
    """Resolve an opaque secret reference to its value.

    Args:
        ref: An opaque secret reference, e.g. ``"env://DB_PASSWORD"`` or
            ``"file:///vault/secrets/analytical-db"``.

    Returns:
        The resolved secret value.

    Raises:
        SecretResolutionError: ``ref``'s scheme is not ``env://`` or
            ``file://`` — including ``vault://`` and any malformed or
            schemeless reference — the named environment variable is unset,
            or the referenced file cannot be read. A raw, unresolved
            reference string is never returned from any code path; every
            unsupported case raises instead.
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
    msg = f"unsupported secret ref scheme {parsed.scheme!r} in {ref!r}"
    raise SecretResolutionError(msg, context={"ref": ref})
