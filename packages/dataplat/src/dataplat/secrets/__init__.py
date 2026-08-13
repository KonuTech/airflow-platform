"""SecretsResolver: opaque secret references resolved to values (SEC-15).

Callers import from the submodule directly, e.g.
``from dataplat.secrets.resolver import resolve_secret`` — this package
marker re-exports nothing, matching ``dataplat/__init__.py``'s own shallow
re-export convention.
"""

from __future__ import annotations
