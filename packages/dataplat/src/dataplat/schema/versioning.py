"""``hash_schema`` — the canonical-JSON sha256 recipe for a resolved column list (SCHEMA-03).

Deliberately mirrors ``dataplat/config/hashing.py``'s ``hash_config`` recipe
(PITFALLS.md C6: compute every hash in exactly one place, never invent a
second canonicalization recipe): the same ``hashlib.sha256`` over a
``json.dumps(..., separators=(",", ":"), ensure_ascii=False)`` canonical
encoding, the same ``(hash, hash_version)`` return shape, and the same
module-level ``..._HASH_VERSION`` constant convention — bumped only when the
canonicalization recipe itself changes, since every
``meta.schema_versions.hash_version`` value traces back to this module
constant (PITFALLS.md #1/C6).

One deliberate divergence from ``hash_config``: this recipe does **not**
sort keys. ``hash_config`` hashes a single YAML-sourced *mapping*, where key
order is an accident of how a human wrote the document — two config
documents with the same keys in a different order must hash identically, so
``hash_config`` uses ``sort_keys=True`` to normalize that accident away.
``hash_schema`` hashes an ordered *list* of column descriptors, where the
list's element order is a column's POSITION in the file — semantically
load-bearing for a CSV schema (swapping two columns' positions is a real
structural change), never an accident to normalize away. ``sort_keys=False``
keeps both the list's element order AND each column dict's own key order
exactly as the caller supplied them: a caller is expected to pass an
already-canonically-shaped column dict (``name``/``type``/``nullable``/
``position``/``format``, ARCHITECTURE.md §2.2 line 232) every time, so no
silent reordering is ever introduced by this function itself.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

# Bumping this constant is the only sanctioned way to signal a change to the
# canonicalization recipe below — every meta.schema_versions.hash_version
# value traces back to this module constant (PITFALLS.md #1/C6).
SCHEMA_HASH_VERSION: int = 1


def hash_schema(columns: Sequence[Mapping[str, object]]) -> tuple[str, int]:
    """Hash a resolved, ordered column list via the canonical-JSON sha256 recipe.

    Mirrors ``dataplat.config.hashing.hash_config`` exactly, with one
    deliberate divergence: keys are never sorted (see module docstring).
    Reordering the SAME set of columns in ``columns`` changes the returned
    hash, because column position is part of a CSV schema's identity.

    Args:
        columns: An already-ordered sequence of JSON-serializable column
            descriptor mappings, matching the shape
            ``meta.schema_versions.columns`` stores (``name``/``type``/
            ``nullable``/``position``/``format`` per ARCHITECTURE.md §2.2).
            Never re-ordered or otherwise normalized by this function —
            pass columns in the exact order they must be hashed in.

    Returns:
        A ``(schema_hash, hash_version)`` tuple: ``schema_hash`` is the
        lowercase hex sha256 digest of the canonical JSON encoding;
        ``hash_version`` is always ``SCHEMA_HASH_VERSION``.
    """
    canonical = json.dumps(
        list(columns),
        sort_keys=False,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    schema_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return schema_hash, SCHEMA_HASH_VERSION
