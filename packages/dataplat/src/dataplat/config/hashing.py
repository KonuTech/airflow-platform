"""Canonical-JSON sha256 hashing for ``DatasetConfig`` — the one recipe every later hash follows.

Implements ARCHITECTURE.md §5.2's exact canonicalization: dump the
*validated model*, never raw YAML text, through ``model_dump(mode="json")``,
serialize with sorted keys and no incidental whitespace, then sha256 the
UTF-8 bytes. Two intentional consequences (ARCHITECTURE.md Q5.2): reordering
YAML keys or editing a comment produces no new hash; changing one field's
value always does.

This is the first hash recipe this project ships (`03-PATTERNS.md` Cluster
C) and the one every later hash (`meta.files.content_sha256`, SCD2's
``_record_hash``) is measured against — see PITFALLS.md C6 for the design
rules this recipe follows: compute the hash in exactly one place (Python,
never recomputed in SQL), and store a ``hash_version`` beside every hash
value it produces.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dataplat.config.model import DatasetConfig

# Bumping this constant is the only sanctioned way to signal a change to the
# canonicalization recipe below — every meta.config_versions.hash_version
# value traces back to this module constant (PITFALLS.md #1/C6).
CONFIG_HASH_VERSION: int = 1


def hash_config(config: DatasetConfig) -> tuple[str, int]:
    """Hash a validated ``DatasetConfig`` via ARCHITECTURE.md §5.2's canonical-JSON recipe.

    Args:
        config: The already-validated config to hash. Never pass raw YAML
            text or an unvalidated mapping — the canonicalization is defined
            over the validated model's JSON-mode dump, not the source
            document.

    Returns:
        A ``(config_hash, hash_version)`` tuple: ``config_hash`` is the
        lowercase hex sha256 digest of the canonical JSON encoding;
        ``hash_version`` is always ``CONFIG_HASH_VERSION``.
    """
    canonical = json.dumps(
        config.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    config_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return config_hash, CONFIG_HASH_VERSION
