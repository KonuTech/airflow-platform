"""``PUBLISHER_REGISTRY`` — resolves a dataset config's ``load.strategy`` key to a ``Publisher``.

A plain module-level dict, matching this codebase's own established
"no entry-points machinery for a small, in-process registry" convention.
``sources``-side registration (a later plan) uses a DIFFERENT mechanism for
a DIFFERENT, cross-package-boundary reason -- do not conflate the two: this
registry has zero cross-package-import problem, since ``MergePublisher``/
``OrdersMergePublisher``/``SCDPublisher`` are entirely ``dataplat``-native.
Three entries today (``"merge"`` for legacy/other whole-table-upsert
datasets, ``"merge_orders"`` for ``normalized.orders``, ``"scd"`` for
``normalized.customers`` since Phase 10 -- D-07, 10-CONTEXT.md) -- each
``Publisher`` is deliberately single-dataset (its own module docstring), so
a second real dataset needs its own registry entry, not a shared one.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dataplat.errors import ConfigurationError
from dataplat.load.publish.merge import MergePublisher
from dataplat.load.publish.merge_orders import OrdersMergePublisher
from dataplat.load.publish.scd import SCDPublisher

if TYPE_CHECKING:
    from dataplat.load.publish.protocol import Publisher

PUBLISHER_REGISTRY: dict[str, Publisher] = {
    "merge": MergePublisher(),
    "merge_orders": OrdersMergePublisher(),
    "scd": SCDPublisher(),
}


def resolve_publisher(strategy: str) -> Publisher:
    """Resolve a ``configs/datasets/*.yaml`` ``load.strategy`` key to its ``Publisher``.

    Args:
        strategy: The strategy key to resolve, e.g. ``"merge"``
            (``DatasetConfig.load.strategy``).

    Returns:
        The registered ``Publisher`` for ``strategy``.

    Raises:
        ConfigurationError: ``strategy`` names no registry entry.
    """
    try:
        return PUBLISHER_REGISTRY[strategy]
    except KeyError:
        msg = (
            "a config names a source/deduplication/publisher strategy key "
            f"that has no registry entry: {strategy!r}"
        )
        raise ConfigurationError(
            msg,
            context={"strategy": strategy, "known": sorted(PUBLISHER_REGISTRY)},
        ) from None
