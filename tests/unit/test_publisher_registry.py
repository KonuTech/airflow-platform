"""Unit tests for ``dataplat.load.publish.registry`` (plan 04-04 Task 2).

No database needed: these tests prove the registry's lookup/error behavior
in isolation, distinct from ``tests/integration/test_publish_merge.py``'s
proof that ``MergePublisher`` itself publishes correctly against real
PostgreSQL.
"""

from __future__ import annotations

import pytest

from dataplat.errors import ConfigurationError
from dataplat.load.publish.merge import MergePublisher
from dataplat.load.publish.registry import PUBLISHER_REGISTRY, resolve_publisher


def test_publisher_registry_resolves_merge_to_a_merge_publisher_instance() -> None:
    assert set(PUBLISHER_REGISTRY) == {"merge"}
    assert isinstance(PUBLISHER_REGISTRY["merge"], MergePublisher)


def test_resolve_publisher_returns_the_registered_merge_publisher() -> None:
    publisher = resolve_publisher("merge")

    assert isinstance(publisher, MergePublisher)
    assert publisher.name == "merge"
    assert publisher is PUBLISHER_REGISTRY["merge"]


def test_resolve_publisher_raises_configuration_error_for_an_unknown_strategy() -> None:
    with pytest.raises(ConfigurationError, match="no registry entry") as exc_info:
        resolve_publisher("does-not-exist")

    assert exc_info.value.context == {"strategy": "does-not-exist", "known": ["merge"]}
