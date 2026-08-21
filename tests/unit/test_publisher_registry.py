"""Unit tests for ``dataplat.load.publish.registry`` (plan 04-04 Task 2; extended 08-05, 10-04).

No database needed: these tests prove the registry's lookup/error behavior
in isolation, distinct from ``tests/integration/test_publish_orders.py``'s/
``tests/integration/test_publish_scd.py``'s proof that ``OrdersMergePublisher``/
``SCDPublisher`` themselves publish correctly against real PostgreSQL.
"""

from __future__ import annotations

import pytest

from dataplat.errors import ConfigurationError
from dataplat.load.publish.merge import MergePublisher
from dataplat.load.publish.merge_orders import OrdersMergePublisher
from dataplat.load.publish.registry import PUBLISHER_REGISTRY, resolve_publisher
from dataplat.load.publish.scd import SCDPublisher


def test_publisher_registry_resolves_merge_to_a_merge_publisher_instance() -> None:
    assert set(PUBLISHER_REGISTRY) == {"merge", "merge_orders", "scd"}
    assert isinstance(PUBLISHER_REGISTRY["merge"], MergePublisher)


def test_resolve_publisher_returns_the_registered_merge_publisher() -> None:
    publisher = resolve_publisher("merge")

    assert isinstance(publisher, MergePublisher)
    assert publisher.name == "merge"
    assert publisher is PUBLISHER_REGISTRY["merge"]


def test_resolve_publisher_returns_the_same_orders_merge_publisher_singleton_every_call() -> None:
    """08-05 Task 1: `resolve_publisher("merge_orders") is` the same singleton every call."""
    first = resolve_publisher("merge_orders")
    second = resolve_publisher("merge_orders")

    assert isinstance(first, OrdersMergePublisher)
    assert first.name == "merge_orders"
    assert first is second
    assert first is PUBLISHER_REGISTRY["merge_orders"]


def test_resolve_publisher_returns_the_same_scd_publisher_singleton_every_call() -> None:
    """Phase 10 plan 04: `resolve_publisher("scd") is` the same singleton every call."""
    first = resolve_publisher("scd")
    second = resolve_publisher("scd")

    assert isinstance(first, SCDPublisher)
    assert first.name == "scd"
    assert first is second
    assert first is PUBLISHER_REGISTRY["scd"]


def test_resolve_publisher_raises_configuration_error_for_an_unknown_strategy() -> None:
    with pytest.raises(ConfigurationError, match="no registry entry") as exc_info:
        resolve_publisher("does-not-exist")

    assert exc_info.value.context == {
        "strategy": "does-not-exist",
        "known": ["merge", "merge_orders", "scd"],
    }
