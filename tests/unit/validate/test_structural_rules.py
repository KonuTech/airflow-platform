"""Unit tests for ``dataplat.validate.registry`` -- VALID-03's config-not-code dispatch.

Proves the registry resolves the ``STRUCTURAL`` key to the existing
``RaggedRowGuard`` (D-08: no new detection logic, only a config-addressable
name) and raises ``ConfigurationError`` for an unknown key, mirroring
``PUBLISHER_REGISTRY``/``resolve_publisher``'s own test shape.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dataplat.errors import ConfigurationError
from dataplat.models.identity import RunContext
from dataplat.models.record import RecordChunk
from dataplat.pipeline.engine import RaggedRowGuard
from dataplat.pipeline.protocol import PipelineContext
from dataplat.validate.registry import resolve_validation_rule


def _make_context() -> PipelineContext:
    """Build a placeholder ``PipelineContext`` -- only ``run``/``config.dataset`` are real.

    Mirrors ``tests/unit/test_pipeline_errors.py``'s own helper: every rule
    under test here reads only ``config.dataset`` (D-04's metric label), so a
    ``SimpleNamespace`` stands in for a full ``DatasetConfig``.
    """
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=SimpleNamespace(dataset="test_dataset"),  # type: ignore[arg-type] -- only .dataset is read
        metadata=None,  # type: ignore[arg-type] -- unused by the code under test
        objects=None,  # type: ignore[arg-type] -- unused by the code under test
        db=None,  # type: ignore[arg-type] -- unused by the code under test
        log=None,  # type: ignore[arg-type] -- unused by the code under test
    )


def _chunk(
    rows: list[tuple[str | bool | None, ...]],
    *,
    first_ordinal: int = 0,
    expected_field_count: int = 4,
) -> RecordChunk:
    return RecordChunk(
        rows=tuple(rows),
        first_ordinal=first_ordinal,
        expected_field_count=expected_field_count,
    )


def test_resolve_validation_rule_structural_returns_ragged_row_guard_class() -> None:
    resolved = resolve_validation_rule("STRUCTURAL")

    assert resolved is RaggedRowGuard


def test_resolve_validation_rule_unknown_key_raises_configuration_error() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        resolve_validation_rule("nonsense")

    assert exc_info.value.context["rule_type"] == "nonsense"
    assert "STRUCTURAL" in exc_info.value.context["known"]  # type: ignore[operator]


def test_structural_rule_resolved_from_registry_rejects_a_ragged_row() -> None:
    # VALID-01's row/error_type/diagnostic proof, reusing RaggedRowGuard's
    # existing behavior under the new registry-addressable name -- no new
    # detection logic (D-08).
    guard_class = resolve_validation_rule("STRUCTURAL")
    guard = guard_class()  # type: ignore[call-arg] -- RaggedRowGuard's ctor takes no required args
    chunk = _chunk(
        [("1", "two", "three")],  # 3 fields, chunk expects 4
        first_ordinal=5,
        expected_field_count=4,
    )

    result = guard.apply(_make_context(), chunk)

    assert result.chunk.rows == ()
    assert len(result.rejected) == 1
    rejected = result.rejected[0]
    assert rejected.error_type == "RAGGED_ROW"
    assert rejected.error_column is None
    assert rejected.source_row_number == 5
