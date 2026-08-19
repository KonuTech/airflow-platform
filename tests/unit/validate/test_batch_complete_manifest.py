"""Unit tests for ``BatchCompleteManifest``/``parse_batch_complete_manifest`` (VALID-06, D-23).

09-03-PLAN.md Task 1's own five ``<behavior>`` bullets, proven here:

1. A valid ``{"expected_row_count": 1200, "expected_checksum": "abc123"}`` body parses to a
   ``BatchCompleteManifest`` with both fields set.
2. ``expected_checksum`` is optional -- ``{"expected_row_count": 1200}`` parses with
   ``expected_checksum=None``.
3. ``{"expected_row_count": -1}`` raises a validation error (V5: non-negative).
4. An unrecognized top-level key raises (``extra="forbid"``, matching ``AssignmentDocument``'s
   own T-04-02 precedent for attacker-influence-adjacent content).
5. ``parse_batch_complete_manifest(raw_bytes_or_str)`` wraps a ``pydantic.ValidationError`` into a
   ``DataPlatformError`` (or the nearest existing config/validation subclass) carrying
   ``context={"marker_key": ...}`` rather than letting ``ValidationError`` propagate raw.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dataplat.errors import DataPlatformError
from dataplat.validate.batch_complete_manifest import (
    BatchCompleteManifest,
    parse_batch_complete_manifest,
)


def test_valid_manifest_body_parses_both_fields() -> None:
    """Behavior 1: a valid body with both fields parses to a manifest carrying both."""
    manifest = BatchCompleteManifest.model_validate_json(
        '{"expected_row_count": 1200, "expected_checksum": "abc123"}',
    )

    assert manifest.expected_row_count == 1200
    assert manifest.expected_checksum == "abc123"


def test_expected_checksum_is_optional() -> None:
    """Behavior 2: ``expected_checksum`` is optional, defaulting to ``None``."""
    manifest = BatchCompleteManifest.model_validate_json('{"expected_row_count": 1200}')

    assert manifest.expected_row_count == 1200
    assert manifest.expected_checksum is None


def test_negative_row_count_raises_validation_error() -> None:
    """Behavior 3: V5 -- a negative ``expected_row_count`` is rejected."""
    with pytest.raises(ValidationError):
        BatchCompleteManifest.model_validate_json('{"expected_row_count": -1}')


def test_unrecognized_top_level_key_raises_validation_error() -> None:
    """Behavior 4: ``extra="forbid"`` rejects an unrecognized top-level key."""
    with pytest.raises(ValidationError):
        BatchCompleteManifest.model_validate_json('{"expected_row_count": 5, "bogus": "x"}')


def test_parse_batch_complete_manifest_wraps_validation_error() -> None:
    """Behavior 5: a malformed body raises a caught, structured ``DataPlatformError``.

    Never a raw ``pydantic.ValidationError``, carrying ``context={"marker_key": ...}``.
    """
    marker_key = "batch/_BATCH_COMPLETE"

    with pytest.raises(DataPlatformError) as exc_info:
        parse_batch_complete_manifest('{"expected_row_count": -1}', marker_key=marker_key)

    assert exc_info.value.context["marker_key"] == marker_key
    assert not isinstance(exc_info.value, ValidationError)


def test_parse_batch_complete_manifest_returns_manifest_on_success() -> None:
    """A valid body handed to the parser (not just the model directly) returns a manifest."""
    manifest = parse_batch_complete_manifest(
        '{"expected_row_count": 42, "expected_checksum": "deadbeef"}',
        marker_key="batch/_BATCH_COMPLETE",
    )

    assert manifest.expected_row_count == 42
    assert manifest.expected_checksum == "deadbeef"
