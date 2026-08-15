"""Unit tests for ``dataplat.schema.evolution`` -- SCHEMA-04/SCHEMA-05's classifier proof.

Dual-analog test shape (06-PATTERNS.md Cluster 6/18): ``pytest.raises(...)``
for the BREAKING path, mirroring ``tests/unit/test_errors.py``'s raise-path
pattern, and direct return-value assertions for the COMPATIBLE path,
mirroring ``tests/unit/test_pipeline_errors.py``'s errors-as-values pattern.
"""

from __future__ import annotations

import pytest

from dataplat.errors import IncompatibleSchemaError
from dataplat.schema.evolution import classify_schema_change


def test_a_genuinely_new_column_is_compatible_and_does_not_raise() -> None:
    old = [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "amount", "type": "decimal"},
    ]
    new = [*old, {"name": "note", "type": "string"}]

    findings = classify_schema_change(old, new)

    assert len(findings) == 1
    assert findings[0].change_type == "column_added"
    assert findings[0].column == "note"


def test_a_disappeared_column_raises_incompatible_schema_error() -> None:
    old = [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "amount", "type": "decimal"},
    ]
    new = [{"name": "id", "type": "string"}, {"name": "name", "type": "string"}]

    with pytest.raises(IncompatibleSchemaError) as exc_info:
        classify_schema_change(old, new)

    assert exc_info.value.context["diagnostic_code"] == "schema-column-disappeared"
    assert exc_info.value.context["column"] == "amount"


def test_a_rename_is_indistinguishable_from_disappearance_and_raises() -> None:
    """amount -> total is structurally "amount disappeared AND total appeared" (D-02).

    Disappearance dominates: the coincidental appearance of ``total`` does
    not rescue the file, and the raised error identifies ``amount`` (the
    disappeared column), never ``total``.
    """
    old = [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "amount", "type": "decimal"},
    ]
    new = [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "total", "type": "decimal"},
    ]

    with pytest.raises(IncompatibleSchemaError) as exc_info:
        classify_schema_change(old, new)

    assert exc_info.value.context["diagnostic_code"] == "schema-column-disappeared"
    assert exc_info.value.context["column"] == "amount"


def test_a_retyped_column_raises_incompatible_schema_error_naming_both_types() -> None:
    old = [{"name": "amount", "type": "integer"}]
    new = [{"name": "amount", "type": "decimal"}]

    with pytest.raises(IncompatibleSchemaError) as exc_info:
        classify_schema_change(old, new)

    assert exc_info.value.context["diagnostic_code"] == "schema-column-retyped"
    assert exc_info.value.context["column"] == "amount"
    assert exc_info.value.context["old_type"] == "integer"
    assert exc_info.value.context["new_type"] == "decimal"


def test_no_change_at_all_returns_an_empty_list_and_does_not_raise() -> None:
    columns = [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "amount", "type": "decimal"},
    ]

    findings = classify_schema_change(columns, columns)

    assert findings == []


def test_a_simultaneous_addition_and_disappearance_raises_breaking_dominates() -> None:
    """One new column AND one disappeared column in the same comparison: still raises.

    A compatible addition never partially rescues a breaking file (D-02) --
    ``note``'s coincidental appearance does not stop ``amount``'s
    disappearance from raising.
    """
    old = [{"name": "id", "type": "string"}, {"name": "amount", "type": "decimal"}]
    new = [{"name": "id", "type": "string"}, {"name": "note", "type": "string"}]

    with pytest.raises(IncompatibleSchemaError) as exc_info:
        classify_schema_change(old, new)

    assert exc_info.value.context["diagnostic_code"] == "schema-column-disappeared"
    assert exc_info.value.context["column"] == "amount"
