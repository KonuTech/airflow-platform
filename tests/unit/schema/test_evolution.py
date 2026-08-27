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


def test_classify_schema_change_has_no_cross_call_state_leakage_d05() -> None:
    """D-05: a breaking classification for one file never affects another file's call.

    ``classify_schema_change`` is called PER FILE and is pure (no globals,
    no caching, no I/O) -- this proves that property empirically rather
    than just asserting it: a breaking call immediately followed by an
    unrelated compatible call must not raise, and the reverse must also
    hold. If any hidden module-level state existed, a breaking call could
    poison a later, unrelated compatible call (or vice versa) -- exactly
    the cross-file blocking D-05 rules out, matching Phase 4's existing
    independent-per-file Dynamic-Task-Mapping architecture (no new
    run-level or batch-level gating logic is needed here).
    """
    breaking_old = [{"name": "amount", "type": "integer"}]
    breaking_new = [{"name": "amount", "type": "decimal"}]
    compatible_old = [{"name": "id", "type": "string"}]
    compatible_new = [{"name": "id", "type": "string"}, {"name": "note", "type": "string"}]

    with pytest.raises(IncompatibleSchemaError):
        classify_schema_change(breaking_old, breaking_new)

    # Immediately after a breaking call: an unrelated compatible call must
    # NOT raise, proving the preceding breaking call left no state behind.
    findings = classify_schema_change(compatible_old, compatible_new)
    assert len(findings) == 1
    assert findings[0].change_type == "column_added"

    # And the reverse direction: a compatible call followed immediately by
    # a breaking call still raises exactly as it would in isolation.
    with pytest.raises(IncompatibleSchemaError):
        classify_schema_change(breaking_old, breaking_new)


# QUAL-12 requires schema evolution to be tested for compatible and
# breaking changes. That is proven by this whole file, but these two
# tests are named explicitly so a future reader can find where SCHEMA-04
# and SCHEMA-05 are proven without grepping.
#
# Corpus-fixture scope note: the corpus fixture named 16_extra_columns.csv
# (tagged for SCHEMA-05 among other requirements) is deliberately NOT
# reused here. That fixture's expectations describe ROW-LEVEL surplus
# fields -- some data rows in that file simply have more fields than the
# header -- which is RaggedRowGuard's existing row-shape handling over in
# the pipeline engine module, a different concern from this function's
# HEADER-LEVEL column-name-list comparison. No fixture in the corpus
# actually exercises header-level schema evolution end-to-end; that is
# expected, not a gap -- this capability is corpus-independent domain
# logic per 06-PATTERNS.md's "No Analog Found" table, and its test oracle
# is this plan's own behavior specification, proven by the synthetic
# cases in this file, not the corpus.
def test_compatible_change_is_tested() -> None:
    old = [{"name": "id", "type": "string"}]
    new = [{"name": "id", "type": "string"}, {"name": "region", "type": "string"}]

    findings = classify_schema_change(old, new)

    assert len(findings) == 1
    assert findings[0].change_type == "column_added"
    assert findings[0].column == "region"


def test_breaking_change_is_tested() -> None:
    old = [{"name": "id", "type": "string"}, {"name": "region", "type": "string"}]
    new = [{"name": "id", "type": "string"}]

    with pytest.raises(IncompatibleSchemaError) as exc_info:
        classify_schema_change(old, new)

    assert exc_info.value.context["diagnostic_code"] == "schema-column-disappeared"
    assert exc_info.value.context["column"] == "region"


# --- D-13 optional-column absence (debug/ci-pipeline-ingestion-timeout ROUND 15,
# finding 20): a contract column declared `required: false` (ColumnContract.required)
# that is absent from a file is NOT a breaking disappearance -- customers.yaml's own
# signup_country comment states the semantics plainly: "files delivered before this
# column existed never carried it ... not 'reject files missing it'". The caller
# (CsvSource._resolve_schema) names its optional columns explicitly via the
# keyword-only `optional_columns` parameter -- the hashed column mappings themselves
# are NEVER widened with a `required` key, because `hash_schema` hashes the whole
# mapping and any new key would silently re-version every dataset's schema history.


def test_a_disappeared_optional_column_is_compatible_when_named_in_optional_columns() -> None:
    old = [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "signup_country", "type": "string"},
    ]
    new = [{"name": "id", "type": "string"}, {"name": "name", "type": "string"}]

    findings = classify_schema_change(old, new, optional_columns=frozenset({"signup_country"}))

    assert findings == []


def test_a_disappeared_required_column_still_raises_even_alongside_optional_columns() -> None:
    old = [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "signup_country", "type": "string"},
    ]
    new = [{"name": "id", "type": "string"}]

    with pytest.raises(IncompatibleSchemaError) as exc_info:
        classify_schema_change(old, new, optional_columns=frozenset({"signup_country"}))

    assert exc_info.value.context["diagnostic_code"] == "schema-column-disappeared"
    assert exc_info.value.context["column"] == "name"


def test_optional_disappearance_combined_with_a_new_column_reports_only_the_addition() -> None:
    old = [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "signup_country", "type": "string"},
    ]
    new = [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "note", "type": "string"},
    ]

    findings = classify_schema_change(old, new, optional_columns=frozenset({"signup_country"}))

    assert len(findings) == 1
    assert findings[0].change_type == "column_added"
    assert findings[0].column == "note"


def test_omitting_optional_columns_preserves_the_original_breaking_behavior() -> None:
    """No `optional_columns` argument == every contract column is required (back-compat)."""
    old = [{"name": "id", "type": "string"}, {"name": "signup_country", "type": "string"}]
    new = [{"name": "id", "type": "string"}]

    with pytest.raises(IncompatibleSchemaError) as exc_info:
        classify_schema_change(old, new)

    assert exc_info.value.context["diagnostic_code"] == "schema-column-disappeared"
    assert exc_info.value.context["column"] == "signup_country"
