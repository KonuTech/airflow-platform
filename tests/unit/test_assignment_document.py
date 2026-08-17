"""Unit tests for ``dataplat.models.assignment``/``dataplat.models.receipt`` (04-03-PLAN.md Task 1).

``AssignmentDocument`` is written by ``discover_files`` and read back by a
later plan's ``ingest`` CLI running in a different pod -- the same model on
both sides, so a writer/reader schema drift is structurally impossible (see
the module's own docstring). ``extra="forbid"`` is exercised directly here
as T-04-02's mitigation proof. ``Receipt`` is tested alongside it: both are
Task 1's "cross-boundary data contracts" pair.
"""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from dataplat.models.assignment import AssignmentDocument
from dataplat.models.receipt import Receipt

# Verbatim shape from 04-03-PLAN.md's Interfaces section (adapted from
# ARCHITECTURE.md Sec 6.2) -- hashes padded to real sha256-hex length (64
# hex chars) so this document also exercises realistic field sizes.
_ASSIGNMENT_DOCUMENT = {
    "assignment_version": 1,
    "run_id": 8123,
    "idempotency_key": "a3f1" + "0" * 60,
    "dataset": "customers",
    "config_version_id": 7,
    "config_hash": "9c2e" + "0" * 60,
    "file": {
        "file_id": 9911,
        "object_uri": "s3://raw/customers/2026/08/11/customers_20260811.csv",
        "content_sha256": "7d1a" + "0" * 60,
        "size_bytes": 18234421,
    },
    "batch": {"batch_key": "customers:7d1a000000000000", "batch_id": 442},
}


def test_assignment_document_round_trips_every_field_from_the_interfaces_json_shape() -> None:
    document = AssignmentDocument.model_validate_json(json.dumps(_ASSIGNMENT_DOCUMENT))

    assert document.assignment_version == 1
    assert document.run_id == 8123
    assert document.idempotency_key == _ASSIGNMENT_DOCUMENT["idempotency_key"]
    assert document.dataset == "customers"
    assert document.config_version_id == 7
    assert document.config_hash == _ASSIGNMENT_DOCUMENT["config_hash"]
    assert document.file.file_id == 9911
    assert document.file.object_uri == _ASSIGNMENT_DOCUMENT["file"]["object_uri"]
    assert document.file.content_sha256 == _ASSIGNMENT_DOCUMENT["file"]["content_sha256"]
    assert document.file.size_bytes == 18234421
    assert document.batch.batch_key == "customers:7d1a000000000000"
    assert document.batch.batch_id == 442

    # Round-trip through dump -> validate again must reproduce an equal model
    # -- the writer-side/reader-side symmetry the module docstring promises.
    reparsed = AssignmentDocument.model_validate_json(document.model_dump_json())
    assert reparsed == document


def test_assignment_document_rejects_an_unrecognized_top_level_key() -> None:
    """T-04-02's mitigation proof: ``extra="forbid"`` is the untrusted-input validation boundary."""
    tampered = {**_ASSIGNMENT_DOCUMENT, "unexpected_field": "should not be allowed"}

    with pytest.raises(ValidationError):
        AssignmentDocument.model_validate_json(json.dumps(tampered))


def test_receipt_model_dump_json_is_valid_and_under_the_4096_byte_xcom_budget() -> None:
    receipt = Receipt(
        run_id=8123,
        status="SUCCEEDED",
        rows_read=182734,
        rows_loaded=182722,
        rows_invalid=0,
        rows_deduplicated=12,
        duration_ms=41022,
        rows_quarantined=0,
        report_uri=None,
    )

    dumped = receipt.model_dump_json()

    assert len(dumped.encode("utf-8")) < 4096
    reparsed = Receipt.model_validate_json(dumped)
    assert reparsed == receipt
