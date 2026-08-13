"""``AssignmentDocument`` -- the frozen manifest crossing the Airflow-DAG-to-pod boundary.

Derived this session from ARCHITECTURE.md Sec 6.2 (lines 706-731), adapted to
this phase's populated fields only: ``schema_version_id``, ``partition`` and
``policy`` are dropped -- no schema-versioning, partitioning or
quarantine-policy concept exists until Phase 6/8 (04-03-PLAN.md Interfaces).
The shape carries a singular ``file``/``batch`` pair, not the array
``files`` ARCHITECTURE.md's general shape shows: this phase is
one-file-one-batch (03-RESEARCH.md's documented simplification), so
pluralizing now would be premature generality with no consumer.

This model is validated on READ (a later plan's ``ingest`` CLI, running in a
different pod, calls ``AssignmentDocument.model_validate_json`` before
trusting any field) as well as constructed on WRITE
(``dataplat.discovery.discover_files``, this plan). The SAME model serves
both directions, so a schema drift between writer and reader is structurally
impossible.

``extra="forbid"`` is this model's T-04-02 mitigation (04-03-PLAN.md threat
model): the assignment document is technically attacker-influenceable, since
a future second writer to ``s3://metadata/assignments/`` could exist, so an
unrecognized top-level key must fail validation rather than being silently
ignored.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class FileAssignment(BaseModel):
    """The single source file one ingestion run processes.

    Attributes:
        file_id: The file's ``meta.files.file_id``.
        object_uri: The object-store URI the file was discovered at.
        content_sha256: The file's content hash, lowercase hex-encoded --
            the JSON-safe encoding of the raw digest
            ``dataplat.discovery`` separately passes to
            ``MetadataRepository`` as ``bytes``.
        size_bytes: Size of the file, in bytes.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    file_id: int
    object_uri: str
    content_sha256: str
    size_bytes: int


class BatchAssignment(BaseModel):
    """The single batch one ingestion run publishes into.

    Attributes:
        batch_key: The batch's natural key.
        batch_id: The batch's ``meta.batches.batch_id``.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    batch_key: str
    batch_id: int


class AssignmentDocument(BaseModel):
    """The frozen manifest one ``discover_files`` unit hands to a ``KubernetesPodOperator`` task.

    Written once, at discovery time, to
    ``s3://metadata/assignments/<dataset>/<run_id>.json``, and never
    mutated afterward (ORCH-08's frozen-manifest requirement) -- a later
    plan's ``ingest`` CLI reads it back via ``model_validate_json`` and
    must not re-derive any of these fields from a live listing.

    Attributes:
        assignment_version: Version of this model's *shape*, so a future
            incompatible change is detectable by a reader pinned to an
            older version.
        run_id: The ``meta.ingestion_runs.run_id`` this assignment was
            written for.
        idempotency_key: The run's idempotency key (ARCHITECTURE.md Q7) --
            ``try_number``/``dag_run_id`` are deliberately absent from the
            formula that produced this, so an Airflow retry recomputes the
            identical key.
        dataset: The dataset's name.
        config_version_id: The ``meta.config_versions`` row this run is
            configured by.
        config_hash: The canonical-JSON sha256 hash of that config version.
        file: The single source file this run processes.
        batch: The single batch this run publishes into.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    assignment_version: int
    run_id: int
    idempotency_key: str
    dataset: str
    config_version_id: int
    config_hash: str
    file: FileAssignment
    batch: BatchAssignment
