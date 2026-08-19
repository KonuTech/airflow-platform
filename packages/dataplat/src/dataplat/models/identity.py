"""Identity value objects — the vocabulary ``meta.*`` rows and ``PipelineContext`` share.

Every model here is a frozen, slotted dataclass mirroring the
identity-relevant subset of ``meta.datasets``, ``meta.files``,
``meta.batches`` and ``meta.ingestion_runs`` (ARCHITECTURE.md Q2.1). None of
these are ORM models — they carry only the fields identity logic needs, not
full row shapes.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DatasetRef:
    """A reference to one registered dataset.

    Attributes:
        dataset_id: Surrogate primary key of the dataset in ``meta.datasets``.
        dataset_name: The dataset's unique, human-readable name.
    """

    dataset_id: int
    dataset_name: str


@dataclass(frozen=True, slots=True)
class FileIdentity:
    """The identity of one arrived file, split by arrival vs. content.

    ARCHITECTURE.md's Q2.1 design note: ``object_uri`` identifies an
    *arrival*, ``content_sha256`` identifies *content*. The same bytes
    re-uploaded to a new path is a new arrival of a known file — a distinct
    situation from a genuinely new file.

    Attributes:
        object_uri: The object-store URI the file arrived at, e.g.
            ``s3://raw/customers/2026/08/11/customers_20260811.csv``.
        content_sha256: The file's content hash — the real file identity.
        hash_version: Version of the hashing scheme that produced
            ``content_sha256``, so a future scheme change is detectable.
        size_bytes: Size of the file, in bytes.
        filename: The file's base name, independent of its full URI.
    """

    object_uri: str
    content_sha256: bytes
    hash_version: int
    size_bytes: int
    filename: str


@dataclass(frozen=True, slots=True)
class BatchIdentity:
    """A reference to one batch of files processed together.

    Attributes:
        batch_id: Surrogate primary key of the batch in ``meta.batches``.
        batch_key: The batch's natural key, e.g.
            ``<dataset>:<business_date>:<seq>``.
        dataset_id: The dataset this batch belongs to.
    """

    batch_id: int
    batch_key: str
    dataset_id: int


@dataclass(frozen=True, slots=True)
class RunContext:
    """Identity and correlation fields for one ingestion run attempt.

    ``attempt`` is deliberately excluded from any future idempotency-key
    computation (ARCHITECTURE.md Q7) — a retry's ``RunContext`` differs only
    in this one field, so the retry shares its predecessor's idempotency key.

    Attributes:
        run_id: Surrogate primary key of the run in ``meta.ingestion_runs``.
        idempotency_key: The unique key that makes retries free (Q7).
        attempt: The 1-based attempt number (``try_number`` in
            ARCHITECTURE.md's vocabulary). Defaults to ``1``, the first
            attempt.
        dag_id: Airflow DAG id, when the run was triggered by Airflow.
        dag_run_id: Airflow DAG run id, when applicable.
        task_id: Airflow task id, when applicable.
        trace_id: OTel trace id for cross-process correlation, when tracing
            is active.
        span_id: OTel span id for cross-process correlation, when tracing is
            active.
        file_id: The single file this run processes, when applicable.
            Populated by the ``ingest`` CLI (plan 04-05) from the
            ``AssignmentDocument`` it parses (``file.file_id``), and consumed
            as ``ctx.run.file_id`` by ``StagingLoader.load()`` (plan 04-04)
            and by the ``finalize_publication()`` call inside ``run_ingest``
            (plan 04-05) — neither assumes nor re-derives it independently.
            Defaults to ``None``.
        batch_id: The batch this run processes, when applicable. Populated
            and consumed the same way as ``file_id``, from
            ``AssignmentDocument.batch.batch_id``. Defaults to ``None``.
        batch_expected_row_count: This batch's claimed control-total row
            count (D-23, VALID-06, plan 09-03), when its triggering
            ``AssignmentDocument`` carried a parsed ``_BATCH_COMPLETE``
            manifest. Populated by the ``stage`` CLI command from
            ``doc.batch_complete_manifest.expected_row_count``, ready for a
            later plan (09-07) to compare against what actually got staged
            -- never trusted as ground truth on its own. Defaults to
            ``None`` (no marker configured, or the marker carried no
            manifest).
        batch_expected_checksum: This batch's claimed control checksum
            (D-23, VALID-06, plan 09-03), populated and consumed the same
            way as ``batch_expected_row_count``, from
            ``doc.batch_complete_manifest.expected_checksum``. Defaults to
            ``None``.
        map_index: This run's Airflow Dynamic Task Mapping index (0-based),
            when triggered by Airflow's mapped ``ingest`` task instance.
            ``None`` when not applicable.
        k8s_namespace: The Kubernetes namespace the launching
            KubernetesPodOperator resolved for this pod's own spec (``etl``
            in production). ``None`` when not applicable.
    """

    run_id: int
    idempotency_key: str
    attempt: int = 1
    dag_id: str | None = None
    dag_run_id: str | None = None
    task_id: str | None = None
    trace_id: str | None = None
    span_id: str | None = None
    file_id: int | None = None
    batch_id: int | None = None
    batch_expected_row_count: int | None = None
    batch_expected_checksum: str | None = None
    map_index: int | None = None
    k8s_namespace: str | None = None
