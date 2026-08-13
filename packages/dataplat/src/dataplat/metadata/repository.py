"""MetadataRepository — the typed CRUD surface for the five slice tables.

Covers ``meta.datasets``, ``meta.files``, ``meta.batches``,
``meta.batch_files`` and ``meta.ingestion_runs`` (ARCHITECTURE.md §2.1,
lines 141-227). This is the proof that META-01's schema is not merely
DDL-valid but genuinely usable from typed Python code — every FK in the
dataset → file → batch → batch_files → ingestion_run chain resolves through
these methods with no hand-written SQL at any call site.

Every ID here is a plain ``int`` — the database surrogate key — not a
``dataplat.models.identity`` dataclass; those value objects serve
``PipelineContext``/in-memory use, not this CRUD layer.
"""

from __future__ import annotations

from typing import Protocol


class MetadataRepository(Protocol):
    """Typed CRUD operations over the `meta` schema's five slice tables."""

    def get_or_create_dataset(self, dataset_name: str) -> int:
        """Return `dataset_name`'s `meta.datasets.dataset_id`, creating the row if absent.

        Maps to ``meta.datasets(dataset_id, dataset_name)``.

        Args:
            dataset_name: The dataset's unique, human-readable name.

        Returns:
            The dataset's `dataset_id`, whether newly created or already
            present.
        """
        ...

    def create_file(  # noqa: PLR0913 -- matches meta.files' column set (ARCHITECTURE.md §2.1)
        self,
        *,
        dataset_id: int,
        object_uri: str,
        content_sha256: bytes,
        hash_version: int,
        size_bytes: int,
        filename: str,
        status: str,
    ) -> int:
        """Insert one row into `meta.files`.

        Maps to ``meta.files(dataset_id, object_uri, content_sha256,
        hash_version, size_bytes, filename, status)``.

        Args:
            dataset_id: The owning dataset's `meta.datasets.dataset_id`.
            object_uri: The object-store URI the file arrived at.
            content_sha256: The file's content hash — the real file identity.
            hash_version: Version of the hashing scheme that produced
                `content_sha256`.
            size_bytes: Size of the file, in bytes.
            filename: The file's base name, independent of its full URI.
            status: The file's processing status.

        Returns:
            The newly inserted row's `file_id`.
        """
        ...

    def find_file_by_content_hash(
        self,
        *,
        dataset_id: int,
        content_sha256: bytes,
    ) -> int | None:
        """Look up a `meta.files` row by dataset and content hash.

        Maps to ``SELECT file_id FROM meta.files WHERE dataset_id = ... AND
        content_sha256 = ...``.

        Args:
            dataset_id: The dataset to search within.
            content_sha256: The content hash to match.

        Returns:
            The matching row's `file_id`, or `None` if no file with this
            content hash has been recorded for this dataset.
        """
        ...

    def create_batch(self, *, dataset_id: int, batch_key: str, status: str) -> int:
        """Insert one row into `meta.batches`.

        Maps to ``meta.batches(dataset_id, batch_key, status)``.

        Args:
            dataset_id: The owning dataset's `meta.datasets.dataset_id`.
            batch_key: The batch's natural key, e.g.
                `<dataset>:<business_date>:<seq>`.
            status: The batch's processing status.

        Returns:
            The newly inserted row's `batch_id`.
        """
        ...

    def link_batch_file(self, *, batch_id: int, file_id: int, sequence_no: int) -> None:
        """Insert one row into `meta.batch_files`, linking a file into a batch.

        Maps to ``meta.batch_files(batch_id, file_id, sequence_no)``.

        Args:
            batch_id: The batch's `meta.batches.batch_id`.
            file_id: The file's `meta.files.file_id`.
            sequence_no: The file's position within the batch.
        """
        ...

    def create_ingestion_run(  # noqa: PLR0913 -- matches ingestion_runs' identity/FK column set
        self,
        *,
        idempotency_key: str,
        dataset_id: int,
        config_version_id: int,
        processor_version: str,
        processor_image_digest: str,
        status: str,
        file_id: int | None = None,
        batch_id: int | None = None,
    ) -> int:
        """Insert one row into `meta.ingestion_runs`.

        Maps to ``meta.ingestion_runs(idempotency_key, dataset_id, file_id,
        batch_id, config_version_id, processor_version,
        processor_image_digest, status)``.

        Args:
            idempotency_key: The unique key that makes retries free (Q7) —
                a duplicate run attempt fails at the database rather than
                racing another writer.
            dataset_id: The dataset this run processes.
            config_version_id: The `meta.config_versions` row this run was
                configured by.
            processor_version: The `dataplat` distribution version that
                executed this run.
            processor_image_digest: The container image digest that
                executed this run.
            status: The run's initial status.
            file_id: The single file this run processes, when applicable.
            batch_id: The batch this run processes, when applicable.

        Returns:
            The newly inserted row's `run_id`.
        """
        ...

    def update_ingestion_run_status(self, *, run_id: int, status: str, **fields: object) -> None:
        """Update `meta.ingestion_runs.status` and any additional named columns.

        Maps to ``UPDATE meta.ingestion_runs SET status = ..., ... WHERE
        run_id = ...``. Implementations must validate `fields`' keys against
        a fixed allow-list of real `meta.ingestion_runs` column names before
        using them to shape the `SET` clause — never build it from
        unchecked caller-supplied keys.

        Args:
            run_id: The run to update.
            status: The run's new status.
            **fields: Additional `meta.ingestion_runs` columns to set, e.g.
                `finished_at=...`, `rows_loaded=...`.
        """
        ...
