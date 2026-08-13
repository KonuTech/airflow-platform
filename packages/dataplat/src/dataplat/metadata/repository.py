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

``get_or_create_ingestion_run`` and ``create_file``'s ``duplicate_of_file_id``
parameter are added by 04-03-PLAN.md Task 2 (``dataplat.discovery.
discover_files``' own dependency, per 04-01-PLAN.md's already-designed
interface for the discovery-time upsert half of the ingestion-run split —
see ``get_or_create_ingestion_run``'s own docstring for the pod-startup-time
``claim_ingestion_run`` counterpart, which this module does not yet define).
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
        duplicate_of_file_id: int | None = None,
    ) -> int:
        """Idempotently insert (or re-fetch) one `meta.files` row.

        Maps to ``INSERT INTO meta.files (dataset_id, object_uri,
        content_sha256, hash_version, size_bytes, filename, status,
        duplicate_of_file_id) VALUES (...) ON CONFLICT (dataset_id,
        object_uri, content_sha256) DO UPDATE SET filename =
        EXCLUDED.filename, duplicate_of_file_id = EXCLUDED.
        duplicate_of_file_id RETURNING file_id`` — the real
        `uq_files_dataset_uri_content` unique constraint (migration 0002)
        is the `ON CONFLICT` target. Calling this twice with identical
        `(dataset_id, object_uri, content_sha256)` returns the SAME
        `file_id` both times and leaves exactly one row in `meta.files`;
        `filename`/`duplicate_of_file_id` are refreshed to whatever this
        call passed, every other column is left as first written. This
        idempotency is what lets `dataplat.discovery.discover_files` be
        re-run over an unchanged object listing with no duplicate rows.

        Args:
            dataset_id: The owning dataset's `meta.datasets.dataset_id`.
            object_uri: The object-store URI the file arrived at.
            content_sha256: The file's content hash — the real file identity.
            hash_version: Version of the hashing scheme that produced
                `content_sha256`.
            size_bytes: Size of the file, in bytes.
            filename: The file's base name, independent of its full URI.
            status: The file's processing status.
            duplicate_of_file_id: The `file_id` of the file this row
                duplicates by content, when known. `None` when this file is
                not (yet) known to duplicate another.

        Returns:
            The row's `file_id`, whether newly inserted or already present.
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

    def get_or_create_ingestion_run(  # noqa: PLR0913 -- matches ingestion_runs' identity/FK column set
        self,
        *,
        idempotency_key: str,
        dataset_id: int,
        config_version_id: int,
        processor_version: str,
        processor_image_digest: str,
        file_id: int | None = None,
        batch_id: int | None = None,
    ) -> tuple[int, str]:
        """Idempotently pre-allocate (or re-fetch) one `meta.ingestion_runs` row, discovery-time.

        Maps to ``INSERT INTO meta.ingestion_runs (...) VALUES (...,
        'PENDING') ON CONFLICT (idempotency_key) DO UPDATE SET
        idempotency_key = EXCLUDED.idempotency_key RETURNING run_id,
        status``. Called twice with the same `idempotency_key` returns the
        same `run_id` both times and performs no second `INSERT` — this is
        the discovery-time half of a two-upsert split: it tolerates being
        re-run over an unchanged file listing, unlike a pod-startup-time
        `claim_ingestion_run` (not yet defined on this Protocol; enforces
        exclusivity via a conditional `UPDATE ... WHERE`, a distinct SQL
        statement with a distinct job). Never conflate the two.

        Args:
            idempotency_key: The run's idempotency key (Q7).
            dataset_id: The dataset this run processes.
            config_version_id: The `meta.config_versions` row this run is
                configured by.
            processor_version: The `dataplat` distribution version
                discovering this run.
            processor_image_digest: The container image digest that will
                execute this run.
            file_id: The single file this run processes, when applicable.
            batch_id: The batch this run processes, when applicable.

        Returns:
            A `(run_id, status)` tuple: `status` is the row's CURRENT
            status after this call — `"PENDING"` on a first call, whatever
            it already was on a repeat call. The caller uses this to decide
            whether to include the unit in a Dynamic-Task-Mapping expand
            list.
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
