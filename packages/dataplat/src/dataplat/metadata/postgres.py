"""PostgresMetadataRepository — the psycopg-backed `MetadataRepository` implementation.

Every method opens one connection from a caller-supplied pool (never
constructs its own — `dataplat.storage.db.create_pool()` is the only place
a `psycopg_pool.ConnectionPool` is built anywhere in the runtime path) and
issues a single parameterized SQL statement. Every value that crosses into
a query does so through a `%s` placeholder — never through string
interpolation. The one place this module builds SQL text dynamically is
`update_ingestion_run_status`'s `SET` clause, and even there only *column
names* are assembled dynamically, always checked first against
`_INGESTION_RUN_UPDATABLE_FIELDS`; the values themselves are still bound
through placeholders.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dataplat.metadata.repository import MetadataRepository

if TYPE_CHECKING:
    from psycopg_pool import ConnectionPool

# Every meta.ingestion_runs column update_ingestion_run_status is allowed to
# set, beyond `status` (handled explicitly by the method's own parameter).
# Deliberately excludes identity/FK columns fixed at creation (run_id,
# idempotency_key, dataset_id, file_id, batch_id, config_version_id,
# processor_version, processor_image_digest) -- those are never mutated by
# this method (T-03-11: tampering via an unchecked SET clause).
_INGESTION_RUN_UPDATABLE_FIELDS = frozenset(
    {
        "schema_version_id",
        "dag_id",
        "dag_run_id",
        "task_id",
        "map_index",
        "try_number",
        "logical_date",
        "data_interval_start",
        "data_interval_end",
        "k8s_namespace",
        "k8s_pod_name",
        "k8s_node_name",
        "trace_id",
        "span_id",
        "lease_expires_at",
        "started_at",
        "finished_at",
        "duration_ms",
        "rows_read",
        "rows_parsed",
        "rows_valid",
        "rows_invalid",
        "rows_deduplicated",
        "rows_loaded",
        "error_type",
        "error_message",
        "error_detail",
        "report_uri",
        "replay_of_run_id",
    },
)


class PostgresMetadataRepository(MetadataRepository):
    """The real `MetadataRepository`, backed by a psycopg connection pool."""

    def __init__(self, pool: ConnectionPool) -> None:
        """Wrap an already-constructed connection pool.

        Args:
            pool: A pool built by `dataplat.storage.db.create_pool()`. This
                class never constructs its own pool.
        """
        self._pool = pool

    def get_or_create_dataset(self, dataset_name: str) -> int:
        """See `MetadataRepository.get_or_create_dataset`.

        Implemented as a single atomic ``INSERT ... ON CONFLICT DO UPDATE``
        (CR-03), never a separate ``SELECT`` followed by an ``INSERT``: two
        concurrent first-time callers for the same new `dataset_name` (e.g.
        a backfill fanning out multiple files of a brand-new dataset in
        parallel under `KubernetesExecutor`) would otherwise both observe
        "no row exists" before either commits, and the loser's plain
        ``INSERT`` would raise a raw, unwrapped
        `psycopg.errors.UniqueViolation` against
        `meta.datasets`' `UNIQUE(dataset_name)` constraint instead of
        resolving to the winner's row. ``DO UPDATE SET dataset_name =
        EXCLUDED.dataset_name`` is a standard no-op-update idiom: it changes
        nothing (the value is identical), but -- unlike ``DO NOTHING``, which
        returns no row on conflict -- it still lets ``RETURNING`` yield the
        existing row's `dataset_id` in one round trip, with no fallback
        `SELECT` needed either way.
        """
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                INSERT INTO meta.datasets (dataset_name) VALUES (%s)
                ON CONFLICT (dataset_name) DO UPDATE
                    SET dataset_name = EXCLUDED.dataset_name
                RETURNING dataset_id
                """,
                (dataset_name,),
            ).fetchone()
            if row is None:  # pragma: no cover - RETURNING always yields a row here
                msg = "INSERT ... ON CONFLICT ... RETURNING dataset_id returned no row"
                raise RuntimeError(msg)
            return int(row[0])

    def create_file(  # noqa: PLR0913 -- matches meta.files' column set (repository.py Protocol)
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
        """See `MetadataRepository.create_file`."""
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                INSERT INTO meta.files (
                    dataset_id, object_uri, content_sha256, hash_version,
                    size_bytes, filename, status, duplicate_of_file_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dataset_id, object_uri, content_sha256) DO UPDATE
                    SET filename = EXCLUDED.filename,
                        duplicate_of_file_id = EXCLUDED.duplicate_of_file_id
                RETURNING file_id
                """,
                (
                    dataset_id,
                    object_uri,
                    content_sha256,
                    hash_version,
                    size_bytes,
                    filename,
                    status,
                    duplicate_of_file_id,
                ),
            ).fetchone()
            if row is None:  # pragma: no cover - RETURNING always yields a row here
                msg = "INSERT ... ON CONFLICT ... RETURNING file_id returned no row"
                raise RuntimeError(msg)
            return int(row[0])

    def find_file_by_content_hash(
        self,
        *,
        dataset_id: int,
        content_sha256: bytes,
    ) -> int | None:
        """See `MetadataRepository.find_file_by_content_hash`."""
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT file_id FROM meta.files
                 WHERE dataset_id = %s AND content_sha256 = %s
                 LIMIT 1
                """,
                (dataset_id, content_sha256),
            ).fetchone()
            return None if row is None else int(row[0])

    def create_batch(self, *, dataset_id: int, batch_key: str, status: str) -> int:
        """See `MetadataRepository.create_batch`."""
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                INSERT INTO meta.batches (dataset_id, batch_key, status)
                VALUES (%s, %s, %s)
                RETURNING batch_id
                """,
                (dataset_id, batch_key, status),
            ).fetchone()
            if row is None:  # pragma: no cover - RETURNING always yields a row here
                msg = "INSERT ... RETURNING batch_id returned no row"
                raise RuntimeError(msg)
            return int(row[0])

    def link_batch_file(self, *, batch_id: int, file_id: int, sequence_no: int) -> None:
        """See `MetadataRepository.link_batch_file`."""
        with self._pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO meta.batch_files (batch_id, file_id, sequence_no)
                VALUES (%s, %s, %s)
                """,
                (batch_id, file_id, sequence_no),
            )

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
        """See `MetadataRepository.create_ingestion_run`."""
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                INSERT INTO meta.ingestion_runs (
                    idempotency_key, dataset_id, file_id, batch_id,
                    config_version_id, processor_version,
                    processor_image_digest, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING run_id
                """,
                (
                    idempotency_key,
                    dataset_id,
                    file_id,
                    batch_id,
                    config_version_id,
                    processor_version,
                    processor_image_digest,
                    status,
                ),
            ).fetchone()
            if row is None:  # pragma: no cover - RETURNING always yields a row here
                msg = "INSERT ... RETURNING run_id returned no row"
                raise RuntimeError(msg)
            return int(row[0])

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
        """See `MetadataRepository.get_or_create_ingestion_run`."""
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                INSERT INTO meta.ingestion_runs (
                    idempotency_key, dataset_id, file_id, batch_id,
                    config_version_id, processor_version,
                    processor_image_digest, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'PENDING')
                ON CONFLICT (idempotency_key) DO UPDATE
                    SET idempotency_key = EXCLUDED.idempotency_key
                RETURNING run_id, status
                """,
                (
                    idempotency_key,
                    dataset_id,
                    file_id,
                    batch_id,
                    config_version_id,
                    processor_version,
                    processor_image_digest,
                ),
            ).fetchone()
            if row is None:  # pragma: no cover - RETURNING always yields a row here
                msg = "INSERT ... ON CONFLICT ... RETURNING run_id, status returned no row"
                raise RuntimeError(msg)
            return int(row[0]), str(row[1])

    def update_ingestion_run_status(self, *, run_id: int, status: str, **fields: object) -> None:
        """See `MetadataRepository.update_ingestion_run_status`.

        Raises:
            ValueError: `fields` names a column outside
                `_INGESTION_RUN_UPDATABLE_FIELDS` (T-03-11) — the `SET`
                clause is never built from an unchecked key.
        """
        unknown_fields = sorted(set(fields) - _INGESTION_RUN_UPDATABLE_FIELDS)
        if unknown_fields:
            msg = f"unknown meta.ingestion_runs column(s) for update: {unknown_fields}"
            raise ValueError(msg)

        assignments = ["status = %s"]
        params: list[object] = [status]
        for column, value in fields.items():
            assignments.append(column + " = %s")
            params.append(value)
        params.append(run_id)
        set_clause = ", ".join(assignments)
        # Suppression rationale (S608): only *column names* are assembled
        # dynamically here, and only after the unknown_fields allow-list check
        # above rejects anything not already a real meta.ingestion_runs column
        # (T-03-11); every *value* still crosses via the %s placeholders bound
        # through `params` below.
        query = "UPDATE meta.ingestion_runs SET " + set_clause + " WHERE run_id = %s"  # noqa: S608

        with self._pool.connection() as conn:
            conn.execute(query, params)
