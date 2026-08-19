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

from typing import TYPE_CHECKING, Any

from psycopg.types.json import Jsonb

from dataplat.metadata.repository import MetadataRepository

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime
    from decimal import Decimal

    from psycopg import Connection
    from psycopg_pool import ConnectionPool

    from dataplat.models.record import RejectedRecord
    from dataplat.models.report import ValidationResult

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
        """See `MetadataRepository.create_file`.

        Idempotent ``INSERT ... ON CONFLICT (dataset_id, object_uri,
        content_sha256) DO UPDATE`` against the real
        `uq_files_dataset_uri_content` UNIQUE constraint (migration 0002) --
        not a plain ``INSERT ... RETURNING``, which would raise
        `UniqueViolation` on a repeat call with the same business identity.
        """
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
        """See `MetadataRepository.find_file_by_content_hash`.

        This query's explicit row ordering, ascending by ``file_id``, is
        load-bearing, not cosmetic (CR-02, `04-REVIEW.md`; live-confirmed
        against the running cluster's `file_id=10` in
        `04-VERIFICATION.md`). PostgreSQL's own documentation treats which
        row ``LIMIT 1`` returns as unspecified once more than one row
        matches a ``WHERE`` clause with no explicit ordering, and
        `discovery.py`'s rediscovery-correction logic depends on this
        method returning the SAME row across repeated calls for the same
        content. Sorting ascending by ``file_id`` makes "the true
        original" a stable, well-defined concept -- the earliest-created
        row -- instead of an accident of current heap layout.
        """
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT file_id FROM meta.files
                 WHERE dataset_id = %s AND content_sha256 = %s
                 ORDER BY file_id ASC
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

    def get_or_create_batch(self, *, dataset_id: int, batch_key: str, status: str) -> int:
        """See `MetadataRepository.get_or_create_batch`.

        Implemented as a single atomic ``INSERT ... ON CONFLICT DO UPDATE``
        (the `get_or_create_dataset` idiom above), never a separate
        ``SELECT`` followed by an ``INSERT`` -- same TOCTOU reasoning as
        `get_or_create_dataset`'s own docstring. ``status`` is deliberately
        absent from the conflict ``SET`` clause so an existing batch's real
        status (e.g. ``PUBLISHED``) is never clobbered by a rediscovery.
        """
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                INSERT INTO meta.batches (dataset_id, batch_key, status)
                VALUES (%s, %s, %s)
                ON CONFLICT (dataset_id, batch_key) DO UPDATE
                    SET batch_key = EXCLUDED.batch_key
                RETURNING batch_id
                """,
                (dataset_id, batch_key, status),
            ).fetchone()
            if row is None:  # pragma: no cover - RETURNING always yields a row here
                msg = "INSERT ... ON CONFLICT ... RETURNING batch_id returned no row"
                raise RuntimeError(msg)
            return int(row[0])

    def link_batch_file(self, *, batch_id: int, file_id: int, sequence_no: int) -> None:
        """See `MetadataRepository.link_batch_file`."""
        with self._pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO meta.batch_files (batch_id, file_id, sequence_no)
                VALUES (%s, %s, %s)
                ON CONFLICT (batch_id, file_id) DO NOTHING
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
        replay_of_run_id: int | None = None,
    ) -> int:
        """See `MetadataRepository.create_ingestion_run`."""
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                INSERT INTO meta.ingestion_runs (
                    idempotency_key, dataset_id, file_id, batch_id,
                    config_version_id, processor_version,
                    processor_image_digest, status, replay_of_run_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    replay_of_run_id,
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
        replay_of_run_id: int | None = None,
    ) -> tuple[int, str]:
        """See `MetadataRepository.get_or_create_ingestion_run`.

        `replay_of_run_id` is deliberately excluded from the `ON CONFLICT
        ... DO UPDATE` clause's `SET` list (D-18): it is a first-insert-only
        value, applied only when this call performs the FIRST insert for
        `idempotency_key` -- a repeat call's `replay_of_run_id` argument
        (which its caller does not necessarily recompute identically) must
        never silently clobber the lineage already recorded on the row.
        """
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                INSERT INTO meta.ingestion_runs (
                    idempotency_key, dataset_id, file_id, batch_id,
                    config_version_id, processor_version,
                    processor_image_digest, status, replay_of_run_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'PENDING', %s)
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
                    replay_of_run_id,
                ),
            ).fetchone()
            if row is None:  # pragma: no cover - RETURNING always yields a row here
                msg = "INSERT ... ON CONFLICT ... RETURNING run_id, status returned no row"
                raise RuntimeError(msg)
            return int(row[0]), str(row[1])

    def find_latest_succeeded_run_for_file(self, *, file_id: int) -> int | None:
        """See `MetadataRepository.find_latest_succeeded_run_for_file`."""
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT run_id FROM meta.ingestion_runs
                 WHERE file_id = %s AND status = 'SUCCEEDED'
                 ORDER BY run_id DESC
                 LIMIT 1
                """,
                (file_id,),
            ).fetchone()
            return None if row is None else int(row[0])

    def claim_ingestion_run(  # noqa: PLR0913 -- matches the run-identity/trace/dag-context columns this method persists in one UPDATE
        self,
        *,
        idempotency_key: str,
        try_number: int,
        pod_name: str,
        trace_id: str | None = None,
        span_id: str | None = None,
        dag_id: str | None = None,
        dag_run_id: str | None = None,
        task_id: str | None = None,
        map_index: int | None = None,
        k8s_namespace: str | None = None,
    ) -> tuple[int, str] | None:
        """See `MetadataRepository.claim_ingestion_run`."""
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                UPDATE meta.ingestion_runs
                   SET status = 'RUNNING',
                       try_number = %(try_number)s,
                       k8s_pod_name = %(pod_name)s,
                       trace_id = %(trace_id)s,
                       span_id = %(span_id)s,
                       dag_id = COALESCE(%(dag_id)s, dag_id),
                       dag_run_id = COALESCE(%(dag_run_id)s, dag_run_id),
                       task_id = COALESCE(%(task_id)s, task_id),
                       map_index = COALESCE(%(map_index)s, map_index),
                       k8s_namespace = COALESCE(%(k8s_namespace)s, k8s_namespace),
                       started_at = COALESCE(started_at, now()),
                       lease_expires_at = now() + interval '5 minutes'
                 WHERE idempotency_key = %(key)s
                   AND (
                       status IN ('PENDING', 'FAILED')
                       OR (status = 'RUNNING' AND lease_expires_at < now())
                   )
                RETURNING run_id, status
                """,
                {
                    "try_number": try_number,
                    "pod_name": pod_name,
                    "trace_id": trace_id,
                    "span_id": span_id,
                    "dag_id": dag_id,
                    "dag_run_id": dag_run_id,
                    "task_id": task_id,
                    "map_index": map_index,
                    "k8s_namespace": k8s_namespace,
                    "key": idempotency_key,
                },
            ).fetchone()
            # A None row here is an EXPECTED outcome, not an invariant
            # violation, unlike every other RETURNING-returned-no-row case
            # in this class: it means one of three legitimate
            # "not claimable right now" states -- the run already
            # SUCCEEDED, a concurrent claim currently holds a live lease,
            # or no row exists yet for this idempotency_key at all (claim
            # called before get_or_create_ingestion_run ever created it).
            # So this is the one RETURNING call site in this class that
            # does not raise RuntimeError on a None row.
            if row is None:
                return None
            return int(row[0]), str(row[1])

    def heartbeat_ingestion_run(
        self,
        *,
        run_id: int,
        lease_expires_at: datetime,
        rows_read: int,
        rows_parsed: int,
    ) -> None:
        """See `MetadataRepository.heartbeat_ingestion_run`.

        The `WHERE run_id = %s AND status = 'RUNNING'` guard (CR-01) is what
        makes a stray post-terminal heartbeat tick a genuine no-op: zero rows
        affected is the correct, silent outcome once the run is no longer
        `RUNNING` -- never raised, logged or branched on here, by design.
        """
        with self._pool.connection() as conn:
            conn.execute(
                """
                UPDATE meta.ingestion_runs
                   SET lease_expires_at = %s,
                       rows_read = %s,
                       rows_parsed = %s
                 WHERE run_id = %s AND status = 'RUNNING'
                """,
                (lease_expires_at, rows_read, rows_parsed, run_id),
            )

    def get_ingestion_run_status(self, *, run_id: int) -> str | None:
        """See `MetadataRepository.get_ingestion_run_status`."""
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT status FROM meta.ingestion_runs WHERE run_id = %s",
                (run_id,),
            ).fetchone()
            return None if row is None else str(row[0])

    def claim_run_stage(
        self,
        *,
        run_id: int,
        stage_name: str,
        try_number: int,
        pod_name: str,
    ) -> int | None:
        """See `MetadataRepository.claim_run_stage`.

        The `INSERT`'s own `WHERE EXISTS (... meta.ingestion_runs ... status
        = 'STAGED')` clause governs the source `SELECT`'s row set, so it
        applies even on a first-ever claim (no pre-existing `run_stages` row
        to gate against yet) -- this is the cross-table guard (T-08.1-15).
        The `ON CONFLICT ... DO UPDATE ... WHERE` clause then governs
        whether an EXISTING `run_stages` row is claimable, mirroring
        `claim_ingestion_run`'s own claimability predicate. Both checks
        evaluate inside this single statement -- no read-then-write race
        window.
        """
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                INSERT INTO meta.run_stages (
                    run_id, stage_name, status, lease_expires_at, pod_name,
                    try_number, started_at
                )
                SELECT %(run_id)s, %(stage_name)s, 'RUNNING',
                       now() + interval '5 minutes', %(pod_name)s,
                       %(try_number)s, now()
                 WHERE EXISTS (
                    SELECT 1 FROM meta.ingestion_runs
                     WHERE run_id = %(run_id)s AND status = 'STAGED'
                 )
                ON CONFLICT (run_id, stage_name) DO UPDATE
                    SET status = 'RUNNING',
                        lease_expires_at = EXCLUDED.lease_expires_at,
                        pod_name = EXCLUDED.pod_name,
                        try_number = EXCLUDED.try_number
                 WHERE meta.run_stages.status IN ('PENDING', 'FAILED')
                    OR (meta.run_stages.status = 'RUNNING'
                        AND meta.run_stages.lease_expires_at < now())
                RETURNING run_stage_id
                """,
                {
                    "run_id": run_id,
                    "stage_name": stage_name,
                    "pod_name": pod_name,
                    "try_number": try_number,
                },
            ).fetchone()
            # A None row here is an EXPECTED outcome (mirrors
            # claim_ingestion_run's own documented contract), not an
            # invariant violation: either the owning run is not yet STAGED,
            # or the run_stages row (if any) is not currently claimable.
            return None if row is None else int(row[0])

    def heartbeat_run_stage(
        self,
        *,
        run_id: int,
        stage_name: str,
        lease_expires_at: datetime,
    ) -> None:
        """See `MetadataRepository.heartbeat_run_stage`.

        The `WHERE run_id = %s AND stage_name = %s AND status = 'RUNNING'`
        guard mirrors `heartbeat_ingestion_run`'s own CR-01 self-guard: a
        stray heartbeat tick landing after `complete_run_stage` has already
        committed a terminal status is a genuine, silent no-op.
        """
        with self._pool.connection() as conn:
            conn.execute(
                """
                UPDATE meta.run_stages
                   SET lease_expires_at = %s
                 WHERE run_id = %s AND stage_name = %s AND status = 'RUNNING'
                """,
                (lease_expires_at, run_id, stage_name),
            )

    def complete_run_stage(
        self,
        *,
        run_id: int,
        stage_name: str,
        status: str,
        finished_at: datetime,
    ) -> None:
        """See `MetadataRepository.complete_run_stage`."""
        with self._pool.connection() as conn:
            conn.execute(
                """
                UPDATE meta.run_stages
                   SET status = %s, finished_at = %s
                 WHERE run_id = %s AND stage_name = %s
                """,
                (status, finished_at, run_id, stage_name),
            )

    def get_run_stage_status(self, *, run_id: int, stage_name: str) -> str | None:
        """See `MetadataRepository.get_run_stage_status`."""
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT status FROM meta.run_stages WHERE run_id = %s AND stage_name = %s",
                (run_id, stage_name),
            ).fetchone()
            return None if row is None else str(row[0])

    def get_run_recovery_status(self, *, run_id: int) -> dict[str, object] | None:
        """See `MetadataRepository.get_run_recovery_status`."""
        with self._pool.connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM meta.v_run_recovery WHERE run_id = %s",
                (run_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            assert cursor.description is not None  # noqa: S101 -- a fetched row implies a description
            columns = [desc.name for desc in cursor.description]
            return dict(zip(columns, row, strict=True))

    def list_staged_run_ids(self, *, dataset_id: int) -> list[tuple[int, int, int, str | None]]:
        """See `MetadataRepository.list_staged_run_ids`."""
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT run_id, file_id, batch_id, report_uri
                  FROM meta.ingestion_runs
                 WHERE dataset_id = %s AND status = 'STAGED'
                 ORDER BY run_id ASC
                """,
                (dataset_id,),
            ).fetchall()
            return [
                (int(row[0]), int(row[1]), int(row[2]), None if row[3] is None else str(row[3]))
                for row in rows
            ]

    def finalize_publication(  # noqa: PLR0913 -- matches the files/batches/ingestion_runs field set this updates
        self,
        *,
        conn: Connection[Any],
        run_id: int,
        file_id: int,
        batch_id: int,
        rows_loaded: int,
        finished_at: datetime,
        duration_ms: int,
        report_uri: str | None,
        schema_version_id: int | None = None,
    ) -> None:
        """See `MetadataRepository.finalize_publication`.

        The one method on this class that does NOT open its own connection
        from `self._pool` (META-03): it must land inside the same
        transaction as `Publisher.publish`'s own `INSERT ... ON CONFLICT`,
        so it executes against the caller-supplied `conn` and never commits
        or rolls it back.
        """
        conn.execute(
            "UPDATE meta.files SET status = 'PROCESSED' WHERE file_id = %s",
            (file_id,),
        )
        conn.execute(
            "UPDATE meta.batches SET status = 'PUBLISHED' WHERE batch_id = %s",
            (batch_id,),
        )
        conn.execute(
            """
            UPDATE meta.ingestion_runs
               SET status = 'SUCCEEDED',
                   finished_at = %s,
                   rows_loaded = %s,
                   duration_ms = %s,
                   report_uri = %s,
                   schema_version_id = COALESCE(%s, schema_version_id)
             WHERE run_id = %s
            """,
            (finished_at, rows_loaded, duration_ms, report_uri, schema_version_id, run_id),
        )

    def record_validation_results(
        self,
        *,
        conn: Connection[Any],
        run_id: int,
        results: list[ValidationResult],
    ) -> None:
        """See `MetadataRepository.record_validation_results`.

        Like `finalize_publication`, this method never opens its own
        connection from `self._pool` and never calls `conn.commit()`/
        `conn.rollback()` — it issues its writes against the caller-supplied
        `conn`, which must already be inside an open transaction (Pattern
        3/D-11). With `results=[]` this executes zero INSERTs and returns
        without raising. `threshold`/`observed` round-trip through JSONB via
        `psycopg.types.json.Jsonb` (matching `config/registry.py`'s own
        usage), never string-interpolated.
        """
        for result in results:
            conn.execute(
                """
                INSERT INTO meta.validation_results (
                    run_id, rule_id, rule_type, severity, outcome,
                    evaluated_count, failed_count, threshold, observed
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    result.rule_id,
                    result.rule_type,
                    result.severity,
                    result.outcome,
                    result.evaluated_count,
                    result.failed_count,
                    Jsonb(result.threshold),
                    Jsonb(result.observed),
                ),
            )

    def record_rejected_records(
        self,
        *,
        conn: Connection[Any],
        run_id: int,
        file_id: int,
        batch_id: int,
        rejected: list[RejectedRecord],
    ) -> None:
        """See `MetadataRepository.record_rejected_records`.

        Same `conn`-never-opened-here, never-committed-here shape as
        `record_validation_results` above. `run_id`/`file_id`/`batch_id`
        come from this method's own arguments, never from `RejectedRecord`
        (which carries none of them). `resolution_type` is never set here —
        every inserted row lands at migration 0015's `'PENDING'` column
        default; `resolve_rejected_records_for_business_keys` below is the
        only method that ever changes it. `RejectedRecord` has no
        `source_byte_offset` field today, so it is always bound `None`.
        `raw_line`/`error_message`/`business_key` — untrusted CSV content —
        cross into SQL exclusively via `%s` placeholders (T-08-07/T-08-28),
        never string-formatted. `record.business_key` (migration 0020) is
        bound `None` when the row's business-key value could not be
        reliably extracted (D-25) — this method never guesses or defaults
        it.
        """
        for record in rejected:
            conn.execute(
                """
                INSERT INTO meta.rejected_records (
                    run_id, file_id, batch_id, source_row_number,
                    source_byte_offset, raw_line, error_type, error_column,
                    error_message, business_key
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    file_id,
                    batch_id,
                    record.source_row_number,
                    None,
                    record.raw_line,
                    record.error_type,
                    record.error_column,
                    record.error_message,
                    record.business_key,
                ),
            )

    def resolve_rejected_records_for_business_keys(
        self,
        *,
        conn: Connection[Any],
        dataset_id: int,
        business_keys: Sequence[str],
        resolved_by_run_id: int,
        resolution_type: str,
    ) -> int:
        """See `MetadataRepository.resolve_rejected_records_for_business_keys`.

        This is the ONLY method on this class — and the ONLY code path
        anywhere in this codebase — that can ever change
        `resolution_type`/`resolved_by_run_id` on `meta.rejected_records`
        (D-04/D-24, T-08-08). No per-row variant exists, and none may be
        added: the single `UPDATE ... FROM meta.batches WHERE ...` below is
        D-24's whole-set-side-effect-only granularity, made concrete against
        D-23's new `(dataset_id, business_key)` matching predicate — this
        method's scoping parameters are a `dataset_id` and a set of
        `business_keys`, never a row id or a list of row ids.

        Guards `business_keys=[]` as an explicit no-op FIRST (mirrors the
        Protocol docstring's documented "0 is a legitimate outcome" framing)
        — an empty Postgres array bound via `= ANY(%s)` would already select
        zero rows, but the explicit guard avoids issuing a statement at all
        for the common empty-call case. `business_keys` is bound as a plain
        Python `list` through a single `%s` placeholder — psycopg3 adapts it
        to a Postgres array automatically (T-08-29) — never individually
        interpolated per-value, never built via string concatenation. A
        `NULL` `business_key` row is structurally never matched by
        `= ANY(%s)` regardless of what `business_keys` contains (D-25) — no
        extra `WHERE business_key IS NOT NULL` clause is needed for that
        guarantee to hold.

        Never opens its own connection and never commits/rolls back `conn`,
        matching `finalize_publication`'s documented exception.
        """
        if not business_keys:
            return 0
        cursor = conn.execute(
            """
            UPDATE meta.rejected_records
               SET resolution_type = %s,
                   resolved_by_run_id = %s
              FROM meta.batches
             WHERE meta.batches.batch_id = meta.rejected_records.batch_id
               AND meta.batches.dataset_id = %s
               AND meta.rejected_records.business_key = ANY(%s)
               AND meta.rejected_records.resolution_type = 'PENDING'
            """,
            (resolution_type, resolved_by_run_id, dataset_id, list(business_keys)),
        )
        return int(cursor.rowcount)

    def record_watermark(  # noqa: PLR0913 -- one keyword per record_watermark Protocol argument
        self,
        *,
        conn: Connection[Any],
        dataset_id: int,
        target_key: str,
        source_table: str,
        watermark_column: str,
        run_id: int,
    ) -> None:
        """See `MetadataRepository.record_watermark`.

        Same `conn`-never-opened-here, never-committed-here shape as
        `finalize_publication`/`resolve_rejected_records_for_business_keys`
        above -- MUST run inside `publish_ingest`'s already-open, advisory-
        locked transaction. `source_table`/`watermark_column` are
        interpolated as SQL IDENTIFIERS via an f-string, never a value
        (T-09-03) -- both are config-resolved (`pipeline/run.py`'s
        `_WATERMARK_COLUMN_BY_DATASET` dict and `f"silver.{dataset}"`),
        never row content. Every genuine VALUE (`dataset_id`/`target_key`)
        still crosses via `%()s` placeholders.
        """
        old_row = conn.execute(
            "SELECT cursor_value FROM meta.watermarks WHERE dataset_id = %s AND target_key = %s",
            (dataset_id, target_key),
        ).fetchone()
        old_value = None if old_row is None else old_row[0]

        new_row = conn.execute(
            f"""
            INSERT INTO meta.watermarks (dataset_id, target_key, cursor_value)
            VALUES (%(dataset_id)s, %(target_key)s,
                    (SELECT max({watermark_column}::timestamptz) FROM {source_table}))
            ON CONFLICT (dataset_id, target_key) DO UPDATE
                SET cursor_value = GREATEST(meta.watermarks.cursor_value, EXCLUDED.cursor_value)
            RETURNING cursor_value
            """,  # noqa: S608 -- source_table/watermark_column are config-resolved identifiers (T-09-03, see this method's own docstring), never row content or user input
            {"dataset_id": dataset_id, "target_key": target_key},
        ).fetchone()
        if new_row is None:  # pragma: no cover - ON CONFLICT DO UPDATE always yields a row here
            msg = "INSERT ... ON CONFLICT ... RETURNING cursor_value returned no row"
            raise RuntimeError(msg)
        new_value = new_row[0]

        # Unconditional -- D-04: logs every write, moved or not.
        conn.execute(
            """
            INSERT INTO meta.watermark_history (
                dataset_id, target_key, old_value, new_value, run_id
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            (dataset_id, target_key, old_value, new_value, run_id),
        )

    def get_current_watermark(self, *, dataset_id: int, target_key: str) -> datetime | None:
        """See `MetadataRepository.get_current_watermark`."""
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT cursor_value FROM meta.watermarks"
                " WHERE dataset_id = %s AND target_key = %s",
                (dataset_id, target_key),
            ).fetchone()
            return None if row is None else row[0]

    def record_reconciliation(  # noqa: PLR0913 -- one keyword per meta.reconciliation_results column this writes
        self,
        *,
        conn: Connection[Any],
        dataset_id: int,
        file_id: int | None,
        hop: str,
        input_count: int,
        output_count: int,
        rejected_count: int = 0,
        dedup_count: int = 0,
        sum_column: str | None = None,
        sum_input: Decimal | None = None,
        sum_output: Decimal | None = None,
        checksum_input: str | None = None,
        checksum_output: str | None = None,
        min_input: datetime | None = None,
        max_input: datetime | None = None,
        min_output: datetime | None = None,
        max_output: datetime | None = None,
        key_count_input: int | None = None,
        key_count_output: int | None = None,
        expected_row_count: int | None = None,
        expected_checksum: str | None = None,
    ) -> int:
        """See `MetadataRepository.record_reconciliation`.

        D-22's exact accounting formula (`discrepancy = input_count -
        (output_count + rejected_count + dedup_count)`) and VALID-06/D-23's
        `control_total_discrepancy` (`expected_row_count - output_count`,
        `NULL` unless `expected_row_count` is supplied) are both computed as
        SQL expressions inside this single `INSERT`'s own `VALUES` clause --
        visible and grep-able in the SQL text itself, never hidden in Python
        arithmetic upstream of this call. Same `conn`-never-opened-here,
        never-committed-here shape as `record_watermark` above.
        """
        row = conn.execute(
            """
            INSERT INTO meta.reconciliation_results (
                dataset_id, file_id, hop, input_count, output_count,
                rejected_count, dedup_count, discrepancy, sum_column,
                sum_input, sum_output, checksum_input, checksum_output,
                min_input, max_input, min_output, max_output,
                key_count_input, key_count_output, expected_row_count,
                expected_checksum, control_total_discrepancy
            ) VALUES (
                %(dataset_id)s, %(file_id)s, %(hop)s,
                %(input_count)s::bigint, %(output_count)s::bigint,
                %(rejected_count)s::bigint, %(dedup_count)s::bigint,
                %(input_count)s::bigint
                    - (%(output_count)s::bigint + %(rejected_count)s::bigint
                       + %(dedup_count)s::bigint),
                %(sum_column)s, %(sum_input)s, %(sum_output)s,
                %(checksum_input)s, %(checksum_output)s,
                %(min_input)s, %(max_input)s, %(min_output)s, %(max_output)s,
                %(key_count_input)s, %(key_count_output)s, %(expected_row_count)s::bigint,
                %(expected_checksum)s,
                CASE WHEN %(expected_row_count)s::bigint IS NOT NULL
                     THEN %(expected_row_count)s::bigint - %(output_count)s::bigint END
            )
            RETURNING reconciliation_id
            """,
            {
                "dataset_id": dataset_id,
                "file_id": file_id,
                "hop": hop,
                "input_count": input_count,
                "output_count": output_count,
                "rejected_count": rejected_count,
                "dedup_count": dedup_count,
                "sum_column": sum_column,
                "sum_input": sum_input,
                "sum_output": sum_output,
                "checksum_input": checksum_input,
                "checksum_output": checksum_output,
                "min_input": min_input,
                "max_input": max_input,
                "min_output": min_output,
                "max_output": max_output,
                "key_count_input": key_count_input,
                "key_count_output": key_count_output,
                "expected_row_count": expected_row_count,
                "expected_checksum": expected_checksum,
            },
        ).fetchone()
        if row is None:  # pragma: no cover - RETURNING always yields a row on a successful INSERT
            msg = (
                "INSERT INTO meta.reconciliation_results ... "
                "RETURNING reconciliation_id returned no row"
            )
            raise RuntimeError(msg)
        return int(row[0])

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
