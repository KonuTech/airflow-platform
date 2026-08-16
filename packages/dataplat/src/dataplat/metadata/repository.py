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

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from datetime import datetime

    from psycopg import Connection


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
        """Idempotently insert (or resolve) one row in `meta.files`.

        Maps to ``INSERT INTO meta.files (..., duplicate_of_file_id) VALUES
        (...) ON CONFLICT (dataset_id, object_uri, content_sha256) DO UPDATE
        SET filename = EXCLUDED.filename, duplicate_of_file_id =
        EXCLUDED.duplicate_of_file_id RETURNING file_id``, against the real
        `uq_files_dataset_uri_content` UNIQUE constraint (migration 0002) --
        not a plain ``INSERT ... RETURNING``, which raises `UniqueViolation`
        on a repeat call with the same business identity.

        Calling this twice with the identical `(dataset_id, object_uri,
        content_sha256)` business identity returns the SAME `file_id` both
        times and leaves exactly one row in `meta.files` -- the
        duplicate-file-content `skip` policy (CONTEXT.md D-13) depends on
        this: re-uploading the same bytes under the same `object_uri` must
        never create a second row.

        Args:
            dataset_id: The owning dataset's `meta.datasets.dataset_id`.
            object_uri: The object-store URI the file arrived at.
            content_sha256: The file's content hash — the real file identity.
            hash_version: Version of the hashing scheme that produced
                `content_sha256`.
            size_bytes: Size of the file, in bytes.
            filename: The file's base name, independent of its full URI.
            status: The file's processing status.
            duplicate_of_file_id: The `file_id` of an earlier file this one
                is a known duplicate of, when applicable. `None` when this
                file is not a known duplicate.

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
        content_sha256 = ... ORDER BY file_id ASC LIMIT 1``.

        The ``ORDER BY file_id ASC`` is load-bearing, not cosmetic (CR-02,
        `04-REVIEW.md`; live-confirmed against the running cluster's
        `file_id=10` in `04-VERIFICATION.md`). PostgreSQL's own
        documentation treats which row ``LIMIT 1`` returns as unspecified
        once more than one row matches a ``WHERE`` clause with no
        ``ORDER BY``, and `discovery.py`'s rediscovery-correction logic
        depends on this method returning the SAME row across repeated
        calls for the same content -- ordering by ``file_id ASC`` makes
        "the true original" a stable, well-defined concept (the earliest-
        created row) instead of an accident of current heap layout.

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

        Maps to ``meta.batches(dataset_id, batch_key, status)`` via a plain
        ``INSERT ... RETURNING`` -- deliberately NOT idempotent. A second
        call with the identical `(dataset_id, batch_key)` raises
        `psycopg.errors.UniqueViolation` against `uq_batches_dataset_batch_key`
        (migration 0003): this is how LOAD-08's uniqueness guarantee is
        proven to be database-enforced rather than decorative. Callers that
        may legitimately re-observe an already-known `(dataset_id,
        batch_key)` -- e.g. `dataplat.discovery.discover_files` on a rerun
        over an unchanged object set -- must use `get_or_create_batch`
        below instead. These are two different SQL statements doing two
        different jobs and must never be conflated.

        Args:
            dataset_id: The owning dataset's `meta.datasets.dataset_id`.
            batch_key: The batch's natural key, e.g.
                `<dataset>:<business_date>:<seq>`.
            status: The batch's processing status.

        Returns:
            The newly inserted row's `batch_id`.
        """
        ...

    def get_or_create_batch(self, *, dataset_id: int, batch_key: str, status: str) -> int:
        """Idempotently insert (or resolve) one row in `meta.batches`.

        Maps to ``INSERT INTO meta.batches (...) VALUES (..., status)
        ON CONFLICT (dataset_id, batch_key) DO UPDATE SET batch_key =
        EXCLUDED.batch_key RETURNING batch_id`` -- the `get_or_create_dataset`
        idiom, not `create_batch`'s raising one. `status` is deliberately
        excluded from the conflict `SET` clause: a rediscovery of a file
        whose batch has already progressed past `OPEN` (e.g. to
        `PUBLISHED` via `finalize_publication`) must never be silently
        reset back to `status`'s caller-supplied value.

        Calling this twice with the identical `(dataset_id, batch_key)`
        returns the SAME `batch_id` both times and leaves exactly one row
        in `meta.batches` -- this is what makes
        `dataplat.discovery.discover_files` safe to call twice over an
        unchanged object set (ORCH-08), which `create_batch` alone is not.

        Args:
            dataset_id: The owning dataset's `meta.datasets.dataset_id`.
            batch_key: The batch's natural key, e.g.
                `<dataset>:<business_date>:<seq>`.
            status: The batch's status, used only when this call performs
                the FIRST insert for `(dataset_id, batch_key)`.

        Returns:
            The row's `batch_id`, whether newly inserted or already present.
        """
        ...

    def link_batch_file(self, *, batch_id: int, file_id: int, sequence_no: int) -> None:
        """Idempotently insert one row into `meta.batch_files`, linking a file into a batch.

        Maps to ``INSERT INTO meta.batch_files (...) VALUES (...)
        ON CONFLICT (batch_id, file_id) DO NOTHING`` -- calling this twice
        with the identical `(batch_id, file_id)` (the table's composite
        primary key, migration 0003) is a no-op the second time, which is
        what a discovery rerun over an unchanged object set requires
        (ORCH-08): `sequence_no` never changes for a given `(batch_id,
        file_id)` pair under this phase's one-file-one-batch simplification.

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
        """Idempotently pre-allocate one `meta.ingestion_runs` row, discovery-time.

        Maps to ``INSERT INTO meta.ingestion_runs (...) VALUES (...,
        'PENDING') ON CONFLICT (idempotency_key) DO UPDATE SET
        idempotency_key = EXCLUDED.idempotency_key RETURNING run_id,
        status``.

        Distinct from `claim_ingestion_run` below (Pitfall 5): this method
        is a no-op upsert meant for discovery-time pre-allocation, called
        every time a unit is discovered regardless of whether it has run
        before -- tolerating repeat calls is the whole point, since a
        discovery pass must be safe to repeat. `claim_ingestion_run` is a
        conditional `UPDATE ... WHERE` meant for pod-startup-time exclusive
        claiming. These are two different SQL statements doing two
        different jobs and must never be conflated or implemented as the
        same query.

        Args:
            idempotency_key: The unique key that makes retries free (Q7).
            dataset_id: The dataset this run processes.
            config_version_id: The `meta.config_versions` row this run was
                configured by.
            processor_version: The `dataplat` distribution version that will
                execute this run.
            processor_image_digest: The container image digest that will
                execute this run.
            file_id: The single file this run processes, when applicable.
            batch_id: The batch this run processes, when applicable.

        Returns:
            A `(run_id, status)` tuple: `run_id` is stable across repeat
            calls with the same `idempotency_key`; `status` is the row's
            CURRENT status after this call (e.g. `"PENDING"` on the first
            call, whatever it already was on a repeat call) -- the caller
            uses this to decide whether to include the unit in a
            Dynamic-Task-Mapping expand list.
        """
        ...

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
        """Exclusively claim one `meta.ingestion_runs` row for execution, pod-startup-time.

        Maps to ``UPDATE meta.ingestion_runs SET status='RUNNING', ...,
        trace_id = ..., span_id = ..., dag_id = ..., dag_run_id = ...,
        task_id = ..., map_index = ..., k8s_namespace = ... WHERE
        idempotency_key = ... AND (status IN ('PENDING','FAILED') OR
        (status='RUNNING' AND lease_expires_at < now())) RETURNING run_id,
        status``.

        Distinct from `get_or_create_ingestion_run` above (Pitfall 5): this
        method enforces exclusivity via a conditional `UPDATE ... WHERE` --
        it never inserts a row -- while `get_or_create_ingestion_run` is a
        no-op upsert. These are two different SQL statements doing two
        different jobs and must never be conflated or implemented as the
        same query.

        Args:
            idempotency_key: The run to claim.
            try_number: This attempt's 1-based try number.
            pod_name: The Kubernetes pod name claiming this run.
            trace_id: This run's own `pipeline.run_ingest` span's trace id
                (OBS-10), as a lowercase 32-hex-character string -- the SAME
                trace id as any extracted parent context (`dataplat.cli`'s
                `TRACEPARENT` extraction), proving cross-process trace
                continuity. `None` when tracing is unconfigured or the
                current span context is invalid, never a garbage
                all-zero-hex string. Defaults to `None` so every existing
                caller keeps compiling unchanged.
            span_id: This run's own `pipeline.run_ingest` span's span id, as
                a lowercase 16-hex-character string -- always a genuinely
                NEW value distinct from any parent's own span id, never a
                copy of it. `None` under the same conditions as `trace_id`.
                Defaults to `None` for the same reason.
            dag_id: The Airflow DAG id that triggered this run (OBS-07),
                populated from the launching `ingest` task instance's own
                `TaskInstance.dag_id` (via `AIRFLOW_CTX_DAG_ID`, injected by
                `TracingKubernetesPodOperator`). `None` outside Airflow, and
                for every pre-existing caller. Defaults to `None` so every
                existing caller keeps compiling unchanged.
            dag_run_id: The Airflow DAG run id that triggered this run,
                populated from `TaskInstance.run_id` (via
                `AIRFLOW_CTX_DAG_RUN_ID`). `None` under the same conditions
                as `dag_id`. Defaults to `None` for the same reason.
            task_id: The Airflow task id that triggered this run, populated
                from `TaskInstance.task_id` (via `AIRFLOW_CTX_TASK_ID`).
                `None` under the same conditions as `dag_id`. Defaults to
                `None` for the same reason.
            map_index: This run's Airflow Dynamic Task Mapping index,
                populated from `TaskInstance.map_index` (via
                `AIRFLOW_CTX_MAP_INDEX`). `None` under the same conditions as
                `dag_id`. Defaults to `None` for the same reason.
            k8s_namespace: The launched pod's own resolved Kubernetes
                namespace (via `AIRFLOW_CTX_K8S_NAMESPACE`). `None` under the
                same conditions as `dag_id`. Defaults to `None` for the same
                reason.

        Returns:
            `(run_id, "RUNNING")` when the claim succeeds -- the row's
            status was `PENDING`/`FAILED`, or `RUNNING` with an expired
            `lease_expires_at`. `None` when the claim is correctly refused:
            the row's status is `SUCCEEDED`, the row is `RUNNING` with a
            still-live lease (a concurrent claim is in progress), or no row
            matches `idempotency_key` at all (nothing to claim yet). All
            three are expected outcomes, not invariant violations.
        """
        ...

    def heartbeat_ingestion_run(
        self,
        *,
        run_id: int,
        lease_expires_at: datetime,
        rows_read: int,
        rows_parsed: int,
    ) -> None:
        """Refresh a RUNNING run's lease and live row counts; a silent no-op once it is not.

        Maps to ``UPDATE meta.ingestion_runs SET lease_expires_at = %s,
        rows_read = %s, rows_parsed = %s WHERE run_id = %s AND status =
        'RUNNING'``.

        Distinct from `update_ingestion_run_status` above (CR-01,
        `04-REVIEW.md`): that method carries no status guard by design --
        it is the generic, unconditional status-setter other callers
        (tests, a future `WR-02` fix) legitimately need to perform genuine
        status *transitions*. This method is narrower and self-guarding:
        it is reserved for `_heartbeat_loop`'s periodic lease/progress
        refresh, which must NEVER be able to regress a run's status. A
        stray heartbeat tick landing after the publish transaction has
        already committed `SUCCEEDED` (the exact race window between that
        commit and `stop_heartbeat.set()` in `run_ingest`'s `finally`
        block) must be a silent no-op -- no exception, no rows affected,
        no status change -- never an overwrite of the just-committed
        terminal status back to `RUNNING` with a fresh 5-minute lease.

        Args:
            run_id: The run to refresh.
            lease_expires_at: The new lease expiry, only applied while the
                run is still `RUNNING`.
            rows_read: The cumulative rows read so far, only applied while
                the run is still `RUNNING`.
            rows_parsed: The cumulative rows parsed so far, only applied
                while the run is still `RUNNING`.
        """
        ...

    def get_ingestion_run_status(self, *, run_id: int) -> str | None:
        """Read one `meta.ingestion_runs` row's current `status`, without claiming it.

        Maps to ``SELECT status FROM meta.ingestion_runs WHERE run_id =
        ...``. A pure read: distinct from `claim_ingestion_run` (which
        conditionally mutates) and from `get_or_create_ingestion_run` (which
        conditionally inserts) -- this method never writes.

        `run_ingest` (plan 04-05) calls this exactly when
        `claim_ingestion_run` refuses a claim, to distinguish
        `SKIPPED_DUPLICATE` (status is `SUCCEEDED`) from
        `SKIPPED_CONCURRENT` (status is `RUNNING` with a still-live lease)
        without re-deriving `dataset_id`/`config_version_id` just to call
        `get_or_create_ingestion_run` for a read.

        Args:
            run_id: The run to read.

        Returns:
            The row's current `status`, or `None` if no row matches
            `run_id`.
        """
        ...

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
        """Mark a file, batch and run SUCCEEDED, inside the caller's own open transaction.

        Maps to three sequential UPDATEs -- ``meta.files.status =
        'PROCESSED'``, ``meta.batches.status = 'PUBLISHED'``,
        ``meta.ingestion_runs`` (``status = 'SUCCEEDED'``, `finished_at`,
        `rows_loaded`, `duration_ms`, `report_uri`, `schema_version_id`) --
        all issued against `conn`.

        The one exception on this Protocol: every other method opens its
        own connection from the pool; this one never does. `conn` must
        already be open, inside an already-open transaction, and this
        method must never commit or roll it back itself (same contract as
        `Publisher.publish`) -- it must land inside the SAME transaction as
        the `Publisher`'s own `INSERT ... ON CONFLICT`, which is META-03's
        atomicity requirement: a file/batch/run only ever flips to
        published/succeeded atomically with the data becoming visible,
        never before and never separately. `conn` must never be supplied by
        anything other than this phase's own trusted publication
        orchestration code -- never exposed to a call site outside the
        publication transaction (T-04-06).

        Args:
            conn: An already-open connection, inside an already-open
                transaction -- the same one `Publisher.publish` is running
                its own `INSERT ... ON CONFLICT` against.
            run_id: The run to mark `SUCCEEDED`.
            file_id: The file to mark `PROCESSED`.
            batch_id: The batch to mark `PUBLISHED`.
            rows_loaded: The row count to record on the run.
            finished_at: The run's completion timestamp.
            duration_ms: Wall-clock milliseconds from claim to publish
                commit, as measured by the caller (`run_ingest`) via
                `time.monotonic()` -- this method never derives it from
                `finished_at` minus some other timestamp.
            report_uri: The object-store URI of this run's validation
                report, when one was written. `None` when no such report
                exists yet (this phase's `run_ingest` never generates one --
                mirrors `Receipt.report_uri`'s own docstring) -- the column
                is nullable (migration 0004), so this is a real, intended
                value, not a workaround.
            schema_version_id: The `meta.schema_versions` row this run's
                file resolved to (SCHEMA-03/06), from `StagingResult.
                schema_version_id`. `None` when the `Source` never resolved
                one (no `dataset_id` wired, or a non-schema-versioned
                `Source` implementation) -- the column is nullable
                (migration 0004, closed by migration 0009's FK), so this is
                a real, intended value for that case, not a workaround.
                Defaults to `None` so a caller pre-dating this parameter
                keeps compiling unchanged.
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
