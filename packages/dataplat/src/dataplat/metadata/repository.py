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
    from collections.abc import Sequence
    from datetime import datetime
    from decimal import Decimal

    from psycopg import Connection

    from dataplat.models.record import RejectedRecord
    from dataplat.models.report import ValidationResult


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
        replay_of_run_id: int | None = None,
    ) -> int:
        """Insert one row into `meta.ingestion_runs`.

        Maps to ``meta.ingestion_runs(idempotency_key, dataset_id, file_id,
        batch_id, config_version_id, processor_version,
        processor_image_digest, status, replay_of_run_id)``.

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
            replay_of_run_id: The `meta.ingestion_runs.run_id` of an OLDER-
                formula run for the same file that already `SUCCEEDED`
                (D-18), when this run exists specifically to re-process that
                file under an extended idempotency-key formula. `None` when
                this run is not a replay of an earlier one -- the common
                case. Defaults to `None` so every existing caller keeps
                compiling unchanged.

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
        replay_of_run_id: int | None = None,
    ) -> tuple[int, str]:
        """Idempotently pre-allocate one `meta.ingestion_runs` row, discovery-time.

        Maps to ``INSERT INTO meta.ingestion_runs (..., replay_of_run_id)
        VALUES (..., 'PENDING', ...) ON CONFLICT (idempotency_key) DO UPDATE
        SET idempotency_key = EXCLUDED.idempotency_key RETURNING run_id,
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
            replay_of_run_id: The `meta.ingestion_runs.run_id` of an OLDER-
                formula run for the same file that already `SUCCEEDED`
                (D-18). Applied ONLY on this call's first-ever insert for
                `idempotency_key` -- the `ON CONFLICT ... DO UPDATE`
                clause's `SET` list deliberately excludes this column
                (mirrors `get_or_create_batch`'s own documented "status
                deliberately excluded from the conflict SET clause"
                reasoning), so a rediscovery of an already-claimed replay
                run never silently clears its own lineage back to `NULL`.
                `None` when this run is not a replay of an earlier one --
                the common case. Defaults to `None` so every existing caller
                keeps compiling unchanged.

        Returns:
            A `(run_id, status)` tuple: `run_id` is stable across repeat
            calls with the same `idempotency_key`; `status` is the row's
            CURRENT status after this call (e.g. `"PENDING"` on the first
            call, whatever it already was on a repeat call) -- the caller
            uses this to decide whether to include the unit in a
            Dynamic-Task-Mapping expand list.
        """
        ...

    def find_latest_succeeded_run_for_file(self, *, file_id: int) -> int | None:
        """Look up the most recent `SUCCEEDED` run for a file, across any idempotency-key formula.

        Maps to ``SELECT run_id FROM meta.ingestion_runs WHERE file_id = %s
        AND status = 'SUCCEEDED' ORDER BY run_id DESC LIMIT 1``.

        This is D-18's lookup step, called by `discovery.py` (plan 08.1-07
        Task 3) BEFORE creating a new run under the extended idempotency-key
        formula, to discover whether an older-formula run for the same file
        already succeeded -- so that new run can carry a `replay_of_run_id`
        pointing back at it. Only ever matches `status = 'SUCCEEDED'`
        (T-08.1-17): a `RUNNING`/`PENDING`/`FAILED` old-formula run is never
        mistaken for a completed one to replay from.

        Args:
            file_id: The `meta.files.file_id` to search for a prior
                `SUCCEEDED` run.

        Returns:
            The most recent matching run's `run_id`, or `None` if this file
            has never had a `SUCCEEDED` run.
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

    def claim_run_stage(
        self,
        *,
        run_id: int,
        stage_name: str,
        try_number: int,
        pod_name: str,
    ) -> int | None:
        """Exclusively claim one `meta.run_stages` row, gated on its owning run's own status.

        Maps to ``INSERT INTO meta.run_stages (run_id, stage_name, status,
        lease_expires_at, pod_name, try_number, started_at) SELECT ...,
        'RUNNING', now() + interval '5 minutes', ..., now() WHERE EXISTS
        (SELECT 1 FROM meta.ingestion_runs WHERE run_id = ... AND status =
        'STAGED') ON CONFLICT (run_id, stage_name) DO UPDATE SET status =
        'RUNNING', lease_expires_at = EXCLUDED.lease_expires_at, pod_name =
        EXCLUDED.pod_name, try_number = EXCLUDED.try_number WHERE
        meta.run_stages.status IN ('PENDING', 'FAILED') OR
        (meta.run_stages.status = 'RUNNING' AND meta.run_stages.
        lease_expires_at < now()) RETURNING run_stage_id``.

        This is the ONE claim method in this Protocol with a cross-table
        guard (D-17, RESEARCH.md Open Question 1): two conditions must BOTH
        hold for a claim to succeed --

        1. `meta.ingestion_runs.status` for `run_id` must be `'STAGED'` --
           enforced by the `INSERT`'s own `WHERE EXISTS` clause, which
           applies even on a first-ever claim (no pre-existing `run_stages`
           row to gate against otherwise). A run whose stage hop has not
           genuinely completed (still `'RUNNING'`, or terminal `'FAILED'`/
           `'SUCCEEDED'` under the wrong hop) can NEVER be claimed here --
           this is what stops a retried `stage` task and a live `publish`
           claim from ever colliding on the same lease (Pitfall 2).
        2. The `run_stages` row itself (if one already exists for `(run_id,
           stage_name)`) must be in a claimable state -- `'PENDING'`/
           `'FAILED'`, or `'RUNNING'` with an expired lease -- enforced by
           the `ON CONFLICT ... DO UPDATE ... WHERE` clause, mirroring
           `claim_ingestion_run`'s own claimability predicate.

        Both checks are evaluated inside the SAME `INSERT` statement as the
        claim itself -- no read-then-write race window between checking
        staged-ness and claiming (T-08.1-15).

        Args:
            run_id: The `meta.ingestion_runs.run_id` whose stage hop is being
                claimed.
            stage_name: The stage being claimed -- `"STAGE_LOAD"` or
                `"PUBLISH"` this phase (migration 0025's own documented
                vocabulary).
            try_number: This attempt's 1-based try number.
            pod_name: The Kubernetes pod name claiming this stage.

        Returns:
            The claimed row's `run_stage_id` on success. `None` when the
            claim is correctly refused -- `run_id`'s own status is not
            `'STAGED'`, or the `run_stages` row (if any) is not in a
            claimable state (a concurrent claim currently holds a live
            lease). Both are expected outcomes, not invariant violations,
            mirroring `claim_ingestion_run`'s own `None`-is-not-an-error
            contract.
        """
        ...

    def heartbeat_run_stage(
        self,
        *,
        run_id: int,
        stage_name: str,
        lease_expires_at: datetime,
    ) -> None:
        """Refresh a RUNNING `run_stages` row's lease; a silent no-op once it is not.

        Maps to ``UPDATE meta.run_stages SET lease_expires_at = %s WHERE
        run_id = %s AND stage_name = %s AND status = 'RUNNING'``.

        Self-guarded exactly like `heartbeat_ingestion_run` (CR-01,
        `04-REVIEW.md`): a stray heartbeat tick landing after
        `complete_run_stage` has already committed a terminal status for
        this `(run_id, stage_name)` must be a silent no-op -- no exception,
        no rows affected, no status change -- never an overwrite of the
        just-committed terminal status back to `RUNNING` with a fresh lease.

        Args:
            run_id: The run whose stage-hop lease is being refreshed.
            stage_name: The stage being refreshed.
            lease_expires_at: The new lease expiry, only applied while the
                row is still `RUNNING`.
        """
        ...

    def complete_run_stage(
        self,
        *,
        run_id: int,
        stage_name: str,
        status: str,
        finished_at: datetime,
    ) -> None:
        """Transition one `meta.run_stages` row to its terminal status.

        Maps to ``UPDATE meta.run_stages SET status = %s, finished_at = %s
        WHERE run_id = %s AND stage_name = %s``.

        Args:
            run_id: The run whose stage hop is completing.
            stage_name: The stage completing.
            status: The terminal status to record -- `'SUCCEEDED'` or
                `'FAILED'`.
            finished_at: The stage's completion timestamp.
        """
        ...

    def get_run_stage_status(self, *, run_id: int, stage_name: str) -> str | None:
        """Read one `meta.run_stages` row's current `status`, without claiming it.

        Maps to ``SELECT status FROM meta.run_stages WHERE run_id = %s AND
        stage_name = %s``. A pure read, mirroring `get_ingestion_run_status`'s
        own read-only contract -- never writes.

        Args:
            run_id: The run to read.
            stage_name: The stage to read.

        Returns:
            The row's current `status`, or `None` if no `run_stages` row
            exists yet for this `(run_id, stage_name)` pair.
        """
        ...

    def get_run_recovery_status(self, *, run_id: int) -> dict[str, object] | None:
        """Read one `meta.v_run_recovery` row for `run_id`, without claiming or writing anything.

        Maps to ``SELECT * FROM meta.v_run_recovery WHERE run_id = %s``. A pure read,
        mirroring `get_run_stage_status`'s own read-only contract -- never writes. LOAD-06's
        single-query recovery answer (D-16): the returned dict's `next_action` key always
        reads `'retry stage <NAME>'` or `'complete'`, never implying a rollback path exists
        (D-15: recovery is retry-only, rollback structurally cannot apply).

        Args:
            run_id: The run to read.

        Returns:
            A dict keyed by `meta.v_run_recovery` column name, or `None` if no
            `meta.ingestion_runs` row exists for `run_id`.
        """
        ...

    def list_staged_run_ids(self, *, dataset_id: int) -> list[tuple[int, int, int, str | None]]:
        """List every run currently ready for `publish_ingest` to claim.

        Maps to ``SELECT run_id, file_id, batch_id, report_uri FROM
        meta.ingestion_runs WHERE dataset_id = %s AND status = 'STAGED'
        ORDER BY run_id ASC``.

        Returns every `(run_id, file_id, batch_id, report_uri)` quadruple
        whose owning run has reached `'STAGED'` for the given dataset --
        `publish_ingest` (plan 08.1-10) needs `file_id`/`batch_id` alongside
        each `run_id` to call `finalize_publication`, and needs the
        ALREADY-set `report_uri` (written by `stage_ingest`'s own
        `update_ingestion_run_status` call) to pass straight back through
        unchanged, since `finalize_publication` always sets this column and
        a `None` would silently clobber the real VALID-04 report URI
        `stage_ingest` already wrote. `ORDER BY run_id ASC` is deterministic,
        mirroring `find_file_by_content_hash`'s own load-bearing-ordering
        precedent (CR-02, `04-REVIEW.md`).

        Args:
            dataset_id: The dataset to list staged runs for.

        Returns:
            Every `(run_id, file_id, batch_id, report_uri)` quadruple for
            this dataset's `'STAGED'` runs, ordered by `run_id` ascending. A
            run that later completes `publish_ingest` (`'SUCCEEDED'`) no
            longer appears here.
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
                schema_version_id`. `None` means "this caller has no new
                value to set" -- the SQL's own `COALESCE(%s,
                schema_version_id)` leaves whatever is already recorded
                untouched in that case, rather than clobbering it back to
                `NULL`. This matters post-08.1-10: `stage_ingest` (not
                `publish_ingest`) is the only place a value is ever
                resolved (`StagingResult.schema_version_id`, from
                `Source.open()`), and it already writes that value via its
                own `update_ingestion_run_status` call, well before
                `publish_ingest`'s own `finalize_publication` call ever
                runs -- `publish_ingest` always passes `None` here (it has
                no `Source`/`StagingResult` of its own, module docstring),
                and must not silently erase what staging already recorded.
                A file that genuinely never resolves a schema version at
                all (no `dataset_id` wired, or a non-schema-versioned
                `Source` implementation) simply stays `NULL` throughout --
                `COALESCE(NULL, NULL)` is still `NULL`. Defaults to `None`
                so a caller pre-dating this parameter keeps compiling
                unchanged.
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

    def record_validation_results(
        self,
        *,
        conn: Connection[Any],
        run_id: int,
        results: list[ValidationResult],
    ) -> None:
        """Bulk-insert one run's validation findings, inside the caller's own open transaction.

        Maps to ``INSERT INTO meta.validation_results (run_id, rule_id,
        rule_type, severity, outcome, evaluated_count, failed_count,
        threshold, observed) VALUES (...)`` for each `ValidationResult` in
        `results` (migration 0014).

        Like `finalize_publication`, `conn` is caller-supplied and never
        committed or rolled back here — this method must land inside the
        SAME transaction as the run's atomic publish (Pattern 3/D-11), so a
        run that escalates to FAIL and rolls back its publication also rolls
        back the validation findings that caused the rollback, keeping
        `meta.validation_results` consistent with what actually happened.

        Args:
            conn: An already-open connection, inside an already-open
                transaction — the same one `Publisher.publish` and
                `finalize_publication` run against.
            run_id: The run these findings belong to.
            results: The validation findings to persist, in any order.
        """
        ...

    def record_rejected_records(
        self,
        *,
        conn: Connection[Any],
        run_id: int,
        file_id: int,
        batch_id: int,
        rejected: list[RejectedRecord],
    ) -> None:
        """Bulk-insert one run's rejected rows, inside the caller's own open transaction.

        Maps to ``INSERT INTO meta.rejected_records (run_id, file_id,
        batch_id, source_row_number, raw_line, error_type, error_column,
        error_message, business_key) VALUES (...)`` for each `RejectedRecord`
        in `rejected` (migration 0015, `business_key` added by migration
        0020). `resolution_type` is left to its `'PENDING'` column default —
        this method never sets it directly. Also inserts `record.business_key`
        for each `RejectedRecord` — `None` when the row's business-key value
        could not be reliably extracted (D-25).

        Like `record_validation_results`, `conn` is caller-supplied and
        never committed or rolled back here — must land inside the SAME
        transaction as the run's atomic publish (Pattern 3/D-11).

        Args:
            conn: An already-open connection, inside an already-open
                transaction — the same one `Publisher.publish` and
                `finalize_publication` run against.
            run_id: The run these rejects belong to.
            file_id: The file these rejects were read from.
            batch_id: The batch these rejects belong to.
            rejected: The rejected rows to persist, in any order.
        """
        ...

    def resolve_rejected_records_for_business_keys(
        self,
        *,
        conn: Connection[Any],
        dataset_id: int,
        business_keys: Sequence[str],
        resolved_by_run_id: int,
        resolution_type: str,
    ) -> int:
        """Resolve every still-PENDING reject sharing a business key, as a whole-set side effect.

        Maps to ``UPDATE meta.rejected_records SET resolution_type = %s,
        resolved_by_run_id = %s FROM meta.batches WHERE
        meta.batches.batch_id = meta.rejected_records.batch_id AND
        meta.batches.dataset_id = %s AND meta.rejected_records.business_key
        = ANY(%s) AND meta.rejected_records.resolution_type = 'PENDING'``
        (migration 0020's `business_key` column, joined against
        `meta.batches.dataset_id` for dataset scoping).

        This REPLACES this Protocol's prior, strictly `batch_id`-scoped
        resolution method (D-23's gap-closure fix, `08-CONTEXT.md` "Gap
        closure: VALID-08 backfill resolution scoping"): `discover_files`'s
        `batch_key` is a
        pure function of a file's `content_sha256`, so a content-differing
        correction of a previously-rejected row always discovers under a
        NEW `batch_id` — a strictly `batch_id`-scoped resolve call could
        never touch the ORIGINAL batch's PENDING row. Matching on
        `(dataset_id, business_key)` instead means this method resolves
        every PENDING reject sharing that business key across ANY `batch_id`
        within the dataset, regardless of which batch originally rejected it
        or which batch the correction discovers under.

        Still a whole-set side effect only (D-24 — D-03's whole-batch-only
        granularity is preserved, only the matching predicate changes): no
        per-row variant exists anywhere in this codebase, and none may be
        added. A backfill run completing, or an explicit batch-level discard
        operation, are the ONLY two call sites this method has. This is the
        ONLY write path to `resolution_type`/`resolved_by_run_id` anywhere
        in this codebase — `record_rejected_records` above never sets
        either column, leaving every newly-inserted row at its `'PENDING'`
        column default.

        A `NULL` `business_key` row is NEVER matched by this method
        regardless of what `business_keys` contains (D-25) — PostgreSQL's
        `= ANY(array)` operator structurally never matches `NULL` against
        any array of non-NULL values, so a row whose business-key value
        could not be reliably extracted at rejection time stays `PENDING`
        until an explicit batch-level discard.

        `business_keys=[]` is a legitimate no-op returning `0`, mirroring
        the old method's "0 rows affected is a legitimate outcome" framing
        (e.g. a caller with nothing new to resolve on this pass).

        Like the two methods above, `conn` is caller-supplied and never
        committed or rolled back here.

        Args:
            conn: An already-open connection, inside an already-open
                transaction.
            dataset_id: The dataset whose PENDING rejects are being
                resolved — scopes the match so the SAME business-key value
                in a DIFFERENT dataset is never touched.
            business_keys: The business-key values whose PENDING rejects
                should resolve. An empty sequence is a legitimate no-op.
            resolved_by_run_id: The run (a backfill run, or the run
                performing an explicit discard) responsible for this
                resolution — the FK lineage answers "was this ever fixed,
                and by what run" (D-05).
            resolution_type: The resolution outcome — `"REDRIVEN"` (resolved
                via a backfill run completing) or `"DISCARDED"` (resolved
                via an explicit batch-level operator action). Never
                `"PENDING"` — this method only ever transitions rows AWAY
                from that state.

        Returns:
            The number of rows resolved by this call. `0` when there were no
            matching PENDING rejects (including when `business_keys=[]`) —
            a legitimate, non-error outcome (e.g. a second resolution
            attempt against an already-resolved set).
        """
        ...

    def record_watermark(  # noqa: PLR0913 -- one keyword per record_watermark Protocol argument
        self,
        *,
        conn: Connection[Any],
        dataset_id: int,
        target_key: str,
        source_table: str,
        watermark_column: str,
        run_id: int,
        run_ids: Sequence[int],
    ) -> None:
        """Advance `meta.watermarks` using `GREATEST()`; always logs to `meta.watermark_history`.

        Maps to ``INSERT INTO meta.watermarks (dataset_id, target_key,
        cursor_value) VALUES (%s, %s, (SELECT max({watermark_column}
        ::timestamptz) FROM {source_table} WHERE _run_id = ANY(%s)))
        ON CONFLICT (dataset_id, target_key) DO UPDATE SET cursor_value =
        GREATEST(meta.watermarks.cursor_value, EXCLUDED.cursor_value)
        RETURNING cursor_value``, followed by an unconditional ``INSERT
        INTO meta.watermark_history (dataset_id, target_key, old_value,
        new_value, run_id)`` using the pre-update
        value (read via a preceding ``SELECT cursor_value FROM
        meta.watermarks WHERE dataset_id = %s AND target_key = %s`` — `None`
        when no row exists yet) and the just-returned new value.

        The `MAX()` subquery is scoped to `run_ids` (this publish pass's own
        staged runs) rather than reading the whole cumulative `source_table`
        — `source_table` is a shared, append-only table that other runs
        (past or concurrent) also write into, so an unscoped `MAX()` can be
        permanently poisoned by any stray/out-of-order row that was ever
        loaded, even by a completely unrelated run (found live: a single
        bad-dated `silver.customers` row froze that dataset's watermark
        forever). `GREATEST()` still enforces INCR-02's "never regress"
        rule against the previously-stored cursor across passes.

        INCR-02's "`>=`, never `>`" rule is enforced structurally by
        `GREATEST()` in the SQL text itself, never a conditional branch in
        Python — a publish carrying an OLDER `max({watermark_column})` than
        the currently-stored cursor can never regress `cursor_value`. D-04's
        "logs every write, moved or not" rule is enforced by the SECOND
        `INSERT` above being unconditional — it always runs, whether
        `cursor_value` actually moved or not.

        `source_table`/`watermark_column` are config-resolved identifiers
        (`_WATERMARK_COLUMN_BY_DATASET` and `f"silver.{ctx.config.dataset}"`
        in `pipeline/run.py`), never row content — the same trust boundary
        `merge.py`'s own `source_table` interpolation already accepts
        (T-09-03).

        MUST be called on the SAME `conn`/transaction `publish_ingest`
        already holds `pg_advisory_xact_lock` on (INCR-02, AP4 avoidance) —
        like `finalize_publication`, this method never opens its own
        connection and never commits or rolls back `conn` itself.

        Args:
            conn: An already-open connection, inside an already-open
                transaction — the same one `Publisher.publish` and
                `finalize_publication` run against.
            dataset_id: The dataset whose watermark is advancing.
            target_key: The watermark's target-key grain (D-03) — this
                phase's only caller always passes the literal `"default"`.
            source_table: The fully-qualified table to compute
                `max({watermark_column})` over, e.g. `"silver.customers"`.
                Interpolated as an identifier only, never a value.
            watermark_column: The column to take the max of, e.g.
                `"event_ts"`/`"order_date"` (D-02). Interpolated as an
                identifier only, never a value.
            run_id: The run attributed to this watermark write, recorded on
                the `meta.watermark_history` row.
            run_ids: Every run_id being finalized this publish pass — scopes
                the `MAX({watermark_column})` subquery to only the rows
                THIS pass staged into `source_table`, never the whole
                cumulative table.
        """
        ...

    def get_current_watermark(self, *, dataset_id: int, target_key: str) -> datetime | None:
        """Read one `meta.watermarks` row's current `cursor_value`, without writing.

        Maps to ``SELECT cursor_value FROM meta.watermarks WHERE dataset_id
        = %s AND target_key = %s``. A pure read, opening its own connection
        from the pool — mirrors `get_run_stage_status`'s own read-only
        contract exactly, never writes.

        Args:
            dataset_id: The dataset to read.
            target_key: The watermark's target-key grain to read.

        Returns:
            The row's current `cursor_value`, or `None` when no watermark
            row exists yet for this `(dataset_id, target_key)` pair, or when
            one exists but `cursor_value` itself is still `NULL`.
        """
        ...

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
        """Insert one `meta.reconciliation_results` row, inside the caller's own open transaction.

        Maps to a single parameterized ``INSERT INTO
        meta.reconciliation_results (dataset_id, file_id, hop, input_count,
        output_count, rejected_count, dedup_count, discrepancy, sum_column,
        sum_input, sum_output, checksum_input, checksum_output, min_input,
        max_input, min_output, max_output, key_count_input,
        key_count_output, expected_row_count, expected_checksum,
        control_total_discrepancy) VALUES (..., %(input_count)s -
        (%(output_count)s + %(rejected_count)s + %(dedup_count)s), ...,
        CASE WHEN %(expected_row_count)s IS NOT NULL THEN
        %(expected_row_count)s - %(output_count)s END) RETURNING
        reconciliation_id``.

        D-22's exact accounting formula (`discrepancy = input_count -
        (output_count + rejected_count + dedup_count)`) is computed as a SQL
        expression AT WRITE TIME, inside the `INSERT` statement's own
        `VALUES` clause — the formula lives in the SQL text itself, visible
        and grep-able, never hidden in Python arithmetic upstream of this
        call. `control_total_discrepancy` (VALID-06, D-23) is computed the
        same way: `NULL` unless `expected_row_count` is supplied (i.e. a
        `_BATCH_COMPLETE` manifest applied to this file), in which case it is
        `expected_row_count - output_count`.

        Like `finalize_publication`/`record_watermark`, `conn` is
        caller-supplied and never committed or rolled back here — this
        method MUST run inside the caller's already-open transaction.

        Args:
            conn: An already-open connection, inside an already-open
                transaction.
            dataset_id: The dataset this reconciliation row belongs to.
            file_id: The file this row belongs to (D-24's per-file-per-hop
                grain). `None` only defensively — every hop this phase
                writes populates it for real.
            hop: The pipeline hop this row reconciles — `"raw_bronze"`,
                `"bronze_silver"` or `"silver_gold"` (app-validated
                vocabulary, migration 0032).
            input_count: The row count on this hop's input side.
            output_count: The row count on this hop's output side.
            rejected_count: Rows quarantined between input and output on
                this hop. Defaults to `0`.
            dedup_count: Rows deduplicated away between input and output on
                this hop. Defaults to `0`.
            sum_column: The numeric column this dataset's reconciliation sum
                check compares, or `None` when the dataset declares no
                `reconciliation.sum_columns` (D-25).
            sum_input: The sum of `sum_column` on the input side, or `None`
                when `sum_column` is `None`.
            sum_output: The sum of `sum_column` on the output side, or
                `None` when `sum_column` is `None`.
            checksum_input: An order-independent aggregate hash of the input
                side's rows, or `None` when not computed for this hop.
            checksum_output: An order-independent aggregate hash of the
                output side's rows, or `None` when not computed for this
                hop.
            min_input: The minimum watermark-column value on the input side,
                or `None` when not computed for this hop.
            max_input: The maximum watermark-column value on the input side,
                or `None` when not computed for this hop.
            min_output: The minimum watermark-column value on the output
                side, or `None` when not computed for this hop.
            max_output: The maximum watermark-column value on the output
                side, or `None` when not computed for this hop.
            key_count_input: The distinct business-key count on the input
                side, or `None` when not computed for this hop.
            key_count_output: The distinct business-key count on the output
                side, or `None` when not computed for this hop.
            expected_row_count: The `_BATCH_COMPLETE` manifest's declared row
                count (VALID-06, D-23), or `None` when no manifest applied to
                this file. Only ever populated at the `raw_bronze` hop.
            expected_checksum: The `_BATCH_COMPLETE` manifest's declared
                checksum, or `None` when no manifest applied to this file.

        Returns:
            The new row's `reconciliation_id`.
        """
        ...
