"""``ConfigRegistry`` — the Postgres-backed half of config-sync (ARCHITECTURE.md §5.1).

``ConfigRegistry`` is a sibling of ``MetadataRepository``, not a consumer of
it (`03-PATTERNS.md` Cluster F): it talks to ``meta.datasets``/
``meta.config_versions`` directly through a pool built by
``dataplat.storage.db.create_pool()``, never constructing its own. This
module implements only the library-side half of config-sync — resolving one
dataset's config to a ``meta.config_versions`` row. The Airflow-side
``config-sync`` DAG that walks ``configs/datasets/`` on a schedule is
CONTEXT.md D-02's explicit out-of-scope boundary for this phase.

``sync()`` implements ARCHITECTURE.md §5.1's exact rule: hash matches the
current version -> no-op; hash differs -> close the old row
(``valid_to = now()``) and insert ``version = max + 1``. The whole
read-then-write sequence for one dataset runs inside a single transaction,
with ``_resolve_dataset_id()``'s row lock on the ``meta.datasets`` row
serializing concurrent ``sync()`` calls for that dataset (T-03-10 in this
plan's threat model) — without it, two concurrent syncs could both observe
"no current row" (or the same stale hash) and each insert a version with
``valid_to IS NULL``, which the partial unique index on
``meta.config_versions`` would then reject for the second writer as a
constraint violation instead of the intended serialized no-op/version
sequence. ``_resolve_dataset_id()`` itself is an atomic
``INSERT ... ON CONFLICT DO UPDATE`` (CR-03), not a plain
``SELECT ... FOR UPDATE`` followed by a plain ``INSERT``: a lock taken by
``FOR UPDATE`` only ever protects a row that already exists, so it does
nothing to serialize the *first-ever* ``sync()`` of a brand new dataset —
exactly the gap that let two concurrent first-time syncs race each other
into a raw ``psycopg.errors.UniqueViolation`` instead of the serialized
outcome this module's design intends.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from psycopg.types.json import Jsonb

from dataplat.config.hashing import hash_config
from dataplat.config.model import DatasetConfig
from dataplat.errors import StorageError

if TYPE_CHECKING:
    from psycopg import Cursor
    from psycopg_pool import ConnectionPool


@dataclass(frozen=True, slots=True)
class ConfigVersionRecord:
    """The outcome of one ``ConfigRegistry.sync()`` call.

    Attributes:
        config_version_id: Surrogate primary key of the current (or
            just-inserted) row in ``meta.config_versions``.
        version: The row's 1-based version number for its dataset.
        config_hash: The canonical-JSON sha256 hash this record reflects.
        is_new: ``True`` when ``sync()`` inserted a new version;
            ``False`` when the config was unchanged and no write happened.
    """

    config_version_id: int
    version: int
    config_hash: str
    is_new: bool


def _require_row(row: tuple[Any, ...] | None, message: str) -> tuple[Any, ...]:
    """Narrow a possibly-``None`` fetched row, raising ``StorageError`` instead of asserting.

    Args:
        row: The row returned by ``Cursor.fetchone()``.
        message: Description used as the raised error's message when ``row``
            is ``None`` — this indicates a schema/constraint invariant this
            module depends on (e.g. every ``INSERT ... RETURNING`` yields
            exactly one row) has been violated.

    Returns:
        ``row``, narrowed to non-``None``.

    Raises:
        StorageError: ``row`` is ``None``.
    """
    if row is None:
        raise StorageError(message)
    return row


class ConfigRegistry:
    """The Postgres-backed system of record for ``meta.config_versions``.

    Constructed with a pool the caller builds via
    ``dataplat.storage.db.create_pool()`` — this class never constructs its
    own ``ConnectionPool``, so pool sizing and construction-failure handling
    stay in that one place.
    """

    def __init__(self, pool: ConnectionPool) -> None:
        """Initialize the registry with a caller-owned connection pool.

        Args:
            pool: A ``psycopg_pool.ConnectionPool`` built by
                ``dataplat.storage.db.create_pool()``. May be open or
                unopened — ``sync()`` opens it implicitly on first use via
                ``pool.connection()``.
        """
        self._pool = pool

    def sync(self, dataset_name: str, config: DatasetConfig) -> ConfigVersionRecord:
        """Sync one dataset's resolved config into ``meta.config_versions``.

        Ensures ``dataset_name`` has a ``meta.datasets`` row (inserting one
        if absent), then compares ``config``'s canonical hash against the
        dataset's current (``valid_to IS NULL``) config version: an
        unchanged hash is a no-op; a changed or absent hash closes the old
        row and inserts a new one at ``version = max + 1``.

        Args:
            dataset_name: The dataset's unique name, matching
                ``meta.datasets.dataset_name``.
            config: The already-validated, resolved config to sync.

        Returns:
            A ``ConfigVersionRecord`` describing the current row after this
            call — ``is_new=False`` when nothing was written.
        """
        config_hash, hash_version = hash_config(config)
        freshness = config.freshness
        expected_frequency = freshness.expected_frequency if freshness is not None else None
        warn_after = freshness.warn_after if freshness is not None else None
        fail_after = freshness.fail_after if freshness is not None else None
        with self._pool.connection() as conn, conn.cursor() as cur:
            dataset_id = self._resolve_dataset_id(
                cur,
                dataset_name,
                expected_frequency=expected_frequency,
                warn_after=warn_after,
                fail_after=fail_after,
            )
            current = cur.execute(
                """
                SELECT config_version_id, version, config_hash
                  FROM meta.config_versions
                 WHERE dataset_id = %s AND valid_to IS NULL
                """,
                (dataset_id,),
            ).fetchone()

            if current is not None and current[2] == config_hash:
                return ConfigVersionRecord(
                    config_version_id=current[0],
                    version=current[1],
                    config_hash=current[2],
                    is_new=False,
                )

            if current is not None:
                cur.execute(
                    "UPDATE meta.config_versions SET valid_to = now() WHERE config_version_id = %s",
                    (current[0],),
                )

            new_row = cur.execute(
                """
                INSERT INTO meta.config_versions
                    (dataset_id, version, config_hash, hash_version,
                     config_document, config_schema_version, valid_from)
                VALUES (
                    %s,
                    COALESCE(
                        (SELECT MAX(version) FROM meta.config_versions
                          WHERE dataset_id = %s) + 1,
                        1
                    ),
                    %s, %s, %s, %s, now()
                )
                RETURNING config_version_id, version
                """,
                (
                    dataset_id,
                    dataset_id,
                    config_hash,
                    hash_version,
                    Jsonb(config.model_dump(mode="json")),
                    config.config_schema_version,
                ),
            ).fetchone()

        inserted = _require_row(new_row, "meta.config_versions insert returned no row")
        return ConfigVersionRecord(
            config_version_id=inserted[0],
            version=inserted[1],
            config_hash=config_hash,
            is_new=True,
        )

    def get_by_id(self, config_version_id: int) -> DatasetConfig:
        """Re-resolve one dataset's config exactly as it was at a specific version.

        This is the mechanism that lets historical reprocessing resolve a
        file's config EXACTLY as it was at ingestion time, without ever
        reading ``configs/*.yaml`` from disk: the pod that calls this (a
        later plan's ``ingest`` CLI) does not have ``configs/`` mounted or
        baked in -- only ``discover`` does.

        Args:
            config_version_id: The ``meta.config_versions.config_version_id``
                to resolve.

        Returns:
            The ``DatasetConfig`` this version's ``config_document``
            validates as.

        Raises:
            StorageError: No ``meta.config_versions`` row matches
                ``config_version_id``.
        """
        with self._pool.connection() as conn, conn.cursor() as cur:
            row = cur.execute(
                "SELECT config_document FROM meta.config_versions WHERE config_version_id = %s",
                (config_version_id,),
            ).fetchone()
        found = _require_row(
            row,
            f"no meta.config_versions row for config_version_id={config_version_id}",
        )
        # The JSONB column already round-trips through psycopg as a Python
        # dict -- no json.loads needed (see module docstring).
        return DatasetConfig.model_validate(found[0])

    @staticmethod
    def _resolve_dataset_id(
        cur: Cursor[Any],
        dataset_name: str,
        *,
        expected_frequency: str | None = None,
        warn_after: str | None = None,
        fail_after: str | None = None,
    ) -> int:
        """Return ``dataset_name``'s ``dataset_id``, inserting the row if absent.

        A single atomic ``INSERT ... ON CONFLICT DO UPDATE`` (CR-03), never a
        plain ``SELECT ... FOR UPDATE`` followed by a plain ``INSERT``:
        ``FOR UPDATE`` can only lock a row that already exists, so it does
        nothing to serialize the *first-ever* ``sync()`` of a brand new
        dataset -- two concurrent first-time callers could both observe "no
        row exists" before either commits, and the loser's plain ``INSERT``
        would raise a raw, unwrapped `psycopg.errors.UniqueViolation`
        against `meta.datasets`' `UNIQUE(dataset_name)` constraint instead of
        resolving to the winner's row. ``DO UPDATE SET dataset_name =
        EXCLUDED.dataset_name`` is a standard no-op-update idiom: it changes
        nothing (the value is identical), but -- unlike ``DO NOTHING``, which
        returns no row on conflict -- it still lets ``RETURNING`` yield the
        existing row's `dataset_id` in one round trip. The ``UPDATE`` half of
        an upsert takes the same row-level lock ``FOR UPDATE`` would have,
        held for the remainder of the caller's transaction, so a concurrent
        ``sync()`` for the same (already-existing) dataset still blocks
        until this one commits or rolls back.

        ``expected_frequency``/``warn_after``/``fail_after`` (07-CONTEXT.md
        D-08, OBS-01/OBS-09) ride the same upsert: each binds through a
        ``%s::interval`` placeholder, so PostgreSQL parses the interval
        literal server-side (binding ``None`` resolves to ``NULL``
        correctly through the same placeholder) and each is also set on
        conflict, exactly like ``dataset_name`` -- so a dataset whose
        ``freshness:`` block is later removed from its YAML correctly nulls
        these columns back out on the next ``sync()``, never leaving stale
        freshness state behind.

        Args:
            cur: An open cursor on the transaction ``sync()`` is running.
            dataset_name: The dataset's unique name.
            expected_frequency: A PostgreSQL interval literal naming this
                dataset's expected delivery frequency, or ``None`` when
                freshness is not tracked for this dataset.
            warn_after: A PostgreSQL interval literal naming the grace
                period before a WARN-severity freshness threshold, or
                ``None``.
            fail_after: A PostgreSQL interval literal naming the grace
                period before a FAIL-severity freshness threshold, or
                ``None``.

        Returns:
            The dataset's ``dataset_id``.
        """
        row = cur.execute(
            """
            INSERT INTO meta.datasets
                (dataset_name, expected_frequency, freshness_warn_after, freshness_fail_after)
            VALUES (%s, %s::interval, %s::interval, %s::interval)
            ON CONFLICT (dataset_name) DO UPDATE
                SET dataset_name = EXCLUDED.dataset_name,
                    expected_frequency = EXCLUDED.expected_frequency,
                    freshness_warn_after = EXCLUDED.freshness_warn_after,
                    freshness_fail_after = EXCLUDED.freshness_fail_after
            RETURNING dataset_id
            """,
            (dataset_name, expected_frequency, warn_after, fail_after),
        ).fetchone()
        return int(_require_row(row, "meta.datasets insert returned no row")[0])
