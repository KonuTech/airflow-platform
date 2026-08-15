"""``SchemaRepository`` — the Postgres-backed half of schema versioning (SCHEMA-03/06).

Sibling of ``dataplat.config.registry.ConfigRegistry`` (explicitly named as
such by CONTEXT.md and RESEARCH.md), transposing its exact versioned-upsert
SQL pattern onto ``meta.schema_versions`` (migration 0009): hash matches the
current version -> no-op; hash differs -> close the old row
(``valid_to = now()``) and insert ``version = max + 1``. Constructed with a
pool the caller builds via ``dataplat.storage.db.create_pool()`` — this
class never constructs its own ``ConnectionPool``, matching
``ConfigRegistry``'s own precedent.

Unlike ``ConfigRegistry.sync()``, ``sync()`` here takes an already-resolved
``dataset_id`` rather than a dataset name: ``meta.datasets`` rows are always
created by config-sync first (ARCHITECTURE.md §5.1), so schema-sync never
needs its own first-write dataset-row-creation race protection — only
serialization against a CONCURRENT schema sync for a dataset that already
exists (T-06-19 below).

``resolve_by_hash()`` is SCHEMA-06's D-16 mechanism: re-derive a file's
structure, hash it via ``dataplat.schema.versioning.hash_schema``, and match
it against ANY historical ``meta.schema_versions`` row for that dataset —
not only the current one — so a file whose structure matches a superseded
schema version resolves to that historical version rather than being forced
through the dataset's newest schema. D-17: this repository builds only this
specific hash-match mechanism, not the general ``config_policy`` replay knob
(``AS_OF_LOGICAL_DATE``/``LATEST``/``PINNED``) ARCHITECTURE.md §5.4 designed
but never implemented — that stays a documented future capability for
whichever phase first needs a human-selectable replay policy.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from psycopg.types.json import Jsonb

from dataplat.errors import StorageError
from dataplat.schema.versioning import hash_schema

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from psycopg_pool import ConnectionPool


@dataclass(frozen=True, slots=True)
class SchemaVersionRecord:
    """One ``meta.schema_versions`` row, as returned by ``SchemaRepository``.

    Attributes:
        schema_version_id: Surrogate primary key of the row.
        version: The row's 1-based version number for its dataset.
        schema_hash: The canonical-JSON sha256 hash this record reflects.
        compatibility: ``"COMPATIBLE"`` or ``"BREAKING"``, as stored.
        is_new: ``True`` when ``sync()`` inserted a new version;
            ``False`` when the schema was unchanged and no write happened,
            or when the record came from a read-only lookup
            (``get_current()``/``resolve_by_hash()``, which never write).
    """

    schema_version_id: int
    version: int
    schema_hash: str
    compatibility: str
    is_new: bool


def _require_row(row: tuple[Any, ...] | None, message: str) -> tuple[Any, ...]:
    """Narrow a possibly-``None`` fetched row, raising ``StorageError`` instead of asserting.

    A private copy of ``dataplat.config.registry``'s own helper of the same
    name (06-PATTERNS.md Cluster 7: a small, generic guard with no
    ``ConfigRegistry``-specific logic in it, safe to duplicate here).

    Args:
        row: The row returned by ``Cursor.fetchone()``.
        message: Description used as the raised error's message when ``row``
            is ``None``.

    Returns:
        ``row``, narrowed to non-``None``.

    Raises:
        StorageError: ``row`` is ``None``.
    """
    if row is None:
        raise StorageError(message)
    return row


class SchemaRepository:
    """The Postgres-backed system of record for ``meta.schema_versions``.

    Constructed with a pool the caller builds via
    ``dataplat.storage.db.create_pool()`` — this class never constructs its
    own ``ConnectionPool``, matching ``ConfigRegistry``'s own precedent.
    """

    def __init__(self, pool: ConnectionPool) -> None:
        """Initialize the repository with a caller-owned connection pool.

        Args:
            pool: A ``psycopg_pool.ConnectionPool`` built by
                ``dataplat.storage.db.create_pool()``. May be open or
                unopened — ``sync()`` opens it implicitly on first use via
                ``pool.connection()``.
        """
        self._pool = pool

    def sync(
        self,
        dataset_id: int,
        *,
        columns: Sequence[Mapping[str, object]],
        derived_from: str,
        compatibility: str = "COMPATIBLE",
        breaking_changes: Mapping[str, object] | None = None,
    ) -> SchemaVersionRecord:
        """Sync one dataset's resolved column list into ``meta.schema_versions``.

        Compares ``columns``' canonical hash (``hash_schema``) against the
        dataset's current (``valid_to IS NULL``) schema version: an
        unchanged hash is a no-op; a changed or absent hash closes the old
        row and inserts a new one at ``version = max + 1``.

        Args:
            dataset_id: The ``meta.datasets.dataset_id`` this schema
                belongs to. Must already exist — always true in practice
                since config-sync creates the ``meta.datasets`` row first
                (ARCHITECTURE.md §5.1).
            columns: The resolved, ordered column list to hash and store,
                matching ``meta.schema_versions.columns``' shape.
            derived_from: ``"CONTRACT"`` or ``"INFERRED"``, app-validated
                (never a native enum — migration 0009's own convention).
            compatibility: ``"COMPATIBLE"`` or ``"BREAKING"``, defaulting to
                ``"COMPATIBLE"`` for a dataset's first-ever schema version
                (there is no prior version to be incompatible with).
            breaking_changes: Structured detail about what changed, or
                ``None`` when there is nothing to record.

        Returns:
            A ``SchemaVersionRecord`` describing the current row after this
            call — ``is_new=False`` when nothing was written.

        Raises:
            StorageError: ``dataset_id`` has no ``meta.datasets`` row, or
                the insert unexpectedly returns no row.
        """
        schema_hash, hash_version = hash_schema(columns)
        with self._pool.connection() as conn, conn.cursor() as cur:
            # T-06-19 mitigation: lock the dataset row for the remainder of
            # this transaction, serializing two concurrent sync() calls for
            # the same dataset_id — the same row-lock discipline
            # ConfigRegistry._resolve_dataset_id's INSERT ... ON CONFLICT DO
            # UPDATE provides for an already-existing dataset (its own
            # docstring: "FOR UPDATE can only lock a row that already
            # exists" — exactly this method's only possible case, since a
            # schema is never synced before its dataset row exists).
            locked = cur.execute(
                "SELECT dataset_id FROM meta.datasets WHERE dataset_id = %s FOR UPDATE",
                (dataset_id,),
            ).fetchone()
            _require_row(locked, f"no meta.datasets row for dataset_id={dataset_id}")

            current = cur.execute(
                """
                SELECT schema_version_id, version, schema_hash, compatibility
                  FROM meta.schema_versions
                 WHERE dataset_id = %s AND valid_to IS NULL
                """,
                (dataset_id,),
            ).fetchone()

            if current is not None and current[2] == schema_hash:
                return SchemaVersionRecord(
                    schema_version_id=current[0],
                    version=current[1],
                    schema_hash=current[2],
                    compatibility=current[3],
                    is_new=False,
                )

            if current is not None:
                cur.execute(
                    "UPDATE meta.schema_versions SET valid_to = now() WHERE schema_version_id = %s",
                    (current[0],),
                )

            new_row = cur.execute(
                """
                INSERT INTO meta.schema_versions
                    (dataset_id, version, schema_hash, hash_version, columns,
                     derived_from, compatibility, breaking_changes, valid_from)
                VALUES (
                    %s,
                    COALESCE(
                        (SELECT MAX(version) FROM meta.schema_versions
                          WHERE dataset_id = %s) + 1,
                        1
                    ),
                    %s, %s, %s, %s, %s, %s, now()
                )
                RETURNING schema_version_id, version
                """,
                (
                    dataset_id,
                    dataset_id,
                    schema_hash,
                    hash_version,
                    Jsonb(list(columns)),
                    derived_from,
                    compatibility,
                    Jsonb(dict(breaking_changes)) if breaking_changes is not None else None,
                ),
            ).fetchone()

        inserted = _require_row(new_row, "meta.schema_versions insert returned no row")
        return SchemaVersionRecord(
            schema_version_id=inserted[0],
            version=inserted[1],
            schema_hash=schema_hash,
            compatibility=compatibility,
            is_new=True,
        )

    def get_current(self, dataset_id: int) -> SchemaVersionRecord | None:
        """Return a dataset's current (``valid_to IS NULL``) schema version, if any.

        Args:
            dataset_id: The ``meta.datasets.dataset_id`` to look up.

        Returns:
            The current ``SchemaVersionRecord``, or ``None`` when the
            dataset has no schema history yet.
        """
        with self._pool.connection() as conn, conn.cursor() as cur:
            row = cur.execute(
                """
                SELECT schema_version_id, version, schema_hash, compatibility
                  FROM meta.schema_versions
                 WHERE dataset_id = %s AND valid_to IS NULL
                """,
                (dataset_id,),
            ).fetchone()
        if row is None:
            return None
        return SchemaVersionRecord(
            schema_version_id=row[0],
            version=row[1],
            schema_hash=row[2],
            compatibility=row[3],
            is_new=False,
        )

    def resolve_by_hash(self, dataset_id: int, schema_hash: str) -> SchemaVersionRecord:
        """Resolve a re-derived structural hash to its historical ``meta.schema_versions`` row.

        SCHEMA-06's D-16 mechanism: matches ANY historical row for
        ``dataset_id`` with this exact ``schema_hash``, not only the
        current one — this is what lets a file from three schema versions
        ago resolve to its own historical version rather than being forced
        through the dataset's newest schema.

        Args:
            dataset_id: The ``meta.datasets.dataset_id`` to search within.
            schema_hash: The ``hash_schema``-computed hash of the file's
                re-derived structure.

        Returns:
            The matching ``SchemaVersionRecord``, current or historical.

        Raises:
            StorageError: No ``meta.schema_versions`` row for
                ``dataset_id`` has this ``schema_hash`` — the file's
                structure has never been recorded for this dataset.
        """
        with self._pool.connection() as conn, conn.cursor() as cur:
            row = cur.execute(
                """
                SELECT schema_version_id, version, schema_hash, compatibility
                  FROM meta.schema_versions
                 WHERE dataset_id = %s AND schema_hash = %s
                """,
                (dataset_id, schema_hash),
            ).fetchone()
        found = _require_row(
            row,
            f"no meta.schema_versions row for dataset_id={dataset_id} schema_hash={schema_hash}",
        )
        return SchemaVersionRecord(
            schema_version_id=found[0],
            version=found[1],
            schema_hash=found[2],
            compatibility=found[3],
            is_new=False,
        )
