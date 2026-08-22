"""D-29's rebuild-from-raw pre/post reconciliation building blocks (Pitfall 7/8).

Plan 11-11. `11-CONTEXT.md`'s D-29 requires a rebuild-from-raw to prove it "reconciles to
its pre-drop state" via four checks: (1) row counts per table match, (2) a content
hash/checksum over each table's business data -- excluding surrogate run/batch identity
columns -- matches, (3) SCD2 version count + `valid_from`/`valid_to`/`is_current` state per
`customer_id` matches, and (4) the comparison reuses the existing
`meta.reconciliation_results`/`record_reconciliation` mechanism (Phase 9/10). Point 4 needs
no new code here -- it is naturally re-exercised by every reprocessed file during the
rebuild's own backfill (11-RESEARCH.md Pitfall 8). This module builds ONLY points 1-3: the
pre-drop snapshot and post-rebuild comparison arithmetic, proven correct in isolation before
plan 11-12 ever wires it into a real drop-and-rebuild.

Every function here is READ-ONLY: `snapshot_table_state`/`snapshot_customers_scd2_state`
issue nothing but `SELECT`s inside the caller's own open transaction, and `compare_snapshots`
performs no I/O at all -- a pure function over two already-captured snapshots. Mutation
(dropping the ETL-owned schemas, triggering the historical backfill) belongs entirely to
plan 11-12's orchestration script, never to this module.

Mirrors `_compute_silver_gold_reconciliation`'s own aggregate-query style
(`dataplat.pipeline.run`): config-resolved identifiers only (T-09-03), interpolated as
identifiers, never row content -- and reuses that module's own `_scalar`/`_table_checksum`
helpers directly rather than re-implementing them.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any

from dataplat.pipeline.run import _scalar, _table_checksum

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from datetime import datetime

    from psycopg import Connection

_NORMALIZED_CUSTOMERS_TABLE = "normalized.customers"


@dataclasses.dataclass(slots=True, frozen=True)
class TableSnapshot:
    """A point-in-time row-count + business-column-scoped checksum of one table (D-29 points 1-2).

    Attributes:
        table: The table this snapshot was captured from, e.g. ``"silver.customers"``.
        row_count: ``count(*)`` over the whole table at snapshot time.
        checksum: Task 1's column-scoped `_table_checksum` output (business columns only,
            excluding the embedded lineage columns) -- ``None`` for an empty table.
        key_count: ``count(DISTINCT business_key_column)`` at snapshot time, mirroring
            ``_compute_silver_gold_reconciliation``'s own ``key_count_input``/
            ``key_count_output`` figures (``run.py``). ``None`` unless
            ``snapshot_table_state`` was called with a ``business_key_column``.
    """

    table: str
    row_count: int
    checksum: str | None
    key_count: int | None = None


@dataclasses.dataclass(slots=True, frozen=True)
class ScdKeySnapshot:
    """One business key's SCD2 version-count + current-row validity state (D-29 point 3).

    Attributes:
        business_key: The business key value, as text (e.g. ``normalized.customers
            .customer_id`` cast to ``text``).
        version_count: How many SCD2 version rows exist for this key.
        current_valid_from: The current version's ``valid_from`` (``event_ts`` for
            `normalized.customers`, migration 0035's D-03 "event_ts doubles as valid_from"),
            or ``None`` if no row for this key is currently marked ``is_current``.
        current_valid_to: The current version's ``valid_to``, or ``None`` under the same
            condition as ``current_valid_from``.
        current_is_current: Whether exactly one row for this key is marked ``is_current``.
    """

    business_key: str
    version_count: int
    current_valid_from: datetime | None
    current_valid_to: datetime | None
    current_is_current: bool


@dataclasses.dataclass(slots=True, frozen=True)
class CustomersScd2Snapshot:
    """`normalized.customers`' whole-table snapshot plus per-`customer_id` SCD2 state (D-29 pt 3).

    Attributes:
        table_snapshot: The base row-count/checksum/key-count snapshot (D-29 points 1-2).
        keys: One `ScdKeySnapshot` per distinct business key present at snapshot time,
            ordered by business key for deterministic comparison.
    """

    table_snapshot: TableSnapshot
    keys: tuple[ScdKeySnapshot, ...]


@dataclasses.dataclass(slots=True, frozen=True)
class RebuildComparisonResult:
    """Named, per-field match/mismatch verdict from comparing two snapshots (D-29, T-11-29).

    Attributes:
        matches: ``True`` iff every compared field was identical.
        mismatches: Every differing field, named explicitly -- e.g. ``("row_count",)`` or
            ``("scd2_key:9995001.version_count",)``. T-11-29's own non-vacuity requirement:
            a caller never has to guess WHAT differed from a bare boolean. Empty when
            ``matches`` is ``True``.
    """

    matches: bool
    mismatches: tuple[str, ...]


def snapshot_table_state(
    conn: Connection[Any],
    table: str,
    *,
    business_columns: Sequence[str],
    business_key_column: str | None = None,
) -> TableSnapshot:
    """Capture `table`'s row count + business-column-scoped checksum (D-29 points 1-2).

    Read-only: issues only `SELECT`s, inside the caller's own already-open transaction.
    Mirrors `_compute_silver_gold_reconciliation`'s own aggregate-query style (`run.py`):
    config-resolved identifiers only, interpolated as identifiers, never row content.

    Args:
        conn: An already-open connection, inside the caller's own open transaction.
        table: A config-resolved table identifier (T-09-03), e.g. ``"silver.customers"``.
            Interpolated as an identifier only, never row content.
        business_columns: The dataset's business columns ONLY -- excludes the embedded
            lineage columns a rebuild-from-raw deliberately re-mints (see
            `_table_checksum`'s own docstring for the full six-column list, D-29/Pitfall 7).
            Config-resolved identifiers, interpolated as identifiers only.
        business_key_column: When given, also captures ``count(DISTINCT
            business_key_column)`` as `TableSnapshot.key_count` -- mirrors
            `_compute_silver_gold_reconciliation`'s own `key_count_input`/`key_count_output`
            figures. ``None`` (the default) leaves `key_count` unset.

    Returns:
        This table's current row-count/checksum/optional-key-count snapshot.
    """
    row_count = int(_scalar(conn, f"SELECT count(*) FROM {table}"))  # noqa: S608 -- T-09-03 identifier
    checksum = _table_checksum(conn, table, columns=business_columns)

    key_count: int | None = None
    if business_key_column is not None:
        key_count = int(
            _scalar(
                conn,
                f"SELECT count(DISTINCT {business_key_column}) FROM {table}",  # noqa: S608
            )
        )

    return TableSnapshot(table=table, row_count=row_count, checksum=checksum, key_count=key_count)


def snapshot_customers_scd2_state(
    conn: Connection[Any],
    *,
    business_columns: Sequence[str],
    table: str = _NORMALIZED_CUSTOMERS_TABLE,
) -> CustomersScd2Snapshot:
    """Capture `normalized.customers`' whole-table snapshot plus per-key SCD2 state (D-29 point 3).

    Read-only: issues only `SELECT`s, inside the caller's own already-open transaction.
    Column names (`customer_id`, `event_ts`, `valid_to`, `is_current`) are read verbatim
    from migration 0035 (`migrations/versions/0035_normalized_customers_scd2.py`), the
    revision that added `normalized.customers`' SCD2 shape.

    Args:
        conn: An already-open connection, inside the caller's own open transaction.
        business_columns: Passed straight through to `snapshot_table_state` for the base
            row-count/checksum snapshot (Task 1's column-scoped `_table_checksum`).
        table: Defaults to `"normalized.customers"` -- the one table this phase's SCD2 work
            (D-07/D-08, migration 0035) applies to. Overridable for testing against a
            differently-named copy; a config-resolved identifier either way, interpolated
            as an identifier only.

    Returns:
        `table`'s base `TableSnapshot` plus one `ScdKeySnapshot` per `customer_id`, ordered
        by `customer_id` for deterministic comparison.
    """
    table_snapshot = snapshot_table_state(
        conn, table, business_columns=business_columns, business_key_column="customer_id"
    )

    rows = conn.execute(
        f"""
        SELECT
            customer_id::text,
            count(*),
            max(event_ts) FILTER (WHERE is_current),
            max(valid_to) FILTER (WHERE is_current),
            count(*) FILTER (WHERE is_current) > 0
        FROM {table}
        GROUP BY customer_id
        ORDER BY customer_id
        """  # noqa: S608 -- `table` is a config-resolved identifier (T-09-03), never row content
    ).fetchall()

    keys = tuple(
        ScdKeySnapshot(
            business_key=row[0],
            version_count=int(row[1]),
            current_valid_from=row[2],
            current_valid_to=row[3],
            current_is_current=bool(row[4]),
        )
        for row in rows
    )
    return CustomersScd2Snapshot(table_snapshot=table_snapshot, keys=keys)


def _table_snapshot_mismatches(before: TableSnapshot, after: TableSnapshot) -> Iterator[str]:
    """Yield the name of every `TableSnapshot` field that differs between `before`/`after`."""
    if before.row_count != after.row_count:
        yield "row_count"
    if before.checksum != after.checksum:
        yield "checksum"
    if before.key_count != after.key_count:
        yield "key_count"


def _scd2_key_mismatches(
    before: Sequence[ScdKeySnapshot], after: Sequence[ScdKeySnapshot]
) -> Iterator[str]:
    """Yield a named mismatch for every `ScdKeySnapshot` field/key that differs.

    A key present on only one side yields a single `"scd2_key:<key>"` mismatch (rather than
    one per field, since there is no "other side" field value to compare against).
    """
    before_by_key = {snap.business_key: snap for snap in before}
    after_by_key = {snap.business_key: snap for snap in after}

    for key in sorted(before_by_key.keys() | after_by_key.keys()):
        before_snap = before_by_key.get(key)
        after_snap = after_by_key.get(key)
        if before_snap is None or after_snap is None:
            yield f"scd2_key:{key}"
            continue
        if before_snap.version_count != after_snap.version_count:
            yield f"scd2_key:{key}.version_count"
        if before_snap.current_valid_from != after_snap.current_valid_from:
            yield f"scd2_key:{key}.current_valid_from"
        if before_snap.current_valid_to != after_snap.current_valid_to:
            yield f"scd2_key:{key}.current_valid_to"
        if before_snap.current_is_current != after_snap.current_is_current:
            yield f"scd2_key:{key}.current_is_current"


def compare_snapshots(
    before: TableSnapshot | CustomersScd2Snapshot,
    after: TableSnapshot | CustomersScd2Snapshot,
) -> RebuildComparisonResult:
    """Compare two snapshots field-by-field, naming every mismatch (D-29, T-11-29).

    A PURE function -- no I/O, no database connection, testable in complete isolation from
    the `snapshot_*` functions above. Accepts either two plain `TableSnapshot`s (D-29 points
    1-2: row count + business-column-scoped checksum) or two `CustomersScd2Snapshot`s (adds
    D-29 point 3: per-`customer_id` SCD2 version-count/current-row-validity state) --
    `before`/`after` must be the SAME snapshot type.

    Args:
        before: The pre-drop snapshot.
        after: The post-rebuild snapshot, of the same snapshot type as `before`.

    Returns:
        `matches=True`/`mismatches=()` when every compared field is identical; otherwise
        `matches=False` and `mismatches` names each differing field.

    Raises:
        TypeError: `before`/`after` are not the same snapshot type.
    """
    if isinstance(before, CustomersScd2Snapshot) and isinstance(after, CustomersScd2Snapshot):
        mismatches = [
            *_table_snapshot_mismatches(before.table_snapshot, after.table_snapshot),
            *_scd2_key_mismatches(before.keys, after.keys),
        ]
        return RebuildComparisonResult(matches=not mismatches, mismatches=tuple(mismatches))

    if isinstance(before, TableSnapshot) and isinstance(after, TableSnapshot):
        mismatches = list(_table_snapshot_mismatches(before, after))
        return RebuildComparisonResult(matches=not mismatches, mismatches=tuple(mismatches))

    msg = "compare_snapshots requires before/after to be the same snapshot type"
    raise TypeError(msg)
