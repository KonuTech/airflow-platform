"""``SCDPublisher`` -- assembles DELETE-detection, per-key recompute and atomic replace (D-07).

This is where Finding F-1 (10-RESEARCH.md) becomes real, executable code:
per-key recompute reads its FULL ordered history from ``staging.customers``
(the durable, cumulative, never-deduplicated bronze table, migration 0022)
-- never ``silver.customers``, which dbt's own ``delete+insert``/
``unique_key=customer_id`` incremental strategy collapses to exactly one
row per business key and would silently make SCD-07's late-arriving
correction unrecoverable.

Publication shape, one transaction (the caller's -- see ``publish()``'s own
docstring):

* **Step A -- DELETE-detection.** ``find_vanished_customer_ids`` (plan
  10-03) diffs THIS pass's own staged snapshot against every currently-
  current ``normalized.customers`` row; ``MassDeleteCircuitBreaker``
  evaluates the vanished/current ratio and raises ``QualityThresholdExceeded``
  uncaught (matching every other ``BarrierStage`` in this codebase's "catches
  nothing" convention) when it breaches ``ctx.config.scd.mass_delete_threshold``.
  When the breaker passes and at least one key vanished, ``apply_delete_semantics``
  dispatches the dataset's configured ``delete_semantics``.
* **Step B -- touched-key discovery.** ``SELECT DISTINCT customer_id FROM
  staging.customers WHERE _run_id = ANY(staged_run_ids)`` -- deliberately
  THIS pass's own already-known ``staged_run_ids``, never a separately
  re-derived watermark (a documented, reasoned divergence from
  10-RESEARCH.md's summary recommendation of a self-derived watermark:
  unlike dbt's own ``post_hook``, this Python code already has direct
  access to ``staged_run_ids``, so re-deriving it would be strictly more
  code for zero behavioral gain).
* **Step C -- per touched key, full-history recompute.** Reads THIS key's
  ENTIRE ``staging.customers`` history -- unscoped to ``staged_run_ids``,
  the one deliberate exception in this whole module (Finding F-1: a late
  correction may have been staged by an EARLIER run than the one that
  first triggered this key's inclusion in ``staged_run_ids``, so the
  recompute must see every bronze row this key has ever had, not just
  this pass's own new ones) -- and calls
  ``dataplat.scd.recompute.recompute_version_chain`` (plan 10-02, a pure
  function, no I/O) to deterministically rebuild the FULL version chain.
* **Step D -- atomic per-key replace.** ``DELETE FROM normalized.customers
  WHERE customer_id = ...`` followed by a bulk ``INSERT`` of the freshly
  recomputed chain, in the SAME transaction the caller already holds
  (SCD-09's idempotent-replay guarantee: replaying identical staged
  content recomputes the identical chain, so the DELETE+INSERT is a
  no-op in effect, not merely in intent).

``recompute_version_chain``'s own return type (``VersionRow``) deliberately
carries no lineage columns (plan 10-02's own settled interface, unchanged
by this plan) -- Step D independently re-derives, for each emitted version,
which bronze row is that version's LATEST contributor (``_select_lineage_rows``
below, reproducing ``recompute_version_chain``'s own
``tracked_attribute_hash``-based grouping rule) so every INSERTed row still
carries real ``_run_id``/``_file_id``/``_batch_id``/``_source_row_number``/
``_record_hash``/``_record_hash_version`` lineage (T-10-09, this plan's
threat model) -- never a blank/aggregate value, preserving this platform's
Core Value (traceability) through a DELETE+INSERT replace, not just an
append.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from dataplat.errors import ConfigurationError
from dataplat.load.publish.protocol import Publisher, PublishResult
from dataplat.scd.delete_detection import (
    MassDeleteCircuitBreaker,
    apply_delete_semantics,
    find_vanished_customer_ids,
)
from dataplat.scd.hashing import tracked_attribute_hash
from dataplat.scd.recompute import BronzeRecord, VersionRow, recompute_version_chain

if TYPE_CHECKING:
    from collections.abc import Sequence

    from psycopg import Connection

    from dataplat.pipeline.protocol import PipelineContext

# dbt's own documented `dbt_valid_to_current` example value, matching
# migration 0035's `_SENTINEL` exactly -- the open-ended "current" marker.
_VALID_TO_SENTINEL = datetime(9999, 12, 31, tzinfo=UTC)

# `staged_run_ids` is the only value below -- every table/column name is a
# literal, hand-written identifier (T-10-01, this plan's threat model).
_TOUCHED_KEYS_SQL = """
SELECT DISTINCT customer_id
FROM   staging.customers
WHERE  _run_id = ANY(%(staged_run_ids)s)
ORDER  BY customer_id
"""

# `customer_id` is the only value below. Deliberately UNSCOPED by
# `staged_run_ids` -- Finding F-1, this module's own docstring: a late
# correction may have been staged by an earlier run than the one that made
# this key "touched" for this pass, so every bronze row this key has ever
# had must be visible to the recompute, not just this pass's own new rows.
# `customer_id::int`/`event_ts::timestamptz` cast bronze's all-TEXT
# convention (migration 0022) into the types `BronzeRecord` declares.
_BRONZE_HISTORY_SQL = """
SELECT customer_id::int, name, country, birth_date, signup_country,
       event_ts::timestamptz, _source_row_number,
       _run_id, _file_id, _batch_id, _record_hash, _record_hash_version
FROM   staging.customers
WHERE  customer_id = %(customer_id)s
"""

_CURRENT_COUNT_SQL = "SELECT count(*) FROM normalized.customers WHERE is_current"

# `staged_run_ids` is the only value below.
_SNAPSHOT_MAX_EVENT_TS_SQL = """
SELECT max(event_ts::timestamptz)
FROM   silver.customers
WHERE  _run_id = ANY(%(staged_run_ids)s)
"""

# `customer_id` is the only value below.
_DELETE_VERSIONS_SQL = "DELETE FROM normalized.customers WHERE customer_id = %(customer_id)s"

# Every value below (`customer_id`/`name`/`country`/`birth_date`/
# `signup_country`/`valid_from`/`valid_to`/`is_current`/lineage columns) is
# bound, never interpolated. `birth_date::date` casts `VersionRow`'s
# all-TEXT-sourced value into `normalized.customers.birth_date`'s real
# `date` column type, matching `merge.py`'s own `birth_date::date`
# precedent.
_INSERT_VERSION_SQL = """
INSERT INTO normalized.customers (
    customer_id, name, country, birth_date, signup_country,
    event_ts, valid_to, is_current,
    _run_id, _file_id, _batch_id, _source_row_number,
    _record_hash, _record_hash_version
) VALUES (
    %(customer_id)s, %(name)s, %(country)s, %(birth_date)s::date, %(signup_country)s,
    %(valid_from)s, %(valid_to)s, %(is_current)s,
    %(run_id)s, %(file_id)s, %(batch_id)s, %(source_row_number)s,
    %(record_hash)s, %(record_hash_version)s
)
"""


@dataclass(frozen=True, slots=True)
class _BronzeLineageRow:
    """One ``staging.customers`` row, carrying both its business values AND its lineage columns.

    ``BronzeRecord`` (plan 10-02) deliberately excludes lineage columns --
    it is a pure-function input with no database concept at all. This local
    type exists only so Step D can independently re-derive, per emitted
    ``VersionRow``, which bronze row is that version's latest contributor
    (see ``_select_lineage_rows``).
    """

    customer_id: int
    name: str | None
    country: str | None
    birth_date: str | None
    signup_country: str | None
    event_ts: datetime
    source_row_number: int
    run_id: int
    file_id: int
    batch_id: int
    record_hash: bytes
    record_hash_version: int


def _select_lineage_rows(history: Sequence[_BronzeLineageRow]) -> list[_BronzeLineageRow]:
    """Return, per Type-2 version group, the group's LAST-ordered bronze row.

    Reproduces ``recompute_version_chain``'s own grouping rule exactly (sort
    by ``(event_ts, source_row_number)`` ascending, then split on a
    ``tracked_attribute_hash(name, country)`` change) so this module can
    source each emitted ``VersionRow``'s lineage columns from the correct
    bronze row without ``recompute_version_chain`` itself needing to expose
    lineage -- plan 10-02's own settled, lineage-free ``VersionRow``
    interface stays unchanged.

    Args:
        history: One customer_id's full bronze history, in any order.

    Returns:
        One lineage row per emitted version group, in the SAME oldest-first
        order ``recompute_version_chain`` returns its ``VersionRow``s in.
    """
    ordered = sorted(history, key=lambda r: (r.event_ts, r.source_row_number))
    hashes = [tracked_attribute_hash(r.name, r.country) for r in ordered]

    group_start_indices: list[int] = [0]
    group_start_indices.extend(
        i for i in range(1, len(ordered)) if hashes[i] != hashes[i - 1]
    )
    group_end_indices = [*group_start_indices[1:], len(ordered)]

    return [ordered[end_index - 1] for end_index in group_end_indices]


class SCDPublisher(Publisher):
    """The ``scd`` publication strategy: DELETE-detection + per-key recompute + atomic replace.

    ``conn`` carries an already-open transaction (``Publisher.publish()``'s
    own docstring): this method never commits or rolls it back, and it
    never takes the advisory lock itself -- ``publish_ingest`` (unchanged
    since plan 04-05) already holds ``pg_advisory_xact_lock`` for the whole
    call, matching ``MergePublisher``'s own ownership split (T-10-03, this
    plan's threat model).
    """

    name = "scd"

    def publish(
        self,
        ctx: PipelineContext,
        source_table: str,  # noqa: ARG002 -- unused; see class docstring below
        conn: Connection[Any],
        *,
        staged_run_ids: Sequence[int],
    ) -> PublishResult:
        """Publish this pass's touched ``customers`` keys as freshly recomputed SCD2 chains.

        Args:
            ctx: The current pipeline context. ``ctx.config.scd`` supplies
                ``mass_delete_threshold``/``delete_semantics`` (D-05/D-06).
            source_table: Unused -- unlike ``MergePublisher``, every read
                this method performs targets a literal, hardcoded table
                name (``staging.customers``/``silver.customers``/
                ``normalized.customers``), never the caller-supplied
                per-pass source table. Accepted only to satisfy the
                ``Publisher`` protocol's shared signature.
            conn: An open connection, inside an open transaction the caller
                owns. Never committed or rolled back here.
            staged_run_ids: THIS pass's own staged run ids -- scopes Step
                A's DELETE-detection snapshot and Step B's touched-key
                discovery; Step C's per-key recompute deliberately does
                NOT use this (see the module docstring's Finding F-1 note).

        Returns:
            A ``PublishResult`` whose ``rows_affected`` sums every row
            Steps A and D actually inserted/updated/deleted, whose
            ``published_business_keys`` is the union of this pass's
            touched keys and any vanished keys ``apply_delete_semantics``
            actually acted on, and whose ``outcome`` is always
            ``"PUBLISHED"``.

        Raises:
            ConfigurationError: ``ctx.config.scd`` is ``None`` -- a dataset
                whose ``load.strategy`` resolves to ``"scd"`` must declare
                an ``scd:`` config block (D-05/D-06).
            QualityThresholdExceeded: Step A's ``MassDeleteCircuitBreaker``
                found the vanished/current ratio exceeds
                ``ctx.config.scd.mass_delete_threshold`` -- propagated
                uncaught, rolling back the whole transaction (this
                module's "catches nothing" convention).
        """
        scd_config = ctx.config.scd
        if scd_config is None:
            msg = (
                f"dataset {ctx.config.dataset!r} resolves to the 'scd' publish strategy "
                "but declares no scd: config block (D-05/D-06)"
            )
            raise ConfigurationError(msg, context={"dataset": ctx.config.dataset})

        total_rows_affected = 0
        published_business_keys: set[str] = set()

        # --- Step A: DELETE-detection ---------------------------------
        vanished_ids = find_vanished_customer_ids(conn, staged_run_ids=staged_run_ids)
        current_count = int(conn.execute(_CURRENT_COUNT_SQL).fetchone()[0])  # type: ignore[index]

        breaker = MassDeleteCircuitBreaker(
            threshold=scd_config.mass_delete_threshold,
            current_count=current_count,
            vanished_count=len(vanished_ids),
        )
        breaker.apply(ctx)  # raises QualityThresholdExceeded uncaught on breach

        if vanished_ids:
            snapshot_max_event_ts = conn.execute(
                _SNAPSHOT_MAX_EVENT_TS_SQL,
                {"staged_run_ids": list(staged_run_ids)},
            ).fetchone()[0]  # type: ignore[index]
            acted_on = apply_delete_semantics(
                conn,
                delete_semantics=scd_config.delete_semantics,
                vanished_ids=vanished_ids,
                snapshot_max_event_ts=snapshot_max_event_ts,
            )
            total_rows_affected += len(acted_on)
            published_business_keys.update(acted_on)

        # --- Step B: touched-key discovery ------------------------------
        touched_keys_cursor = conn.execute(
            _TOUCHED_KEYS_SQL,
            {"staged_run_ids": list(staged_run_ids)},
        )
        touched_keys = [str(row[0]) for row in touched_keys_cursor.fetchall()]

        # --- Step C + D: per touched key, full-history recompute + atomic replace ---
        for customer_id_text in touched_keys:
            bronze_rows = conn.execute(
                _BRONZE_HISTORY_SQL,
                {"customer_id": customer_id_text},
            ).fetchall()

            history = [
                BronzeRecord(
                    customer_id=row[0],
                    name=row[1],
                    country=row[2],
                    birth_date=row[3],
                    signup_country=row[4],
                    event_ts=row[5],
                    source_row_number=row[6],
                )
                for row in bronze_rows
            ]
            lineage_rows = [
                _BronzeLineageRow(
                    customer_id=row[0],
                    name=row[1],
                    country=row[2],
                    birth_date=row[3],
                    signup_country=row[4],
                    event_ts=row[5],
                    source_row_number=row[6],
                    run_id=row[7],
                    file_id=row[8],
                    batch_id=row[9],
                    record_hash=row[10],
                    record_hash_version=row[11],
                )
                for row in bronze_rows
            ]

            versions: list[VersionRow] = recompute_version_chain(
                history,
                valid_to_sentinel=_VALID_TO_SENTINEL,
            )
            version_lineage = _select_lineage_rows(lineage_rows)

            delete_cursor = conn.execute(
                _DELETE_VERSIONS_SQL,
                {"customer_id": int(customer_id_text)},
            )
            total_rows_affected += delete_cursor.rowcount

            for version, lineage in zip(versions, version_lineage, strict=True):
                insert_cursor = conn.execute(
                    _INSERT_VERSION_SQL,
                    {
                        "customer_id": version.customer_id,
                        "name": version.name,
                        "country": version.country,
                        "birth_date": version.birth_date,
                        "signup_country": version.signup_country,
                        "valid_from": version.valid_from,
                        "valid_to": version.valid_to,
                        "is_current": version.is_current,
                        "run_id": lineage.run_id,
                        "file_id": lineage.file_id,
                        "batch_id": lineage.batch_id,
                        "source_row_number": lineage.source_row_number,
                        "record_hash": lineage.record_hash,
                        "record_hash_version": lineage.record_hash_version,
                    },
                )
                total_rows_affected += insert_cursor.rowcount

            published_business_keys.add(customer_id_text)

        return PublishResult(
            rows_affected=total_rows_affected,
            outcome="PUBLISHED",
            published_business_keys=tuple(sorted(published_business_keys)),
        )
