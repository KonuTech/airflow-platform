"""``OrdersMergePublisher`` — the second concrete ``Publisher``: advisory-lock + ``ON CONFLICT``.

Mirrors ``merge.py``'s ``MergePublisher`` exact module structure and
reasoning, targeting ``normalized.orders`` (D-17's four business columns)
instead of ``normalized.customers``.

**Why not literal SQL ``MERGE``** — the same reason as ``merge.py``:
PostgreSQL's ``MERGE`` is not concurrency-safe under PostgreSQL's snapshot
semantics, documented as **PostgreSQL BUG #18279**, where two concurrent
``MERGE`` transactions both independently decide no matching row exists
against their own snapshot and both attempt their own insert-branch, so the
loser raises a unique-violation instead of falling through to its
update-branch. ``INSERT ... ON CONFLICT`` is the verified-correct primitive
instead: `[VERIFIED: postgresql.org/docs/current/sql-insert.html]` -- a
conflicting row is either updated in place (still inside the same
statement's snapshot-independent conflict handling) or, when its
``DO UPDATE ... WHERE`` clause evaluates false, "locked but left unchanged"
and correctly excluded from the affected-row count.

``DISTINCT ON (order_id)`` inside the publish ``SELECT`` is required even
though ``configs/datasets/orders.yaml`` already declares
``deduplication.strategy: business_key_latest`` (the same reasoning as
``merge.py``'s own C1 note): an ``ON CONFLICT DO UPDATE`` whose source
contains duplicate keys raises ``ON CONFLICT DO UPDATE command cannot
affect row a second time`` -- the guard must be structural in this SQL, not
merely configured correctly upstream.

This ``OrdersMergePublisher`` is deliberately single-dataset, matching
``MergePublisher``'s own precedent: the target table and its business-column
list are hardcoded against ``normalized.orders`` (migration ``0016``), not
resolved from ``ctx.config.load.target`` -- a generic upsert-any-table
publisher remains future work, not this phase's scope.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dataplat.load.publish.protocol import Publisher, PublishResult

if TYPE_CHECKING:
    from psycopg import Connection

    from dataplat.pipeline.protocol import PipelineContext

# The ONLY dynamic SQL fragment is `{staging_table}`, interpolated as an
# IDENTIFIER only, never a value (T-08-11, this plan's threat model, mirrors
# `merge.py`'s own T-04-01 precedent): `staging_table` is built by
# `StagingLoader` from `ctx.config.dataset` + a numeric `run_id`, never from
# CSV row content. Every column/table name below is a literal, hardcoded
# against normalized.orders's real schema (migration 0016).
#
# `normalized.orders.order_date IS NULL OR ...` (CR-01-adjacent finding,
# phase-08 code review, WR-04): unlike `merge.py`'s `event_ts` (declared
# `nullable: false`), `order_date` IS `nullable: true` (orders.yaml) --
# plain `EXCLUDED.order_date >= normalized.orders.order_date` is NULL
# (not TRUE) in three-valued SQL logic whenever the EXISTING row's
# order_date is NULL, so the whole `WHERE` clause would evaluate NULL and
# the row would be "locked but left unchanged" FOREVER, even by an update
# that legitimately fills in a real date. The `IS NULL OR` branch treats an
# existing NULL as "always supersede-able", matching this platform's core
# value that no data is ever silently dropped or left uncorrectable.
_PUBLISH_SQL = """
INSERT INTO normalized.orders (
    order_id, customer_id, order_date, amount,
    _run_id, _file_id, _batch_id, _source_row_number,
    _record_hash, _record_hash_version
)
SELECT DISTINCT ON (order_id)
       order_id::int, customer_id::int, order_date::date, amount::numeric,
       _run_id, _file_id, _batch_id, _source_row_number,
       _record_hash, _record_hash_version
FROM   {staging_table}
ORDER  BY order_id, order_date DESC, _source_row_number DESC
ON CONFLICT (order_id) DO UPDATE
   SET customer_id = EXCLUDED.customer_id, order_date = EXCLUDED.order_date,
       amount = EXCLUDED.amount,
       _record_hash = EXCLUDED._record_hash,
       _record_hash_version = EXCLUDED._record_hash_version,
       _run_id = EXCLUDED._run_id, _file_id = EXCLUDED._file_id,
       _batch_id = EXCLUDED._batch_id, _source_row_number = EXCLUDED._source_row_number
 WHERE normalized.orders._record_hash IS DISTINCT FROM EXCLUDED._record_hash
   AND (normalized.orders.order_date IS NULL
        OR EXCLUDED.order_date >= normalized.orders.order_date)
RETURNING order_id
"""


class OrdersMergePublisher(Publisher):
    """The ``merge_orders`` publication strategy: ``pg_advisory_xact_lock`` + ``ON CONFLICT``.

    ``conn`` carries an already-open transaction (``Publisher.publish``'s
    own docstring): this method never commits or rolls it back, and it
    never takes the advisory lock itself. The caller -- ``run_ingest``'s
    orchestration, same as ``MergePublisher`` -- takes
    ``pg_advisory_xact_lock`` immediately before invoking ``publish()``,
    inside the same transaction, so the lock protects exactly this
    statement and nothing else. Placing the lock call here instead would be
    redundant at best (it would just re-acquire a lock the caller already
    holds) and, if this method were ever called without the caller's lock,
    would silently remove the single-writer guarantee LOAD-09 exists to
    provide -- so the ownership split is deliberate, not an oversight
    (identical reasoning to ``MergePublisher``'s own class docstring).
    """

    name = "merge_orders"

    def publish(
        self,
        ctx: PipelineContext,  # noqa: ARG002 -- unused; see class docstring + Args below
        source_table: str,
        conn: Connection[Any],
    ) -> PublishResult:
        """Publish ``source_table``'s rows into ``normalized.orders``.

        Args:
            ctx: The current pipeline context. Unused -- see the class
                docstring.
            source_table: The fully-qualified table to read from -- a
                per-run scratch staging table before plan 08.1-10, e.g.
                ``"staging.orders__r8123"``, or ``silver.orders`` from plan
                08.1-10 onward. Interpolated into the statement as an
                identifier only -- see the module docstring.
            conn: An open connection, inside an open transaction the caller
                owns. Never committed or rolled back here.

        Returns:
            A ``PublishResult`` whose ``rows_affected`` is
            ``cursor.rowcount`` after the ``INSERT ... ON CONFLICT``
            executes (PostgreSQL's own command-tag count of rows inserted
            or updated -- a row "locked but left unchanged" by a false
            ``DO UPDATE ... WHERE`` is correctly excluded), whose
            ``published_business_keys`` is the ``order_id`` of every row
            this statement's ``RETURNING`` clause actually surfaced --
            i.e. the exact same set ``rows_affected`` counts, never the
            source table's own contents (CR-01, phase-08 code review) --
            and whose ``outcome`` is always ``"PUBLISHED"``.
        """
        cursor = conn.execute(
            _PUBLISH_SQL.format(staging_table=source_table),
        )
        published_business_keys = tuple(str(row[0]) for row in cursor.fetchall())
        return PublishResult(
            rows_affected=cursor.rowcount,
            outcome="PUBLISHED",
            published_business_keys=published_business_keys,
        )
