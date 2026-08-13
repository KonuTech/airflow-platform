"""``MergePublisher`` — the first concrete ``Publisher``: advisory-lock + ``ON CONFLICT``.

**Why not literal SQL ``MERGE``**, which is what ``ARCHITECTURE.md``'s own
worked publication-transaction example shows: PostgreSQL's ``MERGE`` is not
concurrency-safe under PostgreSQL's snapshot semantics -- documented as
**PostgreSQL BUG #18279**, where two concurrent ``MERGE`` transactions both
independently decide no matching row exists against their own snapshot and
both attempt their own insert-branch, so the loser raises a unique-violation
instead of falling through to its update-branch. LOAD-09 and
04-RESEARCH.md's PITFALLS #14/C1 reject ``MERGE`` for exactly this reason.
``INSERT ... ON CONFLICT`` is the
verified-correct primitive instead: `[VERIFIED: postgresql.org/docs/current/
sql-insert.html]` -- a conflicting row is either updated in place (still
inside the same statement's snapshot-independent conflict handling) or, when
its ``DO UPDATE ... WHERE`` clause evaluates false, "locked but left
unchanged" and correctly excluded from the affected-row count.

``DISTINCT ON (customer_id)`` inside the publish ``SELECT`` is required even
though ``configs/datasets/customers.yaml`` already declares
``deduplication.strategy: business_key_latest`` (PITFALLS C1): an
``ON CONFLICT DO UPDATE`` whose source contains duplicate keys raises
``ON CONFLICT DO UPDATE command cannot affect row a second time`` -- the
guard must be structural in this SQL, not merely configured correctly
upstream.

This ``MergePublisher`` is deliberately single-dataset for this phase: the
target table and its business-column list are hardcoded against
``normalized.customers`` (migrations ``0005``/``0006``), not resolved from
``ctx.config.load.target`` -- a generic upsert-any-table publisher is future
work, not this vertical slice's scope.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dataplat.load.publish.protocol import Publisher, PublishResult

if TYPE_CHECKING:
    from psycopg import Connection

    from dataplat.pipeline.protocol import PipelineContext

# The corrected publication statement (04-RESEARCH.md Pattern 1). The ONLY
# dynamic SQL fragment is `{staging_table}`, and it is interpolated as an
# IDENTIFIER only, never a value (T-04-01, this plan's threat model):
# `staging_table` is built by `StagingLoader` from `ctx.config.dataset` + a
# numeric `run_id`, never from CSV row content. Every column/table name
# below is a literal, hardcoded against normalized.customers's real schema.
_PUBLISH_SQL = """
INSERT INTO normalized.customers (
    customer_id, name, country, birth_date, event_ts,
    _run_id, _file_id, _batch_id, _source_row_number,
    _record_hash, _record_hash_version
)
SELECT DISTINCT ON (customer_id)
       customer_id::int, name, country, birth_date::date, event_ts::timestamptz,
       _run_id, _file_id, _batch_id, _source_row_number,
       _record_hash, _record_hash_version
FROM   {staging_table}
ORDER  BY customer_id, event_ts DESC, _source_row_number DESC
ON CONFLICT (customer_id) DO UPDATE
   SET name = EXCLUDED.name, country = EXCLUDED.country,
       birth_date = EXCLUDED.birth_date, event_ts = EXCLUDED.event_ts,
       _record_hash = EXCLUDED._record_hash,
       _record_hash_version = EXCLUDED._record_hash_version,
       _run_id = EXCLUDED._run_id, _file_id = EXCLUDED._file_id,
       _batch_id = EXCLUDED._batch_id, _source_row_number = EXCLUDED._source_row_number
 WHERE normalized.customers._record_hash IS DISTINCT FROM EXCLUDED._record_hash
   AND EXCLUDED.event_ts >= normalized.customers.event_ts
"""


class MergePublisher(Publisher):
    """The ``merge`` publication strategy: ``pg_advisory_xact_lock`` + ``INSERT ... ON CONFLICT``.

    ``conn`` carries an already-open transaction (``Publisher.publish``'s
    own docstring): this method never commits or rolls it back, and it
    never takes the advisory lock itself. The caller -- plan 04-05's
    ``run_ingest`` orchestration -- takes ``pg_advisory_xact_lock``
    immediately before invoking ``publish()``, inside the same transaction,
    so the lock protects exactly this statement and nothing else. Placing
    the lock call here instead would be redundant at best (it would just
    re-acquire a lock the caller already holds) and, if this method were
    ever called without the caller's lock, would silently remove the
    single-writer guarantee LOAD-09 exists to provide -- so the ownership
    split is deliberate, not an oversight.
    """

    name = "merge"

    def publish(
        self,
        ctx: PipelineContext,  # noqa: ARG002 -- unused; see class docstring + Args below
        staging_table: str,
        conn: Connection[Any],
    ) -> PublishResult:
        """Publish ``staging_table``'s rows into ``normalized.customers``.

        Args:
            ctx: The current pipeline context. Unused -- see the class
                docstring.
            staging_table: The fully-qualified staging table to read from,
                e.g. ``"staging.customers__r8123"``. Interpolated into the
                statement as an identifier only -- see the module docstring.
            conn: An open connection, inside an open transaction the caller
                owns. Never committed or rolled back here.

        Returns:
            A ``PublishResult`` whose ``rows_affected`` is
            ``cursor.rowcount`` after the ``INSERT ... ON CONFLICT``
            executes (PostgreSQL's own command-tag count of rows inserted
            or updated -- a row "locked but left unchanged" by a false
            ``DO UPDATE ... WHERE`` is correctly excluded), and whose
            ``outcome`` is always ``"PUBLISHED"``.
        """
        cursor = conn.execute(
            _PUBLISH_SQL.format(staging_table=staging_table),
        )
        return PublishResult(rows_affected=cursor.rowcount, outcome="PUBLISHED")
