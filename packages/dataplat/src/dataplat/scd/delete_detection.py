"""SCD DELETE-detection: snapshot diff, mass-delete breaker, semantics dispatch (D-04..D-06).

Finding F-2 (10-RESEARCH.md): ``silver.customers`` is dbt's own incremental
``delete+insert`` model target (migration 0023's ``UNIQUE(customer_id)``) --
it holds exactly one row per business key EVER seen, continuously updated in
place, and it never drops a key just because that key's source row is
missing from the CURRENT pass's file(s). An unscoped
``SELECT customer_id FROM silver.customers`` therefore always contains every
customer this dataset has ever ingested, which makes naive DELETE-detection
("is this gold-current key present in silver?") permanently vacuous -- a key
can vanish from a source file forever and this check would still see it,
via its stale row from some earlier pass. ``find_vanished_customer_ids``
below scopes its read to ``_run_id = ANY(staged_run_ids)`` -- THIS pass's
own staged runs only -- so a key this pass's files did not deliver
correctly reads as vanished, even though it is still sitting in the
cumulative history. This mirrors ``metadata/repository.py``'s own
``record_watermark`` run-scoping precedent (its own docstring, cited in this
plan's ``<interfaces>``), for the identical reason: an unscoped read of a
shared, cumulative, append/upsert table produces a permanently-poisoned
result.

Debug session ci-pipeline-ingestion-timeout ROUND 12 (root cause 16, live
run 32884691063): the run-scoped snapshot MUST be read from
``staging.customers`` (bronze), never from ``silver.customers``. Silver's
incremental dedup keeps exactly one row per business key, ranked by
``(event_ts desc, _source_row_number desc, _file_id desc)``, and the
winning row keeps its OWN ``_run_id`` -- so when a pass re-stages content
BYTE-IDENTICAL to already-resident rows (exactly what D-18's
idempotency-key formula produces after ``meta.schema_versions`` changes:
every already-SUCCEEDED file becomes eligible again and is re-staged under
a new ``_run_id`` but the SAME ``event_ts``/``_source_row_number``/
``_file_id``), every ranking term ties and the winner is ARBITRARY. A
silver-scoped snapshot then silently drops every tie-loser key from "this
pass's snapshot" even though the pass's own files contain it -- observed
live as a deterministic 54% (27/50) vanished ratio tripping
``MassDeleteCircuitBreaker`` on every replay pass, wedging every DagRun,
and reproduced 1:1 by
``tests/integration/test_scd_replay_delete_detection.py`` (48% locally --
the split is genuinely arbitrary). Bronze scoped by ``staged_run_ids`` IS
the pass's delivered key set by construction -- immune to dedup-tie
lineage -- and is the same table/scoping Step B's touched-key discovery
(``load/publish/scd.py``'s ``_TOUCHED_KEYS_SQL``) has always used.

10-07-PLAN.md Task 1 (Rule 4, user-approved live finding): a SECOND, related
unscoped-read bug, found live against the real cluster's own
``normalized.customers`` -- 12,001,043 rows, ALL ``is_current = true``, the
overwhelming majority inserted by Phase 4's original vertical-slice proof
(``MergePublisher``, weeks before ``staging.customers``/``silver.customers``
or SCD existed at all). ``find_vanished_customer_ids``'s own ``WHERE
is_current`` predicate has NO scope beyond that -- it reads every
``is_current`` row in the whole table, Phase-4-era legacy rows included.
Since those legacy rows were never staged through the bronze pipeline, they
can NEVER appear in ANY ``staged_snapshot`` (``silver.customers`` itself
only holds 1,020 distinct customer_ids, dbt's own genuinely SCD-managed
working set) -- meaning they are permanently, structurally "vanished" by
this check's own logic, for every single publish call, forever. As the
shared, cluster-wide ``normalized.customers`` table accumulates more
unrelated legacy/other-dataset rows over the project's life, the vanished
ratio mathematically trends toward 100%, eventually tripping
``MassDeleteCircuitBreaker`` permanently -- not because anything was
actually mass-deleted, but because the denominator/numerator both include
keys this DELETE-detection mechanism was never designed to reason about.

Fix: both ``_VANISHED_SQL`` below and ``load/publish/scd.py``'s
``_CURRENT_COUNT_SQL`` are now additionally scoped to ``customer_id``s that
have EVER appeared in ``staging.customers`` (bronze) -- the durable,
cumulative table the snapshot-fed SCD pipeline actually populates.
DELETE-detection only makes sense for keys the snapshot pipeline has ever
legitimately observed; a ``normalized.customers`` row with no corresponding
bronze row at all is, by construction, unreachable via this dataset's own
CSV-ingestion path and must never be considered for vanished-ratio
accounting. This correctly excludes Phase 4's pre-bronze legacy rows from
BOTH the denominator (``current_count``) and the numerator (vanished set)
while still catching a real mass-deletion among genuinely SCD-managed keys.

``MassDeleteCircuitBreaker`` mirrors ``validate/circuit_breaker.py``'s
``RejectionRateCircuitBreaker`` shape exactly (constructor-parameterized
totals, ``apply(ctx)`` never re-derives them, ``current_count == 0`` is the
trivial-PASS empty-input guard) -- see that module's own docstring for the
full rationale, substituted here for "vanished/current" instead of
"rejected/read".

``apply_delete_semantics`` dispatches D-05's three DELETE-semantics values.
The ``new_record`` action is a single, atomic SQL statement (a
data-modifying CTE: the invalidating ``UPDATE`` feeds its own ``RETURNING``
rows straight into the following ``INSERT``) rather than two separate
statements run one after another -- running the plain ``UPDATE ... WHERE
is_current`` a SECOND time, after an ``INSERT`` that (by column default)
also lands with ``is_current = true``, would incorrectly re-match and close
out the just-inserted row too, since both the old and new row would share
``customer_id = ANY(vanished_ids) AND is_current`` at that point. Wrapping
the invalidating ``UPDATE`` as a CTE and feeding its ``RETURNING`` output
into the ``INSERT`` reuses the exact same ``UPDATE`` text (still "the SAME
UPDATE invalidate uses to close the old row") while guaranteeing the
``INSERT`` reads a stable, pre-update snapshot of exactly the row(s) being
closed -- correct regardless of execution order, because it is one
statement, not two.

Every interpolated SQL fragment below is a literal, hand-written identifier
(table/column names) -- ``staged_run_ids``, ``vanished_ids`` and
``snapshot_max_event_ts`` are ALWAYS bound as values via ``%(...)s``, never
interpolated into the SQL text (T-10-01, this plan's threat model, matching
``merge.py``/``referential.py``'s own T-04-01/T-08-11 precedent).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dataplat.errors import ConfigurationError, QualityThresholdExceeded
from dataplat.models.record import RecordChunk, StageResult
from dataplat.models.report import ValidationResult
from dataplat.pipeline.protocol import BarrierStage

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime

    from psycopg import Connection

    from dataplat.pipeline.protocol import PipelineContext

# `staged_run_ids` is the ONLY value here -- `staging.customers`/
# `normalized.customers`/`customer_id`/`is_current` are all literal,
# hand-written identifiers (T-10-01). The cast to `::text` on the
# `normalized.customers` side is required because `normalized.customers.
# customer_id` is `integer` (migration 0005) while `staging.customers`'s
# own `customer_id` is `text` (migration 0022, staging's own all-text
# convention) -- comparing them directly would raise `operator does not
# exist: integer = text`.
#
# `staged_snapshot` reads BRONZE (`staging.customers`), never
# `silver.customers` -- ROUND 12 fix for root cause (16), see the module
# docstring: silver's dedup-tie winner keeps an arbitrary `_run_id` when a
# pass re-stages byte-identical content (D-18 replay), silently dropping
# tie-loser keys from a silver-scoped snapshot and manufacturing a
# mass-delete trip out of a correct replay. Bronze scoped by
# `staged_run_ids` is exactly the key set this pass's files delivered.
# The `customer_id IS NOT NULL` guard is defensive: staging's all-text
# columns are nullable, and a single NULL inside a `NOT IN (...)` subquery
# would silently empty the whole vanished set.
#
# `bronze_known` (10-07-PLAN.md Task 1, live finding): scopes the vanished
# candidate set to customer_ids that have EVER appeared in `staging.
# customers` (bronze) -- see the module docstring's live-cluster finding
# (12,001,043 is_current rows, 1,020 of them ever staged through bronze).
# Without this scope, every one of Phase 4's pre-bronze legacy rows is
# permanently "vanished" (never in ANY staged_snapshot, since they never
# went through the bronze pipeline at all), mathematically guaranteeing the
# mass-delete ratio trends toward 100% as the shared table accumulates more
# unrelated legacy data over the project's life.
_VANISHED_SQL = """
WITH staged_snapshot AS (
    SELECT DISTINCT customer_id
    FROM   staging.customers
    WHERE  _run_id = ANY(%(staged_run_ids)s)
      AND  customer_id IS NOT NULL
),
bronze_known AS (
    SELECT DISTINCT customer_id
    FROM   staging.customers
)
SELECT customer_id
FROM   normalized.customers
WHERE  is_current
  AND  customer_id::text IN (SELECT customer_id FROM bronze_known)
  AND  customer_id::text NOT IN (SELECT customer_id FROM staged_snapshot)
"""


def find_vanished_customer_ids(
    conn: Connection[Any], *, staged_run_ids: Sequence[int]
) -> set[str]:
    """Return every currently-current ``customer_id`` missing from THIS pass's staged bronze.

    Scoped to keys that have ever appeared in ``staging.customers`` (bronze)
    -- see the module docstring's live-cluster finding on why an unscoped
    read across ``normalized.customers``' full history is permanently
    poisoned by pre-bronze legacy rows. The pass's own snapshot is read from
    bronze scoped by ``staged_run_ids``, never from ``silver.customers`` --
    see the module docstring's ROUND 12 paragraph: silver's dedup-tie winner
    carries an arbitrary ``_run_id`` under a byte-identical replay, so a
    silver-scoped snapshot misreports every tie-loser key as vanished.

    Args:
        conn: An already-open connection. Read-only -- never committed or
            rolled back here.
        staged_run_ids: The run ids THIS publish pass staged (the caller,
            plan 10-04's ``SCDPublisher.publish()``, already has this list --
            see the module docstring on why an unscoped read is vacuous).

    Returns:
        The ``customer_id`` (as ``str``, matching
        ``PublishResult.published_business_keys``'s own string convention)
        of every ``normalized.customers`` row with ``is_current = true``
        AND a bronze presence in ``staging.customers``, whose key does not
        appear in ``staging.customers`` among rows tagged with one of
        ``staged_run_ids``. Empty when ``normalized.customers`` has no
        bronze-known current rows at all (nothing can vanish from nothing).
    """
    cursor = conn.execute(_VANISHED_SQL, {"staged_run_ids": list(staged_run_ids)})
    return {str(row[0]) for row in cursor.fetchall()}


class MassDeleteCircuitBreaker(BarrierStage):
    """Raises ``QualityThresholdExceeded`` when a pass's vanished-key ratio breaches its threshold.

    One instance is constructed per publish pass, already parameterized
    with that pass's own totals -- mirrors
    ``validate.circuit_breaker.RejectionRateCircuitBreaker`` exactly (see
    that module's own docstring for why ``apply()`` never reads counts from
    ``ctx``).

    T-10-08 (accepted): a misconfigured ``mass_delete_threshold`` of ``0.0``
    on a dataset with any natural key churn is a deliberate,
    developer-configured strictness choice (D-06 makes the threshold
    dataset-configurable), not a platform defect -- same accepted-risk
    framing as T-08-14 (``RejectionRateCircuitBreaker``'s own precedent).
    """

    name = "mass_delete_circuit_breaker"

    def __init__(
        self,
        *,
        threshold: float,
        current_count: int,
        vanished_count: int,
        rule_id: str = "mass_delete_circuit_breaker",
    ) -> None:
        """Configure this pass's mass-delete threshold and already-known totals.

        Args:
            threshold: The dataset's configured
                ``scd.mass_delete_threshold`` (D-06), e.g. ``0.10`` for 10%.
            current_count: The number of ``normalized.customers`` rows with
                ``is_current = true`` BEFORE this pass's DELETE-detection
                ran.
            vanished_count: ``len(find_vanished_customer_ids(...))`` for
                this pass.
            rule_id: Stable identifier for this rule instance, for
                diagnostics and the resulting ``ValidationResult``.
        """
        self._threshold = threshold
        self._current_count = current_count
        self._vanished_count = vanished_count
        self._rule_id = rule_id

    def apply(self, ctx: PipelineContext) -> StageResult:
        """Evaluate this pass's vanished/current ratio against its threshold.

        Args:
            ctx: The current pipeline context. Unused for counts (see class
                docstring) -- present only to satisfy the ``BarrierStage``
                Protocol.

        Returns:
            A trivial-PASS ``StageResult`` when ``current_count == 0``
            ("gold has no current customers yet" -- nothing can vanish from
            nothing) or when the observed ratio is at or below
            ``threshold``.

        Raises:
            QualityThresholdExceeded: The observed vanished/current ratio
                exceeds ``threshold``. ``context`` names the observed ratio
                and configured threshold (D-06).
        """
        del ctx  # unused -- totals come from the constructor, see class docstring
        placeholder_chunk = RecordChunk(rows=(), first_ordinal=0, expected_field_count=0)

        if self._current_count == 0:
            return StageResult(
                chunk=placeholder_chunk,
                rejected=[],
                findings=[
                    ValidationResult(
                        rule_id=self._rule_id,
                        rule_type="QUALITY",
                        severity="ERROR",
                        outcome="PASS",
                        evaluated_count=0,
                        failed_count=0,
                        message="no current customers; ratio trivially within threshold",
                        threshold={"mass_delete_threshold": self._threshold},
                        observed={"ratio": 0.0},
                    )
                ],
            )

        ratio = self._vanished_count / self._current_count
        if ratio > self._threshold:
            msg = (
                f"vanished-key ratio {ratio:.2%} exceeds configured "
                f"mass-delete threshold {self._threshold:.2%}"
            )
            raise QualityThresholdExceeded(
                msg,
                context={
                    "rule_id": self._rule_id,
                    "observed_ratio": ratio,
                    "threshold": self._threshold,
                    "current_count": self._current_count,
                    "vanished_count": self._vanished_count,
                },
            )

        return StageResult(
            chunk=placeholder_chunk,
            rejected=[],
            findings=[
                ValidationResult(
                    rule_id=self._rule_id,
                    rule_type="QUALITY",
                    severity="ERROR",
                    outcome="PASS",
                    evaluated_count=self._current_count,
                    failed_count=self._vanished_count,
                    message="mass-delete ratio within threshold",
                    threshold={"mass_delete_threshold": self._threshold},
                    observed={"ratio": ratio},
                )
            ],
        )


# `vanished_ids`/`snapshot_max_event_ts` are the ONLY values bound below --
# `normalized.customers` and every column name are literal identifiers
# (T-10-01). `vanished_ids` is bound as `integer[]` (see
# `apply_delete_semantics`'s own int-cast), matching
# `normalized.customers.customer_id`'s real column type (migration 0005).
#
# The bare `SET`/`WHERE` clause (no `RETURNING`) is factored out so both the
# standalone "invalidate" statement AND the "new_record" CTE below embed the
# exact same closing-UPDATE text -- never two independently-maintained
# copies of the one statement that actually closes a row.
_CLOSE_CURRENT_ROW_CLAUSE = """
UPDATE normalized.customers
SET    valid_to = %(snapshot_max_event_ts)s, is_current = false
WHERE  customer_id = ANY(%(vanished_ids)s) AND is_current
"""

_INVALIDATE_SQL = _CLOSE_CURRENT_ROW_CLAUSE + "RETURNING customer_id\n"

# A single, atomic data-modifying CTE -- see the module docstring for why
# this must NOT be two separately-executed statements (INSERT then UPDATE,
# or UPDATE then a second SELECT-based INSERT): the closing `UPDATE`
# (`_CLOSE_CURRENT_ROW_CLAUSE`'s own text, embedded verbatim here) fully
# executes first and commits its `RETURNING` rows as a stable, pre-update
# snapshot that the following `INSERT` reads from -- the newly-inserted row
# is never at risk of being re-matched by the same `WHERE ... AND
# is_current` the `UPDATE` used, because that `UPDATE` has already closed
# the old row before the `INSERT` ever runs, inside this one statement.
_NEW_RECORD_SQL_TEMPLATE = """
WITH closed AS (
    {close_clause}
    RETURNING customer_id, name, country, birth_date, signup_country,
              _run_id, _file_id, _batch_id, _source_row_number,
              _record_hash, _record_hash_version
)
INSERT INTO normalized.customers (
    customer_id, name, country, birth_date, signup_country, event_ts,
    _run_id, _file_id, _batch_id, _source_row_number,
    _record_hash, _record_hash_version
)
SELECT customer_id, name, country, birth_date, signup_country,
       %(snapshot_max_event_ts)s,
       _run_id, _file_id, _batch_id, _source_row_number,
       _record_hash, _record_hash_version
FROM   closed
RETURNING customer_id
"""
_NEW_RECORD_SQL = _NEW_RECORD_SQL_TEMPLATE.format(close_clause=_CLOSE_CURRENT_ROW_CLAUSE)

_VALID_DELETE_SEMANTICS = frozenset({"ignore", "invalidate", "new_record"})


def apply_delete_semantics(
    conn: Connection[Any],
    *,
    delete_semantics: str,
    vanished_ids: set[str],
    snapshot_max_event_ts: datetime,
) -> tuple[str, ...]:
    """Act on a pass's vanished business keys per the dataset's configured DELETE semantics.

    Args:
        conn: An already-open connection, inside the caller's already-open
            transaction. Never committed or rolled back here.
        delete_semantics: One of ``"ignore"``, ``"invalidate"``,
            ``"new_record"`` (``ScdConfig.delete_semantics``'s closed
            vocabulary, D-05).
        vanished_ids: The ``customer_id`` values
            ``find_vanished_customer_ids`` returned for this pass.
        snapshot_max_event_ts: This pass's snapshot's own maximum
            ``event_ts`` -- the effective-dating timestamp used to close
            (and, for ``new_record``, open) affected rows. Never
            ``datetime.now()``/ingestion time (SCD-06's effective-dating
            requirement applied to the DELETE path).

    Returns:
        The ``customer_id`` of every row this call actually acted on, for
        ``PublishResult.published_business_keys`` -- an empty tuple for
        ``"ignore"`` (a deliberate no-op: no database write happens for
        these keys at all).

    Raises:
        ConfigurationError: ``delete_semantics`` is not one of the three
            valid values (T-10-07, this plan's threat model -- defensive:
            ``ScdConfig.delete_semantics``'s Pydantic ``Literal`` should
            already prevent this upstream, but this function has no way to
            know its caller validated first).
    """
    if delete_semantics == "ignore":
        return ()
    if delete_semantics not in _VALID_DELETE_SEMANTICS:
        msg = (
            f"unknown delete_semantics {delete_semantics!r}; expected one of "
            f"{sorted(_VALID_DELETE_SEMANTICS)}"
        )
        raise ConfigurationError(msg, context={"delete_semantics": delete_semantics})

    params = {
        "vanished_ids": [int(customer_id) for customer_id in vanished_ids],
        "snapshot_max_event_ts": snapshot_max_event_ts,
    }
    sql = _INVALIDATE_SQL if delete_semantics == "invalidate" else _NEW_RECORD_SQL
    cursor = conn.execute(sql, params)
    return tuple(str(row[0]) for row in cursor.fetchall())
