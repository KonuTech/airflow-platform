"""``StagingLoader`` — chunked ``COPY`` into a clean, retry-safe, all-TEXT staging table.

Streams whatever ``Source`` ``ctx.source`` resolves to through
``self._build_stages(ctx)`` via ``dataplat.pipeline.engine.run_streaming``, and
``COPY``-ies every surviving chunk's rows into
``staging.<dataset>__r<run_id>`` -- one throwaway, ``UNLOGGED`` table per
ingestion-run attempt (LOAD-05).

``_build_stages(ctx)`` (plan 06-16) assembles, in fixed order: ``RaggedRowGuard()``
first (an already-structurally-wrong row never reaches value-level
normalization); then, per ``ColumnContract`` in ``ctx.config.columns``, that
column's ``NullTokenNormalizer`` -- constructed ONLY when the column is
``nullable: true`` -- immediately followed by its type-specific normalizer
(``DateNormalizer`` for ``date``/``timestamp``, ``NumericNormalizer`` for
``decimal``/``integer``, ``BooleanNormalizer`` for ``boolean``); and finally
exactly one ``UnicodeNormalizer()``, unconditionally, LAST, over every column
of every row (D-15 -- no per-dataset opt-out). The nullable-column ordering --
``NullTokenNormalizer`` before its own column's type-specific normalizer,
never after -- is a documented platform invariant, not an implementation
accident: it is what keeps an empty/null-token value on a nullable column
(``customers.birth_date`` is the platform's one real case) from being wrongly
rejected as an invalid calendar date, or later crashing ``UnicodeNormalizer``
with a ``TypeError`` when it reaches a field that should already be ``None``.

Two corrections this module encodes, both verified against real PostgreSQL
this phase (04-RESEARCH.md):

- **Pitfall 2** -- ``CREATE UNLOGGED TABLE ... ON COMMIT DROP`` is not valid
  syntax (``ON COMMIT`` only applies to ``TEMPORARY`` tables) and would be
  semantically wrong even if it parsed: this table must survive across
  multiple chunked-``COPY`` calls and into the *separate* publish
  transaction that reads from it. Cleanup is an explicit ``DROP TABLE``,
  issued by the caller after publication commits -- never ``ON COMMIT``.
  Every attempt also begins with its own ``DROP TABLE IF EXISTS`` (C5: "an
  idempotent undo of its own prior partial work"), so a retry always starts
  from a clean table regardless of what a crashed prior attempt left behind.
- **Pitfall 9** -- "all-TEXT" applies only to the *business* columns
  (``target_columns``). The six lineage columns are populated entirely in
  Python, never parsed from unreliable source text, so they keep their real
  target types (``bigint``/``bytea``/``smallint``) -- weakening them would
  only add avoidable cast bugs at publish time.

``_record_hash`` is computed exactly once, here, in Python (Pitfall 10 /
PITFALLS C6: never recomputed in SQL) -- a canonical, pipe-joined encoding
over the row's business values in ``target_columns`` order, taken AFTER every
stage in ``_build_stages(ctx)`` has run (plan 06-16): normalization MUST
precede hashing (the exact edge
``dataplat.normalize.unicode.UnicodeNormalizer``'s own module docstring
names), so this hash is genuinely NFC-invariant in the real pipeline, not
merely in each normalizer's own isolated unit test. A field already
normalized to ``None`` (absent) or ``bool`` (a normalized boolean) is
rendered as ``""``/``str(value)`` respectively before joining -- mirroring
``dataplat.normalize.numeric._row_to_raw_line``'s own convention for a
partially-normalized row -- never passed to ``str.join`` directly, which
raises ``TypeError`` on a non-``str`` element.

**06-15-PLAN.md addition** -- ``load()`` now truncates a row wider than
``target_columns`` down to exactly ``len(target_columns)`` fields, right
before ``_record_hash`` is computed: this is D-01's "the file still loads
successfully using its known columns" for a genuinely new, contract-unknown
TRAILING column, which ``CsvSource.inspect()`` has already classified and
recorded as a schema-evolution proposal (never auto-DDL) before this method
ever runs. A row narrower than ``target_columns`` is left untouched -- a
missing contract column is a BREAKING change ``inspect()`` already raises
on before ``load()`` is ever reached.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from dataplat.errors import ConfigurationError
from dataplat.normalize.boolean_null import BooleanNormalizer, NullTokenNormalizer
from dataplat.normalize.dates import DateNormalizer
from dataplat.normalize.numeric import NumericNormalizer
from dataplat.normalize.unicode import UnicodeNormalizer
from dataplat.observability.logging import get_logger
from dataplat.pipeline.engine import RaggedRowGuard, run_streaming
from dataplat.validate.registry import resolve_validation_rule
from dataplat.validate.strategy_dispatch import StrategyDispatchStage

if TYPE_CHECKING:
    from collections.abc import Callable

    from psycopg import Connection

    from dataplat.config.model import NormalizationConfig, QualityRuleConfig
    from dataplat.models.record import RejectedRecord
    from dataplat.pipeline.protocol import PipelineContext, StreamingStage

# `ctx.config.quality.rules[].rule_type` values `_build_stages` (plan 08-10)
# dispatches through `StrategyDispatchStage` as streaming stages. `StreamingStage`/
# `BarrierStage` are `Protocol`s, not `@runtime_checkable`, so this local
# frozenset is the dispatch gate, not `issubclass`. `STRUCTURAL` is a member
# of this set (it names a real `StreamingStage`, `RaggedRowGuard`) but is
# deliberately SKIPPED inside the loop body below: `RaggedRowGuard()` is
# ALREADY unconditionally first in this method's stage list per D-08, so a
# `STRUCTURAL`-typed config entry is a documented no-op here, never a second
# `RaggedRowGuard` instance. `REFERENTIAL`/`CIRCUIT_BREAKER`/`VOLUME` are
# `BarrierStage`s, wired into the publish transaction by plan 08-11, never
# into this streaming stage list -- absent from this set entirely.
_STREAMING_RULE_TYPES: frozenset[str] = frozenset(
    {
        "STRUCTURAL",
        "QUALITY_COMPLETENESS",
        "QUALITY_UNIQUENESS",
        "QUALITY_VALIDITY_RANGE",
        "QUALITY_PATTERN",
    },
)

# `ColumnContract.type` values (dataplat.config.model) that route to
# DateNormalizer / NumericNormalizer respectively -- mirrors
# `dates.py`/`numeric.py`'s own module-level frozenset-constant convention.
_DATE_LIKE_TYPES: frozenset[str] = frozenset({"date", "timestamp"})
_NUMERIC_TYPES: frozenset[str] = frozenset({"decimal", "integer"})

# The six embedded lineage columns every staged/normalized row carries,
# verbatim from `migrations/versions/0005_normalized_customers.py` --
# `(column_name, staging_sql_type)`. Kept as their real target types (never
# weakened to `text`) per Pitfall 9: they are populated entirely in Python,
# never parsed from unreliable source text.
_LINEAGE_COLUMN_TYPES: tuple[tuple[str, str], ...] = (
    ("_run_id", "bigint"),
    ("_file_id", "bigint"),
    ("_batch_id", "bigint"),
    ("_source_row_number", "bigint"),
    ("_record_hash", "bytea"),
    ("_record_hash_version", "smallint"),
)
_LINEAGE_COLUMN_NAMES: tuple[str, ...] = tuple(name for name, _ in _LINEAGE_COLUMN_TYPES)


def _null_tokens_for_column(
    normalization: NormalizationConfig | None,
    column_name: str,
) -> tuple[str, ...]:
    """Resolve one nullable column's exact-match NULL-token set for its ``NullTokenNormalizer``.

    D-14: with no ``normalization`` block at all (``customers``' real case),
    the platform default is the empty string only. When a profile IS
    declared, a column's tokens are the dataset-wide
    ``normalization.null_tokens`` UNION this column's own
    ``normalization.null_sentinels`` entry --
    ``NormalizationConfig.null_sentinels``'s own docstring: "Checked in
    addition to ``null_tokens``".

    Args:
        normalization: The dataset's normalization profile, or ``None``.
        column_name: The column to resolve tokens for.

    Returns:
        The exact-match token tuple to hand to this column's
        ``NullTokenNormalizer``.
    """
    if normalization is None:
        return ("",)  # D-14 default
    sentinels = normalization.null_sentinels.get(column_name, [])
    return (*normalization.null_tokens, *sentinels)


@dataclass(frozen=True, slots=True)
class StagingResult:
    """The outcome of one ``StagingLoader.load()`` call.

    Attributes:
        staging_table: The fully-qualified staging table rows were COPY-ed
            into, e.g. ``"staging.customers__r8123"``.
        rows_read: Total rows encountered from the source across every
            chunk, before ``RaggedRowGuard`` filtering (``rows_parsed +
            rows_rejected``).
        rows_parsed: Rows that survived ``RaggedRowGuard`` and were actually
            COPY-ed into ``staging_table``.
        rows_rejected: Rows ``RaggedRowGuard`` rejected (field-count
            mismatch) and did not stage.
        schema_version_id: ``ctx.source``'s resolved ``meta.schema_versions``
            row id (SCHEMA-03/06), read back from ``ctx.source.last_profile``
            after ``open()`` completes -- a ``Source`` implementation not
            wired for schema resolution (no ``last_profile`` attribute, or
            one that never populated it) leaves this ``None``, never an
            error (post-wave-5 code review verification Gap 1).
        rejected_records: Every ``RejectedRecord`` this run's staging pass
            accumulated, across every chunk -- the SAME objects
            ``rows_rejected`` already counts, now carried as data (not just
            a count) so ``run_ingest`` (plan 08-11) can persist them to
            ``meta.rejected_records`` and report on them. Empty when the run
            rejected nothing.
    """

    staging_table: str
    rows_read: int
    rows_parsed: int
    rows_rejected: int
    schema_version_id: int | None = None
    rejected_records: list[RejectedRecord] = field(default_factory=list)


class StagingLoader:
    """Streams a ``Source`` through this run's normalizer stages into a per-run staging table."""

    name = "staging"

    def __init__(self, *, target_columns: tuple[str, ...], chunk_size: int = 1000) -> None:
        """Configure which business columns this loader stages, and in what order.

        Args:
            target_columns: The ordered business-column tuple this dataset
                stages, e.g. ``("customer_id", "name", "country",
                "birth_date", "event_ts")`` for ``customers``. Passed in by
                the caller, never hardcoded here, since a future dataset's
                columns differ. Each surviving row's fields are assumed to
                already be in this exact order (this phase's naive
                ``CsvSource`` has no header-to-column name mapping --
                CONTEXT.md's "no header edge cases" scope note -- so
                positional correspondence is the only contract this phase
                offers; per-name reordering is Phase 6's header-detection
                territory).
            chunk_size: Mirrors ``csv_processor.source.CsvSource``'s own
                ``chunk_size`` knob and shares its default. Stored for
                forward compatibility / symmetry only: COPY/on_progress/
                log-line granularity in ``load()`` is owned entirely by
                whichever chunks ``ctx.source`` itself yields (e.g.
                ``CsvSource(..., chunk_size=...)``), never re-batched here
                -- see ``load()``'s docstring.
        """
        self._target_columns = target_columns
        self._chunk_size = chunk_size

    def _build_stages(self, ctx: PipelineContext) -> list[StreamingStage]:
        """Assemble this run's normalizer pipeline from ``ctx.config`` (plan 06-16).

        Always starts with ``RaggedRowGuard()`` -- the existing row-shape
        guard runs first, before any value-level normalization touches a row
        that is already structurally wrong. Then, for each ``ColumnContract``
        in ``ctx.config.columns``, at that column's actual index within
        ``self._target_columns`` (found by name -- never assumed to align
        positionally): a nullable column's ``NullTokenNormalizer`` FIRST,
        immediately followed by its type-specific normalizer -- this order,
        never reversed, is what keeps an empty/null-token value from being
        wrongly rejected as an invalid calendar date (``customers.birth_date``
        is the platform's one real, nullable typed column). Ends with exactly
        one ``UnicodeNormalizer()``, unconditionally, last (D-15: no
        per-dataset opt-out).

        Args:
            ctx: The current pipeline context. Only ``ctx.config`` is read.

        Returns:
            The ordered stage list for this run's ``run_streaming(...)`` call.

        Raises:
            ValueError: A ``ColumnContract.name`` has no corresponding entry
                in ``self._target_columns`` -- a genuine contract/target-
                schema mismatch this phase's architecture has no other place
                to catch.
        """
        stages: list[StreamingStage] = [RaggedRowGuard()]
        normalization = ctx.config.normalization

        for column in ctx.config.columns:
            try:
                column_index = self._target_columns.index(column.name)
            except ValueError as exc:
                msg = (
                    f"ColumnContract {column.name!r} has no corresponding entry in "
                    f"target_columns {self._target_columns!r} -- a contract/"
                    "target-schema mismatch"
                )
                raise ValueError(msg) from exc

            # FIRST: this column's NullTokenNormalizer, when nullable --
            # BEFORE its own type-specific normalizer below, never after.
            if column.nullable:
                stages.append(
                    NullTokenNormalizer(
                        column_index=column_index,
                        column_name=column.name,
                        null_tokens=_null_tokens_for_column(normalization, column.name),
                    ),
                )

            # THEN: exactly one type-specific normalizer, per the column's
            # declared type. The nullable column's own NullTokenNormalizer
            # (just above) is never duplicated here.
            if column.type in _DATE_LIKE_TYPES:
                stages.append(
                    DateNormalizer(
                        column_index=column_index,
                        column_name=column.name,
                        format=column.format,
                        two_digit_year_pivot=(
                            normalization.two_digit_year_pivot
                            if normalization is not None
                            else None
                        ),
                        spreadsheet_epoch=(
                            normalization.spreadsheet_epoch if normalization is not None else None
                        ),
                        timezone=normalization.timezone if normalization is not None else None,
                        ambiguous_time_policy=(
                            normalization.ambiguous_time_policy
                            if normalization is not None
                            else "reject"
                        ),
                    ),
                )
            elif column.type in _NUMERIC_TYPES:
                stages.append(
                    NumericNormalizer(
                        column_index=column_index,
                        column_name=column.name,
                        decimal_separator=(
                            normalization.decimal_separator if normalization is not None else "."
                        ),
                        thousands_separator=(
                            normalization.thousands_separator if normalization is not None else None
                        ),
                        currency_symbols=(
                            tuple(normalization.currency_symbols)
                            if normalization is not None
                            else ()
                        ),
                        percent_as_fraction=(
                            normalization.percent_as_fraction if normalization is not None else True
                        ),
                        negative_style=(
                            normalization.negative_style
                            if normalization is not None
                            else "leading-minus"
                        ),
                        reject_scientific_notation=column.reject_scientific_notation,
                        fixed_width=column.fixed_width,
                        # This column's OWN declared null_sentinels entry only --
                        # never the platform-wide null_tokens default
                        # `_null_tokens_for_column` also mixes in for
                        # `NullTokenNormalizer` above. That default (`[""]`)
                        # exists for NULLABLE columns; blindly applying it here
                        # too would make a blank value in a non-nullable numeric
                        # column silently become an absent value instead of the
                        # invalid-numeric-value rejection a required field should
                        # get. A nullable column's own sentinel is already caught
                        # by NullTokenNormalizer above (constructed first, same
                        # column) before NumericNormalizer ever sees the raw
                        # string, so this only ever matters for non-nullable
                        # columns that still declare a literal absent-value
                        # sentinel (corpus fixture 59's documented use case).
                        null_sentinels=(
                            tuple(normalization.null_sentinels.get(column.name, []))
                            if normalization is not None
                            else ()
                        ),
                    ),
                )
            elif column.type == "boolean":
                stages.append(
                    BooleanNormalizer(
                        column_index=column_index,
                        column_name=column.name,
                        true_tokens=(
                            tuple(normalization.boolean_true_tokens)
                            if normalization is not None
                            else ()
                        ),
                        false_tokens=(
                            tuple(normalization.boolean_false_tokens)
                            if normalization is not None
                            else ()
                        ),
                    ),
                )

        # LAST: unconditionally, for every dataset, regardless of whether
        # ctx.config.normalization is set at all (D-15: no per-dataset
        # opt-out) -- other normalizers above may still be matching
        # not-yet-NFC-normalized raw tokens for their own lookups.
        stages.append(UnicodeNormalizer())

        # FOURTH section (plan 08-10): ctx.config.quality's STREAMING
        # rule_type entries, dispatched through VALIDATION_RULE_REGISTRY and
        # each wrapped in StrategyDispatchStage, appended AFTER every
        # normalizer above -- a quality rule must evaluate fully-normalized
        # values (NFC-normalized, type-coerced-to-None-or-bool where
        # applicable), matching this module's own "normalization MUST
        # precede hashing" invariant extended one step further to
        # "normalization MUST precede quality evaluation".
        stages.extend(self._build_quality_stages(ctx))

        return stages

    def _build_quality_stages(self, ctx: PipelineContext) -> list[StreamingStage]:
        """Dispatch ``ctx.config.quality``'s STREAMING rule_type entries into wrapped stages.

        Split out of ``_build_stages`` purely to keep that method's cyclomatic
        complexity in check -- this is the fourth, independent section
        ``_build_stages``'s own docstring describes, appended in
        ``ctx.config.quality.rules``' own declared order (deterministic, no
        reordering).

        Args:
            ctx: The current pipeline context. Only ``ctx.config.quality``
                and ``self._target_columns`` are read.

        Returns:
            One ``StrategyDispatchStage`` per streaming-scoped quality rule,
            in declared order. Empty when ``ctx.config.quality`` is ``None``.

        Raises:
            ConfigurationError: A streaming quality rule_type declares no
                ``column`` -- every streaming rule_type requires one.
            ValueError: A quality rule names a ``column`` absent from
                ``self._target_columns`` -- a config/target-schema mismatch
                (T-08-18).
        """
        if ctx.config.quality is None:
            return []

        # Computed ONCE per run (D-23), not per rule: find the single
        # ColumnContract with business_key: True and resolve its position
        # via the SAME idiom `_build_one_quality_stage` uses for its own
        # `column_index` below. `None` when the dataset declares no
        # business_key column at all.
        business_key_column = next(
            (column for column in ctx.config.columns if column.business_key),
            None,
        )
        business_key_index: int | None = None
        if business_key_column is not None:
            try:
                business_key_index = self._target_columns.index(business_key_column.name)
            except ValueError as exc:
                msg = (
                    f"ColumnContract {business_key_column.name!r} (business_key: true) "
                    f"has no corresponding entry in target_columns "
                    f"{self._target_columns!r} -- a contract/target-schema mismatch"
                )
                raise ValueError(msg) from exc

        quality_stages: list[StreamingStage] = []
        for rule in ctx.config.quality.rules:
            if rule.rule_type not in _STREAMING_RULE_TYPES:
                # REFERENTIAL/CIRCUIT_BREAKER/VOLUME are BarrierStages, wired
                # into the publish transaction by plan 08-11 -- never into
                # this streaming stage list. Silently skipped here, never
                # raised on.
                continue
            if rule.rule_type == "STRUCTURAL":
                # RaggedRowGuard() is ALREADY unconditionally first in
                # _build_stages' own stage list (D-08) -- a STRUCTURAL-typed
                # config entry is a documented no-op here, never a second
                # RaggedRowGuard instance.
                continue

            quality_stages.append(
                self._build_one_quality_stage(rule, business_key_index=business_key_index),
            )
        return quality_stages

    def _build_one_quality_stage(
        self,
        rule: QualityRuleConfig,
        *,
        business_key_index: int | None,
    ) -> StreamingStage:
        """Construct one streaming quality rule, wrapped in ``StrategyDispatchStage``.

        Args:
            rule: The dataset config's quality rule to construct.
            business_key_index: The 0-based position of the dataset's
                configured business-key column within each row tuple (D-23),
                computed once by the caller (``_build_quality_stages``).
                ``None`` when the dataset declares no ``business_key``
                column. Threaded unconditionally into ``rule_kwargs`` below
                -- safe for every streaming rule type this method ever
                dispatches to (``REFERENTIAL``/``CIRCUIT_BREAKER``/
                ``VOLUME``/``STRUCTURAL`` are filtered out earlier in
                ``_build_quality_stages`` and never reach this method).

        Returns:
            A ``StrategyDispatchStage`` wrapping the resolved, constructed
            inner rule.

        Raises:
            ConfigurationError: ``rule.column`` is ``None`` -- every
                streaming quality rule_type requires one.
            ValueError: ``rule.column`` has no corresponding entry in
                ``self._target_columns`` -- a config/target-schema mismatch
                (T-08-18).
        """
        if rule.column is None:
            msg = (
                f"quality rule {rule.rule_id!r} (rule_type={rule.rule_type!r}) "
                "declares no column, but every streaming quality rule_type "
                "requires one"
            )
            raise ConfigurationError(
                msg,
                context={"rule_id": rule.rule_id, "rule_type": rule.rule_type},
            )
        try:
            column_index = self._target_columns.index(rule.column)
        except ValueError as exc:
            msg = (
                f"quality rule {rule.rule_id!r} names column {rule.column!r}, "
                f"which has no corresponding entry in target_columns "
                f"{self._target_columns!r} -- a config/target-schema mismatch"
            )
            raise ValueError(msg) from exc

        stage_class = resolve_validation_rule(rule.rule_type)
        rule_kwargs: dict[str, object] = {
            "column_index": column_index,
            "column_name": rule.column,
            "strategy": rule.strategy,
            "rule_id": rule.rule_id,
            "business_key_index": business_key_index,
        }
        if rule.rule_type == "QUALITY_VALIDITY_RANGE":
            rule_kwargs["minimum"] = rule.params.get("minimum")
            rule_kwargs["maximum"] = rule.params.get("maximum")
        elif rule.rule_type == "QUALITY_PATTERN":
            rule_kwargs["pattern"] = rule.params["pattern"]

        inner_stage = stage_class(**rule_kwargs)
        return StrategyDispatchStage(
            inner=inner_stage,  # type: ignore[arg-type]
            strategy=rule.strategy,
            rule_id=rule.rule_id,
            rule_type=rule.rule_type,
        )

    def load(
        self,
        ctx: PipelineContext,
        conn: Connection[Any],
        *,
        on_progress: Callable[[int, int], None] | None = None,
    ) -> StagingResult:
        """Stage ``ctx.source``'s rows into a clean ``staging.<dataset>__r<run_id>`` table.

        Never commits or rolls back ``conn`` itself (same ownership split as
        ``Publisher.publish``) -- the caller decides when the staged table
        becomes visible to other connections.

        One ``COPY`` runs per chunk ``ctx.source`` yields -- this method
        never re-batches rows by ``self._chunk_size``; chunk boundaries are
        entirely the ``Source``'s own concern.

        Args:
            ctx: The current pipeline context. ``ctx.source`` must be an
                opened-able ``Source`` (not ``None``); ``ctx.config.dataset``
                and ``ctx.run.run_id``/``.file_id``/``.batch_id`` name the
                staging table and populate every staged row's lineage
                columns.
            conn: An open connection this method issues DDL/``COPY``
                against. Never committed or rolled back here.
            on_progress: Invoked after every chunk's ``COPY`` completes, with
                the cumulative ``(rows_read, rows_parsed)`` staged so far as
                two positional ``int`` arguments. This is plan 04-05's
                ``run_ingest`` heartbeat mechanism: it keeps
                ``meta.ingestion_runs.rows_read``/``rows_parsed`` genuinely
                live during a long staging load (D-11), not left ``NULL``
                until ``finalize_publication``. Omitting it (the default,
                ``None``) changes no other behavior.

        Returns:
            A ``StagingResult`` describing the staged table and its row
            counts.

        Raises:
            ValueError: ``ctx.source`` is ``None``.
        """
        source = ctx.source
        if source is None:
            msg = "ctx.source is None; StagingLoader.load() requires PipelineContext.source"
            raise ValueError(msg)

        staging_table = f"staging.{ctx.config.dataset}__r{ctx.run.run_id}"
        log = get_logger()

        # C5 / Pitfall 2: every attempt begins with an idempotent undo of any
        # prior partial work, so a retry always starts clean regardless of
        # what a crashed prior attempt left behind.
        conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
        # `target_columns` and `staging_table` are the only two dynamic SQL
        # fragments below, and both are IDENTIFIERS derived from config/run
        # identity (`ctx.config.dataset`, `ctx.run.run_id`, and the
        # dataset's configured column list) -- never from CSV row content
        # (T-04-01, this plan's threat model). Business columns are TEXT
        # (Pitfall 9: a COPY never fails on a bad date/number here -- that
        # becomes a later, set-based validation pass); lineage columns keep
        # their real types.
        business_columns_ddl = ", ".join(f"{column} text" for column in self._target_columns)
        lineage_columns_ddl = ", ".join(
            f"{column} {sql_type}" for column, sql_type in _LINEAGE_COLUMN_TYPES
        )
        conn.execute(
            f"CREATE UNLOGGED TABLE {staging_table} "
            f"({business_columns_ddl}, {lineage_columns_ddl})",
        )
        column_list = ", ".join((*self._target_columns, *_LINEAGE_COLUMN_NAMES))

        rows_read = 0
        rows_parsed = 0
        rows_rejected = 0
        next_source_row_number = 1
        all_rejected: list[RejectedRecord] = []

        with source.open(ctx) as stream:
            for chunk_ordinal, result in run_streaming(
                ctx,
                stream.chunks(),
                stages=self._build_stages(ctx),
            ):
                # `RecordChunk.rows`'s element type is `str | bool | None`
                # (plan 06-11's platform-wide convention): `self._build_stages(ctx)`
                # (plan 06-16) now threads real normalizers through this call,
                # so a field may genuinely be `None` (a nullable column's
                # absent value, e.g. `customers.birth_date`) or `bool` (a
                # normalized boolean column) by the time it reaches the hash
                # computation below -- handled there, never assumed away.
                surviving_rows = result.chunk.rows
                rows_in_chunk = len(surviving_rows)
                rows_read += rows_in_chunk + len(result.rejected)
                rows_rejected += len(result.rejected)
                all_rejected.extend(result.rejected)

                enriched_rows: list[tuple[Any, ...]] = []
                for row in surviving_rows:
                    # D-01 (06-15-PLAN.md): a file whose OWN header has MORE
                    # fields than this dataset's contract stages successfully
                    # using only its KNOWN columns -- a genuinely new
                    # trailing column is classified and recorded as a
                    # schema-evolution proposal by `CsvSource.inspect()`
                    # (`meta.schema_versions`), never auto-loaded here.
                    # `_build_stages(ctx)` above only ever reads/writes a
                    # row's first `len(self._target_columns)` positions (its
                    # own `column_index = self._target_columns.index(...)`
                    # lookup), so truncating BEFORE the hash below -- never
                    # after -- is what keeps a new column's own value out of
                    # `_record_hash` too, matching this docstring's "row's
                    # business values in target_columns order". A row
                    # narrower than `target_columns` is a pre-existing,
                    # separately-guarded case (a missing contract column is
                    # a BREAKING change `CsvSource.inspect()` already raises
                    # on before this method ever runs) -- left untouched.
                    staged_row = (
                        row[: len(self._target_columns)]
                        if len(row) > len(self._target_columns)
                        else row
                    )
                    # C6 / Pitfall 10: computed exactly once, in Python, here
                    # -- canonical pipe-joined encoding, fixed column order
                    # from `target_columns`, taken AFTER every stage in
                    # `_build_stages(ctx)` has run (plan 06-16): normalization
                    # precedes hashing, so an NFC/NFD pair collapses to the
                    # SAME hash here, not only in each normalizer's own unit
                    # test. Never recomputed in SQL at publish time. A field
                    # already normalized to `None` (a nullable column's
                    # absent value) or `bool` (a normalized boolean) is
                    # rendered as `""`/`str(value)` before joining --
                    # mirroring `dataplat.normalize.numeric._row_to_raw_line`'s
                    # own convention -- since `str.join` raises `TypeError` on
                    # a non-`str` element.
                    record_hash = hashlib.sha256(
                        "|".join(
                            "" if field is None else str(field) for field in staged_row
                        ).encode(
                            "utf-8",
                        ),
                    ).digest()
                    enriched_rows.append(
                        (
                            *staged_row,
                            ctx.run.run_id,
                            ctx.run.file_id,
                            ctx.run.batch_id,
                            next_source_row_number,
                            record_hash,
                            1,  # _record_hash_version (META-02)
                        ),
                    )
                    next_source_row_number += 1

                with conn.cursor().copy(
                    f"COPY {staging_table} ({column_list}) FROM STDIN",
                ) as copy:
                    for enriched_row in enriched_rows:
                        copy.write_row(enriched_row)

                rows_parsed += len(enriched_rows)
                # PITFALLS B4: one log line per chunk -- silence during a
                # long COPY is indistinguishable from a hang.
                log.info(
                    "staging chunk copied",
                    dataset=ctx.config.dataset,
                    run_id=ctx.run.run_id,
                    chunk_ordinal=chunk_ordinal,
                    rows_in_chunk=rows_in_chunk,
                )
                if on_progress is not None:
                    on_progress(rows_read, rows_parsed)

        # `source` may be any Source implementation -- `last_profile` is a
        # CsvSource-specific attribute (deliberately not part of the generic
        # Source protocol, which a future non-CSV Source need not implement
        # any schema-versioning concept for), so this is read defensively.
        resolved_profile = getattr(source, "last_profile", None)
        schema_version_id = (
            resolved_profile.schema_version_id if resolved_profile is not None else None
        )
        return StagingResult(
            staging_table=staging_table,
            rows_read=rows_read,
            rows_parsed=rows_parsed,
            rows_rejected=rows_rejected,
            schema_version_id=schema_version_id,
            rejected_records=all_rejected,
        )

    def promote_to_durable_bronze(
        self,
        ctx: PipelineContext,
        conn: Connection[Any],
        staging_result: StagingResult,
    ) -> None:
        """Append this attempt's staged rows into the durable, cumulative ``staging.<dataset>``.

        This is a SEPARATE method from ``load()`` rather than folded into
        it, deliberately: ``load()`` is Phase 6/8's already-proven
        chunked-``COPY`` path into the per-run scratch buffer
        (``staging.<dataset>__r<run_id>``) -- left untouched here. This
        method is the NEW D-01 promotion step (``08.1-CONTEXT.md``), called
        by ``stage_ingest`` (plan 08.1-10) only AFTER any referential-
        integrity/circuit-breaker filtering has already run against the
        scratch buffer, so exactly what survives that filtering is what
        gets promoted into the durable, dbt-readable bronze table --
        never the raw, pre-filtered scratch contents.

        ``durable_table`` (``staging.<dataset>``, migration 0022) has no
        ``__r<run_id>`` suffix: it is the one stable, cumulative table for
        this dataset, and this call only ever appends to it (never
        ``TRUNCATE``/``DELETE`` first) -- D-01's "stable, cumulative,
        append-only" requirement. The scratch buffer's own lifecycle now
        fully collapses into this one connection's transaction scope: it is
        dropped here, on the SAME ``conn``, immediately after the append,
        rather than needing to survive into a later, separate publish
        transaction the way it did before this method existed.

        Never commits or rolls back ``conn`` itself -- same ownership
        contract as ``load()`` and ``Publisher.publish()``: the caller
        decides when the promoted rows become visible to other connections.

        Args:
            ctx: The current pipeline context. Only ``ctx.config.dataset``
                is read.
            conn: An open connection this method issues the ``INSERT``/
                ``DROP TABLE`` against. Never committed or rolled back here.
            staging_result: The ``StagingResult`` this attempt's own
                ``load()`` call returned -- its ``staging_table`` names the
                scratch buffer to append from and then drop.
        """
        durable_table = f"staging.{ctx.config.dataset}"
        column_list = ", ".join((*self._target_columns, *_LINEAGE_COLUMN_NAMES))
        conn.execute(
            f"INSERT INTO {durable_table} ({column_list}) "  # noqa: S608 -- durable_table/column_list/staging_result.staging_table are config/run-derived identifiers (T-08.1-13, this plan's threat model), never CSV content
            f"SELECT {column_list} FROM {staging_result.staging_table}",
        )
        conn.execute(f"DROP TABLE IF EXISTS {staging_result.staging_table}")
