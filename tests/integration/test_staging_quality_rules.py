"""Integration proof: a configured quality rule genuinely changes a real staged load (plan 08-10).

Mirrors ``test_staging_loader.py``'s ``_FakeSource``/``_FakeRecordStream``
harness (pure in-memory ``Source``/``RecordStream`` test doubles, exact
control of chunk boundaries), driving a REAL ``StagingLoader.load()`` call
against a real testcontainers PostgreSQL, migrated to head.

Also covers plan 08-10 Task 2's own acceptance criteria at the
``StagingLoader._build_stages`` level (no DB needed for those, but kept in
this file/marker for the plan's single declared test file and its own
``-m integration`` verify command): ``quality=None`` produces an identical
stage-class list to before this plan; a ``QUALITY_COMPLETENESS`` rule
appends exactly one ``StrategyDispatchStage``; a ``REFERENTIAL``/
``CIRCUIT_BREAKER``/``VOLUME``-typed entry is silently skipped.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from dataplat.config.model import (
    BatchingConfig,
    ColumnContract,
    DatasetConfig,
    DeduplicationConfig,
    LoadConfig,
    QualityConfig,
    QualityRuleConfig,
    SourceConfig,
)
from dataplat.load.staging import StagingLoader
from dataplat.models.identity import RunContext
from dataplat.models.record import RecordChunk
from dataplat.pipeline.protocol import PipelineContext
from dataplat.sources.protocol import RecordStream, Source
from dataplat.validate.strategy_dispatch import StrategyDispatchStage

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

pytestmark = pytest.mark.integration

TARGET_COLUMNS = ("customer_id", "name")


class _FakeRecordStream(RecordStream):
    """Yields a fixed, pre-built sequence of ``RecordChunk``s -- no real CSV/S3 I/O.

    Mirrors ``test_staging_loader.py``'s own test double of the same name.
    """

    def __init__(self, chunks: list[RecordChunk]) -> None:
        self._chunks = chunks

    def chunks(self, *, start_ordinal: int | None = None) -> Iterator[RecordChunk]:  # noqa: ARG002
        yield from self._chunks


class _FakeSource(Source):
    """A ``Source`` test double: ``.open()`` yields a FRESH stream every call."""

    def __init__(self, rows_by_chunk: Sequence[tuple[tuple[str, ...], ...]]) -> None:
        self._rows_by_chunk = rows_by_chunk

    @contextlib.contextmanager
    def open(self, ctx: PipelineContext) -> Iterator[_FakeRecordStream]:  # noqa: ARG002
        ordinal = 0
        chunks = []
        for rows in self._rows_by_chunk:
            chunks.append(
                RecordChunk(
                    rows=rows,
                    first_ordinal=ordinal,
                    expected_field_count=len(TARGET_COLUMNS),
                ),
            )
            ordinal += len(rows)
        yield _FakeRecordStream(chunks)


def _base_config_kwargs() -> dict[str, Any]:
    """The customers-shaped config fields every test in this file shares."""
    return {
        "dataset": "quality_rule_proof",
        "config_schema_version": 1,
        "source": SourceConfig(
            type="csv",
            bucket="raw",
            path="quality-rule-proof/",
            change_semantics="snapshot",
            duplicate_policy="skip",
        ),
        "deduplication": DeduplicationConfig(
            strategy="business_key_latest",
            keys=["customer_id"],
            order_by=["customer_id desc"],
        ),
        "load": LoadConfig(strategy="merge", target="normalized.quality_rule_proof"),
        "batching": BatchingConfig(max_units_per_run=100),
        "columns": [
            ColumnContract(
                name="customer_id",
                type="string",
                nullable=False,
                required=True,
                business_key=True,
            ),
            ColumnContract(name="name", type="string", nullable=False, required=True),
        ],
    }


def _make_config(*, quality: QualityConfig | None = None) -> DatasetConfig:
    return DatasetConfig(**_base_config_kwargs(), quality=quality)


def _make_context(
    *,
    run_id: int,
    source: _FakeSource,
    quality: QualityConfig | None,
) -> PipelineContext:
    return PipelineContext(
        run=RunContext(run_id=run_id, idempotency_key=f"quality-rule-proof-{run_id}"),
        config=_make_config(quality=quality),
        metadata=None,  # type: ignore[arg-type] -- unused by StagingLoader.load()
        objects=None,  # type: ignore[arg-type] -- unused by StagingLoader.load()
        db=None,  # type: ignore[arg-type] -- unused by StagingLoader.load()
        log=None,  # type: ignore[arg-type] -- unused by StagingLoader.load()
        source=source,
    )


_COMPLETENESS_RULE_REJECT = QualityRuleConfig(
    rule_id="name_not_empty",
    rule_type="QUALITY_COMPLETENESS",
    strategy="REJECT_RECORD",
    column="name",
)
_COMPLETENESS_RULE_WARN = QualityRuleConfig(
    rule_id="name_not_empty",
    rule_type="QUALITY_COMPLETENESS",
    strategy="WARN_AND_CONTINUE",
    column="name",
)

# One row (customer_id="2") has an empty `name` -- the one known violation.
_ROWS = (
    ("1", "Alice"),
    ("2", ""),
    ("3", "Carol"),
)


@pytest.fixture
def conn(migrated_dsn: str) -> Iterator[psycopg.Connection[Any]]:
    """One open psycopg connection per test, over the migrated database."""
    with psycopg.connect(migrated_dsn) as connection:
        yield connection


# --- Task 2 acceptance criteria: _build_stages' own stage-list shape ---


def test_build_stages_with_quality_none_matches_pre_plan_stage_list() -> None:
    ctx = _make_context(run_id=1, source=_FakeSource([_ROWS]), quality=None)
    loader = StagingLoader(target_columns=TARGET_COLUMNS)

    stages = loader._build_stages(ctx)  # noqa: SLF001 -- proving the private assembly method directly

    stage_names = [type(stage).__name__ for stage in stages]
    assert "StrategyDispatchStage" not in stage_names
    assert stage_names[0] == "RaggedRowGuard"
    assert stage_names[-1] == "UnicodeNormalizer"


def test_build_stages_appends_one_strategy_dispatch_stage_for_completeness_rule() -> None:
    quality = QualityConfig(rules=[_COMPLETENESS_RULE_REJECT])
    ctx = _make_context(run_id=2, source=_FakeSource([_ROWS]), quality=quality)
    loader = StagingLoader(target_columns=TARGET_COLUMNS)

    stages = loader._build_stages(ctx)  # noqa: SLF001

    dispatch_stages = [s for s in stages if isinstance(s, StrategyDispatchStage)]
    assert len(dispatch_stages) == 1
    dispatch_stage = dispatch_stages[0]
    inner = dispatch_stage._inner  # noqa: SLF001
    assert inner.__class__.__name__ == "CompletenessRule"
    assert inner._column_index == 1  # type: ignore[attr-defined]  # noqa: SLF001 -- "name" is index 1


@pytest.mark.parametrize("rule_type", ["REFERENTIAL", "CIRCUIT_BREAKER", "VOLUME"])
def test_build_stages_skips_barrier_scoped_rule_types(rule_type: str) -> None:
    rule = QualityRuleConfig(
        rule_id="skip_me",
        rule_type=rule_type,
        strategy="FAIL_FILE",
        column=None,
    )
    quality = QualityConfig(rules=[rule])
    ctx = _make_context(run_id=3, source=_FakeSource([_ROWS]), quality=quality)
    loader = StagingLoader(target_columns=TARGET_COLUMNS)

    stages = loader._build_stages(ctx)  # noqa: SLF001

    assert not any(isinstance(s, StrategyDispatchStage) for s in stages)


# --- Task 3: a configured rule genuinely rejects a bad row during a real staged load ---


def test_row_failing_reject_record_completeness_never_lands_in_staging_table(
    conn: psycopg.Connection[Any],
) -> None:
    quality = QualityConfig(rules=[_COMPLETENESS_RULE_REJECT])
    ctx = _make_context(run_id=101, source=_FakeSource([_ROWS]), quality=quality)
    loader = StagingLoader(target_columns=TARGET_COLUMNS)

    result = loader.load(ctx, conn)

    assert result.rows_rejected == 1
    staged = conn.execute(
        f"SELECT customer_id FROM {result.staging_table} ORDER BY customer_id",  # noqa: S608
    ).fetchall()
    assert [row[0] for row in staged] == ["1", "3"]


def test_the_same_row_set_with_quality_none_stages_all_three_rows(
    conn: psycopg.Connection[Any],
) -> None:
    """The regression guarantee, proven at a real load, not just the stage-list comparison above."""
    ctx = _make_context(run_id=102, source=_FakeSource([_ROWS]), quality=None)
    loader = StagingLoader(target_columns=TARGET_COLUMNS)

    result = loader.load(ctx, conn)

    assert result.rows_rejected == 0
    staged = conn.execute(
        f"SELECT customer_id FROM {result.staging_table} ORDER BY customer_id",  # noqa: S608
    ).fetchall()
    assert [row[0] for row in staged] == ["1", "2", "3"]


def test_the_same_row_under_warn_and_continue_stages_all_three_rows(
    conn: psycopg.Connection[Any],
) -> None:
    """Proves ``StrategyDispatchStage``'s wiring (D-07 ``WARN_AND_CONTINUE``), not the bare rule.

    Without the wiring, ``_build_stages`` would still construct a bare
    ``CompletenessRule`` that unconditionally excludes the bad row.
    """
    quality = QualityConfig(rules=[_COMPLETENESS_RULE_WARN])
    ctx = _make_context(run_id=103, source=_FakeSource([_ROWS]), quality=quality)
    loader = StagingLoader(target_columns=TARGET_COLUMNS)

    result = loader.load(ctx, conn)

    assert result.rows_rejected == 0
    staged = conn.execute(
        f"SELECT customer_id, name FROM {result.staging_table} ORDER BY customer_id",  # noqa: S608
    ).fetchall()
    assert [row[0] for row in staged] == ["1", "2", "3"]
    # customer_id="2"'s empty `name` genuinely survived -- WARN_AND_CONTINUE
    # kept the row instead of excluding it. `name` is non-nullable, so no
    # NullTokenNormalizer ever converts "" to NULL here -- the raw empty
    # string reaches CompletenessRule, and (kept) reaches the staging table.
    assert dict(staged)["2"] == ""
