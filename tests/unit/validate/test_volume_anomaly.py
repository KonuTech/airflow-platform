"""Unit tests for ``dataplat.validate.volume_anomaly.VolumeAnomalyBarrier``.

Proves this plan's own ``must_haves.truths`` in isolation, before plan 08-11
wires it into a live pipeline run: an anomalous run (>10x historical
average, >=2 prior SUCCEEDED VOLUME runs) flags per the configured
strategy's outcome mapping; a within-bounds run never flags; and fewer than
2 prior VOLUME rows (cold start) never flags, regardless of
``current_row_count``.

``ctx_db_query`` is the testing seam ``VolumeAnomalyBarrier`` exposes
specifically so these tests can exercise the anomaly/cold-start arithmetic
without a live PostgreSQL connection -- mirrors ``test_circuit_breaker.py``'s
own "prove the arithmetic in isolation" precedent.
"""

from __future__ import annotations

from dataplat.models.identity import RunContext
from dataplat.pipeline.protocol import PipelineContext
from dataplat.validate.volume_anomaly import VolumeAnomalyBarrier


def _make_context() -> PipelineContext:
    """Build a placeholder ``PipelineContext`` -- unused when ``ctx_db_query`` is supplied."""
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=None,  # type: ignore[arg-type] -- unused by VolumeAnomalyBarrier.apply()
        metadata=None,  # type: ignore[arg-type] -- unused by VolumeAnomalyBarrier.apply()
        objects=None,  # type: ignore[arg-type] -- unused by VolumeAnomalyBarrier.apply()
        db=None,  # type: ignore[arg-type] -- unused by VolumeAnomalyBarrier.apply()
        log=None,  # type: ignore[arg-type] -- unused by VolumeAnomalyBarrier.apply()
    )


def test_an_anomalous_run_flags_per_the_configured_strategys_outcome_mapping() -> None:
    """5 prior runs averaging 80 rows; this run has 1000 (>800) -- flags QUARANTINE."""
    barrier = VolumeAnomalyBarrier(
        ctx_db_query=lambda: (80.0, 5),
        dataset_id=1,
        current_row_count=1000,
        multiplier=10.0,
        rule_id="r6",
        strategy="QUARANTINE_FILE",
    )

    result = barrier.apply(_make_context())

    assert len(result.findings) == 1
    finding = result.findings[0]
    assert finding.rule_type == "VOLUME"
    assert finding.outcome == "QUARANTINE"
    assert finding.failed_count == 1
    assert finding.evaluated_count == 1000
    assert finding.observed == {
        "current_row_count": 1000,
        "historical_average": 80.0,
        "multiplier": 10.0,
    }
    assert result.rejected == []


def test_strategy_outcome_mapping_fail_file_maps_to_fail_outcome() -> None:
    barrier = VolumeAnomalyBarrier(
        ctx_db_query=lambda: (80.0, 5),
        dataset_id=1,
        current_row_count=1000,
        multiplier=10.0,
        rule_id="r6",
        strategy="FAIL_FILE",
    )

    result = barrier.apply(_make_context())

    assert result.findings[0].outcome == "FAIL"


def test_strategy_outcome_mapping_warn_and_continue_maps_to_pass_with_warning() -> None:
    barrier = VolumeAnomalyBarrier(
        ctx_db_query=lambda: (80.0, 5),
        dataset_id=1,
        current_row_count=1000,
        multiplier=10.0,
        rule_id="r6",
        strategy="WARN_AND_CONTINUE",
    )

    result = barrier.apply(_make_context())

    assert result.findings[0].outcome == "PASS_WITH_WARNING"


def test_a_within_bounds_run_never_flags() -> None:
    """5 prior runs averaging 80 rows; this run has 500 (<=800) -- PASS."""
    barrier = VolumeAnomalyBarrier(
        ctx_db_query=lambda: (80.0, 5),
        dataset_id=1,
        current_row_count=500,
        multiplier=10.0,
        rule_id="r6",
        strategy="QUARANTINE_FILE",
    )

    result = barrier.apply(_make_context())

    assert result.findings[0].outcome == "PASS"
    assert result.findings[0].failed_count == 0


def test_a_run_exactly_at_the_threshold_boundary_never_flags() -> None:
    """current_row_count == historical_average * multiplier is NOT > threshold -- PASS."""
    barrier = VolumeAnomalyBarrier(
        ctx_db_query=lambda: (100.0, 5),
        dataset_id=1,
        current_row_count=1000,
        multiplier=10.0,
        rule_id="r6",
        strategy="QUARANTINE_FILE",
    )

    result = barrier.apply(_make_context())

    assert result.findings[0].outcome == "PASS"


def test_zero_prior_runs_never_flags_regardless_of_current_row_count() -> None:
    barrier = VolumeAnomalyBarrier(
        ctx_db_query=lambda: (None, 0),
        dataset_id=1,
        current_row_count=999_999,
        multiplier=10.0,
        rule_id="r6",
        strategy="QUARANTINE_FILE",
    )

    result = barrier.apply(_make_context())

    finding = result.findings[0]
    assert finding.outcome == "PASS"
    assert finding.rule_type == "VOLUME"
    assert finding.observed == {"historical_average": None, "prior_run_count": 0}


def test_one_prior_run_never_flags_regardless_of_current_row_count() -> None:
    barrier = VolumeAnomalyBarrier(
        ctx_db_query=lambda: (80.0, 1),
        dataset_id=1,
        current_row_count=999_999,
        multiplier=10.0,
        rule_id="r6",
        strategy="QUARANTINE_FILE",
    )

    result = barrier.apply(_make_context())

    finding = result.findings[0]
    assert finding.outcome == "PASS"
    assert finding.observed == {"historical_average": None, "prior_run_count": 1}


def test_the_historical_average_query_binds_dataset_id_via_a_placeholder() -> None:
    """T-08-17: the real query string never string-formats `dataset_id` in."""
    from dataplat.validate.volume_anomaly import _HISTORICAL_AVERAGE_SQL

    assert "%s" in _HISTORICAL_AVERAGE_SQL
    assert "{" not in _HISTORICAL_AVERAGE_SQL
