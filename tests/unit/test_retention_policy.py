"""Unit tests for ``dataplat.retention.policy.evaluate_retention``.

Proves D-38's dry-run-by-default arithmetic in isolation, before a future
plan (11-08) wires it into the ``platform_retention`` maintenance DAG.
Mirrors ``tests/unit/validate/test_circuit_breaker.py``'s "state the
property in the function name" discipline and boundary-value-test
convention, but needs no ``_make_context()``-style ``PipelineContext``
fixture: ``evaluate_retention`` is a plain function, not a ``BarrierStage``
(see ``dataplat.retention.policy``'s own module docstring for why).
"""

from __future__ import annotations

from dataplat.config.model import RetentionConfig
from dataplat.retention.policy import RetentionCandidate, evaluate_retention


def test_a_dry_run_never_deletes_anything() -> None:
    config = RetentionConfig(quarantine_days=180)  # enforce defaults to False
    candidates = [
        RetentionCandidate(layer="quarantine", identifier="a", age_days=200),
        RetentionCandidate(layer="quarantine", identifier="b", age_days=181),
    ]

    report = evaluate_retention(config, candidates)

    assert report.enforce is False
    layer_report = report.layers["quarantine"]
    assert layer_report.would_delete_count == 2
    assert layer_report.deleted_count == 0


def test_zero_candidates_never_raises() -> None:
    config = RetentionConfig()

    report = evaluate_retention(config, [])

    assert report.layers["raw"].candidate_count == 0
    for layer_report in report.layers.values():
        assert layer_report.candidate_count == 0
        assert layer_report.would_delete_count == 0
        assert layer_report.deleted_count == 0


def test_a_window_of_none_never_selects_any_candidate() -> None:
    config = RetentionConfig()  # raw_days defaults to None (D-36)
    candidates = [RetentionCandidate(layer="raw", identifier="ancient", age_days=100_000)]

    report = evaluate_retention(config, candidates)

    raw_report = report.layers["raw"]
    assert raw_report.candidate_count == 1
    assert raw_report.would_delete_count == 0


def test_boundary_age_is_not_selected_but_one_day_older_is() -> None:
    """Retention windows are exclusive of the boundary (see module docstring)."""
    config = RetentionConfig(processed_days=60)
    candidates = [
        RetentionCandidate(layer="processed", identifier="at-boundary", age_days=60),
        RetentionCandidate(layer="processed", identifier="one-day-older", age_days=61),
    ]

    report = evaluate_retention(config, candidates)

    processed_report = report.layers["processed"]
    assert processed_report.candidate_count == 2
    assert processed_report.would_delete_count == 1


def test_enforce_true_never_performs_io_itself() -> None:
    """``enforce=True`` marks candidates for deletion, but the evaluator still performs no I/O."""
    config = RetentionConfig(quarantine_days=180, enforce=True)
    candidates = [RetentionCandidate(layer="quarantine", identifier="a", age_days=200)]

    report = evaluate_retention(config, candidates)

    assert report.enforce is True
    layer_report = report.layers["quarantine"]
    assert layer_report.would_delete_count == 1
    # Never performs I/O regardless of enforce -- deleted_count is
    # structurally pinned to 0; only the caller (a future DAG task) may act.
    assert layer_report.deleted_count == 0


def test_would_delete_size_and_age_are_summarized_in_the_observed_dict() -> None:
    config = RetentionConfig(processed_days=30)
    candidates = [
        RetentionCandidate(layer="processed", identifier="old", age_days=45, size_bytes=1000),
        RetentionCandidate(layer="processed", identifier="older", age_days=90, size_bytes=2000),
        RetentionCandidate(layer="processed", identifier="fresh", age_days=10, size_bytes=500),
    ]

    report = evaluate_retention(config, candidates)

    processed_report = report.layers["processed"]
    assert processed_report.would_delete_count == 2
    assert processed_report.threshold == {"window_days": 30}
    assert processed_report.observed["total_size_bytes"] == 3000
    assert processed_report.observed["oldest_age_days"] == 90
    assert processed_report.observed["newest_age_days"] == 45
