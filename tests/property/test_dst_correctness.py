"""Property test for ``dataplat.normalize.dates`` -- QUAL-17's DST-correctness proof.

``tests/unit/normalize/test_dates.py`` proves DST gap/overlap classification
against corpus fixture 55's three fixed rows. This file generalizes that
proof across *arbitrary* local times in each transition's bounded window,
mirroring ``tests/property/test_chunking_properties.py``'s structure: every
nonexistent local time in the spring-forward window is rejected with the
named diagnostic, and every ambiguous local time in the autumn-overlap
window resolves exactly as its declared ``ambiguous_time_policy`` says it
should, not just for the corpus's one fixed row.

Both strategies use the verified ``st.datetimes(..., timezones=st.just(...),
allow_imaginary=True)`` shape from 06-RESEARCH.md's Code Examples section,
bounded to a four-hour window around each real 2026 Europe/Warsaw
transition (verified empirically this session: the gap window yields both
``"nonexistent"`` and ``"unambiguous"`` classifications, the overlap window
yields both ``"ambiguous"`` and ``"unambiguous"`` -- so neither property is
vacuous with respect to the behavior it exists to prove).
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from hypothesis import given, settings
from hypothesis import strategies as st

from dataplat.models.identity import RunContext
from dataplat.models.record import RecordChunk
from dataplat.normalize.dates import DateNormalizer, classify_naive_local
from dataplat.pipeline.protocol import PipelineContext

if TYPE_CHECKING:
    from dataplat.models.record import RejectedRecord

_WARSAW = ZoneInfo("Europe/Warsaw")
_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

# Bounded to a four-hour window around each real, verified 2026
# Europe/Warsaw transition (spring-forward 2026-03-29T01:00:00Z, autumn
# overlap 2026-10-25T01:00:00Z -- corpus fixture 55) -- never an
# open-ended range (T-06-29's accepted DoS disposition: bounded generation,
# not adversarial). min_value/max_value are deliberately naive: they are
# LOCAL-time bounds for the strategy, which itself attaches `_WARSAW` via
# `timezones=st.just(...)` -- passing an already-aware bound here would be
# a type error against hypothesis's own `st.datetimes` signature.
_GAP_WINDOW = st.datetimes(
    min_value=dt.datetime(2026, 3, 29, 0, 0),  # noqa: DTZ001
    max_value=dt.datetime(2026, 3, 29, 4, 0),  # noqa: DTZ001
    timezones=st.just(_WARSAW),
    allow_imaginary=True,
).map(lambda candidate: candidate.replace(microsecond=0))

_OVERLAP_WINDOW = st.datetimes(
    min_value=dt.datetime(2026, 10, 25, 0, 0),  # noqa: DTZ001
    max_value=dt.datetime(2026, 10, 25, 4, 0),  # noqa: DTZ001
    timezones=st.just(_WARSAW),
    allow_imaginary=True,
).map(lambda candidate: candidate.replace(microsecond=0))


def _make_context() -> PipelineContext:
    """Build a placeholder ``PipelineContext``, matching ``test_dates.py``'s convention."""
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=None,  # type: ignore[arg-type]  # unused by the code under test
        metadata=None,  # type: ignore[arg-type]  # unused by the code under test
        objects=None,  # type: ignore[arg-type]  # unused by the code under test
        db=None,  # type: ignore[arg-type]  # unused by the code under test
        log=None,  # type: ignore[arg-type]  # unused by the code under test
    )


def _apply_one(
    normalizer: DateNormalizer, raw_value: str
) -> tuple[list[RejectedRecord], str | None]:
    """Run ``normalizer`` over a single-row, single-column chunk holding ``raw_value``.

    Returns:
        A ``(rejected, resolved_value)`` pair: ``rejected`` is the
        ``StageResult.rejected`` list (empty on success), ``resolved_value``
        is the surviving row's sole field, or ``None`` when the row was
        rejected.
    """
    chunk = RecordChunk(rows=((raw_value,),), first_ordinal=0, expected_field_count=1)
    result = normalizer.apply(_make_context(), chunk)
    resolved = result.chunk.rows[0][0] if result.chunk.rows else None
    return result.rejected, resolved


# max_examples=200 per property: each example is one naive-datetime
# classification plus one DateNormalizer.apply() call over a single-row
# chunk -- no I/O, no DB -- comfortably inside the phase's ~90-second
# feedback-latency budget (03-VALIDATION.md), matching
# tests/property/test_chunking_properties.py's own budget convention.
@settings(max_examples=200)
@given(candidate=_GAP_WINDOW)
def test_nonexistent_local_times_are_always_rejected_with_the_named_diagnostic(
    candidate: dt.datetime,
) -> None:
    naive = candidate.replace(tzinfo=None)
    normalizer = DateNormalizer(
        column_index=0,
        column_name="ts_local",
        format=_TIMESTAMP_FORMAT,
        timezone="Europe/Warsaw",
    )
    raw_value = naive.strftime(_TIMESTAMP_FORMAT)

    rejected, resolved = _apply_one(normalizer, raw_value)

    if classify_naive_local(naive, _WARSAW) == "nonexistent":
        assert len(rejected) == 1
        assert rejected[0].error_type == "nonexistent-local-time"
        assert resolved is None
    else:
        # unambiguous (this window never produces "ambiguous" -- verified
        # empirically; classify_naive_local's own three-way return is
        # still handled generically here, not just the two seen in practice).
        assert rejected == []
        assert resolved is not None
        expected_utc = naive.replace(tzinfo=_WARSAW).astimezone(dt.UTC)
        assert resolved == expected_utc.isoformat()


@settings(max_examples=200)
@given(candidate=_OVERLAP_WINDOW)
def test_ambiguous_local_times_resolve_per_the_declared_fold_policy(candidate: dt.datetime) -> None:
    naive = candidate.replace(tzinfo=None)
    raw_value = naive.strftime(_TIMESTAMP_FORMAT)
    classification = classify_naive_local(naive, _WARSAW)

    reject_normalizer = DateNormalizer(
        column_index=0,
        column_name="ts_local",
        format=_TIMESTAMP_FORMAT,
        timezone="Europe/Warsaw",
        ambiguous_time_policy="reject",
    )
    rejected, resolved = _apply_one(reject_normalizer, raw_value)

    if classification == "ambiguous":
        # Never silently takes the first fold (corpus fixture 55's own
        # framing): the default "reject" policy always declines.
        assert len(rejected) == 1
        assert rejected[0].error_type == "ambiguous-local-time-requires-a-declared-fold-policy"
        assert resolved is None

        for policy, fold in (("earliest", 0), ("latest", 1)):
            policy_normalizer = DateNormalizer(
                column_index=0,
                column_name="ts_local",
                format=_TIMESTAMP_FORMAT,
                timezone="Europe/Warsaw",
                ambiguous_time_policy=policy,
            )
            policy_rejected, policy_resolved = _apply_one(policy_normalizer, raw_value)

            assert policy_rejected == []
            expected_utc = naive.replace(tzinfo=_WARSAW, fold=fold).astimezone(dt.UTC)
            assert policy_resolved == expected_utc.isoformat()
    elif classification == "nonexistent":
        assert len(rejected) == 1
        assert rejected[0].error_type == "nonexistent-local-time"
    else:  # unambiguous -- this window also produces plenty of these
        assert rejected == []
        expected_utc = naive.replace(tzinfo=_WARSAW).astimezone(dt.UTC)
        assert resolved == expected_utc.isoformat()
