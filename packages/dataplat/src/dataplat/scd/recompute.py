"""``recompute_version_chain`` -- Type-0/1/2 dispatch and version-boundary detection.

SCD-01/02/04, and this plan's own flagged RESEARCH.md "Assumption A2
spike": proves the LEAD()/change-point recompute shape (RESEARCH.md
Pattern 2's illustrative SQL, read as design inspiration only) as a plain
Python algorithm against concrete edge cases -- including the two
RESEARCH.md explicitly left open (identical ``event_ts`` ties, Open
Question 3's schema-evolution irrelevance) -- BEFORE plan 10-04's
``SCDPublisher`` assembles the real, database-touching pipeline around it.

Pure function, no I/O: takes one customer's full ordered bronze history in,
returns a version chain out. No database connection, no ``ctx``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dataplat.scd.hashing import tracked_attribute_hash

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime


@dataclass(frozen=True)
class BronzeRecord:
    """One bronze-layer observation of a customer at a point in source time.

    Ordering key is ``(event_ts, file_id, source_row_number)`` (debug session
    ``rebuild-scd2-reconciliation``, 2026-08-29): ``source_row_number`` alone is
    NOT a safe tie-break for two records sharing the exact same ``event_ts``
    (RESEARCH.md's Assumption A2 edge case) -- it is only the row's ordinal
    position WITHIN ITS OWN SOURCE FILE (``models/record.py``'s own docstring),
    not unique across different files. Two different raw files can legitimately
    deliver a row for the same ``customer_id`` at the same in-file row position
    with the same ``event_ts`` (observed live: `snapshot_complete_customers_csv`
    echoes the current gold roster ``ORDER BY customer_id`` with each key's
    unchanged ``event_ts`` verbatim, so long-lived, low, rank-stable customer_ids
    recur at the same row position across many separately-uploaded files).
    Without a file-level discriminator, that tie was silently broken by
    whatever arbitrary row order an un-ordered SQL read happened to return --
    non-deterministic between an incrementally-loaded run and a from-scratch
    bulk reload (``rebuild-from-raw``), breaking README §67. ``file_id`` is
    globally unique per staged file and, per ``discovery.discover_files``'s own
    sorted-manifest guarantee, is assigned in a deterministic,
    filename-order-consistent sequence regardless of incremental vs. bulk
    discovery -- so ``(event_ts, file_id, source_row_number)`` is a genuine
    total order, stable across reprocessing.

    ``file_id`` defaults to ``0`` so existing single-file test scenarios
    (every row conceptually from the SAME source file) need no change --
    ties among same-``file_id`` rows still fall through to
    ``source_row_number`` exactly as before. Real bronze reads
    (``load/publish/scd.py``) always pass the row's real ``_file_id``.
    """

    customer_id: int
    name: str | None
    country: str | None
    birth_date: str | None
    event_ts: datetime
    signup_country: str | None
    source_row_number: int
    file_id: int = 0


@dataclass(frozen=True)
class VersionRow:
    """One emitted SCD2 version. Deliberately carries NO surrogate/identity field (SCD-04).

    The surrogate key is assigned later, by the database's own Identity
    column, only at INSERT time -- this function has no opinion on it and
    cannot influence it.
    """

    customer_id: int
    name: str | None
    country: str | None
    birth_date: str | None
    signup_country: str | None
    valid_from: datetime
    valid_to: datetime
    is_current: bool


def recompute_version_chain(
    history: Sequence[BronzeRecord],
    *,
    valid_to_sentinel: datetime,
) -> list[VersionRow]:
    """Deterministically recompute the full SCD2 version chain for one customer's bronze history.

    Args:
        history: One customer_id's full bronze history, in any order --
            this function sorts it itself.
        valid_to_sentinel: The open-ended "current" marker used as
            ``valid_to`` for the last (current) version.

    Returns:
        The version chain, oldest version first. Each version's Type-2
        columns (``name``, ``country``) come from the FIRST row of its
        version group. Every version row uniformly carries the SAME
        Type-1 value (``birth_date``, latest-wins across the whole
        history) and the SAME Type-0 value (``signup_country``,
        earliest-wins across the whole history) -- Type-1/Type-0 columns
        have no per-version history at all, even retroactively.

    Ordering and tie-break (RESEARCH.md Assumption A2, revised by debug session
    ``rebuild-scd2-reconciliation``): rows are sorted by
    ``(event_ts, file_id, source_row_number)`` ascending. ``file_id`` is
    globally unique per staged file, so this ordering is always total even
    when two different files deliver the same customer at the same in-file
    row position with the same ``event_ts`` -- an identical ``event_ts``
    never causes a raise or a silently dropped row, and never resolves
    arbitrarily either.

    NULL-safety (RESEARCH.md Pitfall 5's bugfix class): version-boundary
    detection compares two ``bytes | None`` hash values with Python's own
    ``!=``. Python's ``!=`` is ALREADY NULL-safe/IS-DISTINCT-FROM-equivalent
    for this comparison -- ``None != None`` is ``False`` (no change) and
    ``None != b"..."`` is ``True`` (a change), exactly matching SQL's
    ``IS DISTINCT FROM`` three-valued-logic semantics. This is a property
    of Python's equality operator on these types, not special-cased code;
    it WOULD need special-casing if this were SQL, where ``NULL != NULL``
    evaluates to ``NULL`` (falsy), not ``TRUE`` -- documented here so a
    future SQL port of this logic doesn't silently inherit a bug this
    Python version never had.
    """
    ordered = sorted(history, key=lambda r: (r.event_ts, r.file_id, r.source_row_number))

    # Type-1 (latest-wins) and Type-0 (earliest-wins) are computed once,
    # across the WHOLE history, and applied uniformly to every version --
    # they have no per-version story at all.
    latest_row = max(ordered, key=lambda r: (r.event_ts, r.file_id, r.source_row_number))
    earliest_row = min(ordered, key=lambda r: (r.event_ts, r.file_id, r.source_row_number))
    latest_birth_date = latest_row.birth_date
    earliest_signup_country = earliest_row.signup_country

    # Group into Type-2 version boundaries via each row's tracked-attribute hash.
    hashes = [tracked_attribute_hash(r.name, r.country) for r in ordered]

    group_start_indices: list[int] = [0]
    group_start_indices.extend(
        i for i in range(1, len(ordered)) if hashes[i] != hashes[i - 1]
    )

    versions: list[VersionRow] = []
    for group_position, start_index in enumerate(group_start_indices):
        first_row = ordered[start_index]
        is_last_group = group_position == len(group_start_indices) - 1
        if is_last_group:
            valid_to = valid_to_sentinel
        else:
            next_start_index = group_start_indices[group_position + 1]
            valid_to = ordered[next_start_index].event_ts

        versions.append(
            VersionRow(
                customer_id=first_row.customer_id,
                name=first_row.name,
                country=first_row.country,
                birth_date=latest_birth_date,
                signup_country=earliest_signup_country,
                valid_from=first_row.event_ts,
                valid_to=valid_to,
                is_current=is_last_group,
            ),
        )

    return versions
