r"""A deterministic, dated backfill corpus — one manifest entry per day, not per file.

``tools/corpus/generators.py`` and ``tools/corpus/manifest.py`` declare *one
fixture, one fixed-content file*: a manifest entry names a single output and a
row count. Plan 09-11's live 2-year backfill sweep needs the opposite shape —
~730 daily files per dataset, combining a regular cadence with three
deliberately-injected anomalies (a missing day, a schema-version-change
boundary, and one late/out-of-order event) inside a single generated set. That
has no representation in ``Fixture``/``Manifest`` — a "day" is not a fixture,
and "skip day 10" is not a generator kind — so this module is new generator
code, not a manifest addition (RESEARCH.md Open Question 2, resolved by this
plan).

It reuses ``tools.corpus.generators.stream_for`` directly (R1: every day's
file draws from its own stream, derived from
``sha256(f"{master_seed}|{filename}")``) and follows the same determinism
discipline documented in ``docs/adr/0005-fixture-corpus-generated-from-a-seed.md``
and enforced by ``tests/policy/test_generator_determinism_rules.py`` (which
scans this whole package, this module included, by directory walk — no
separate wiring needed):

* Randomness is consumed only through ``Random.random()`` (R2) — every pick
  and every decimal is index/integer arithmetic over it, never ``choice`` or
  ``randint``.
* Every file's bytes are built as ``str``, encoded ``utf-8`` and returned as
  ``bytes`` — never written through a text-mode file object (R3).
* The line terminator is explicit (``\\r\\n``), never a writer default (R4).
* No wall-clock, process-identity or OS-entropy call appears anywhere below
  (R6) — every date in a generated row is derived from ``start_date`` plus an
  integer day offset, never ``datetime.now()``.

This module performs no I/O of its own: ``generate_dated_series`` returns
in-memory bytes plus a manifest recording exactly where each injected
property lives, so it is unit-testable without S3, MinIO or a real dataset
config loader. Plan 09-11 is the only caller that uploads these bytes.

## The roster model (plan 10-06)

``customers.yaml`` declares ``source.change_semantics: snapshot`` (D-04):
every discovered file is the FULL current extent of the customer population,
not an incremental delta. The SCD Publisher's DELETE-detection sweep (plan
10-03) treats each pass's file that way — comparing it against the
currently-``is_current`` gold rows and closing out anything absent. Before
this plan, the customers path here minted ``rows_per_day`` brand-new
``customer_id`` values EVERY day (a permanently-growing population, never
repeating) — under a real ``snapshot``-semantics publisher that shape would
make every previously-known customer look "vanished" on day two, tripping
D-06's mass-delete circuit breaker on ordinary, correct traffic.

The fix is structural, not a patch: customers is now a bounded,
``rows_per_day``-sized ROSTER, generated once (customer_id = ``_CUSTOMER_ID_BASE
+ member_index``, day-independent — the same formula this module always used
for day 0, just no longer scaled by day index), and RESENT IN FULL every
non-gap day. Each roster member's baseline name/country/birth_date/
signup_country is drawn ONCE from a CUSTOMER-scoped stream (keyed by that
member's own ``customer_id``, not by any day's filename) — this is what makes
a member's values stable across every day's file. ``event_ts`` is the only
field that always varies by day, since it must show that day's own delivery.

The ``orders`` path is completely untouched by this redesign (D-01 excludes
orders from SCD; it keeps the original "new IDs born per day" model) — every
call in this module explicitly branches on ``dataset`` before reaching any
roster logic, and ``_render_row`` (orders' own per-row renderer) is unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import TYPE_CHECKING, Final

from .generators import stream_for

if TYPE_CHECKING:
    import random
    from collections.abc import Sequence

# configs/datasets/customers.yaml / orders.yaml `columns:` blocks, read this
# session — column order matches each dataset's real schema exactly.
# `signup_country` (D-13, plan 10-01) is customers' new Type-0 column: its
# value is picked once per customer_id and never revised, which is exactly
# what the roster model's per-member baseline stream already gives every
# other customer field by construction.
_DATASET_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    "customers": ("customer_id", "name", "country", "birth_date", "event_ts", "signup_country"),
    "orders": ("order_id", "customer_id", "order_date", "amount"),
}

# The column each schema-change boundary (D-10) appends, never empty.
_SCHEMA_CHANGE_COLUMN: Final[dict[str, str]] = {
    "customers": "loyalty_tier",
    "orders": "discount_code",
}

_SCHEMA_CHANGE_VALUES: Final[dict[str, tuple[str, ...]]] = {
    "customers": ("bronze", "silver", "gold"),
    "orders": ("NONE", "SAVE10", "SAVE20"),
}

# customers.yaml's own QUALITY_PATTERN rule (`^[A-Z]{2}$`) is the contract
# this pick-list must satisfy — the same 18-value ISO-3166-1-alpha-2 list
# tests/fixtures/slice-corpus.yaml already uses for the same column.
_COUNTRIES: Final[tuple[str, ...]] = (
    "PL", "US", "GB", "DE", "FR", "ES", "IT", "NL", "SE", "NO",
    "DK", "FI", "JP", "CN", "BR", "IN", "CA", "AU",
)  # fmt: skip

# A deliberately small, deterministic name pool — ASCII-only so every value
# encodes under `utf-8`/`strict` without needing a fallback path.
_NAMES: Final[tuple[str, ...]] = (
    "Anna Kowalski", "Piotr Nowak", "Maria Wisniewska", "Jan Wojcik",
    "Emily Johnson", "James Smith", "Sophie Muller", "Lukas Schmidt",
    "Chiara Rossi", "Marco Bianchi", "Fatima Al-Sayed", "Ahmed Hassan",
    "Yuki Tanaka", "Haruto Sato", "Wei Zhang", "Li Chen",
    "Olga Ivanova", "Dmitri Petrov", "Lucas Silva", "Ana Santos",
)  # fmt: skip

_BIRTH_DATES: Final[tuple[str, ...]] = (
    "1950-03-14", "1958-09-18", "1964-06-11", "1970-05-06",
    "1976-07-08", "1982-04-30", "1988-12-01", "1994-10-19",
)  # fmt: skip

# Postgres-INTEGER-castable business keys (bugfix, discovered live during
# plan 09-11 -- this generator's first real exercise against the deployed
# schema). `normalized.customers.customer_id` and
# `normalized.orders.{order_id,customer_id}` are all `sa.Integer()`
# (migrations 0005/0016), cast via `::int` inside `MergePublisher`'s/
# `OrdersMergePublisher`'s own `_PUBLISH_SQL` (`packages/dataplat/src/
# dataplat/load/publish/merge.py`/`merge_orders.py`, verified this session).
# The original alphanumeric business key (`"CUST-000010-0010"`) fails that
# cast with `invalid input syntax for type integer`, aborting the WHOLE
# publish transaction for the run (WR-04's documented "one bad value blocks
# the whole publish" limitation) -- every dataset config still declares
# these columns `type: string` (contract-level; only the FINAL gold-layer
# INSERT casts), so a plain decimal-digit string satisfies both the
# contract and the cast. Ranges are deliberately disjoint from every other
# live e2e test's own customer_id space (`test_referential_orphan.py`'s
# `[1_500_000_000, 1_999_000_000)`, `test_backfill_reentry.py`'s
# `[2_000_000_000, 2_100_000_000)`, `test_pod_kill_retry.py`'s
# `[2_000_000, 1_000_000_000)`) so a concurrently-running e2e test can never
# collide with this generator's own IDs. `_ID_DAY_MULTIPLIER` remains a
# per-day spacing constant for `orders` (unchanged); customers' roster
# formula (plan 10-06) is deliberately DAY-INDEPENDENT now, so it no longer
# uses this multiplier at all -- kept exported since
# `tests/e2e/slice/test_backfill_2year_sweep.py` imports it directly for its
# own order_id bound computation.
_CUSTOMER_ID_BASE: Final = 2_100_100_000
_ORDER_ID_BASE: Final = 2_110_000_000
_ID_DAY_MULTIPLIER: Final = 10_000

# `orders.customer_id` (D-17's one real referential relationship,
# `ReferentialIntegrityBarrier` -> `normalized.customers`, `strategy:
# QUARANTINE_RECORD`) must reference customer_id values the customers
# series ITSELF will actually publish for the referenced rows to resolve as
# genuine, non-orphaned matches -- the roster's own first 30 members, using
# the IDENTICAL `_CUSTOMER_ID_BASE + n` formula the roster itself uses. A
# caller generating both series must keep the customers series'
# `rows_per_day >= 30` (the module default, 50, already satisfies this) for
# every one of these references to resolve; a caller using a smaller
# `rows_per_day` intentionally exercises the barrier's QUARANTINE_RECORD
# path for the remainder instead -- both are valid corpus shapes, D-22's
# quarantine-aware reconciliation accounting nets out either way. This
# generator does not also emit the matching `customers` series in the same
# call, so the CALLER is responsible for uploading both series from a
# mutually-consistent `rows_per_day` choice if it needs full referential
# coverage.
_ORDER_CUSTOMER_IDS: Final[tuple[str, ...]] = tuple(str(_CUSTOMER_ID_BASE + n) for n in range(30))

_AMOUNT_MIN: Final = Decimal("1.00")
_AMOUNT_MAX: Final = Decimal("9999.99")
_AMOUNT_SCALE: Final = 2

# D-10's late/out-of-order proof scenario: a genuine 3-month-late arrival.
_DEFAULT_LATE_EVENT_OFFSET_DAYS: Final = 90
_DEFAULT_ROWS_PER_DAY: Final = 50


@dataclass(frozen=True, slots=True)
class BackfillCorpusManifest:
    """Records exactly where each of D-10's four combined properties lives.

    A downstream live test (plan 09-11) asserts against these known values
    instead of re-deriving them from the generated bytes.

    Attributes:
        dataset: The dataset this series was generated for.
        num_days: Total number of calendar days spanned, gap day included.
        start_date: Calendar date of day index 0.
        gap_day_index: Day index with no generated file (D-06/D-10).
        schema_change_day_index: First day index whose header carries the
            extra column; every earlier day's header matches the dataset's
            current real column list exactly.
        late_event_day_index: Day index containing one out-of-order row.
        late_event_row_index: 0-based row/roster-member index, within that
            day's file, whose date column is backdated instead of matching
            the file's own day.
        filenames: Every generated filename, gap day excluded, in day order.
    """

    dataset: str
    num_days: int
    start_date: date
    gap_day_index: int
    schema_change_day_index: int
    late_event_day_index: int
    late_event_row_index: int
    filenames: tuple[str, ...]


def generate_dated_series(  # noqa: PLR0913 -- one keyword per D-10 injected-anomaly control point
    dataset: str,
    *,
    master_seed: str,
    start_date: date,
    num_days: int,
    gap_day_index: int,
    schema_change_day_index: int,
    late_event_day_index: int,
    late_event_offset_days: int = _DEFAULT_LATE_EVENT_OFFSET_DAYS,
    rows_per_day: int = _DEFAULT_ROWS_PER_DAY,
) -> tuple[dict[str, bytes], BackfillCorpusManifest]:
    r"""Generate a deterministic, dated CSV corpus with three injected anomalies.

    One file per calendar day in ``[start_date, start_date + num_days)``,
    except ``gap_day_index`` (D-06/D-10's missing-file gap). ``customers`` is
    a bounded, ``rows_per_day``-sized ROSTER resent in full every non-gap day
    (plan 10-06's roster model, see the module docstring); ``orders`` keeps
    the original "new IDs born per day" model (D-01 excludes it from SCD).
    Every day's file draws from its own random stream (R1), so inserting or
    removing another day never perturbs any other day's bytes. This function
    performs no I/O: it is a pure function of its arguments.

    Args:
        dataset: ``"customers"`` or ``"orders"`` — the two datasets this
            platform's config-driven pipeline currently knows.
        master_seed: Root of every derived random stream (R1).
        start_date: Calendar date of day index 0.
        num_days: Total number of calendar days to span.
        gap_day_index: Day index to omit entirely (no key in the returned
            mapping, no entry in ``filenames``).
        schema_change_day_index: First day index whose header carries one
            additional column.
        late_event_day_index: Day index containing one backdated row.
        late_event_offset_days: How many days earlier the late row's date
            column is backdated. Defaults to a genuine 3-month-late arrival.
        rows_per_day: Data rows per generated file; for ``customers`` this is
            also the roster's fixed size.

    Returns:
        A ``(files, manifest)`` pair. ``files`` maps each generated filename
        to its exact bytes (``\\r\\n``-terminated, ``utf-8``-encoded),
        gap day excluded. ``manifest`` records exactly where each of D-10's
        four combined properties lives.

    Raises:
        ValueError: If ``dataset`` is not a known dataset name.
    """
    if dataset not in _DATASET_COLUMNS:
        known = ", ".join(sorted(_DATASET_COLUMNS))
        msg = f"unknown dataset {dataset!r} (known: {known})"
        raise ValueError(msg)

    columns = _DATASET_COLUMNS[dataset]
    extra_column = _SCHEMA_CHANGE_COLUMN[dataset]
    extra_values = _SCHEMA_CHANGE_VALUES[dataset]
    late_event_row_index = rows_per_day // 2

    roster_ids: tuple[int, ...] = ()
    baselines: tuple[dict[str, str], ...] = ()
    if dataset == "customers":
        roster_ids = tuple(_CUSTOMER_ID_BASE + m for m in range(rows_per_day))
        baselines = tuple(
            _customer_baseline(master_seed, customer_id) for customer_id in roster_ids
        )

    files: dict[str, bytes] = {}
    filenames: list[str] = []

    for day_index in range(num_days):
        day = start_date + timedelta(days=day_index)
        filename = f"{dataset}_{day.strftime('%Y%m%d')}.csv"

        if day_index == gap_day_index:
            continue  # D-06/D-10: the gap day emits no file at all.

        filenames.append(filename)

        include_extra = day_index >= schema_change_day_index
        header = (*columns, extra_column) if include_extra else columns
        lines = [",".join(header)]

        if dataset == "customers":
            lines.extend(
                _render_customer_day_lines(
                    master_seed=master_seed,
                    filename=filename,
                    roster_ids=roster_ids,
                    baselines=baselines,
                    day=day,
                    day_index=day_index,
                    late_event_day_index=late_event_day_index,
                    late_event_row_index=late_event_row_index,
                    late_event_offset_days=late_event_offset_days,
                    include_extra=include_extra,
                    extra_values=extra_values,
                )
            )
        else:
            # R1: this file's stream depends on nothing but the master seed
            # and its own name -- never on how many rows any other day
            # consumed. Unchanged from before plan 10-06 (orders' own model).
            rng = stream_for(master_seed, filename)
            for row_index in range(rows_per_day):
                is_late = day_index == late_event_day_index and row_index == late_event_row_index
                fields = _render_row(
                    rng=rng,
                    day_index=day_index,
                    row_index=row_index,
                    day=day,
                    is_late=is_late,
                    late_event_offset_days=late_event_offset_days,
                )
                if include_extra:
                    fields = (*fields, _pick(rng, extra_values))
                lines.append(",".join(fields))

        # R3: built as str, encoded explicitly, never written text-mode.
        # R4: the terminator is explicit, never a writer's default.
        body = "\r\n".join(lines) + "\r\n"
        files[filename] = body.encode("utf-8", "strict")

    manifest = BackfillCorpusManifest(
        dataset=dataset,
        num_days=num_days,
        start_date=start_date,
        gap_day_index=gap_day_index,
        schema_change_day_index=schema_change_day_index,
        late_event_day_index=late_event_day_index,
        late_event_row_index=late_event_row_index,
        filenames=tuple(filenames),
    )
    return files, manifest


def _customer_baseline(master_seed: str, customer_id: int) -> dict[str, str]:
    """Pick one roster member's baseline fields ONCE, from a customer-scoped stream.

    Keyed by the member's own ``customer_id`` (never a day's filename) --
    this is the roster model's core property: a member's baseline is stable
    across every day's file by construction, not by coincidence.
    """
    rng = stream_for(master_seed, f"customer-baseline:{customer_id}")
    return {
        "name": _pick(rng, _NAMES),
        "country": _pick(rng, _COUNTRIES),
        "birth_date": _pick(rng, _BIRTH_DATES),
        "signup_country": _pick(rng, _COUNTRIES),
    }


def _render_customer_day_lines(  # noqa: PLR0913 -- one keyword per roster-rendering context value
    *,
    master_seed: str,
    filename: str,
    roster_ids: tuple[int, ...],
    baselines: tuple[dict[str, str], ...],
    day: date,
    day_index: int,
    late_event_day_index: int,
    late_event_row_index: int,
    late_event_offset_days: int,
    include_extra: bool,
    extra_values: tuple[str, ...],
) -> list[str]:
    """Render one day's full roster resend.

    Every roster member is emitted in ascending customer_id order (index
    order).
    """
    event_ts = f"{day.strftime('%Y-%m-%d')}T08:15:00Z"
    # R1: the schema-change bonus column's per-day values depend only on
    # this day's own filename -- orthogonal to every customer-scoped
    # baseline stream above, which depends only on customer_id.
    extra_rng = stream_for(master_seed, filename) if include_extra else None

    lines: list[str] = []
    for member_index, customer_id in enumerate(roster_ids):
        baseline = baselines[member_index]

        row_event_ts = event_ts
        if day_index == late_event_day_index and member_index == late_event_row_index:
            late_date = day - timedelta(days=late_event_offset_days)
            row_event_ts = f"{late_date.strftime('%Y-%m-%d')}T08:15:00Z"

        fields: tuple[str, ...] = (
            str(customer_id),
            baseline["name"],
            baseline["country"],
            baseline["birth_date"],
            row_event_ts,
            baseline["signup_country"],
        )
        if include_extra and extra_rng is not None:
            fields = (*fields, _pick(extra_rng, extra_values))
        lines.append(",".join(fields))

    return lines


def _render_row(  # noqa: PLR0913 -- one keyword per row-rendering context value, unchanged shape from before plan 10-06
    *,
    rng: random.Random,
    day_index: int,
    row_index: int,
    day: date,
    is_late: bool,
    late_event_offset_days: int,
) -> tuple[str, ...]:
    """Render one `orders` data row's fields (order_id, customer_id, order_date, amount).

    `orders`-only since plan 10-06: customers rows are now rendered by
    `_render_customer_day_lines` from each roster member's own customer-scoped
    baseline, not a file-scoped stream. This function's own logic and its
    file-scoped `stream_for(master_seed, filename)` caller convention are
    UNCHANGED from before plan 10-06 (D-01 excludes orders from SCD).
    """
    date_value = day - timedelta(days=late_event_offset_days) if is_late else day
    order_id = str(_ORDER_ID_BASE + day_index * _ID_DAY_MULTIPLIER + row_index)
    order_date = date_value.strftime("%Y-%m-%d")
    return (
        order_id,
        _pick(rng, _ORDER_CUSTOMER_IDS),
        order_date,
        _decimal_value(rng, _AMOUNT_MIN, _AMOUNT_MAX, _AMOUNT_SCALE),
    )


def _pick(rng: random.Random, values: Sequence[str]) -> str:
    """Select from a fixed list by index arithmetic over ``random()`` (R2)."""
    count = len(values)
    return values[min(int(rng.random() * count), count - 1)]


def _decimal_value(rng: random.Random, minimum: Decimal, maximum: Decimal, scale: int) -> str:
    """Render an exact decimal via integer arithmetic — never a float (R10)."""
    power = 10**scale
    low = int(minimum.scaleb(scale).to_integral_value(rounding=ROUND_CEILING))
    high = int(maximum.scaleb(scale).to_integral_value(rounding=ROUND_FLOOR))
    span = high - low + 1
    units = low + min(int(rng.random() * span), span - 1)
    return f"{units // power}.{units % power:0{scale}d}"
