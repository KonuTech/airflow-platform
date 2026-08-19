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
_DATASET_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    "customers": ("customer_id", "name", "country", "birth_date", "event_ts"),
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

# A synthetic customer pool for `orders.customer_id` — this generator does not
# also emit the matching `customers` series in the same call, so referential
# integrity against a real customers file is out of scope here (no behavior
# test in this plan asserts it); plan 09-11 is responsible for uploading
# datasets whose customer_id values are mutually consistent if it needs that.
_ORDER_CUSTOMER_IDS: Final[tuple[str, ...]] = tuple(f"CUST-{n:06d}" for n in range(1, 31))

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
        late_event_row_index: 0-based row index, within that day's file,
            whose date column is backdated instead of matching the file's
            own day.
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
    except ``gap_day_index`` (D-06/D-10's missing-file gap). Every day's file
    draws from its own random stream (R1), so inserting or removing another
    day never perturbs any other day's bytes. This function performs no I/O:
    it is a pure function of its arguments.

    Args:
        dataset: ``"customers"`` or ``"orders"`` — the two datasets this
            platform's config-driven pipeline currently knows.
        master_seed: Root of every day's derived random stream (R1).
        start_date: Calendar date of day index 0.
        num_days: Total number of calendar days to span.
        gap_day_index: Day index to omit entirely (no key in the returned
            mapping, no entry in ``filenames``).
        schema_change_day_index: First day index whose header carries one
            additional column.
        late_event_day_index: Day index containing one backdated row.
        late_event_offset_days: How many days earlier the late row's date
            column is backdated. Defaults to a genuine 3-month-late arrival.
        rows_per_day: Data rows per generated file.

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

    files: dict[str, bytes] = {}
    filenames: list[str] = []

    for day_index in range(num_days):
        day = start_date + timedelta(days=day_index)
        filename = f"{dataset}_{day.strftime('%Y%m%d')}.csv"

        if day_index == gap_day_index:
            continue  # D-06/D-10: the gap day emits no file at all.

        filenames.append(filename)
        # R1: this file's stream depends on nothing but the master seed and
        # its own name — never on how many rows any other day consumed.
        rng = stream_for(master_seed, filename)

        include_extra = day_index >= schema_change_day_index
        header = (*columns, extra_column) if include_extra else columns

        lines = [",".join(header)]
        for row_index in range(rows_per_day):
            is_late = day_index == late_event_day_index and row_index == late_event_row_index
            fields = _render_row(
                dataset,
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


def _render_row(  # noqa: PLR0913 -- one keyword per row-rendering context value, mirrors the caller's own shape
    dataset: str,
    *,
    rng: random.Random,
    day_index: int,
    row_index: int,
    day: date,
    is_late: bool,
    late_event_offset_days: int,
) -> tuple[str, ...]:
    """Render one data row's fields, in the dataset's declared column order."""
    business_key = f"{dataset[:4].upper()}-{day_index:06d}-{row_index:04d}"
    date_value = day - timedelta(days=late_event_offset_days) if is_late else day

    if dataset == "customers":
        event_ts = f"{date_value.strftime('%Y-%m-%d')}T08:15:00Z"
        return (
            business_key,
            _pick(rng, _NAMES),
            _pick(rng, _COUNTRIES),
            _pick(rng, _BIRTH_DATES),
            event_ts,
        )

    order_date = date_value.strftime("%Y-%m-%d")
    return (
        business_key,
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
