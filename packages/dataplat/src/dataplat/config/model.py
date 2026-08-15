"""``DatasetConfig`` — the pydantic model every ``configs/datasets/*.yaml`` validates against.

Every model in this module is ``ConfigDict(extra="forbid", frozen=True)``
(CLAUDE.md's "Data modelling / validation" table, `03-PATTERNS.md` Cluster
E): ``extra="forbid"`` turns a config typo into a validation-time error
instead of a silently-ignored key, and ``frozen=True`` supports the
determinism constraint (PROJECT.md Constraints) by making a resolved config
immutable for the lifetime of a run.

Strategy/source fields (``SourceConfig.type``, ``DeduplicationConfig.strategy``,
``LoadConfig.strategy``) are plain ``str``, resolved through string-keyed
registries elsewhere (ARCHITECTURE.md Q4.4 line 523) — never a Python
``Enum`` baked into this model. That indirection is what makes "config not
code" (README §65) literally true: a new source or publisher strategy is a
registry entry, not a change to this file.

Phase 6 adds ``csv.delimiter``/``normalization.decimal_separator`` fields and,
with them, the STACK.md §15 "do not confuse CSV delimiters with decimal
separators" collision validator (a ``DatasetConfig`` model validator below).
An earlier revision of this docstring named this as the moment to add it —
not before, since a ``model_validator`` with no reachable raise site is dead
code wearing a design decision's clothes (CONTEXT.md D-06's reasoning). That
point has now arrived.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

# ColumnContract.type's closed set. Deliberately NOT the "config not code"
# plain-str-resolved-through-a-registry pattern this module's docstring
# describes for SourceConfig.type/DeduplicationConfig.strategy/
# LoadConfig.strategy: those genuinely dispatch through a string-keyed
# registry elsewhere, so a new valid value really is "a registry entry, not
# a change to this file." ColumnContract.type has no such registry --
# StagingLoader._build_stages (dataplat.load.staging) is the ONLY place it
# is interpreted, via a hardcoded if/elif chain over exactly these six
# values, with no `else` branch. An unconstrained `str` here does not buy
# the extensibility the registry pattern buys elsewhere; it only lets a typo
# (e.g. "date" misspelled "dat") pass DatasetConfig validation cleanly and
# then silently receive zero type-specific normalization in `_build_stages`
# (post-wave-5 code review WR-02) -- the opposite of `extra="forbid"`'s own
# stated purpose one paragraph above.
_COLUMN_TYPES = Literal["string", "integer", "decimal", "date", "timestamp", "boolean"]


class SourceConfig(BaseModel):
    r"""Where a dataset's source files live and how change is signaled.

    Attributes:
        type: Source engine key resolved through ``SOURCE_REGISTRY``, e.g.
            ``"csv"``. A string, never an enum — see the module docstring.
        bucket: Object-store bucket the source files arrive in.
        path: Prefix within ``bucket`` the source discovers files under.
        change_semantics: How the source signals change, e.g.
            ``"snapshot"`` or ``"cdc"``.
        duplicate_policy: What ``dataplat.discovery.discover_files`` does
            when a newly-listed object's content hash already exists for
            this dataset under a different ``object_uri`` (D-13, a locked
            decision). Plain ``str``, matching this file's own convention
            — the only value this phase defines is ``"skip"``.
        multipart_pattern: An opt-in regex naming two capture groups --
            ``group`` (the shared identity every part of one logical
            dataset has in common) and ``index`` (the numeric part
            ordinal) -- e.g. ``r"(?P<group>.+)/part-(?P<index>\d+)"``,
            matching Spark-style ``part-00000``/``part-00001`` delivery
            (CSV-11, corpus fixture ``62_multipart_split``). ``None`` when
            a dataset has no multi-part delivery shape (``customers``
            does not), mirroring D-10's opt-in pattern for filename masks.
            Consumed by ``dataplat.discovery.group_multipart_units``.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    type: str
    bucket: str
    path: str
    change_semantics: str
    duplicate_policy: str
    multipart_pattern: str | None = None


class DeduplicationConfig(BaseModel):
    """How a dataset's deduplication stage collapses duplicate records.

    Attributes:
        strategy: Deduplication strategy key resolved through
            ``DEDUP_REGISTRY``, e.g. ``"business_key_latest"``.
        keys: Business-key column names that identify one logical record.
        order_by: Column expressions (e.g. ``"event_ts desc"``) that decide
            which duplicate wins.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    strategy: str
    keys: list[str]
    order_by: list[str]


class LoadConfig(BaseModel):
    """How a dataset's records are published to their target table.

    Attributes:
        strategy: Publisher strategy key resolved through
            ``PUBLISHER_REGISTRY``, e.g. ``"merge"``.
        target: Fully-qualified target table, e.g.
            ``"normalized.customers"``.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    strategy: str
    target: str


class BatchingConfig(BaseModel):
    """How many discovery units one ``discover_files`` call may hand to Dynamic Task Mapping.

    Attributes:
        max_units_per_run: The maximum number of discovered units
            ``dataplat.discovery.discover_files`` returns in one call
            (ORCH-03). Required, never defaulted — a missing cap must fail
            config validation loudly instead of silently defaulting to an
            unbounded fan-out.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_units_per_run: int


class ColumnContract(BaseModel):
    """One column's type, nullability, business-key and semantic contract.

    ``columns:`` is the source of truth for per-column type, nullability,
    required-ness, business-key marking and semantics (D-18, SCHEMA-02).
    ``DeduplicationConfig.keys`` is cross-validated against this list by
    ``DatasetConfig``'s own model validator below — the two can never
    silently disagree.

    Attributes:
        name: The column's name, as it appears in the file header (after
            detection/normalization) and in the target table.
        type: The column's declared type — one of ``"string"``,
            ``"integer"``, ``"decimal"``, ``"date"``, ``"timestamp"``,
            ``"boolean"``. A closed ``Literal`` set, not the module
            docstring's "config not code" plain-``str`` convention — that
            convention is for fields resolved through a string-keyed
            registry elsewhere (``SourceConfig.type`` etc.); no such
            registry exists for this field, only ``StagingLoader.
            _build_stages``'s hardcoded if/elif chain, so an unconstrained
            ``str`` here would only let a typo silently receive zero
            type-specific normalization instead of a validation error.
        nullable: Whether a present column's value may be empty.
        required: Whether the column must appear in the file's structure at
            all. ``required: False`` with the column absent from a file is
            the "column disappearance" case (D-04), classified breaking.
            ``required`` and ``nullable`` are deliberately two distinct
            fields (D-20), never collapsed into one.
        business_key: Whether this column participates in the dataset's
            business key. ``deduplication.keys`` may only name columns
            where this is ``True`` (D-18's cross-check, enforced below).
        description: Free-text description of the column's semantics
            (D-19). Controlled semantic tags (e.g. ``pii: true``) are
            deferred; this field is the only semantics carrier today.
        format: The ``strptime`` format string, for ``type`` in
            ``{"date", "timestamp"}`` (CSV-09). ``None`` for every other
            type — dates are never parsed by inference (STACK.md §F).
        fixed_width: The declared character width for a ``"string"``
            identifier column that must never be silently re-padded or
            truncated (corpus fixture 51). ``None`` when the column has no
            fixed width.
        reject_scientific_notation: Whether a ``"string"`` identifier
            column rendered in scientific notation (e.g. a spreadsheet
            re-export of a long numeric ID) must be rejected as
            unrecoverable rather than silently accepted (corpus fixture 50).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    type: _COLUMN_TYPES
    nullable: bool
    required: bool
    business_key: bool = False
    description: str = ""
    format: str | None = None
    fixed_width: int | None = None
    reject_scientific_notation: bool = False


class FilenameMaskConfig(BaseModel):
    """A dataset's opt-in filename mask (CSV-01).

    Absent entirely when a dataset has no filename structure to parse
    (D-10) — ``customers.yaml`` declares none. Mask syntax is
    strptime-style named tokens, not raw regex (D-07); an individual facet
    can be marked optional within one mask using bracket syntax, e.g.
    ``[_{seq:03d}]`` (D-08). A file that does not match its dataset's
    configured mask at all is rejected with a named diagnostic, never
    processed with the unmatched facets left null (D-09).

    Attributes:
        mask: The strptime-style mask pattern, e.g.
            ``"{dataset}_{country}_{business_date:%Y%m%d}[_{seq:03d}].csv"``.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    mask: str


class NormalizationConfig(BaseModel):
    """A dataset's opt-in, single locale/normalization profile (CSV-10).

    One profile per dataset, not per-column overridable (D-12); fields are
    explicit, never a named locale preset like ``locale: pl-PL`` (D-13).
    Absent entirely when a dataset has no numeric/currency/boolean columns
    needing one (e.g. ``customers``).

    Attributes:
        decimal_separator: Character separating the integer and fractional
            parts of a number. ``DatasetConfig``'s own model validator
            below rejects a document where this equals ``csv.delimiter`` —
            no parser could ever read such a file correctly.
        thousands_separator: Character grouping digits in a large number,
            or ``None`` when the dataset's numbers carry no grouping
            separator.
        currency_symbols: Currency symbols/codes to strip before parsing a
            numeric value, e.g. ``["$", "PLN"]``.
        percent_as_fraction: Whether a value carrying a ``%`` suffix is
            stored as a fraction of 1 (``True``) or as the literal number
            before the sign (``False``).
        negative_style: How a negative value is rendered — one of
            ``"leading-minus"``, ``"trailing-minus"``, ``"parentheses"``.
        null_tokens: Values treated as NULL for any column with no more
            specific ``null_sentinels`` entry. Defaults to empty-string
            only (D-14) — any other token (``"N/A"``, ``"NULL"``, ``"-"``)
            must be declared explicitly per dataset.
        null_sentinels: Per-column value sentinels treated as NULL, keyed
            by column name, e.g. ``{"amount": ["-1"]}``. Checked in
            addition to ``null_tokens``.
        boolean_true_tokens: Values recognized as boolean ``True``. Empty
            by default — an unmapped token is a validation failure
            (``"unmapped-boolean-token"``), never a silent guess (CSV-10's
            "1/0 must never become boolean absent evidence").
        boolean_false_tokens: Values recognized as boolean ``False``. Same
            default-empty, fail-loudly behavior as ``boolean_true_tokens``.
        two_digit_year_pivot: The pivot year used to resolve a two-digit
            year to its century, or ``None`` when the dataset has no
            two-digit-year dates.
        spreadsheet_epoch: The spreadsheet serial-date epoch a numeric date
            is relative to — ``"1900"`` or ``"1904"`` — or ``None`` when the
            dataset carries no spreadsheet serial dates.
        timezone: The IANA zone name applied to a naive local timestamp
            with no explicit offset, or ``None`` when the dataset has none.
        ambiguous_time_policy: How a naive local timestamp that falls in a
            DST-transition's ambiguous window is resolved — one of
            ``"reject"``, ``"earliest"``, ``"latest"``. Defaults to
            ``"reject"``: an ambiguous local time is never silently guessed.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    decimal_separator: str = "."
    thousands_separator: str | None = None
    currency_symbols: list[str] = []
    percent_as_fraction: bool = True
    negative_style: str = "leading-minus"
    null_tokens: list[str] = [""]
    null_sentinels: dict[str, list[str]] = {}
    boolean_true_tokens: list[str] = []
    boolean_false_tokens: list[str] = []
    two_digit_year_pivot: int | None = None
    spreadsheet_epoch: str | None = None
    timezone: str | None = None
    ambiguous_time_policy: str = "reject"


class CsvParsingConfig(BaseModel):
    """Structural CSV-parsing contract overrides (CSV-05/07/08).

    Every field left ``None``/default means "detect it" — ``customers.yaml``
    declares no ``csv:`` key at all, matching Phase 3's already-correct
    UTF-8/comma/header-row-0 shape once real detection confirms the same
    answer.

    Attributes:
        encoding: The file's text encoding, or ``None`` to run encoding
            detection (BOM sniff, then ``charset-normalizer``/``chardet``).
        delimiter: The field delimiter, or ``None`` to run dialect
            detection (``clevercsv``). ``DatasetConfig``'s own model
            validator below rejects a document where this equals
            ``normalization.decimal_separator``.
        quotechar: The quoting character. Defaults to ``'"'`` — RFC 4180's
            own default — since quoting convention rarely varies and is
            cheap to detect wrong; override only for a dataset that
            genuinely needs a different one.
        max_field_bytes: The maximum size, in bytes, a single field may
            reach before the row is quarantined (LOAD-07). Supersedes
            ``csv_processor.source.FIELD_SIZE_LIMIT``'s hardcoded module
            constant — this is now a per-dataset contract value.
        header_row: The 0-based row index the header lives on, or ``None``
            to run header/metadata detection.
        skip_footer_rows: The number of trailing rows to discard as a
            footer/totals block. ``0`` when the file has none.
        header_trim: Whether to strip leading/trailing whitespace from each
            header field before matching it against ``columns:``.
        header_case_sensitive: Reserved for header-to-``columns:`` name
            matching's case sensitivity. **Not yet wired to any code path**
            (post-wave-5 code review WR-01): no header-to-contract
            name-matching step exists anywhere in this codebase today --
            ``StagingLoader``/``CsvRecordStream`` map a row's fields to
            ``target_columns`` by physical position alone (see
            ``csv_processor.detect.header``'s module docstring for why
            that matching step is a distinct, later concern this field
            anticipates). Declaring a value here has no observable effect
            until that step is built. Defaults to ``True``.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    encoding: str | None = None
    delimiter: str | None = None
    quotechar: str = '"'
    max_field_bytes: int = 1_048_576
    header_row: int | None = None
    skip_footer_rows: int = 0
    header_trim: bool = False
    header_case_sensitive: bool = True


class DatasetConfig(BaseModel):
    """The complete, validated configuration for one dataset.

    Every ``configs/datasets/<name>.yaml``, merged over
    ``configs/defaults.yaml``, must validate against this model with zero
    errors (``dataplat.config.loader.load_config``). ``extra="forbid"``
    rejects an unknown top-level key at validation time; ``frozen=True``
    rejects any post-construction mutation.

    Attributes:
        dataset: The dataset's unique name, matching ``meta.datasets.dataset_name``.
        config_schema_version: Version of this model's *shape* — lets
            ``loader.py`` migrate an older document into a newer model when
            replaying a historical run (ARCHITECTURE.md Q5.2).
        source: Where and how source files arrive.
        deduplication: How duplicate records are collapsed.
        load: How records are published to their target table.
        batching: The cap on how many units one ``discover_files`` call may
            hand to Dynamic Task Mapping in a single run (ORCH-03).
        columns: Per-column type/nullability/business-key/semantic
            contracts (D-18's source of truth). Required, never defaulted —
            an omitted ``columns:`` must fail config validation loudly,
            matching ``batching``'s own precedent.
        filename: The dataset's opt-in filename mask, or ``None`` when the
            dataset has no filename structure to parse (D-10).
        normalization: The dataset's opt-in locale/normalization profile,
            or ``None`` when the dataset has no numeric/currency/boolean
            columns needing one (D-12's consequence).
        csv: Structural CSV-parsing overrides. Defaults to "detect
            everything" (``CsvParsingConfig``'s own field defaults).
        schema_evolution_on_new_column: Policy applied when a file
            declares a column absent from the dataset's known schema — D-01
            names this the COMPATIBLE case. Defaults to ``"evolve"``
            (``configs/defaults.yaml``, D-03).
        schema_evolution_on_missing_or_retyped_column: Policy applied when
            a known column disappears or changes type — D-02/D-04 name
            this the BREAKING case. Defaults to ``"freeze"``
            (``configs/defaults.yaml``, D-03).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    dataset: str
    config_schema_version: int
    source: SourceConfig
    deduplication: DeduplicationConfig
    load: LoadConfig
    batching: BatchingConfig
    columns: list[ColumnContract]
    filename: FilenameMaskConfig | None = None
    normalization: NormalizationConfig | None = None
    csv: CsvParsingConfig = Field(default_factory=CsvParsingConfig)
    schema_evolution_on_new_column: str = "evolve"
    schema_evolution_on_missing_or_retyped_column: str = "freeze"

    @model_validator(mode="after")
    def _check_delimiter_does_not_collide_with_decimal_separator(self) -> DatasetConfig:
        """Reject a delimiter that is also the decimal separator (STACK.md §15).

        Dialect detection runs before numeric normalization, so such a
        declaration is unsatisfiable by construction: the parser would
        split a number in half. Mirrors
        ``tools/corpus/manifest.py::_validate_delimiter_does_not_collide``,
        which enforces the identical rule over the fixture corpus.
        """
        if (
            self.csv.delimiter is not None
            and self.normalization is not None
            and self.csv.delimiter == self.normalization.decimal_separator
        ):
            msg = (
                f"csv.delimiter {self.csv.delimiter!r} is also "
                f"normalization.decimal_separator {self.normalization.decimal_separator!r}; "
                "dialect detection runs before numeric normalization, so no parser "
                "could ever read this file correctly"
            )
            raise ValueError(msg)
        return self

    @model_validator(mode="after")
    def _check_deduplication_keys_are_business_key_columns(self) -> DatasetConfig:
        """Reject a deduplication key not marked ``business_key: true`` in ``columns:`` (D-18).

        ``deduplication.keys`` and ``columns[].business_key`` are two
        independent declarations; this validator is what keeps them from
        silently disagreeing.
        """
        columns_by_name = {column.name: column for column in self.columns}
        for key in self.deduplication.keys:
            column = columns_by_name.get(key)
            if column is None:
                msg = f"deduplication.keys names {key!r}, which is not declared in columns:"
                raise ValueError(msg)
            if not column.business_key:
                msg = (
                    f"deduplication.keys names {key!r}, but its columns: entry has "
                    "business_key: false"
                )
                raise ValueError(msg)
        return self
