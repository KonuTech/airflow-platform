"""The validated manifest model — the gate on corpus correctness (QUAL-08).

``tests/fixtures/corpus.yaml`` is the only untrusted input this phase
deserialises, so it is parsed with the safe YAML loader and nothing else.
Arbitrary object construction from it would be a straight deserialisation
vulnerability (threat T-01-13, ASVS V5).

Every model here is **frozen** and the outer schema **forbids unknown keys**. A
manifest that silently ignores a misspelled field generates a corpus that
silently means something else, and a corpus that means something else has
stopped being a specification.

There is exactly one relaxation: unknown keys *inside* an ``expect:`` block are
accepted and preserved. This is the adopted resolution to 01-RESEARCH.md Open
Question 3. The whole corpus is authored in Phase 1, but some ``expect:`` fields
name concepts whose vocabulary is fixed later — the encoding-confidence floor in
Phase 6, the quarantine reason in Phase 8. Keeping ``expect:`` permissive means
those are writable today without a model migration. The relaxation is written
down here because an unexplained relaxation is the one a later reader widens
into a schema that checks nothing.

Note on validation style: this module hand-writes its checks rather than
delegating to a validation library. The reason is the error *message*. Every
rejection below names the offending key **and** the fixture it appeared in,
because "which fixture is broken" is the only question a reader of this error
actually has, and a positional path into a YAML list does not answer it.
"""

from __future__ import annotations

import codecs
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final, Literal

# PyYAML 6.0.3 ships no `py.typed`, and `types-PyYAML` is not in the dev group
# (this plan's declared file scope does not include `pyproject.toml`/`uv.lock`).
# The suppression is deliberately narrowed to this one import rather than added
# as a project-wide `ignore_missing_imports` override, so it is visible here and
# disappears the moment the stubs are installed. Everything `yaml` returns is
# re-validated below before it reaches a typed model, so the untyped boundary is
# one line wide.
import yaml  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from collections.abc import Mapping

GeneratorKind = Literal["tabular", "literal", "literal_unicode", "wrapper"]
NormalisationForm = Literal["NFC", "NFD", "NFKC", "NFKD"]
Profile = Literal["default", "large"]

_GENERATOR_KINDS: Final[tuple[str, ...]] = ("tabular", "literal", "literal_unicode", "wrapper")
_NORMALISATION_FORMS: Final[tuple[str, ...]] = ("NFC", "NFD", "NFKC", "NFKD")
_PROFILES: Final[tuple[str, ...]] = ("default", "large")
_COMPRESSIONS: Final[tuple[str, ...]] = ("gzip",)

# R4: the terminator is always an explicit manifest field, never a writer's
# default and never a platform-translated "\n".
_LINE_TERMINATORS: Final[tuple[str, ...]] = ("\n", "\r\n", "\r")

_ROOT_KEY_ORDER: Final[tuple[str, ...]] = ("version", "master_seed", "fixtures")

# Declared in schema order rather than as a bare set, so the "known keys" hint
# in an error message reads in the order an author would write them — and so no
# message depends on set iteration order (R7).
_FIXTURE_KEY_ORDER: Final[tuple[str, ...]] = (
    "name",
    "covers",
    "generator",
    "encoding",
    "bom",
    "delimiter",
    "quotechar",
    "line_terminator",
    "header",
    "rows",
    "row_spec",
    "rows_spec",
    "content",
    "wraps",
    "compression",
    "gzip_mtime",
    "gzip_filename",
    "profile",
    "expect",
)

_COLUMN_KEY_ORDER: Final[Mapping[str, tuple[str, ...]]] = MappingProxyType(
    {
        "zero_padded_int": ("kind", "width", "start"),
        "pick": ("kind", "values"),
        "decimal": ("kind", "min", "max", "scale", "decimal_separator"),
    }
)

_SUPPORTED_VERSION: Final = 1


class ManifestError(ValueError):
    """A manifest is malformed, self-contradictory or unsupported.

    Raised at load time so that a corrupt manifest fails loudly instead of
    producing a corrupt corpus.
    """


@dataclass(frozen=True, slots=True)
class ZeroPaddedIntColumn:
    """A monotonically increasing integer rendered with leading zeros.

    Attributes:
        width: Total rendered width, zero-padded on the left.
        start: Value of the first data row.
        kind: Discriminator, fixed for this column type.
    """

    width: int
    start: int
    kind: Literal["zero_padded_int"] = "zero_padded_int"


@dataclass(frozen=True, slots=True)
class PickColumn:
    """A value chosen from a fixed list by the fixture's own random stream.

    Attributes:
        values: Candidate values, in declared order. Selection is index
            arithmetic over ``Random.random()`` so it obeys determinism rule R2.
        kind: Discriminator, fixed for this column type.
    """

    values: tuple[str, ...]
    kind: Literal["pick"] = "pick"


@dataclass(frozen=True, slots=True)
class DecimalColumn:
    """An exact decimal in a closed interval, rendered at a fixed scale.

    ``minimum`` and ``maximum`` are always written in canonical notation with a
    ``.`` separator; ``decimal_separator`` affects only how the value is
    rendered into the file.

    Attributes:
        minimum: Inclusive lower bound.
        maximum: Inclusive upper bound.
        scale: Number of fractional digits in the rendered value.
        decimal_separator: Character rendered between integer and fraction.
        kind: Discriminator, fixed for this column type.
    """

    minimum: Decimal
    maximum: Decimal
    scale: int
    decimal_separator: str = "."
    kind: Literal["decimal"] = "decimal"


ColumnSpec = ZeroPaddedIntColumn | PickColumn | DecimalColumn


@dataclass(frozen=True, slots=True)
class UnicodeField:
    """A field whose Unicode normalisation form is stated explicitly.

    Determinism rule R9: normalisation variants are built with explicit
    ``unicodedata.normalize`` calls, never by pasting two visually identical
    strings into a source file, because editors and git filters silently
    collapse exactly the distinction such a fixture exists to test.

    Attributes:
        text: The source text, before normalisation.
        form: The normalisation form to apply.
    """

    text: str
    form: NormalisationForm


@dataclass(frozen=True, slots=True)
class Fixture:
    """One declared fixture: how to build it and what it is expected to mean.

    Attributes:
        name: File name written under the corpus output directory.
        generator: Which generator kind builds it.
        covers: Requirement IDs this fixture exercises.
        encoding: Codec the rendered text is encoded with (R3).
        bom: Whether the encoding's byte-order mark is prepended.
        delimiter: Field separator.
        quotechar: Quoting character.
        line_terminator: Explicit line terminator, joined by hand (R4).
        header: Header row, in declared order.
        rows: Number of data rows for ``tabular``.
        row_spec: Per-column generation spec, keyed by header name.
        rows_spec: Explicit rows for ``literal_unicode``.
        content: Literal file content for ``literal``, with escapes resolved.
        wraps: Name of an earlier fixture, for ``wrapper``.
        compression: Compression applied by ``wrapper``.
        gzip_mtime: Embedded gzip timestamp — 0 is non-negotiable (R5).
        gzip_filename: Embedded gzip source name — empty is non-negotiable (R5).
        profile: ``large`` fixtures are skipped by the fast development loop.
        expect: The fixture's declared meaning. Permissive by design.
    """

    name: str
    generator: GeneratorKind
    covers: tuple[str, ...]
    encoding: str
    bom: bool
    delimiter: str
    quotechar: str
    line_terminator: str
    header: tuple[str, ...]
    rows: int
    row_spec: Mapping[str, ColumnSpec]
    rows_spec: tuple[Mapping[str, str | UnicodeField], ...]
    content: str | None
    wraps: str | None
    compression: str | None
    gzip_mtime: int
    gzip_filename: str
    profile: Profile
    expect: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class Manifest:
    """The whole corpus specification.

    Attributes:
        version: Manifest schema version.
        master_seed: Root of every fixture's derived random stream (R1).
        fixtures: Fixtures in declared order — never sorted, never re-ordered.
    """

    version: int
    master_seed: str
    fixtures: tuple[Fixture, ...]


def load_manifest(path: Path | str) -> Manifest:
    """Load and fully validate a corpus manifest.

    Args:
        path: Path to the manifest YAML file.

    Returns:
        The validated, frozen manifest with fixtures in declared order.

    Raises:
        ManifestError: If the file is unreadable, is not valid YAML, requests
            object construction, or violates any schema or cross-field rule.
    """
    path = Path(path)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:  # pragma: no cover - filesystem failure
        msg = f"{path}: cannot read manifest: {exc}"
        raise ManifestError(msg) from exc

    return parse_manifest(text, source=str(path))


def parse_manifest(text: str, *, source: str = "<manifest>") -> Manifest:
    """Parse and fully validate a corpus manifest from YAML text.

    Args:
        text: The manifest document.
        source: Label used in error messages.

    Returns:
        The validated, frozen manifest with fixtures in declared order.

    Raises:
        ManifestError: If the document is not valid YAML, requests object
            construction, or violates any schema or cross-field rule.
    """
    try:
        # safe_load only: never `yaml.load`, never a custom Loader that can
        # construct Python objects (threat T-01-13).
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        msg = f"{source}: not a valid safe-YAML document: {exc}"
        raise ManifestError(msg) from exc

    root = _require_mapping(raw, source)
    _reject_extra_keys(root, _ROOT_KEY_ORDER, source)

    version = _require_int(root, "version", source)
    if version != _SUPPORTED_VERSION:
        msg = f"{source}: manifest version {version} is unsupported (want {_SUPPORTED_VERSION})"
        raise ManifestError(msg)

    master_seed = _require_str(root, "master_seed", source)
    if not master_seed:
        msg = f"{source}: master_seed must be a non-empty string"
        raise ManifestError(msg)

    raw_fixtures = root.get("fixtures")
    if not isinstance(raw_fixtures, list) or not raw_fixtures:
        msg = f"{source}: fixtures must be a non-empty list"
        raise ManifestError(msg)

    fixtures: list[Fixture] = []
    seen: dict[str, int] = {}
    for index, entry in enumerate(raw_fixtures):
        fixture = _parse_fixture(entry, index=index, source=source)
        if fixture.name in seen:
            msg = (
                f"{source}: duplicate fixture name {fixture.name!r} "
                f"(first declared at index {seen[fixture.name]}, again at index {index})"
            )
            raise ManifestError(msg)
        if fixture.generator == "wrapper":
            _check_wrapper_target(fixture, seen, source)
        seen[fixture.name] = index
        fixtures.append(fixture)

    return Manifest(version=version, master_seed=master_seed, fixtures=tuple(fixtures))


def _check_wrapper_target(fixture: Fixture, seen: Mapping[str, int], source: str) -> None:
    """Reject a wrapper whose target is not already declared above it."""
    target = fixture.wraps
    if target is None:  # pragma: no cover - guarded by per-generator validation
        return
    if target not in seen:
        msg = (
            f"{source}: fixture {fixture.name!r} wraps {target!r}, which is not "
            f"declared earlier in the manifest. Generation is a single ordered "
            f"pass, so a forward reference has no bytes to wrap."
        )
        raise ManifestError(msg)


def _parse_fixture(entry: object, *, index: int, source: str) -> Fixture:
    """Validate one fixture entry into a frozen model."""
    where = f"{source}: fixture at index {index}"
    data = _require_mapping(entry, where)

    name = _require_str(data, "name", where)
    if not name:
        msg = f"{where}: name must be a non-empty string"
        raise ManifestError(msg)

    # From here on every message names the fixture, not its index: "which
    # fixture is broken" is the question the reader actually has.
    where = f"{source}: fixture {name!r}"
    _reject_extra_keys(data, _FIXTURE_KEY_ORDER, where)

    generator = _require_str(data, "generator", where)
    if generator not in _GENERATOR_KINDS:
        known = ", ".join(_GENERATOR_KINDS)
        msg = f"{where}: unknown generator kind {generator!r} (known: {known})"
        raise ManifestError(msg)

    encoding = _optional_str(data, "encoding", where, default="utf-8")
    try:
        codecs.lookup(encoding)
    except LookupError as exc:
        msg = f"{where}: unknown encoding {encoding!r}"
        raise ManifestError(msg) from exc

    line_terminator = _optional_str(data, "line_terminator", where, default="\n")
    if line_terminator not in _LINE_TERMINATORS:
        msg = (
            f"{where}: line_terminator {line_terminator!r} is not one of "
            f"{list(_LINE_TERMINATORS)!r}"
        )
        raise ManifestError(msg)

    profile = _optional_str(data, "profile", where, default="default")
    if profile not in _PROFILES:
        msg = f"{where}: unknown profile {profile!r} (known: {', '.join(_PROFILES)})"
        raise ManifestError(msg)

    compression = data.get("compression")
    if compression is not None:
        compression = _require_str(data, "compression", where)
        if compression not in _COMPRESSIONS:
            msg = f"{where}: unknown compression {compression!r}"
            raise ManifestError(msg)

    fixture = Fixture(
        name=name,
        generator=_as_generator_kind(generator),
        covers=_optional_str_tuple(data, "covers", where),
        encoding=encoding,
        bom=_optional_bool(data, "bom", where, default=False),
        delimiter=_optional_str(data, "delimiter", where, default=","),
        quotechar=_optional_str(data, "quotechar", where, default='"'),
        line_terminator=line_terminator,
        header=_optional_str_tuple(data, "header", where),
        rows=_optional_int(data, "rows", where, default=0),
        row_spec=_parse_row_spec(data.get("row_spec"), where=where),
        rows_spec=_parse_rows_spec(data.get("rows_spec"), where=where),
        content=_decode_content(data.get("content"), where=where),
        wraps=None if data.get("wraps") is None else _require_str(data, "wraps", where),
        compression=compression,
        gzip_mtime=_optional_int(data, "gzip_mtime", where, default=0),
        gzip_filename=_optional_str(data, "gzip_filename", where, default=""),
        profile=_as_profile(profile),
        expect=_parse_expect(data.get("expect"), where=where),
    )

    _VALIDATORS[fixture.generator](fixture, where)
    _validate_delimiter_does_not_collide(fixture, where)
    _validate_large_profile(fixture, where)
    return fixture


def _validate_tabular(fixture: Fixture, where: str) -> None:
    """Reject a tabular fixture without a header, rows, or a spec per column."""
    if not fixture.header:
        msg = f"{where}: a tabular fixture needs a non-empty header"
        raise ManifestError(msg)
    if fixture.rows <= 0:
        msg = f"{where}: a tabular fixture needs rows > 0 (got {fixture.rows})"
        raise ManifestError(msg)
    for column in fixture.header:
        if column not in fixture.row_spec:
            msg = (
                f"{where}: header column {column!r} has no row_spec entry. "
                f"Every column is specified explicitly; there is no default."
            )
            raise ManifestError(msg)
    for column in fixture.row_spec:
        if column not in fixture.header:
            msg = f"{where}: row_spec declares {column!r}, which is not in the header"
            raise ManifestError(msg)


def _validate_literal(fixture: Fixture, where: str) -> None:
    """Reject a literal fixture with no declared content."""
    if fixture.content is None:
        msg = f"{where}: a literal fixture needs a content field"
        raise ManifestError(msg)


def _validate_literal_unicode(fixture: Fixture, where: str) -> None:
    """Reject a literal_unicode fixture with no rows or with ragged field sets."""
    if not fixture.rows_spec:
        msg = f"{where}: a literal_unicode fixture needs a non-empty rows_spec"
        raise ManifestError(msg)
    first = tuple(fixture.rows_spec[0])
    for row_index, row in enumerate(fixture.rows_spec):
        if tuple(row) != first:
            msg = (
                f"{where}: rows_spec row {row_index} declares fields {tuple(row)!r}, "
                f"but row 0 declares {first!r}. The header is derived from row 0, "
                f"so every row must declare the same fields in the same order."
            )
            raise ManifestError(msg)


def _validate_wrapper(fixture: Fixture, where: str) -> None:
    """Reject a wrapper fixture that names no target or no compression."""
    if fixture.wraps is None:
        msg = f"{where}: a wrapper fixture needs a wraps field"
        raise ManifestError(msg)
    if fixture.compression is None:
        msg = f"{where}: a wrapper fixture needs a compression field"
        raise ManifestError(msg)


_VALIDATORS: Final[Mapping[str, Any]] = MappingProxyType(
    {
        "tabular": _validate_tabular,
        "literal": _validate_literal,
        "literal_unicode": _validate_literal_unicode,
        "wrapper": _validate_wrapper,
    }
)


def _validate_delimiter_does_not_collide(fixture: Fixture, where: str) -> None:
    """Reject a fixture whose delimiter is also a column's decimal separator.

    Dialect detection runs before numeric normalisation, so such a declaration
    is unsatisfiable by construction: the parser would split a number in half.
    """
    for column, spec in fixture.row_spec.items():
        if isinstance(spec, DecimalColumn) and spec.decimal_separator == fixture.delimiter:
            msg = (
                f"{where}: column {column!r} declares decimal_separator "
                f"{spec.decimal_separator!r}, which is also the fixture delimiter. "
                f"Dialect detection runs before numeric normalisation, so no parser "
                f"could ever read this file correctly."
            )
            raise ManifestError(msg)


def _validate_large_profile(fixture: Fixture, where: str) -> None:
    """Reject a large fixture that is not comfortably bigger than its limit.

    The bounded-memory assertion is vacuous unless the file is more than twice
    the address-space limit it is streamed under.
    """
    if fixture.profile != "large":
        return

    approx = fixture.expect.get("approx_bytes")
    limit = fixture.expect.get("rlimit_as_bytes")
    if not isinstance(approx, int) or isinstance(approx, bool):
        msg = f"{where}: a large-profile fixture needs an integer expect.approx_bytes"
        raise ManifestError(msg)
    if not isinstance(limit, int) or isinstance(limit, bool):
        msg = f"{where}: a large-profile fixture needs an integer expect.rlimit_as_bytes"
        raise ManifestError(msg)
    if approx <= 2 * limit:
        msg = (
            f"{where}: expect.approx_bytes ({approx}) must be more than twice "
            f"expect.rlimit_as_bytes ({limit}); otherwise the bounded-memory "
            f"assertion passes for a file that never needed streaming."
        )
        raise ManifestError(msg)


def _parse_row_spec(raw: object, *, where: str) -> Mapping[str, ColumnSpec]:
    """Validate the per-column generation spec, preserving declared order."""
    if raw is None:
        return MappingProxyType({})
    data = _require_mapping(raw, f"{where}: row_spec")

    parsed: dict[str, ColumnSpec] = {}
    for column, value in data.items():
        parsed[column] = _parse_column(value, where=f"{where}: row_spec.{column}")
    return MappingProxyType(parsed)


def _parse_column(raw: object, *, where: str) -> ColumnSpec:
    """Validate one column generation spec."""
    data = _require_mapping(raw, where)
    kind = _require_str(data, "kind", where)
    allowed = _COLUMN_KEY_ORDER.get(kind)
    if allowed is None:
        msg = f"{where}: unknown column kind {kind!r} (known: {', '.join(_COLUMN_KEY_ORDER)})"
        raise ManifestError(msg)
    _reject_extra_keys(data, allowed, where)

    if kind == "zero_padded_int":
        return ZeroPaddedIntColumn(
            width=_require_int(data, "width", where),
            start=_optional_int(data, "start", where, default=1),
        )
    if kind == "pick":
        values = _optional_str_tuple(data, "values", where)
        if not values:
            msg = f"{where}: pick needs a non-empty values list"
            raise ManifestError(msg)
        return PickColumn(values=values)

    minimum = _require_decimal(data, "min", where)
    maximum = _require_decimal(data, "max", where)
    if minimum > maximum:
        msg = f"{where}: min ({minimum}) is greater than max ({maximum})"
        raise ManifestError(msg)
    return DecimalColumn(
        minimum=minimum,
        maximum=maximum,
        scale=_require_int(data, "scale", where),
        decimal_separator=_optional_str(data, "decimal_separator", where, default="."),
    )


def _parse_rows_spec(raw: object, *, where: str) -> tuple[Mapping[str, str | UnicodeField], ...]:
    """Validate explicit rows for the ``literal_unicode`` generator."""
    if raw is None:
        return ()
    if not isinstance(raw, list):
        msg = f"{where}: rows_spec must be a list"
        raise ManifestError(msg)

    rows: list[Mapping[str, str | UnicodeField]] = []
    for index, entry in enumerate(raw):
        row_where = f"{where}: rows_spec[{index}]"
        data = _require_mapping(entry, row_where)
        fields: dict[str, str | UnicodeField] = {}
        for key, value in data.items():
            fields[key] = _parse_unicode_field(value, where=f"{row_where}.{key}")
        rows.append(MappingProxyType(fields))
    return tuple(rows)


def _parse_unicode_field(value: object, *, where: str) -> str | UnicodeField:
    """Validate one field of a ``literal_unicode`` row."""
    if isinstance(value, str):
        return value
    if not isinstance(value, dict):
        msg = f"{where}: must be a string or a {{text, form}} mapping"
        raise ManifestError(msg)

    _reject_extra_keys(value, ("text", "form"), where)
    form = _require_str(value, "form", where)
    if form not in _NORMALISATION_FORMS:
        msg = (
            f"{where}: unknown normalisation form {form!r} "
            f"(known: {', '.join(_NORMALISATION_FORMS)})"
        )
        raise ManifestError(msg)
    return UnicodeField(text=_require_str(value, "text", where), form=_as_form(form))


def _parse_expect(raw: object, *, where: str) -> Mapping[str, Any]:
    """Validate the ``expect:`` block — the one permissive sub-schema.

    Unknown keys are accepted and preserved on purpose; see the module
    docstring for why.
    """
    if raw is None:
        return MappingProxyType({})
    data = _require_mapping(raw, f"{where}: expect")
    return MappingProxyType(dict(data))


def _decode_content(raw: object, *, where: str) -> str | None:
    r"""Resolve the escapes in a literal fixture's declared content.

    Pathological byte sequences live inside the manifest as escaped literals so
    that 100% of the generated corpus stays generated. ``\x00`` in the YAML
    therefore has to become a real NUL here.
    """
    if raw is None:
        return None
    if not isinstance(raw, str):
        msg = f"{where}: content must be a string"
        raise ManifestError(msg)
    try:
        return raw.encode("utf-8").decode("unicode_escape").encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError) as exc:
        msg = f"{where}: content contains an escape sequence that is not valid UTF-8: {exc}"
        raise ManifestError(msg) from exc


def _require_mapping(value: object, where: str) -> Mapping[str, Any]:
    """Require a YAML mapping with string keys."""
    if not isinstance(value, dict):
        msg = f"{where}: expected a mapping, got {type(value).__name__}"
        raise ManifestError(msg)
    for key in value:
        if not isinstance(key, str):
            msg = f"{where}: mapping keys must be strings, got {key!r}"
            raise ManifestError(msg)
    return value


def _reject_extra_keys(data: Mapping[str, Any], allowed: tuple[str, ...], where: str) -> None:
    """Reject any key outside the schema, naming the key and its context."""
    known = frozenset(allowed)
    # Iterate the document, not the schema set: the reported key order is the
    # author's, and no message depends on hash-salted set ordering (R7).
    for key in data:
        if key not in known:
            msg = f"{where}: extra key {key!r} is not allowed (known keys: {', '.join(allowed)})"
            raise ManifestError(msg)


def _require_str(data: Mapping[str, Any], key: str, where: str) -> str:
    """Require a string field."""
    value = data.get(key)
    if not isinstance(value, str):
        msg = f"{where}: {key} must be a string, got {type(value).__name__}"
        raise ManifestError(msg)
    return value


def _optional_str(data: Mapping[str, Any], key: str, where: str, *, default: str) -> str:
    """Read an optional string field."""
    value = data.get(key)
    if value is None:
        return default
    if not isinstance(value, str):
        msg = f"{where}: {key} must be a string, got {type(value).__name__}"
        raise ManifestError(msg)
    return value


def _require_int(data: Mapping[str, Any], key: str, where: str) -> int:
    """Require an integer field, rejecting the ``bool`` subclass."""
    value = data.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        msg = f"{where}: {key} must be an integer, got {type(value).__name__}"
        raise ManifestError(msg)
    return value


def _optional_int(data: Mapping[str, Any], key: str, where: str, *, default: int) -> int:
    """Read an optional integer field."""
    value = data.get(key)
    if value is None:
        return default
    if not isinstance(value, int) or isinstance(value, bool):
        msg = f"{where}: {key} must be an integer, got {type(value).__name__}"
        raise ManifestError(msg)
    return value


def _optional_bool(data: Mapping[str, Any], key: str, where: str, *, default: bool) -> bool:
    """Read an optional boolean field."""
    value = data.get(key)
    if value is None:
        return default
    if not isinstance(value, bool):
        msg = f"{where}: {key} must be a boolean, got {type(value).__name__}"
        raise ManifestError(msg)
    return value


def _optional_str_tuple(data: Mapping[str, Any], key: str, where: str) -> tuple[str, ...]:
    """Read an optional list-of-strings field, preserving declared order."""
    value = data.get(key)
    if value is None:
        return ()
    if not isinstance(value, list):
        msg = f"{where}: {key} must be a list of strings"
        raise ManifestError(msg)
    for item in value:
        if not isinstance(item, str):
            msg = f"{where}: {key} must contain only strings, got {item!r}"
            raise ManifestError(msg)
    return tuple(value)


def _require_decimal(data: Mapping[str, Any], key: str, where: str) -> Decimal:
    """Require an exact decimal, declared as a string in canonical notation.

    R10: numbers are never round-tripped through ``float``.
    """
    value = data.get(key)
    if not isinstance(value, (str, int)) or isinstance(value, bool):
        msg = (
            f"{where}: {key} must be a decimal written as a string "
            f"(a YAML float would lose exactness), got {type(value).__name__}"
        )
        raise ManifestError(msg)
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        msg = f"{where}: {key} is not a valid decimal: {value!r}"
        raise ManifestError(msg) from exc


def _as_generator_kind(value: str) -> GeneratorKind:
    """Narrow a validated string to the generator-kind literal type."""
    if value not in _GENERATOR_KINDS:  # pragma: no cover - validated by the caller
        raise AssertionError(value)
    return value  # type: ignore[return-value]


def _as_profile(value: str) -> Profile:
    """Narrow a validated string to the profile literal type."""
    if value not in _PROFILES:  # pragma: no cover - validated by the caller
        raise AssertionError(value)
    return value  # type: ignore[return-value]


def _as_form(value: str) -> NormalisationForm:
    """Narrow a validated string to the normalisation-form literal type."""
    if value not in _NORMALISATION_FORMS:  # pragma: no cover - validated by the caller
        raise AssertionError(value)
    return value  # type: ignore[return-value]
