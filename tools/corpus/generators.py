"""Deterministic fixture generation — determinism rules R1 through R10.

THE PSEUDO-RANDOM GENERATOR HERE IS THE CORRECT ONE. ``random`` is used, not
``secrets``, and that is deliberate: the requirement is *reproducibility*, not
unpredictability. A well-meaning "security fix" swapping in ``secrets`` (or
``os.urandom``) would silently destroy the byte-identity guarantee this whole
module exists to provide, and every downstream detector test would quietly
re-baseline. There is no secret here — the seed is committed in the manifest.

Each rule below defeats one specific, named mechanism:

* **R1** — every fixture draws from its *own* stream, derived from a digest of
  the master seed joined with the fixture name. A single shared stream would
  make fixture *N*'s bytes depend on how many values fixtures 1..*N*-1 consumed,
  so inserting one fixture would rewrite every later digest.
* **R2** — randomness is consumed *only* through ``Random.random()``. CPython
  guarantees that method's sequence across versions; ``choice``, ``shuffle``,
  ``sample``, ``randrange`` and ``randint`` are documented as subject to change.
  Integers and selections are derived by arithmetic over ``random()``.
* **R3** — text is built as ``str``, encoded to the declared encoding, and
  written in binary mode. A text-mode write would let the platform's preferred
  encoding leak in.
* **R4** — lines are joined with the manifest's declared terminator by hand. No
  writer ever chooses one.
* **R5** — compressed fixtures zero the embedded timestamp and clear the
  embedded source filename. Two runs a second apart otherwise differ.
* **R6** — no wall-clock, process-identity or OS-entropy call appears anywhere
  in this package. ``tests/policy/test_generator_determinism_rules.py`` enforces
  this by source inspection.
* **R7** — the manifest is iterated in declared order. No unordered collection
  is ever iterated as generation input, because string hashing is
  process-salted.
* **R8** — directory listing order is never read as generation input.
* **R9** — normalisation variants are built with explicit ``unicodedata``
  calls, never by pasting two visually identical strings into this source.
* **R10** — numbers are formatted with explicit format strings over exact
  integer arithmetic. No value ever passes through ``float``.
"""

from __future__ import annotations

import codecs
import gzip
import hashlib
import random
import unicodedata
from decimal import ROUND_CEILING, ROUND_FLOOR
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

# Relative, not absolute: `tools/` is a namespace package (it holds no
# `__init__.py`), so an absolute `tools.corpus.x` import would let mypy resolve
# this same file under two module names — `corpus.x` when walking the tree and
# `tools.corpus.x` when following the import — and fail. Relative imports name
# the package exactly once and work identically under `python -m tools.corpus`.
from .digests import sha256_file
from .manifest import (
    DecimalColumn,
    Fixture,
    PickColumn,
    UnicodeField,
    ZeroPaddedIntColumn,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

    from .manifest import ColumnSpec, Manifest

    # A renderer turns (stream, row index) into one field's text. Annotations
    # are lazy under `from __future__ import annotations`, so this alias never
    # needs to exist at run time.
    Renderer = Callable[[random.Random, int], str]

# Rows are accumulated as string parts and flushed in batches: the large fixture
# is ~300 MB and must never be materialised whole (bounded memory is structural
# here, not configured).
_FLUSH_PARTS: Final = 200_000

_READ_CHUNK: Final = 1 << 20

# gzip's compression level is part of the output bytes, so it is pinned rather
# than left to the default.
_GZIP_LEVEL: Final = 9

_BOMS: Final[Mapping[str, bytes]] = MappingProxyType(
    {
        "utf-8": codecs.BOM_UTF8,
        "utf-16-le": codecs.BOM_UTF16_LE,
        "utf-16-be": codecs.BOM_UTF16_BE,
        "utf-32-le": codecs.BOM_UTF32_LE,
        "utf-32-be": codecs.BOM_UTF32_BE,
    }
)


class GeneratorError(RuntimeError):
    """A fixture could not be generated from its declaration."""


def stream_for(master_seed: str, name: str) -> random.Random:
    """Derive a fixture's private random stream (R1).

    Args:
        master_seed: The manifest's master seed.
        name: The fixture name.

    Returns:
        A generator seeded so that this fixture's bytes depend on nothing but
        the master seed and its own name.
    """
    digest = hashlib.sha256(f"{master_seed}|{name}".encode()).digest()
    return random.Random(int.from_bytes(digest, "big"))  # noqa: S311 - see module docstring


def generate_corpus(manifest: Manifest, out_dir: Path, *, fast: bool = False) -> dict[str, str]:
    """Materialise every declared fixture and return its digest.

    Args:
        manifest: The validated corpus specification.
        out_dir: Directory the fixtures are written into. Created if absent.
        fast: Skip ``profile: large`` fixtures. For the inner development loop
            only — the gate and CI always run the full set, because a fast path
            that is also the default is a fast path that stops testing.

    Returns:
        Fixture name to hex SHA-256 digest, in declared order.

    Raises:
        GeneratorError: If a fixture cannot be generated.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    digests: dict[str, str] = {}
    # R7: declared order. Never a set, never a re-sort.
    for fixture in manifest.fixtures:
        if fast and fixture.profile == "large":
            continue
        path = out_dir / fixture.name
        _write_fixture(fixture, manifest.master_seed, out_dir, path)
        digests[fixture.name] = sha256_file(path)
    return digests


def _write_fixture(fixture: Fixture, master_seed: str, out_dir: Path, path: Path) -> None:
    """Dispatch to the generator kind the fixture declares."""
    if fixture.generator == "tabular":
        _write_tabular(fixture, master_seed, path)
    elif fixture.generator == "literal":
        _write_literal(fixture, path)
    elif fixture.generator == "literal_unicode":
        _write_literal_unicode(fixture, path)
    else:
        _write_wrapper(fixture, out_dir, path)


def _write_tabular(fixture: Fixture, master_seed: str, path: Path) -> None:
    """Write a header plus parameterised rows, streaming and bounded."""
    rng = stream_for(master_seed, fixture.name)
    renderers = [_renderer_for(fixture.row_spec[column]) for column in fixture.header]
    encoder = codecs.getincrementalencoder(fixture.encoding)()
    delimiter = fixture.delimiter
    terminator = fixture.line_terminator

    # R3: binary mode, always. A text-mode open would consult
    # locale.getpreferredencoding(False), which differs between this machine and
    # a CI runner.
    with path.open("wb") as handle:
        if fixture.bom:
            handle.write(_bom_for(fixture.encoding))

        parts: list[str] = [delimiter.join(fixture.header), terminator]
        for row_index in range(fixture.rows):
            for position, render in enumerate(renderers):
                if position:
                    parts.append(delimiter)
                parts.append(render(rng, row_index))
            parts.append(terminator)  # R4: explicit, never a writer's default
            if len(parts) >= _FLUSH_PARTS:
                handle.write(encoder.encode("".join(parts)))
                parts.clear()
        if parts:
            handle.write(encoder.encode("".join(parts)))
        handle.write(encoder.encode("", final=True))


def _write_literal(fixture: Fixture, path: Path) -> None:
    """Write bytes declared in the manifest as escaped literals."""
    if fixture.content is None:  # pragma: no cover - guarded at load time
        msg = f"{fixture.name}: literal fixture has no content"
        raise GeneratorError(msg)
    with path.open("wb") as handle:
        if fixture.bom:
            handle.write(_bom_for(fixture.encoding))
        handle.write(fixture.content.encode(fixture.encoding))


def _write_literal_unicode(fixture: Fixture, path: Path) -> None:
    """Write rows whose fields carry an explicit normalisation form (R9)."""
    header = tuple(fixture.rows_spec[0])
    lines = [fixture.delimiter.join(header)]
    lines.extend(
        fixture.delimiter.join(_render_unicode(row[column]) for column in header)
        for row in fixture.rows_spec
    )

    text = fixture.line_terminator.join(lines) + fixture.line_terminator
    with path.open("wb") as handle:
        if fixture.bom:
            handle.write(_bom_for(fixture.encoding))
        handle.write(text.encode(fixture.encoding))


def _render_unicode(value: str | UnicodeField) -> str:
    """Apply the declared normalisation form, explicitly (R9)."""
    if isinstance(value, UnicodeField):
        return unicodedata.normalize(value.form, value.text)
    return value


def _write_wrapper(fixture: Fixture, out_dir: Path, path: Path) -> None:
    """Compress an already-materialised fixture with deterministic headers."""
    if fixture.wraps is None:  # pragma: no cover - guarded at load time
        msg = f"{fixture.name}: wrapper fixture has no target"
        raise GeneratorError(msg)

    target = out_dir / fixture.wraps
    if not target.is_file():
        msg = (
            f"{fixture.name}: wraps {fixture.wraps!r}, which has not been "
            f"generated. A wrapper whose target was skipped cannot be built."
        )
        raise GeneratorError(msg)

    if fixture.compression != "gzip":  # pragma: no cover - guarded at load time
        msg = f"{fixture.name}: unsupported compression {fixture.compression!r}"
        raise GeneratorError(msg)

    # R5: mtime=0 and filename="" are non-negotiable. gzip embeds the current
    # wall-clock time and the source file name in its header, so without both
    # of these two runs a second apart produce different bytes.
    with (
        path.open("wb") as raw,
        target.open("rb") as source,
        gzip.GzipFile(
            fileobj=raw,
            mode="wb",
            compresslevel=_GZIP_LEVEL,
            mtime=fixture.gzip_mtime,
            filename=fixture.gzip_filename,
        ) as compressed,
    ):
        while chunk := source.read(_READ_CHUNK):
            compressed.write(chunk)


def _bom_for(encoding: str) -> bytes:
    """Return the byte-order mark for an encoding that has one."""
    name = codecs.lookup(encoding).name
    bom = _BOMS.get(name)
    if bom is None:
        msg = f"encoding {encoding!r} has no byte-order mark; remove `bom: true`"
        raise GeneratorError(msg)
    return bom


def _renderer_for(spec: ColumnSpec) -> Renderer:
    """Build the per-row renderer for one column spec."""
    if isinstance(spec, ZeroPaddedIntColumn):
        return _zero_padded_renderer(spec)
    if isinstance(spec, PickColumn):
        return _pick_renderer(spec)
    return _decimal_renderer(spec)


def _zero_padded_renderer(spec: ZeroPaddedIntColumn) -> Renderer:
    """Render a monotonic integer with leading zeros — consumes no randomness."""
    width = spec.width
    start = spec.start

    def _render(rng: random.Random, row_index: int) -> str:
        del rng
        return f"{start + row_index:0{width}d}"

    return _render


def _pick_renderer(spec: PickColumn) -> Renderer:
    """Select from a fixed list by index arithmetic over ``random()`` (R2)."""
    values = spec.values
    count = len(values)

    def _render(rng: random.Random, row_index: int) -> str:
        del row_index
        return values[min(int(rng.random() * count), count - 1)]

    return _render


def _decimal_renderer(spec: DecimalColumn) -> Renderer:
    """Render an exact decimal via integer arithmetic — never a float (R10).

    The bounds are converted once into integer units of ``10**-scale``, so the
    hot path does integer arithmetic and one explicit format string. ``Decimal``
    is used for the conversion (where exactness matters) and nowhere per row
    (where it would cost 9 million object constructions).
    """
    scale = spec.scale
    separator = spec.decimal_separator
    power = 10**scale
    low = int(spec.minimum.scaleb(scale).to_integral_value(rounding=ROUND_CEILING))
    high = int(spec.maximum.scaleb(scale).to_integral_value(rounding=ROUND_FLOOR))
    span = high - low + 1

    if scale == 0:

        def _render_integral(rng: random.Random, row_index: int) -> str:
            del row_index
            return str(low + min(int(rng.random() * span), span - 1))

        return _render_integral

    def _render(rng: random.Random, row_index: int) -> str:
        del row_index
        units = low + min(int(rng.random() * span), span - 1)
        return f"{units // power}{separator}{units % power:0{scale}d}"

    return _render
