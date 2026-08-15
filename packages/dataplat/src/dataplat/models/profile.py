"""``CsvProfile`` — the plain-data aggregate ``Source.inspect()`` assembles.

Sibling of ``dataplat.models.record``, same ``@dataclass(frozen=True,
slots=True)`` convention. Every field is a primitive or a plain
``tuple``/``Mapping`` of primitives — never one of ``csv_processor.detect``'s
richer per-detector result types (``EncodingDetection``, ``DialectDetection``,
``HeaderDetection``, ``TypeInference``). That constraint is load-bearing, not
stylistic: ``setup.cfg`` import-linter contract 1 forbids ``dataplat`` from
importing anything from ``csv_processor`` at all, so this module can never
import those types even under ``TYPE_CHECKING`` — ``CsvSource.inspect()``
(in ``csv_processor``, which MAY import ``dataplat``) is the only place that
translates each detector's rich result into this dataclass's plain fields.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping


@dataclass(frozen=True, slots=True)
class CsvProfile:
    """Everything ``Source.open()``/chunking need, assembled by ``Source.inspect()`` (Pattern 1).

    Attributes:
        encoding: The detected (or contract-declared) text encoding, a real
            codec name usable directly with ``bytes.decode``.
        encoding_confidence: How confident the encoding detection is, in
            ``[0.0, 1.0]``. Always exactly ``1.0`` for
            ``encoding_source in ("contract", "bom")``.
        encoding_source: Where ``encoding`` came from — ``"contract"``,
            ``"bom"``, ``"detected"``, or ``"undetermined"``.
        delimiter: The detected or contract-supplied field delimiter, or
            ``None`` when dialect detection declined and no contract
            override was ever supplied.
        quotechar: The detected or contract-supplied quote character.
            Always a real character, never empty.
        dialect_declined: ``True`` when dialect detection could not
            determine a usable delimiter at all and no contract override
            was supplied — never a guess. Always paired with
            ``delimiter is None``.
        header_row_index: The 0-based row index the header was found at, or
            ``None`` when no row cleared the detection threshold (or the
            file is empty).
        header: The header row's field values, trimmed per
            ``CsvParsingConfig.header_trim``. Empty when no header was
            found.
        preamble_row_count: The number of rows before the header — a
            metadata preamble, or ``0`` when the header is at row 0 or none
            was found.
        footer_row_count: The number of trailing rows classified as a
            footer, never loaded as records. ``0`` when no footer was
            detected or no header was found.
        max_field_bytes: The maximum size, in bytes, a single field may
            reach before the row is quarantined (LOAD-07). Resolved from
            the dataset's contract, replacing
            ``csv_processor.source.FIELD_SIZE_LIMIT``'s former hardcoded
            role.
        compression: The object key's detected compression —
            ``None`` (uncompressed), ``"gzip"``, or ``"zip"``.
        filename_facets: Every facet extracted from the object's filename
            against the dataset's opt-in filename mask (CSV-01), or an
            empty mapping when the dataset declares no mask (D-10).
    """

    encoding: str
    encoding_confidence: float
    encoding_source: str  # "contract" | "bom" | "detected" | "undetermined"
    delimiter: str | None
    quotechar: str
    dialect_declined: bool
    header_row_index: int | None
    header: tuple[str, ...]
    preamble_row_count: int
    footer_row_count: int
    max_field_bytes: int
    compression: str | None  # None | "gzip" | "zip"
    filename_facets: Mapping[str, object]
