"""``UnicodeNormalizer`` -- CSV-12's unconditional NFC pass, D-15's fixed platform rule.

A ``StreamingStage`` mirroring ``dataplat.pipeline.engine.RaggedRowGuard``'s
shape (a ``name`` class attribute, an ``apply(ctx, chunk) -> StageResult``
that never raises for a row-level problem), but with no configuration
parameters at all -- that absence is the entire point of this stage.

**D-15, verbatim:** Unicode NFC normalization (CSV-12) is a fixed,
non-configurable platform rule -- every string value is NFC-normalized
before hashing/comparison, for every dataset, unconditionally. No
per-dataset NFC/NFD/none choice.

**The hard ordering edge this stage exists to close:** normalization MUST
precede hashing. Both deduplication (README's dedup framework) and SCD
change detection hash normalized content -- if two byte-distinct but
visually-identical strings (an NFC-composed name and an NFD-decomposed
rendering of the exact same name) reached a hash computation unnormalized,
they would hash to different keys and be treated as two different records,
silently doubling data that is actually one fact observed twice. Corpus
fixture ``44_unicode_nfc_vs_nfd.csv`` is the direct proof: the NFC form
``"Wiśniewski"`` and the NFD form ``"Wiśniewski"`` are visually identical and
byte-distinct, and must become byte-identical strings after this stage runs
-- wiring this stage to run before any hash computation in the real
pipeline is plan 06-16's responsibility; this plan proves the transform
itself is correct in isolation.

**NFC is not invisible-character stripping.** Corpus fixture
``42_zero_width_and_bidi.csv`` values containing U+200B (zero-width space)
and U+200E (left-to-right mark) are NOT removed by NFC normalization alone
-- NFC only canonicalizes composed/decomposed forms of a character, it does
not delete code points with no canonical decomposition. This stage
deliberately builds exactly D-15's locked rule (NFC only), nothing broader;
invisible-character stripping, if ever needed, is a distinct, separately
contract-declared concern outside this stage's scope.

**Never rejects.** ``StageResult.rejected`` is always empty here, since D-15
states NFC is a pure, always-succeeding transform -- the one stage in this
phase whose ``apply()`` cannot produce a ``RejectedRecord``.

**The ``None``-passthrough guard matters more here than anywhere else in
this phase.** Unlike the per-column normalizers in
``dataplat.normalize.boolean_null``, this stage runs unconditionally, LAST,
over EVERY column of EVERY row (plan 06-16's wiring) -- including nullable
columns it did not itself null out, and including a boolean column
``dataplat.normalize.boolean_null.BooleanNormalizer`` has already converted
to a real Python ``bool``. ``unicodedata.normalize()`` raises ``TypeError``
on any non-``str`` argument, so the guard below checks ``isinstance(field,
str)`` -- not merely ``field is not None`` -- specifically so a ``bool``
field (which is just as much a non-``str`` as ``None`` is) is equally never
passed to it. A crash here would abort an entire chunk's processing rather
than failing one row, which is exactly the kind of denial-of-service this
stage's own threat model calls out.
"""

from __future__ import annotations

import unicodedata
from typing import TYPE_CHECKING

from dataplat.models.record import StageResult
from dataplat.pipeline.protocol import StreamingStage

if TYPE_CHECKING:
    from dataplat.models.record import RecordChunk
    from dataplat.pipeline.protocol import PipelineContext


class UnicodeNormalizer(StreamingStage):
    """NFC-normalizes every ``str`` field of every row, unconditionally, never rejecting.

    No constructor parameters: there is no per-dataset knob for this stage
    -- its entire point is having none (D-15).
    """

    name = "unicode_normalizer"

    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult:  # noqa: ARG002
        """NFC-normalize every ``str`` field in every row; pass any non-``str`` field through.

        Args:
            ctx: The current pipeline context. Unused: this stage's decision
                depends only on ``chunk``, and the parameter exists to
                satisfy ``StreamingStage``.
            chunk: The chunk to normalize.

        Returns:
            A ``StageResult`` whose ``chunk`` holds every input row with
            each ``str`` field replaced by its NFC-normalized form. A
            ``None`` field (absent, from an upstream ``NullTokenNormalizer``)
            or a ``bool`` field (a normalized boolean, from
            ``dataplat.normalize.boolean_null.BooleanNormalizer``) is left
            exactly as it was -- never passed to ``unicodedata.normalize()``,
            which raises ``TypeError`` on a non-``str`` argument. ``rejected``
            and ``findings`` are always empty: this stage never rejects a
            row.
        """
        new_rows: list[tuple[str | bool | None, ...]] = [
            tuple(
                unicodedata.normalize("NFC", field) if isinstance(field, str) else field
                for field in row
            )
            for row in chunk.rows
        ]
        return StageResult(chunk=chunk.replace(rows=tuple(new_rows)), rejected=[], findings=[])
