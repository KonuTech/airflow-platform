"""``PipelineContext`` — composes every subsystem Waves 1-2 built into one object.

``PipelineContext`` cannot exist before ``DatasetConfig`` (plan 03-04) and
``MetadataRepository``/``ObjectStore`` (plan 03-05) do: every field here names
a type built in an earlier wave, which is why this module is a Wave-3, not
Wave-1, plan — a genuine dependency, not an arbitrary sequencing choice.

``StreamingStage`` and ``BarrierStage`` are the two stage shapes a pipeline
run assembles from. A streaming stage runs once per chunk, so its memory use
is bounded by chunk size (README §39) and its checkpoint can only ever land
between chunks. A barrier stage runs once per run, after every chunk has been
staged, for work that genuinely needs the whole run (cross-batch
deduplication, threshold evaluation, atomic publication). Keeping that split
in the type system — rather than in a comment or a runtime flag — is what
stops checkpointing (README §38) and atomic publication (README §36) from
fighting each other (ARCHITECTURE.md Q4.3).

``log`` is typed ``structlog.typing.FilteringBoundLogger``: confirmed to
resolve against the pinned ``structlog==26.1.0`` this session
(``from structlog.typing import FilteringBoundLogger`` succeeds), so no
fallback to ``structlog.stdlib.BoundLogger`` is needed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from psycopg_pool import ConnectionPool
    from structlog.typing import FilteringBoundLogger

    from dataplat.config.model import DatasetConfig
    from dataplat.metadata.repository import MetadataRepository
    from dataplat.models.identity import RunContext
    from dataplat.models.record import RecordChunk, StageResult
    from dataplat.sources.protocol import Source
    from dataplat.storage.objectstore import ObjectStore


@dataclass(frozen=True)
class PipelineContext:
    """Everything one stage, source or publisher needs, composed into one object.

    Built once per run and passed by reference through every stage — no
    global state, no re-resolving a subsystem mid-run.

    Attributes:
        run: Identity and correlation fields for this run attempt.
        config: The resolved, hash-carrying dataset configuration this run
            executes under.
        metadata: Typed CRUD access to the ``meta`` schema's slice tables.
        objects: Read access to the object store the source files live in.
        db: The connection pool staging and publication code executes
            against.
        log: The structlog logger bound with this run's context.
        source: This run's opened ``Source``, once one has been resolved.
            Defaults to ``None`` and is appended after ``log`` (never
            inserted earlier) specifically so no existing
            ``PipelineContext(...)`` construction breaks.
    """

    run: RunContext
    config: DatasetConfig
    metadata: MetadataRepository
    objects: ObjectStore
    db: ConnectionPool
    log: FilteringBoundLogger
    # TYPE_CHECKING-only mutual reference with `sources/protocol.py` (which
    # already TYPE_CHECKING-imports `PipelineContext` from this module): this
    # is safe -- neither import executes at runtime, both are guarded by
    # `from __future__ import annotations` + `if TYPE_CHECKING:`, so there is
    # no real circular import, only two forward-reference type names each
    # module resolves lazily.
    source: Source | None = None


class StreamingStage(Protocol):
    """A stage that runs once per chunk — bounded memory by construction (README §39).

    Attributes:
        name: The stage's stable, human-readable identifier, e.g.
            ``"ragged_row_guard"``.
    """

    name: str

    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult:
        """Apply this stage to one chunk.

        Must never raise for a row-level problem (QUAL-03): a malformed row
        becomes a ``RejectedRecord`` inside the returned ``StageResult``
        instead of aborting the run.

        Args:
            ctx: The current pipeline context.
            chunk: The chunk to process.

        Returns:
            The chunk that survived this stage, plus anything it rejected or
            found.
        """
        ...


class BarrierStage(Protocol):
    """A stage that runs once per run, after every chunk has been staged.

    Cross-batch deduplication, threshold evaluation, publication and
    reconciliation are barriers — each needs the whole run, not one chunk —
    so a barrier is never checkpointed (ARCHITECTURE.md Q4.3).

    Attributes:
        name: The stage's stable, human-readable identifier.
    """

    name: str

    def apply(self, ctx: PipelineContext) -> StageResult:
        """Apply this stage once, for the whole run.

        Args:
            ctx: The current pipeline context.

        Returns:
            The outcome of this barrier stage.
        """
        ...
