"""``dataplat.retention.policy`` -- D-38's dry-run-by-default retention evaluator.

Mirrors ``validate.circuit_breaker.RejectionRateCircuitBreaker`` and
``scd.delete_detection.MassDeleteCircuitBreaker``'s shared shape --
argument-parameterized already-known totals, never re-queries its own
inputs, a trivial-pass empty-input guard is always the first branch,
findings are structured ``threshold``/``observed``-dict-shaped objects
(``dataplat.models.report.ValidationResult``'s own established convention,
reused here for the same "config value vs. observed value"
machine-readable comparison) -- but diverges in ONE load-bearing way: the
circuit breakers RAISE on breach; this evaluator must REPORT and only
conditionally act, and must NEVER raise merely because something is old
enough to delete. ``evaluate_retention`` therefore does not raise under
ANY input, including malformed/empty input (T-11-16's own mitigation).

Deliberately a plain function, not a ``BarrierStage``: retention is NOT
part of the CSV ingest pipeline (D-35 -- ``platform_retention`` is its own
maintenance DAG, deliberately outside every ingestion DAG's task graph), so
this module has no reason to implement ``pipeline.protocol.BarrierStage``'s
``apply(ctx: PipelineContext)`` signature, and imports nothing from
``dataplat.pipeline`` at all. A free function is also the simplest shape
for a future DAG task to call once per run -- no instance to construct and
discard.

Retention windows are EXCLUSIVE of the boundary: a candidate aged EXACTLY
``N`` days is NOT yet over an ``N``-day window; one day older IS. This
mirrors ``RetentionCandidate.age_days`` being compared with a strict ``>``,
never ``>=``, against each layer's configured window.

This module imports no ``boto3``, no ``psycopg``, and performs no I/O of
any kind -- every ``RetentionCandidate`` arrives already queried by the
caller, exactly as ``RejectionRateCircuitBreaker`` receives its totals
already computed by ``StagingLoader.load()``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dataplat.config.model import RetentionConfig

# Layer name -> the RetentionConfig field naming that layer's window, in
# days. Every layer RetentionConfig can describe gets an entry in every
# RetentionReport this module produces, even when zero candidates were
# supplied for that layer (D-38: "every run reports ... regardless of
# configuration") -- never a sparse report that silently omits a layer.
_LAYER_WINDOW_FIELDS: dict[str, str] = {
    "raw": "raw_days",
    "processed": "processed_days",
    "quarantine": "quarantine_days",
    "validation_reports": "validation_reports_days",
    "ingestion_metadata": "ingestion_metadata_days",
    "logs": "logs_days",
}


@dataclass(frozen=True, slots=True)
class RetentionCandidate:
    """One already-queried artifact a caller is asking this evaluator to judge.

    Attributes:
        layer: Which retention layer this candidate belongs to -- one of
            ``_LAYER_WINDOW_FIELDS``'s six keys (``"raw"``, ``"processed"``,
            ``"quarantine"``, ``"validation_reports"``,
            ``"ingestion_metadata"``, ``"logs"``). A candidate naming an
            unrecognized layer is silently excluded from every
            ``RetentionReport`` this module produces -- never raised on,
            matching this module's own never-raising contract.
        identifier: A caller-meaningful identifier for this artifact (an S3
            key, a table row's primary key, ...) -- opaque to this module,
            never interpreted or validated.
        age_days: How many days old this artifact is, as already computed
            by the caller (this module never reads a clock or a database).
        size_bytes: This artifact's size in bytes, when known -- ``None``
            when the caller has no size figure available. Used only to
            populate a ``RetentionReport`` layer's ``observed`` summary.
    """

    layer: str
    identifier: str
    age_days: int
    size_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class LayerRetentionReport:
    """One retention layer's dry-run/enforce outcome for a single evaluation.

    Attributes:
        layer: The layer this report describes -- one of
            ``_LAYER_WINDOW_FIELDS``'s six keys.
        candidate_count: How many ``RetentionCandidate`` objects this layer
            was given, whether or not any of them are over-window.
        would_delete_count: How many of this layer's candidates are
            strictly older than the configured window (D-38's "what WOULD
            be deleted"). Computed identically regardless of ``enforce`` --
            ``RetentionReport.enforce`` is what tells a caller whether to
            treat this count as "would delete" (informational) or "will
            delete" (actionable); this module treats the two identically
            and performs no I/O either way.
        deleted_count: ALWAYS ``0``. This module performs no I/O and issues
            no deletes under any input, including ``enforce=True`` -- this
            field is the structural, always-checkable proof of that
            invariant, not a count that could ever legitimately be
            nonzero from this module's own return value.
        threshold: The configured value(s) this layer's candidates were
            judged against, dict-shaped exactly like
            ``dataplat.models.report.ValidationResult.threshold`` -- e.g.
            ``{"window_days": 60}``, or ``{"window_days": None}`` for an
            indefinite (D-36) layer.
        observed: The actually-observed value(s) among this layer's
            over-window (would-delete) candidates, dict-shaped exactly
            like ``ValidationResult.observed`` -- ``total_size_bytes``
            (sum of every would-delete candidate's known ``size_bytes``,
            or ``None`` when none of them have a known size),
            ``oldest_age_days``/``newest_age_days`` (the age range among
            would-delete candidates, or ``None`` when there are none).
    """

    layer: str
    candidate_count: int
    would_delete_count: int
    deleted_count: int
    threshold: dict[str, object] = field(default_factory=dict)
    observed: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RetentionReport:
    """The complete, structured outcome of one ``evaluate_retention`` call.

    Attributes:
        enforce: Echoed back from the ``RetentionConfig`` this report was
            evaluated against (D-38) -- ``False`` unless the dataset's own
            config explicitly opts in. This module never reads this flag
            itself (it always computes the same ``would_delete_count`` for
            every layer regardless); it exists purely for the CALLER (a
            future DAG task) to decide whether to act on the report.
        layers: Every layer named in ``_LAYER_WINDOW_FIELDS``, keyed by
            layer name -- always fully populated, even for a layer with
            zero candidates (D-38: "every run reports ... regardless of
            configuration").
    """

    enforce: bool
    layers: dict[str, LayerRetentionReport] = field(default_factory=dict)


def evaluate_retention(
    config: RetentionConfig,
    candidates: Sequence[RetentionCandidate],
) -> RetentionReport:
    """Judge each candidate against its layer's configured retention window.

    Never queries MinIO, PostgreSQL, or any other store -- ``candidates``
    must already be fully populated by the caller (mirrors
    ``RejectionRateCircuitBreaker``'s "totals come from the constructor"
    rule; see the module docstring). Never raises, for any input, including
    an empty ``candidates`` sequence or a candidate naming an unrecognized
    layer.

    A candidate is over-window (counted in ``would_delete_count``) only
    when ``candidate.age_days`` is STRICTLY GREATER THAN the layer's
    configured window -- retention windows are exclusive of the boundary,
    so an item aged EXACTLY the window is NOT yet a candidate; one day
    older IS. A layer configured with a ``None`` window (D-36's raw
    default) never selects any candidate, structurally -- there is no
    numeric comparison capable of selecting against an absent window.

    This function performs no I/O and issues no deletes under any
    circumstance, including ``config.enforce=True`` -- see
    ``LayerRetentionReport.deleted_count``'s own docstring. The caller (a
    future DAG task) is solely responsible for acting on a report whose
    ``enforce`` is ``True``.

    Args:
        config: The dataset's retention configuration -- one window per
            layer, plus the ``enforce`` flag this function echoes back
            unread.
        candidates: Every artifact to judge, already queried by the
            caller. A candidate whose ``layer`` does not match one of
            ``_LAYER_WINDOW_FIELDS``'s six keys is silently excluded from
            every layer's report.

    Returns:
        A ``RetentionReport`` with one ``LayerRetentionReport`` per known
        layer, always fully populated regardless of ``candidates`` or
        ``config``.
    """
    by_layer: dict[str, list[RetentionCandidate]] = {name: [] for name in _LAYER_WINDOW_FIELDS}
    for candidate in candidates:
        bucket = by_layer.get(candidate.layer)
        if bucket is not None:
            bucket.append(candidate)

    layers: dict[str, LayerRetentionReport] = {}
    for layer_name, window_field in _LAYER_WINDOW_FIELDS.items():
        window_days: int | None = getattr(config, window_field)
        layer_candidates = by_layer[layer_name]

        would_delete = (
            [] if window_days is None else [c for c in layer_candidates if c.age_days > window_days]
        )

        sizes = [c.size_bytes for c in would_delete if c.size_bytes is not None]
        ages = [c.age_days for c in would_delete]

        layers[layer_name] = LayerRetentionReport(
            layer=layer_name,
            candidate_count=len(layer_candidates),
            would_delete_count=len(would_delete),
            deleted_count=0,
            threshold={"window_days": window_days},
            observed={
                "total_size_bytes": sum(sizes) if sizes else None,
                "oldest_age_days": max(ages) if ages else None,
                "newest_age_days": min(ages) if ages else None,
            },
        )

    return RetentionReport(enforce=config.enforce, layers=layers)
