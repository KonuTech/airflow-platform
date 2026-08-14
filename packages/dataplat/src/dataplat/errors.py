"""dataplat's QUAL-03 exception hierarchy.

Only ``ConfigurationError``, ``StorageError`` and ``SecretResolutionError``
exist here today. ``SourceError``, ``SchemaError``, ``QualityThresholdExceeded``
and ``PublicationError`` are deliberately absent: each is added by the phase
that first raises it (CONTEXT.md D-06). A subclass with no raise site is dead
code wearing a design decision's clothes.

Every exception carries a ``context: dict[str, object]`` populated by the
raising code, so a later ``cli.py`` catch-once handler (plan 03-07) can log
structured detail instead of a bare message. Row-level problems never raise
any of these — a malformed row becomes a ``RejectedRecord`` inside
``StageResult.rejected`` instead (``dataplat.models.record``).

``context``'s keys are reserved against ``error_type``/``error_message``
(WR-03): ``cli.py``'s catch-once handler logs every ``DataPlatformError`` as
``log.error(..., error_type=..., error_message=..., **exc.context)``, and
``context`` is spread as *top-level* keyword arguments there rather than
nested under one key, specifically so ``dataplat.observability.logging``'s
OBS-05 redaction processor (which only scans an event's top-level keys) can
still redact a secret-pattern key a raise site puts in ``context`` (e.g.
``context={"dsn": ...}``). That design means a ``context`` key colliding
with one of the handler's own fixed keyword names would raise
``TypeError: ... got multiple values for keyword argument`` from *that*
call instead — the one place a raw, unhandled exception must never
originate. Rejecting the collision here, at construction time, fails loudly
at the raise site instead.
"""

from __future__ import annotations

# Keys `cli.py`'s catch-once handler passes as fixed keyword arguments
# alongside a spread `**context` (WR-03) — reserved so a raise site can
# never collide with them and crash the handler whose entire purpose is to
# never crash.
_RESERVED_CONTEXT_KEYS = frozenset({"error_type", "error_message"})


class DataPlatformError(Exception):
    """Base class for every run-fatal condition dataplat raises.

    Row-level problems never raise this or any subclass — see the module
    docstring. This hierarchy is reserved for conditions that abort the run.

    Attributes:
        context: Structured detail about the failure, empty when the raising
            code supplied none.
    """

    def __init__(self, message: str, context: dict[str, object] | None = None) -> None:
        """Initialize the error with a message and optional structured context.

        Args:
            message: Human-readable description of the failure.
            context: Structured detail about the failure. Defaults to an
                empty dict when omitted.

        Raises:
            ValueError: ``context`` uses a reserved key (``error_type`` or
                ``error_message`` — WR-03; see module docstring).
        """
        super().__init__(message)
        context = context if context is not None else {}
        reserved_keys_used = sorted(_RESERVED_CONTEXT_KEYS & context.keys())
        if reserved_keys_used:
            msg = (
                f"{type(self).__name__} context uses reserved key(s) "
                f"{reserved_keys_used}; {sorted(_RESERVED_CONTEXT_KEYS)} are "
                "reserved for cli.py's catch-once handler (WR-03)"
            )
            raise ValueError(msg)
        self.context: dict[str, object] = context


class ConfigurationError(DataPlatformError):
    """Bad or missing configuration, or an unknown registry key.

    Raised when a dataset config fails validation, a config file cannot be
    found or parsed, or a config names a source/deduplication/publisher
    strategy key that has no registry entry.
    """


class StorageError(DataPlatformError):
    """A storage backend is unreachable, refuses access, or fails a guard.

    Raised when MinIO/S3 or PostgreSQL cannot be reached, a permission is
    denied, or a wrong-database guard trip is detected (e.g. an analytical-DB
    operation aimed at the Airflow metadata database).
    """


class SecretResolutionError(DataPlatformError):
    """An opaque secret reference could not be resolved to a value.

    Raised when a ``SecretRef`` (``env://``, ``file://`` or ``vault://``, or
    an unrecognized/malformed scheme) cannot be turned into a usable secret
    value.
    """
