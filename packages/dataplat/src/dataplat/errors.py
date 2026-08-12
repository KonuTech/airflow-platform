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
"""

from __future__ import annotations


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
        """
        super().__init__(message)
        self.context: dict[str, object] = context if context is not None else {}


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

    Raised when a ``SecretRef`` (``env://``, ``file://``, or an unrecognized
    scheme such as ``vault://`` before Phase 5 implements it) cannot be
    turned into a usable secret value.
    """
