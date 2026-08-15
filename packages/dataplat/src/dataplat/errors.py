"""dataplat's QUAL-03 exception hierarchy.

``ConfigurationError``, ``StorageError``, ``SecretResolutionError``,
``SourceError`` (+ ``FileInspectionError``, ``FilenameParsingError``,
``EncodingDetectionError``, ``CsvDialectDetectionError``, ``CsvParsingError``)
and ``SchemaError`` (+ ``SchemaValidationError``, ``IncompatibleSchemaError``)
exist here today. ``QualityThresholdExceeded`` and ``PublicationError`` are
still deliberately absent: each is added by the phase that first raises it
(CONTEXT.md D-06). A subclass with no raise site is dead code wearing a
design decision's clothes — the eight ``SourceError``/``SchemaError``
subclasses added this phase (06) are pre-declared contracts for Wave 2's
detector/schema plans, each of which adds its own raise site later in this
same phase, so by the end of the phase every one is actually raised
somewhere.

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


class SourceError(DataPlatformError):
    """Base class for file-inspection/parsing failures (ARCHITECTURE.md §4.5).

    Raised when a source file cannot be inspected, its filename cannot be
    parsed, its encoding or dialect cannot be determined, or it cannot be
    streamed as CSV. Each subclass below covers one stage of that pipeline;
    catch ``SourceError`` to handle any of them uniformly.
    """


class FileInspectionError(SourceError):
    """A source file cannot be inspected at all.

    The umbrella ``SourceError`` subclass for a failure not covered by the
    four more specific ones below — e.g. a corrupted or undecompressable
    archive (CSV-11), or the file cannot be read/opened in the first place.
    """


class FilenameParsingError(SourceError):
    """A source file's name does not match its dataset's configured mask.

    Raised by Wave 2's filename-mask detector plan (CSV-01) when a file
    fails to match its dataset's ``FilenameMaskConfig.mask`` at all (D-09) —
    never processed with the unmatched facets left null.
    """


class EncodingDetectionError(SourceError):
    """A source file's text encoding cannot be determined or is unsupported.

    Raised by Wave 2's encoding-detection plan (CSV-02/03) when neither a
    BOM sniff nor the ``charset-normalizer``/``chardet`` fallback can
    determine a usable encoding for the file.
    """


class CsvDialectDetectionError(SourceError):
    """A source file's CSV dialect (delimiter/quote/escape) cannot be determined.

    Raised by Wave 2's dialect-detection plan (CSV-04/05/06) when
    ``clevercsv``'s detector cannot resolve a usable dialect for the file.
    """


class CsvParsingError(SourceError):
    """A source file's CSV stream is malformed in a way parsing cannot recover from.

    Stream-fatal — e.g. an unterminated quote at EOF. Raised by Wave 2's
    dialect/parsing plans (CSV-04/05/06) when the stream itself, not just a
    single row, cannot be parsed.
    """


class SchemaError(DataPlatformError):
    """Base class for schema-contract failures (ARCHITECTURE.md §4.5).

    Raised when a file's structure fails to validate against its dataset's
    ``columns:`` contract, or when a schema change is classified breaking.
    """


class SchemaValidationError(SchemaError):
    """A file's structure fails to validate against its dataset's column contract.

    Raised by Wave 2's schema-versioning plans (SCHEMA-01/02) when a file's
    detected structure cannot be reconciled with its dataset's ``columns:``
    contract at all (e.g. a multi-row/hierarchical header, detected and
    rejected rather than flattened).
    """


class IncompatibleSchemaError(SchemaError):
    """A file's schema change is a BREAKING change under a strict policy.

    D-02: a business-key rename, column disappearance, or data-type retype
    makes the whole file fail — nothing loads — raised before any row is
    staged. This is "§13 BREAKING change under a strict policy"
    (ARCHITECTURE.md §4.5), which D-02 makes this platform's only policy.
    """
