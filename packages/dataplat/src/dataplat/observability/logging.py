"""Structured logging: dual JSON/console renderer, contextvars, redaction.

``configure()`` builds one ``structlog`` processor chain that works unmodified
across local development, Docker, Kubernetes and Airflow task-pod contexts
(OBS-02) — the caller only chooses ``in_cluster=True`` (JSON, machine-parseable)
or ``in_cluster=False`` (console, human-readable). Context bound once via
``bind_contextvars`` at pipeline entry (e.g. ``dataset``, ``run_id``) then
appears on every subsequent log event with no logger threading (OBS-04). A
redaction processor (OBS-05) runs immediately before the renderer so a
secret-pattern key never reaches rendered output, regardless of which
renderer is active.

This module re-exports ``structlog.contextvars.bind_contextvars``,
``structlog.contextvars.clear_contextvars`` and ``structlog.get_logger`` as
``bind_contextvars``, ``clear_contextvars`` and ``get_logger`` so every call
site in ``dataplat``/``csv_processor`` imports this one module rather than
reaching into ``structlog`` directly — the same reason ``storage/db.py`` is
the one place a connection pool is constructed: one seam, not one convention
repeated at every call site.
"""

from __future__ import annotations

from typing import cast

import structlog
import structlog.typing

_SECRET_KEY_PATTERN = ("password", "secret", "token", "credential", "dsn", "conninfo")
_TRUNCATE_KEYS = ("raw_line", "record")
_TRUNCATE_AT = 200


def _redact(_logger: object, _name: str, event_dict: dict[str, object]) -> dict[str, object]:
    """Redact secret-pattern values and truncate long raw-text fields (OBS-05).

    Positioned immediately before the renderer in ``configure()``'s processor
    chain — the one choke point every event dict passes through before
    becoming text, regardless of which renderer is active.

    Args:
        _logger: The wrapped logger instance. Unused; required by
            ``structlog``'s processor signature.
        _name: Name of the method called on the logger (e.g. ``"info"``).
            Unused; required by ``structlog``'s processor signature.
        event_dict: The structured event being built for this log call.

    Returns:
        The same ``event_dict``, mutated in place per ``structlog``'s
        processor protocol: secret-pattern keys hold the literal string
        ``"***REDACTED***"``, and any ``raw_line``/``record`` string longer
        than ``_TRUNCATE_AT`` characters is truncated with a length suffix.
    """
    for key in list(event_dict):
        if any(pattern in key.lower() for pattern in _SECRET_KEY_PATTERN):
            event_dict[key] = "***REDACTED***"
        elif key in _TRUNCATE_KEYS:
            value = event_dict[key]
            if isinstance(value, str) and len(value) > _TRUNCATE_AT:
                event_dict[key] = value[:_TRUNCATE_AT] + f"...[{len(value)} chars total]"
    return event_dict


def configure(*, in_cluster: bool, level: str = "INFO") -> None:
    """Configure ``structlog``'s global processor chain for this process.

    The same call site works unmodified in local, Docker, Kubernetes and
    Airflow task-pod contexts (OBS-02): only ``in_cluster`` changes across
    those environments.

    Args:
        in_cluster: When ``True``, render each event as one JSON line via
            ``structlog.processors.JSONRenderer``. When ``False``, render
            with ``structlog.dev.ConsoleRenderer`` for local development.
        level: Minimum level name (e.g. ``"INFO"``, ``"DEBUG"``) below which
            events are dropped before any processor runs. Defaults to
            ``"INFO"``.
    """
    renderer = (
        structlog.processors.JSONRenderer() if in_cluster else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,  # MUST be first
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            # OBS-05 -- one choke point, MUST run before the renderer. Cast
            # only at this call site (mirrors 03-01's RecordChunk.replace()
            # precedent): _redact's public signature stays dict[str, object]
            # exactly as specified, but structlog.typing.Processor declares
            # its event_dict parameter as MutableMapping[str, Any], and
            # Callable parameters are checked contravariantly -- dict[str,
            # object] is not a supertype of MutableMapping[str, Any], so
            # mypy strict rejects the bare reference without this cast.
            cast("structlog.typing.Processor", _redact),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level.upper()),
        logger_factory=structlog.PrintLoggerFactory(),
    )


bind_contextvars = structlog.contextvars.bind_contextvars
clear_contextvars = structlog.contextvars.clear_contextvars
get_logger = structlog.get_logger
