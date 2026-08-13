"""``dataplat``'s CLI entry point: ``--version`` and the QUAL-03/D-06 catch-once error boundary.

``main()`` is the console-script target declared by
``packages/dataplat/pyproject.toml``'s ``[project.scripts]`` (Cluster O) and
the pod ``ENTRYPOINT`` this plan's Dockerfile builds. It configures structured
logging exactly once, then dispatches to the ``cli`` click group inside a
single ``try/except DataPlatformError`` block. A ``DataPlatformError`` raised
by any subcommand's callback is logged with its structured ``context`` and
turned into exit code ``1`` -- never a raw Python traceback. Any OTHER
exception is deliberately NOT caught here (ARCHITECTURE.md Sec 4.5 bans a
blanket catch-all clause anywhere outside this scoped boundary) and
propagates to the process, because it is a bug to surface loudly, not a
condition this boundary should paper over.

This phase's only subcommand is the ``--version`` flag on the group itself;
``ingest`` (Phase 4) and later subcommands attach to the same ``cli`` group
and inherit this boundary and the one-time logging configuration for free.
"""

from __future__ import annotations

import os
import sys

import click
import structlog

from dataplat.errors import DataPlatformError
from dataplat.observability.logging import configure, get_logger
from dataplat.version import resolve_version

_LOG_JSON_TRUTHY = frozenset({"1", "true", "yes", "on"})


def _log_json_enabled() -> bool:
    """Whether ``DATAPLAT_LOG_JSON`` opts into JSON (in-cluster) log rendering.

    Defaults to the human-readable console renderer, so a local invocation
    (e.g. ``uv run dataplat --version``) stays pleasant to read. Kubernetes
    pod specs set ``DATAPLAT_LOG_JSON=true`` to switch to the
    machine-parseable JSON renderer OBS-02 requires in-cluster.

    Returns:
        ``True`` when the environment variable is set to a recognized truthy
        value (``1``, ``true``, ``yes``, ``on``, case-insensitive), ``False``
        otherwise -- including when the variable is unset.
    """
    return os.environ.get("DATAPLAT_LOG_JSON", "").strip().lower() in _LOG_JSON_TRUTHY


@click.group(no_args_is_help=True)
@click.version_option(version=resolve_version(), prog_name="dataplat")
def cli() -> None:
    """Dataplat -- the source-agnostic ETL platform core's command line."""


def main(argv: list[str] | None = None) -> int:
    """Run the ``dataplat`` CLI: the ``[project.scripts]`` entry point.

    Configures structured logging once, near the top, before dispatching to
    the ``cli`` click group, so every present and future subcommand inherits
    it without reconfiguring. A ``DataPlatformError`` raised by any
    subcommand is caught exactly once here (D-06), logged with structured
    context, and turned into exit code ``1``. Any other exception is not
    caught by this boundary and propagates to the caller.

    Args:
        argv: Argument vector. Defaults to ``None``, in which case click
            reads ``sys.argv[1:]`` itself.

    Returns:
        Process exit status: ``0`` on success, ``1`` when a
        ``DataPlatformError`` was raised and caught.
    """
    if not structlog.is_configured():
        configure(in_cluster=_log_json_enabled())
    log = get_logger()

    try:
        cli.main(args=argv, prog_name="dataplat", standalone_mode=False)
    except DataPlatformError as exc:
        # log.error, not log.exception: QUAL-03/D-06 require structured context
        # WITHOUT a raw Python traceback anywhere in the output; .exception()
        # would embed exactly that traceback in the rendered event.
        log.error(  # noqa: TRY400 -- see comment above
            "dataplat command failed",
            error_type=type(exc).__name__,
            error_message=str(exc),
            **exc.context,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
