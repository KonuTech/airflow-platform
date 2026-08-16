"""``dataplat``'s CLI entry point: ``--version`` and the QUAL-03/D-06 catch-once error boundary.

``main()`` is the console-script target declared by
``packages/dataplat/pyproject.toml``'s ``[project.scripts]`` (Cluster O) and
the pod ``ENTRYPOINT`` this plan's Dockerfile builds. It configures structured
logging exactly once, extracts an incoming W3C ``TRACEPARENT`` env var into
the active OTel context and configures both observability backends
(``tracing``/``metrics``, OBS-10/OBS-08) exactly once -- all near the top,
before dispatching to the ``cli`` click group -- then dispatches inside a
``try/except`` block scoped to two well-defined exception families. A
``DataPlatformError`` raised by any subcommand's callback is logged with its
structured ``context`` and turned into exit code ``1`` -- never a raw Python
traceback. Click's own control-flow/usage-error family
(``click.exceptions.ClickException``, ``.Exit``, ``.Abort`` -- raised by
``no_args_is_help``, an unknown option, an unknown subcommand, ``--version``,
etc. with ``standalone_mode=False``) is converted the same way
``standalone_mode=True`` would have: the message is shown and the matching
exit code returned (CR-01 -- a usage error is the single most expected form
of user/operator error a CLI has to handle, not a bug). Any OTHER exception
is deliberately NOT caught here (ARCHITECTURE.md Sec 4.5 bans a blanket
catch-all clause anywhere outside this scoped boundary) and propagates to the
process, because it is a bug to surface loudly, not a condition this
boundary should paper over. A ``finally`` block flushes both observability
backends unconditionally before returning (plan 07-07, discovered live: this
CLI's own invocations are short-lived enough that neither provider's
internal export timer ever fires on its own -- see ``tracing.flush()``/
``metrics.flush()``'s own docstrings) -- this runs on every exit path,
including an uncaught exception propagating past this function entirely.

This phase's only subcommand is the ``--version`` flag on the group itself;
``ingest`` (Phase 4) and later subcommands attach to the same ``cli`` group
and inherit this boundary and the one-time logging configuration for free.
"""

from __future__ import annotations

import os
import sys
from importlib.metadata import entry_points

import click
import structlog
from opentelemetry import context as otel_context
from opentelemetry import propagate

from dataplat.errors import DataPlatformError
from dataplat.observability import metrics, tracing
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


def _extract_incoming_trace_context() -> None:
    """Extract an incoming W3C ``TRACEPARENT`` env var into the active OTel context.

    OBS-10's pod-side half: the Airflow KPO pod-spec side (a separate, earlier
    plan) injects a ``TRACEPARENT`` env var carrying the Airflow task span's
    context; this is where ``dataplat`` picks it up, so ``run_ingest``'s own
    ``pipeline.run_ingest`` span (``pipeline/run.py``) becomes a genuine CHILD
    of that Airflow task span rather than an unrelated root span.

    A no-op when ``TRACEPARENT`` is unset -- the active context is left
    exactly as ``opentelemetry`` initialized it, with no parent attached.
    ``opentelemetry.propagate.extract()`` is the reference W3C traceparent
    parser (T-07-14): a malformed value degrades to "no parent context"
    rather than raising, so a bad env var -- set by an operator, or by a
    misconfigured pod-spec injection -- can never crash the CLI (verified
    directly against the installed ``opentelemetry`` propagator, not assumed).

    The extracted context is attached via ``opentelemetry.context.attach()``
    and deliberately never detached: this runs once, near process start, and
    the extracted parent must stay active for this process's entire
    remaining lifetime so every span created afterwards -- starting with
    ``run_ingest``'s own -- nests under it.
    """
    traceparent = os.environ.get("TRACEPARENT")
    if not traceparent:
        return
    ctx = propagate.extract({"traceparent": traceparent})
    otel_context.attach(ctx)


@click.group(no_args_is_help=True)
@click.version_option(version=resolve_version(), prog_name="dataplat")
def cli() -> None:
    """Dataplat -- the source-agnostic ETL platform core's command line."""


def main(argv: list[str] | None = None) -> int:
    """Run the ``dataplat`` CLI: the ``[project.scripts]`` entry point.

    Configures structured logging once, near the top, before dispatching to
    the ``cli`` click group, so every present and future subcommand inherits
    it without reconfiguring. Also extracts an incoming W3C ``TRACEPARENT``
    env var into the active OTel context and configures both the ``tracing``
    and ``metrics`` observability backends exactly once, at this same point
    -- before any subcommand (including the plugin-loaded ``ingest``) can
    create a span or increment a counter (OBS-08/OBS-10). Flushes both
    backends unconditionally, in a ``finally`` block, before returning --
    this short-lived batch process would otherwise exit before either
    provider's own internal export timer ever fires, silently discarding
    every span/metric recorded during the run (discovered live, plan 07-07).

    A ``DataPlatformError`` raised by any subcommand is caught exactly once
    here (D-06), logged with structured context, and turned into exit code
    ``1``. Click's own usage/control-flow
    exceptions (``ClickException`` and subclasses -- e.g. a bare invocation,
    an unknown option, an unknown subcommand -- plus ``Exit`` and ``Abort``)
    are converted to the matching exit code instead of propagating, since
    ``standalone_mode=False`` disables click's own handling of them (CR-01).
    Any other exception is not caught by this boundary and propagates to the
    caller.

    Before dispatching, this function also loads every installed
    ``dataplat.plugins`` entry point (e.g. ``csv_processor.cli``). This is
    the ONLY place ``dataplat`` ever causes ``csv_processor`` code to load.
    ADR-0002's Decision Outcome states plainly that the CSV plugin
    "registers via an entry point"; ``setup.cfg``'s import-linter contract 1
    (``dataplat core must not depend on the CSV plugin``) is a HARD gate
    that fails the build on a static ``import csv_processor`` anywhere under
    ``dataplat`` -- including a lazy, conditional, or function-body import,
    since import-linter's analysis is a static AST scan, not a runtime
    trace. ``importlib.metadata.entry_points()`` sidesteps that entirely: it
    resolves an installed distribution's advertised entry point by NAME, at
    runtime, from package metadata -- no ``import csv_processor`` token
    exists anywhere in this module's (or any ``dataplat`` module's) source,
    so import-linter has nothing to flag, while ``ep.load()`` still imports
    and executes ``csv_processor.cli``'s module body, running its
    ``@cli.command()`` decorators against the very same ``cli`` group object
    this function dispatches to below. A broken installed plugin raising on
    load is a genuine startup failure and is not swallowed here.

    Args:
        argv: Argument vector. Defaults to ``None``, in which case click
            reads ``sys.argv[1:]`` itself.

    Returns:
        Process exit status: ``0`` on success; ``1`` when a
        ``DataPlatformError`` was raised and caught, or on ``Abort``;
        otherwise the exit code carried by the click ``Exit``/
        ``ClickException`` that was caught (e.g. ``2`` for a usage error).
    """
    if not structlog.is_configured():
        configure(in_cluster=_log_json_enabled())
    log = get_logger()

    otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    tracing.configure(otlp_endpoint=otlp_endpoint)
    metrics.configure(otlp_endpoint=otlp_endpoint)
    _extract_incoming_trace_context()

    for entry_point in entry_points(group="dataplat.plugins"):
        entry_point.load()

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
    except click.exceptions.Exit as exc:
        # Raised by click's own eager options (e.g. --version) and by any
        # future explicit ctx.exit() call. standalone_mode=True would
        # translate this into sys.exit(exc.exit_code); replicate that exit
        # code here instead of letting a bare RuntimeError subclass escape
        # (CR-01).
        return exc.exit_code
    except click.exceptions.ClickException as exc:
        # Click's own usage-error family (NoArgsIsHelpError, NoSuchOption,
        # NoSuchCommand, missing/invalid arguments, ...). With
        # standalone_mode=False, click does NOT print or exit for these --
        # it re-raises them to the caller instead. Show the same message
        # standalone mode would have printed and exit with the same code,
        # so a usage error -- the single most expected form of user/operator
        # error a CLI has to handle -- never surfaces as a raw traceback
        # (CR-01).
        exc.show()
        return exc.exit_code
    except click.exceptions.Abort:
        # EOF/Ctrl-C during a prompt. standalone_mode=True would print
        # "Aborted!" and exit 1; mirror the exit code here (CR-01).
        return 1
    finally:
        # OBS-08/OBS-10, discovered live (plan 07-07): `configure()` above
        # wires a real `PeriodicExportingMetricReader`/`BatchSpanProcessor`
        # -- both buffer in-process and only export on their own internal
        # timer (metrics default: 60s) or an explicit flush. This CLI is a
        # short-lived batch invocation (a real `ingest` run against a small
        # file completes in ~3s, verified live against a running pod) that
        # exits long before either internal timer would ever fire on its
        # own, so every span/metric recorded during THIS process's entire
        # lifetime was being silently discarded on every single invocation
        # until this `finally` block existed -- confirmed live: the OTel
        # Collector's own `/metrics` endpoint showed zero `dataplat`-owned
        # series after multiple real, successfully-completed ingestion runs
        # with `tracing.configure()`/`metrics.configure()` correctly wired
        # to a reachable collector. Runs on EVERY exit path (the success
        # fallthrough, every `except` branch above, AND an uncaught
        # exception propagating past this function entirely) -- `finally`
        # semantics guarantee that; a call placed only after the `try`
        # block would miss the uncaught-exception path (ARCHITECTURE.md
        # Sec 4.5's own "propagate loudly, don't paper over" case).
        # No-op-safe in every case: both `flush()` functions are genuine
        # no-ops when `configure()` was never given a real endpoint.
        tracing.flush()
        metrics.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
