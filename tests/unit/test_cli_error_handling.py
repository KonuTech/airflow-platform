"""Unit tests for ``dataplat.cli`` -- QUAL-03's CLI-side catch-once boundary (D-06).

Covers: ``--version`` resolves through ``resolve_version()`` (Cluster O), a
``DataPlatformError`` raised inside a subcommand is caught exactly once by
``main()``, logged with structured context, and turned into exit code ``1``
with no raw traceback; and an undeclared, non-``DataPlatformError`` exception
is NOT caught by that boundary and propagates -- proving the catch is scoped
to ``DataPlatformError`` only (ARCHITECTURE.md Sec 4.5 bans a blanket
catch-all clause anywhere outside this scoped boundary).

Two test-only commands are registered onto the real ``cli`` group here,
deliberately NOT inside ``cli.py`` itself, so ``cli.py``'s production surface
stays limited to ``--version`` in this phase. ``main()`` (not
``CliRunner.invoke()``) is what actually exercises the catch-once boundary
below, because that boundary lives in ``main()``'s own ``try/except`` around
the click group dispatch -- a plain function ``CliRunner`` cannot invoke
directly -- so Tests 2 and 3 call it directly and capture real process
stdout/stderr via ``capsys``, exactly as their behaviors specify.
"""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from dataplat.cli import cli, main
from dataplat.errors import ConfigurationError
from dataplat.version import resolve_version


@cli.command("raise-configuration-error")
def _raise_configuration_error() -> None:
    """Test-only command: raises a DataPlatformError subclass."""
    msg = "bad config"
    raise ConfigurationError(msg, context={"detail": "unit test"})


@cli.command("raise-plain-exception")
def _raise_plain_exception() -> None:
    """Test-only command: raises an exception outside the DataPlatformError hierarchy."""
    msg = "not a DataPlatformError"
    raise RuntimeError(msg)


def test_version_flag_prints_the_resolved_version_and_exits_zero() -> None:
    runner = CliRunner()

    result = runner.invoke(cli, ["--version"])

    assert result.exit_code == 0
    assert resolve_version() in result.output


def test_configuration_error_is_caught_once_logged_and_exits_one(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = main(["raise-configuration-error"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "Traceback" not in captured.out
    assert "Traceback" not in captured.err
    assert "bad config" in captured.out


def test_an_undeclared_exception_is_not_caught_and_propagates() -> None:
    """Proves the D-06 boundary is scoped to DataPlatformError, never bare Exception."""
    with pytest.raises(RuntimeError, match="not a DataPlatformError"):
        main(["raise-plain-exception"])


def test_zero_arguments_does_not_crash(capsys: pytest.CaptureFixture[str]) -> None:
    """``no_args_is_help=True`` turns a bare invocation into click's own
    controlled usage/help exit (``NoArgsIsHelpError``, exit code 2 -- the
    standard Unix usage-error convention), never an unhandled Python
    traceback.

    Calls ``main()`` directly (CR-01), not ``CliRunner.invoke()``:
    ``CliRunner`` has its own independent exception-catching wrapper around
    click's dispatch, so it observes a controlled ``exit_code`` regardless of
    what ``main()`` itself does with the underlying
    ``click.exceptions.ClickException`` -- exactly the gap that let CR-01's
    raw-traceback regression go undetected by this test previously.
    """
    exit_code = main([])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "Traceback" not in captured.out
    assert "Traceback" not in captured.err
    assert "Usage" in captured.err


def test_zero_arguments_via_cli_runner_still_exits_two() -> None:
    """The ``cli`` group's own ``no_args_is_help`` behavior, exercised the
    original way (``CliRunner.invoke()``) -- kept alongside the ``main()``
    -level test above since it proves a different thing: ``cli`` itself, not
    ``main()``'s error boundary around it.
    """
    runner = CliRunner()

    result = runner.invoke(cli, [])

    assert result.exit_code == 2
    assert "Traceback" not in result.output
    assert "Usage" in result.output


def test_unknown_option_does_not_crash(capsys: pytest.CaptureFixture[str]) -> None:
    """An unrecognized option raises click's ``NoSuchOption`` (a
    ``ClickException``/``UsageError`` subclass), not a ``DataPlatformError``
    -- ``main()`` must still convert it to a controlled exit, never a raw
    Python traceback (CR-01).
    """
    exit_code = main(["--bogus-option"])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "Traceback" not in captured.out
    assert "Traceback" not in captured.err
    assert "No such option" in captured.err


def test_unknown_command_does_not_crash(capsys: pytest.CaptureFixture[str]) -> None:
    """An unrecognized subcommand raises click's ``NoSuchCommand`` (a
    ``ClickException``/``UsageError`` subclass), not a ``DataPlatformError``
    -- ``main()`` must still convert it to a controlled exit, never a raw
    Python traceback (CR-01).
    """
    exit_code = main(["no-such-command"])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "Traceback" not in captured.out
    assert "Traceback" not in captured.err
    assert "No such command" in captured.err


def test_main_returns_zero_on_a_successful_invocation(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``main()``'s own literal contract: 0 on success, exercised directly
    (not just via ``cli``/``CliRunner``) since Tests 1-4 above never call
    ``main()`` on a non-raising path.
    """
    exit_code = main(["--version"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert resolve_version() in captured.out
