"""Proves the pair: a resolved secret never reaches a captured log line.

SEC-15 (``resolve_secret()``) and OBS-05 (the redaction processor) are only
meaningful together — a resolver that never leaks a value into a log line is
the actual guarantee, not two independently-plausible pieces
(03-VALIDATION.md's SEC-15/OBS-05 row: "A credential value passed through
the resolver never appears in a captured log line").

Every fake secret value below is passed as a structured keyword to a log
call, never interpolated into an f-string message -- the processor chain is
what is under test, not string formatting.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
import structlog

from dataplat.observability import logging as dataplat_logging
from dataplat.secrets.resolver import resolve_secret

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(autouse=True)
def _clear_bound_context() -> Iterator[None]:
    """Reset bound contextvars after every test, matching test_logging_config.py."""
    yield
    structlog.contextvars.clear_contextvars()


def test_redaction_processor_drops_secret_pattern_keys(
    capsys: pytest.CaptureFixture[str],
) -> None:
    dataplat_logging.configure(in_cluster=True)
    log = dataplat_logging.get_logger()

    log.info(
        "connecting",
        password="hunter2-fake-password",  # noqa: S106 - fake value, proves redaction
        db_dsn="postgresql://user:hunter2-fake-password@host/db",
        api_token="fake-token-abc123",  # noqa: S106 - fake value, proves redaction
        credential_ref="fake-credential-xyz",
    )

    captured = capsys.readouterr().out
    for fake_value in ("hunter2-fake-password", "fake-token-abc123", "fake-credential-xyz"):
        assert fake_value not in captured
    assert "***REDACTED***" in captured


def test_redaction_processor_truncates_raw_line_and_record(
    capsys: pytest.CaptureFixture[str],
) -> None:
    dataplat_logging.configure(in_cluster=True)
    log = dataplat_logging.get_logger()
    long_value = "x" * 500

    log.info("parsed a line", raw_line=long_value, record=long_value)

    payload = json.loads(capsys.readouterr().out.strip())
    expected = ("x" * 200) + "...[500 chars total]"
    assert payload["raw_line"] == expected
    assert payload["record"] == expected


def test_resolved_secret_never_appears_in_logs(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The end-to-end pair: SecretsResolver's output, logged, is unrecoverable."""
    monkeypatch.setenv("FAKE_API_KEY", "sk_test_do_not_leak_12345")
    dataplat_logging.configure(in_cluster=True)
    log = dataplat_logging.get_logger()

    resolved_value = resolve_secret("env://FAKE_API_KEY")
    log.info("connected to upstream", password=resolved_value)

    captured = capsys.readouterr().out
    assert "sk_test_do_not_leak_12345" not in captured
    assert "***REDACTED***" in captured


def test_redact_processor_runs_before_renderer() -> None:
    """Structural check: a future reordering must not silently defeat OBS-05.

    Inspects the configured processor chain itself (rather than only
    observing today's output) so a reordering that happens to still redact
    today's specific test values by coincidence would still be caught.
    """
    dataplat_logging.configure(in_cluster=True)
    processors = list(structlog.get_config()["processors"])
    names = [getattr(proc, "__name__", type(proc).__name__) for proc in processors]

    assert "_redact" in names
    redact_index = names.index("_redact")
    renderer_index = next(
        i
        for i, proc in enumerate(processors)
        if isinstance(proc, structlog.processors.JSONRenderer)
    )

    assert redact_index < renderer_index
