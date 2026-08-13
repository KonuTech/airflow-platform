"""Unit tests for ``dataplat.errors`` — WR-03's reserved-context-key guard.

``cli.py``'s catch-once handler logs every ``DataPlatformError`` as
``log.error(..., error_type=..., error_message=..., **exc.context)``. Before
WR-03's fix, a ``context`` dict using either of those two keys would raise
``TypeError: ... got multiple values for keyword argument`` from *inside*
that handler — the one place a raw, unhandled exception must never
originate. ``DataPlatformError.__init__`` now rejects the collision at
construction time (the raise site), where a contributor gets immediate,
actionable feedback instead of a confusing crash three call frames away.
"""

from __future__ import annotations

import pytest

from dataplat.errors import ConfigurationError, DataPlatformError, StorageError


def test_reserved_key_error_type_is_rejected_at_construction() -> None:
    with pytest.raises(ValueError, match="error_type"):
        ConfigurationError("bad config", context={"error_type": "should not be allowed"})


def test_reserved_key_error_message_is_rejected_at_construction() -> None:
    with pytest.raises(ValueError, match="error_message"):
        StorageError("storage failed", context={"error_message": "should not be allowed"})


def test_reserved_key_check_applies_to_every_subclass_not_just_the_base() -> None:
    """The guard lives in ``DataPlatformError.__init__``, inherited by every subclass."""
    for cls in (DataPlatformError, ConfigurationError, StorageError):
        with pytest.raises(ValueError, match="error_type"):
            cls("failure", context={"error_type": "bad"})


def test_non_reserved_context_keys_still_work_normally() -> None:
    exc = ConfigurationError("bad config", context={"detail": "unit test", "path": "/x/y.yaml"})

    assert exc.context == {"detail": "unit test", "path": "/x/y.yaml"}


def test_omitted_context_defaults_to_an_empty_dict() -> None:
    exc = ConfigurationError("bad config")

    assert exc.context == {}


def test_cli_py_error_boundary_call_shape_never_collides_for_a_valid_context() -> None:
    """Reproduces cli.py's exact ``log.error(..., **exc.context)`` call shape.

    A ``context`` dict that already passed ``DataPlatformError.__init__``'s
    guard must never collide with the handler's fixed ``error_type``/
    ``error_message`` keyword arguments -- this is the end-to-end proof that
    WR-03's regression (``TypeError: got multiple values for keyword
    argument``) cannot occur once construction itself has rejected the
    reserved keys.
    """

    def _fake_log_error(_event: str, **kwargs: object) -> dict[str, object]:
        return kwargs

    exc = ConfigurationError("bad config", context={"detail": "safe"})

    result = _fake_log_error(
        "dataplat command failed",
        error_type=type(exc).__name__,
        error_message=str(exc),
        **exc.context,
    )

    assert result == {
        "error_type": "ConfigurationError",
        "error_message": "bad config",
        "detail": "safe",
    }
