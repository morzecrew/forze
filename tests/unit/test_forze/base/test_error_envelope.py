"""Unit tests for :mod:`forze.base.exceptions.envelope`."""

from forze.base.exceptions import (
    GENERIC_INTERNAL_DETAIL,
    INTERNAL_ERROR_CODE,
    ExceptionKind,
    error_envelope,
    exc,
    is_server_error_kind,
    unhandled_error_envelope,
)

# ----------------------- #


def test_is_server_error_kind() -> None:
    # >= 500 kinds are server-side
    assert is_server_error_kind(ExceptionKind.INTERNAL)
    assert is_server_error_kind(ExceptionKind.INFRASTRUCTURE)
    assert is_server_error_kind(ExceptionKind.CONFIGURATION)
    assert is_server_error_kind(ExceptionKind.TIMEOUT)  # 504

    # client-safe kinds are not
    assert not is_server_error_kind(ExceptionKind.NOT_FOUND)
    assert not is_server_error_kind(ExceptionKind.VALIDATION)
    assert not is_server_error_kind(ExceptionKind.THROTTLED)  # 429


# ....................... #


def test_client_safe_envelope_keeps_summary_and_exposes_context() -> None:
    error = exc.not_found("Note not found", details={"id": "n-1"})

    envelope = error_envelope(error)

    assert envelope.server_error is False
    assert envelope.detail == "Note not found"
    assert envelope.code == error.code
    assert envelope.kind is ExceptionKind.NOT_FOUND
    assert envelope.status == 404
    assert envelope.retryable is False
    assert envelope.context == {"id": "n-1"}


# ....................... #


def test_client_safe_envelope_without_details_has_no_context() -> None:
    envelope = error_envelope(exc.not_found("missing"))

    assert envelope.context is None


# ....................... #


def test_non_exposing_kind_drops_context() -> None:
    # authentication does not expose details even though it is client-facing
    error = exc.authentication("bad token", details={"token": "secret"})

    envelope = error_envelope(error)

    assert envelope.server_error is False
    assert envelope.status == 401
    assert envelope.context is None


# ....................... #


def test_server_error_masks_detail_and_context() -> None:
    error = exc.internal("boom: db at 10.0.0.1", details={"dsn": "secret"})

    envelope = error_envelope(error)

    assert envelope.server_error is True
    assert envelope.status == 500
    assert envelope.detail == GENERIC_INTERNAL_DETAIL
    assert envelope.context is None
    # the code is NOT masked — only the human-facing summary is
    assert envelope.code == error.code


# ....................... #


def test_unhandled_envelope_is_generic() -> None:
    envelope = unhandled_error_envelope()

    assert envelope.server_error is True
    assert envelope.status == 500
    assert envelope.code == INTERNAL_ERROR_CODE
    assert envelope.kind is ExceptionKind.INTERNAL
    assert envelope.detail == GENERIC_INTERNAL_DETAIL
    assert envelope.context is None
