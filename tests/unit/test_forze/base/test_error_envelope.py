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


# ....................... #


def test_context_is_coerced_to_json_renderable_values() -> None:
    # ``details`` is typed as JSON but not enforced — handlers idiomatically pass
    # UUIDs, datetimes, Decimals — and every transport renders the context with a
    # plain ``json.dumps`` (an HTTP response, a Socket.IO ack, a WS frame, where a
    # TypeError unwinds the whole connection). The envelope must expose only what
    # every renderer can actually serialize.
    import json
    from datetime import UTC, datetime
    from decimal import Decimal
    from uuid import UUID

    order_id = UUID("11111111-1111-1111-1111-111111111111")
    error = exc.validation(
        "Order is not payable",
        details={
            "order_id": order_id,
            "created": datetime(2026, 1, 1, tzinfo=UTC),
            "total": Decimal("19.90"),
            "count": 3,
        },
    )

    envelope = error_envelope(error)

    assert envelope.context is not None
    json.dumps(envelope.context)  # the whole point: renderable everywhere
    assert envelope.context["order_id"] == str(order_id)
    assert envelope.context["count"] == 3  # already-JSON values pass through untouched


def test_unrenderable_context_is_dropped_not_raised() -> None:
    # ``default=`` never applies to dict KEYS, so a non-str key defeats the coercion
    # (cycles don't get this far: sanitize depth-caps them). The error must still
    # render, minus its context — never raise out of a transport's serializer.
    import json
    from uuid import uuid4

    envelope = error_envelope(exc.validation("bad", details={"ids": {uuid4(): 1}}))

    assert envelope.context == {"context_unrenderable": True}
    json.dumps(envelope.context)
