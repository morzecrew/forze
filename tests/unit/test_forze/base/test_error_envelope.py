"""Unit tests for :mod:`forze.base.exceptions.envelope`."""

import asyncio
from collections.abc import Iterator

import pytest
import structlog

from forze.base.exceptions import (
    GENERIC_INTERNAL_DETAIL,
    INTERNAL_ERROR_CODE,
    ChainExceptionMapper,
    CoreException,
    ExceptionInterceptor,
    ExceptionKind,
    error_envelope,
    exc,
    is_server_error_kind,
    unhandled_error_envelope,
)
from forze.base.logging import Logger, log_withheld_details
from tests.support.logging import reset_forze_stdlib_loggers

# ----------------------- #


@pytest.fixture
def _default_logging() -> Iterator[None]:
    # A ``configure_logging(level="info")`` another test left in force drops debug
    # before ``capture_logs`` sees it; start from the defaults and leave them behind.
    reset_forze_stdlib_loggers()
    yield
    reset_forze_stdlib_loggers()


# ....................... #


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
    # an operator-chosen state, not a fault, despite its 503
    assert not is_server_error_kind(ExceptionKind.UNAVAILABLE)


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


def test_an_error_that_withholds_its_details_has_no_context() -> None:
    error = exc.not_found("Note not found", details={"id": "n-1"})
    error.expose_details = False

    envelope = error_envelope(error)

    assert envelope.detail == "Note not found"
    assert envelope.status == 404
    assert envelope.context is None


# ....................... #


def test_a_mapped_backend_error_keeps_its_details_from_the_client() -> None:
    def mapper(error: BaseException, *, site: str, details: object = None) -> CoreException:
        return exc.not_found("Object not found.", details=details)  # type: ignore[arg-type]

    interceptor = ExceptionInterceptor(mapper=mapper)

    @interceptor.coroutine("store.get")
    async def get(bucket: str, key: str) -> None:
        raise LookupError(key)

    with pytest.raises(CoreException) as raised:
        asyncio.run(get("internal-bucket", "k"))

    # Kept for the code that made the call and for the server, never for the client.
    assert raised.value.details == {"bucket": "internal-bucket", "key": "k", "site": "store.get"}
    assert isinstance(raised.value.__cause__, LookupError)
    assert error_envelope(raised.value).context is None


# ....................... #


def test_a_mapper_that_fails_does_not_replace_the_original_error() -> None:
    def mapper(error: BaseException, *, site: str, details: object = None) -> CoreException:
        raise RuntimeError("mapper bug")

    interceptor = ExceptionInterceptor(mapper=mapper)

    @interceptor.function("store.get")
    def get() -> None:
        raise LookupError("original")

    with pytest.raises(CoreException) as raised:
        get()

    assert raised.value.kind is ExceptionKind.INTERNAL
    assert isinstance(raised.value.__cause__, LookupError)
    assert raised.value.expose_details is False


# ....................... #


def test_a_chain_mapper_withholds_what_it_translates() -> None:
    chain = ChainExceptionMapper.chain(
        lambda e, *, site, details: exc.not_found("Object not found.", details=details)
    )

    mapped = chain(LookupError("k"), site="store.get", details={"bucket": "internal"})
    passthrough = exc.not_found("Note not found", details={"id": "n-1"})

    assert mapped is not None and mapped.expose_details is False
    assert chain(passthrough, site="store.get") is passthrough
    assert passthrough.expose_details is True


# ....................... #


@pytest.mark.usefixtures("_default_logging")
def test_withheld_details_are_logged_scrubbed_at_debug() -> None:
    error = exc.not_found(
        "Object not found.", details={"bucket": "internal", "password": "fixture-7chr"}
    )
    error.expose_details = False

    with structlog.testing.capture_logs() as logs:
        log_withheld_details(Logger("test.errors"), error)
        log_withheld_details(Logger("test.errors"), exc.not_found("shown", details={"a": 1}))

    (record,) = logs
    assert record["log_level"] == "debug"
    assert record["error_code"] == error.code
    assert record["details"]["bucket"] == "internal"
    assert record["details"]["password"] != "fixture-7chr"


# ....................... #


def test_non_exposing_kind_drops_context() -> None:
    # authentication does not expose details even though it is client-facing
    error = exc.authentication("bad token", details={"token": "secret"})

    envelope = error_envelope(error)

    assert envelope.server_error is False
    assert envelope.status == 401
    assert envelope.context is None


# ....................... #


def test_unavailable_keeps_summary_and_code_but_withholds_details() -> None:
    error = exc.unavailable(
        "Exports are switched off for maintenance",
        code="exports_off",
        details={"flag": "exports_enabled"},
    )

    envelope = error_envelope(error)

    assert envelope.status == 503
    assert envelope.server_error is False
    assert envelope.retryable is True
    assert envelope.detail == "Exports are switched off for maintenance"
    assert envelope.code == "exports_off"
    assert envelope.kind is ExceptionKind.UNAVAILABLE
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


def test_arbitrary_object_leaves_drop_the_context_instead_of_stringifying() -> None:
    # NOT ``default=str``: stringifying an arbitrary object AFTER sanitization mints
    # fresh text the scrubber never saw — a client object's repr can carry a DSN.
    # Only the closed set of idiomatic value types renders; anything else drops the
    # whole context (the error itself still reaches the client).
    class _Conn:
        def __repr__(self) -> str:
            return "Conn(dsn=postgres://user:fixture-7chr@db.internal)"

    envelope = error_envelope(exc.validation("bad", details={"conn": _Conn()}))

    assert envelope.context == {"context_unrenderable": True}
