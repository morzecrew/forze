from forze.base.primitives import JsonDict
from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import math
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from typing import Final

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from forze.application.execution.resilience.backoff import RETRY_HINT_KINDS, retry_after
from forze.base.exceptions import (
    CoreException,
    ErrorEnvelope,
    error_envelope,
    exc,
    unhandled_error_envelope,
)
from forze.base.logging import Logger, log_server_error, log_withheld_details
from forze.base.scrubbing import sanitize_pydantic_errors

# ----------------------- #

ERROR_CODE_HEADER: Final[str] = "X-Error-Code"
"""Key of the header used for error code."""

error_logger = Logger("fastapi.errors")
"""Logger for FastAPI server-side error diagnostics."""


# ....................... #


def _render_envelope(envelope: ErrorEnvelope) -> JSONResponse:
    """Render *envelope* as ``{detail, code, kind, context?}`` plus the error-code header.

    The shape the Socket.IO ack and the WebSocket error frame carry. Every field comes
    from the envelope, which has already masked server-side errors and sanitized the
    context, so nothing here decides what a client may see.
    """

    content: JsonDict = {
        "detail": envelope.detail,
        "code": envelope.code,
        "kind": envelope.kind.value,
    }

    if envelope.context is not None:
        content["context"] = envelope.context

    return JSONResponse(
        status_code=envelope.status,
        content=content,
        headers={ERROR_CODE_HEADER: envelope.code},
    )


# ....................... #


def _retry_after_header(exc: CoreException) -> str | None:
    """The ``Retry-After`` value (RFC 9110) a throttled or unavailable *exc* asks for, or None.

    ``details["retry_after"]`` — seconds, read as the retry layer reads it — is rounded up
    to whole seconds; otherwise ``details["retry_at"]``, an aware :class:`~datetime.datetime`,
    becomes an HTTP-date. Any other value gives no header: a bad hint never fails the response.
    """

    if exc.kind not in RETRY_HINT_KINDS:
        return None

    seconds = retry_after(exc)

    if seconds is not None and math.isfinite(seconds):
        return str(math.ceil(seconds))

    at = exc.details.get("retry_at") if exc.details else None

    if not isinstance(at, datetime) or at.utcoffset() is None:
        return None

    try:
        at = at.astimezone(UTC)

        # An HTTP-date has whole seconds; rounding down would send the client back early.
        if at.microsecond:
            at = at.replace(microsecond=0) + timedelta(seconds=1)

        return format_datetime(at, usegmt=True)

    except (OverflowError, ValueError):  # an instant UTC cannot represent
        return None


# ....................... #


def build_core_exception_response(exc: CoreException) -> JSONResponse:
    """Build the standard JSON response for a :class:`CoreException`.

    Renders the shared core :func:`~forze.base.exceptions.error_envelope`
    projection — server-error masking and egress sanitization already applied —
    into a FastAPI :class:`JSONResponse`, logging server-side errors on the way.
    The same envelope the Socket.IO transport renders, so both stay in lock-step.
    A throttled or unavailable error's ``retry_after`` (seconds) or ``retry_at`` (aware
    datetime) detail also sets ``Retry-After``; its other details stay withheld.
    """

    envelope = error_envelope(exc)

    if envelope.server_error:
        log_server_error(error_logger, exc, core=exc)

    log_withheld_details(error_logger, exc)

    response = _render_envelope(envelope)

    if (when := _retry_after_header(exc)) is not None:
        response.headers["Retry-After"] = when

    return response


# ....................... #


def build_unhandled_exception_response() -> JSONResponse:
    """Build the generic 500 response for an unhandled, non-:class:`CoreException` error.

    Nothing of the exception reaches it: the body is the
    :func:`~forze.base.exceptions.unhandled_error_envelope`. Logging the exception is the
    caller's job.
    """

    return _render_envelope(unhandled_error_envelope())


# ....................... #


async def _forze_exception_handler(_: Request, exc: CoreException) -> JSONResponse:
    """FastAPI exception handler that converts :class:`exc.internal` to a JSON response."""

    return build_core_exception_response(exc)


# ....................... #


async def _unhandled_exception_handler(_: Request, exc: Exception) -> JSONResponse:
    """FastAPI exception handler for unhandled non-:class:`CoreException` errors."""

    log_server_error(error_logger, exc)

    return build_unhandled_exception_response()


# ....................... #


async def _request_validation_handler(_: Request, exc_: RequestValidationError) -> JSONResponse:
    """Render FastAPI's request-validation error in the shared Forze envelope.

    Without this, a body/query/path that fails FastAPI's own parsing returns FastAPI's default
    422 shape (``{"detail": [{"loc": …}]}``), while a validation error raised inside an operation
    returns the Forze envelope — two incompatible 422 schemas on one API. Converting it to a
    ``validation`` :class:`CoreException` unifies both on the same envelope + ``X-Error-Code``. The
    per-error ``loc`` / ``msg`` / ``type`` are kept (JSON-safe); the raw ``ctx`` / ``input`` are
    dropped so a non-serializable cause or echoed request input can't leak or break rendering,
    and a ``msg`` that can quote the input is replaced (:func:`sanitize_pydantic_errors`).
    """

    errors = [
        {
            "loc": list(err.get("loc", ())),
            "msg": err.get("msg", ""),
            "type": err.get("type", ""),
        }
        for err in sanitize_pydantic_errors(list(exc_.errors()))
    ]
    core = exc.validation(
        "Request validation failed",
        code="request_validation_error",
        details={"errors": errors},
    )

    return build_core_exception_response(core)


# ....................... #


def register_exception_handlers(app: FastAPI) -> None:
    """Register exception handlers on *app*."""

    app.exception_handler(RequestValidationError)(_request_validation_handler)
    app.exception_handler(CoreException)(_forze_exception_handler)
    app.exception_handler(Exception)(_unhandled_exception_handler)
