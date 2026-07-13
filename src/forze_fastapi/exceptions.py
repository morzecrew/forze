from forze.base.primitives import JsonDict
from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Final

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from forze.base.exceptions import (
    CoreException,
    error_envelope,
    exc,
    unhandled_error_envelope,
)
from forze.base.logging import Logger
from forze_fastapi._logging import ForzeFastAPILogger

# ----------------------- #

ERROR_CODE_HEADER: Final[str] = "X-Error-Code"
"""Key of the header used for error code."""

error_logger = Logger(ForzeFastAPILogger.ERRORS)
"""Logger for FastAPI server-side error diagnostics."""

# ....................... #


def _log_server_error(exc: BaseException, *, core: CoreException | None = None) -> None:
    """Log a server-side error with appropriate severity and traceback policy."""

    if core is not None and core.__cause__ is not None:
        error_logger.critical_exception(
            "Server error",
            exc=core.__cause__,
            error_code=core.code,
            error_kind=core.kind.value,
        )

    elif core is not None:
        error_logger.error(
            "Server error",
            error_code=core.code,
            error_kind=core.kind.value,
            detail=core.summary,
        )

    else:
        error_logger.critical_exception("Unhandled exception", exc=exc)


# ....................... #


def build_core_exception_response(exc: CoreException) -> JSONResponse:
    """Build the standard JSON response for a :class:`CoreException`.

    Renders the shared core :func:`~forze.base.exceptions.error_envelope`
    projection — server-error masking and egress sanitization already applied —
    into a FastAPI :class:`JSONResponse`, logging server-side errors on the way.
    The same envelope the Socket.IO transport renders, so both stay in lock-step.
    """

    envelope = error_envelope(exc)

    if envelope.server_error:
        _log_server_error(exc, core=exc)

    content: JsonDict = {"detail": envelope.detail}

    if envelope.context is not None:
        content["context"] = envelope.context

    return JSONResponse(
        status_code=envelope.status,
        content=content,
        headers={ERROR_CODE_HEADER: envelope.code},
    )


# ....................... #


async def _forze_exception_handler(_: Request, exc: CoreException) -> JSONResponse:
    """FastAPI exception handler that converts :class:`exc.internal` to a JSON response."""

    return build_core_exception_response(exc)


# ....................... #


async def _unhandled_exception_handler(_: Request, exc: Exception) -> JSONResponse:
    """FastAPI exception handler for unhandled non-:class:`CoreException` errors."""

    _log_server_error(exc)

    envelope = unhandled_error_envelope()

    return JSONResponse(
        status_code=envelope.status,
        content={"detail": envelope.detail},
    )


# ....................... #


async def _request_validation_handler(_: Request, exc_: RequestValidationError) -> JSONResponse:
    """Render FastAPI's request-validation error in the shared Forze envelope.

    Without this, a body/query/path that fails FastAPI's own parsing returns FastAPI's default
    422 shape (``{"detail": [{"loc": …}]}``), while a validation error raised inside an operation
    returns the Forze envelope — two incompatible 422 schemas on one API. Converting it to a
    ``validation`` :class:`CoreException` unifies both on the same envelope + ``X-Error-Code``. The
    per-error ``loc`` / ``msg`` / ``type`` are kept (JSON-safe); the raw ``ctx`` / ``input`` are
    dropped so a non-serializable cause or echoed request input can't leak or break rendering.
    """

    errors = [
        {
            "loc": list(err.get("loc", ())),
            "msg": err.get("msg", ""),
            "type": err.get("type", ""),
        }
        for err in exc_.errors()
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
