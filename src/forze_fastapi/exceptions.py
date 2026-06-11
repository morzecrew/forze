from forze.base.primitives import JsonDict
from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Final

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from forze.base.exceptions import CoreException, ExceptionKind, exception_egress_policy
from forze.base.logging import Logger
from forze.base.scrubbing import sanitize
from forze_fastapi._logging import ForzeFastAPILogger

# ----------------------- #

ERROR_CODE_HEADER: Final[str] = "X-Error-Code"
"""Key of the header used for error code."""

GENERIC_500_DETAIL: Final[str] = "Internal server error"
"""Generic detail message for unhandled server errors."""

error_logger = Logger(ForzeFastAPILogger.ERRORS)
"""Logger for FastAPI server-side error diagnostics."""

# ....................... #


def _status_code_mapper(kind: ExceptionKind) -> int:
    """Map a :class:`exc.internal` subclass to the appropriate HTTP status code."""

    match kind:
        case ExceptionKind.NOT_FOUND:
            return 404

        case ExceptionKind.CONFLICT:
            return 409

        case ExceptionKind.VALIDATION:
            return 422

        case ExceptionKind.DOMAIN:
            return 400

        case ExceptionKind.PRECONDITION:
            return 400

        case ExceptionKind.AUTHENTICATION:
            return 401

        case ExceptionKind.AUTHORIZATION:
            return 403

        case _:
            return 500


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

    Server errors (``>= 500``) are logged and their summary is replaced with a
    generic detail message so internal diagnostics never leak to clients. The
    sanitized ``context`` field is only included when the egress policy for
    the exception kind allows exposing details.
    """

    policy = exception_egress_policy(exc.kind)
    status_code = _status_code_mapper(exc.kind)

    if status_code >= 500:
        _log_server_error(exc, core=exc)

    detail = GENERIC_500_DETAIL if status_code >= 500 else exc.summary
    content: JsonDict = {"detail": detail}

    if exc.details and policy.expose_details and status_code < 500:
        content["context"] = sanitize(exc.details, context="egress")

    return JSONResponse(
        status_code=status_code,
        content=content,
        headers={ERROR_CODE_HEADER: exc.code},
    )


# ....................... #


async def _forze_exception_handler(_: Request, exc: CoreException) -> JSONResponse:
    """FastAPI exception handler that converts :class:`exc.internal` to a JSON response."""

    return build_core_exception_response(exc)


# ....................... #


async def _unhandled_exception_handler(_: Request, exc: Exception) -> JSONResponse:
    """FastAPI exception handler for unhandled non-:class:`CoreException` errors."""

    _log_server_error(exc)

    return JSONResponse(
        status_code=500,
        content={"detail": GENERIC_500_DETAIL},
    )


# ....................... #


def register_exception_handlers(app: FastAPI) -> None:
    """Register exception handlers on *app*."""

    app.exception_handler(CoreException)(_forze_exception_handler)
    app.exception_handler(Exception)(_unhandled_exception_handler)
