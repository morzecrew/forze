from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from forze.base.errors import ConflictError, CoreError, NotFoundError, ValidationError
from forze.base.logging import getLogger

from ..constants import ERROR_CODE_HEADER

# ----------------------- #

logger = getLogger(__name__).bind(scope="api")

# ....................... #


def _status_code_mapper(exc: CoreError) -> int:
    """Map a :class:`CoreError` subclass to the appropriate HTTP status code."""

    match exc:
        case NotFoundError():
            return 404

        case ConflictError():
            return 409

        case ValidationError():
            return 422

        case _:
            return 500


# ....................... #


async def forze_unhandled_exception_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    """Catch unhandled exceptions, log at CRITICAL with Rich traceback, return 500."""

    logger.critical_exception(
        "Unhandled exception: {exc_type}: {message}",
        sub={"exc_type": type(exc).__name__, "message": str(exc)},
        exc=exc,
    )

    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


# ....................... #


async def forze_exception_handler(request: Request, exc: CoreError) -> JSONResponse:
    """FastAPI exception handler that converts :class:`CoreError` to a JSON response."""

    logger.exception(
        "Exception occurred: {message} (code={code}, details={details})",
        sub={
            "message": exc.message,
            "code": exc.code,
            "details": exc.details,
        },
    )

    content: dict[str, Any] = {"detail": exc.message}

    if exc.details:
        content["context"] = exc.details

    return JSONResponse(
        status_code=_status_code_mapper(exc),
        content=content,
        headers={ERROR_CODE_HEADER: exc.code},
    )


# ....................... #


def register_exception_handlers(app: FastAPI) -> None:
    """Register exception handlers on *app*.

    - :class:`CoreError` → :func:`forze_exception_handler` (mapped status codes)
    - :class:`Exception` → :func:`forze_unhandled_exception_handler` (CRITICAL + 500)
    """

    app.exception_handler(CoreError)(forze_exception_handler)
    app.exception_handler(Exception)(forze_unhandled_exception_handler)
