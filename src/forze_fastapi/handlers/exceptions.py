from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from forze.base.errors import ConflictError, CoreError, NotFoundError, ValidationError

from ..constants import ERROR_CODE_HEADER

# ----------------------- #
#! TODO: review, maybe repurpose to a middleware or so


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


async def uncaught_exception_handler(_: Request, __: Exception) -> JSONResponse:
    """Catch uncaught exceptions."""

    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


# ....................... #


async def forze_exception_handler(_: Request, exc: CoreError) -> JSONResponse:
    """FastAPI exception handler that converts :class:`CoreError` to a JSON response."""

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
    """Register exception handlers on *app*."""

    app.exception_handler(CoreError)(forze_exception_handler)
    app.exception_handler(Exception)(uncaught_exception_handler)
