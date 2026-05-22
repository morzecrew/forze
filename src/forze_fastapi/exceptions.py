from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any, Final

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from forze.base.errors import (
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    CoreError,
    DomainError,
    InvalidOperationError,
    NotFoundError,
    ValidationError,
)
from forze.base.scrubbing import sanitize

# ----------------------- #

ERROR_CODE_HEADER: Final[str] = "X-Error-Code"
"""Key of the header used for error code."""

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

        case DomainError():
            return 400

        case InvalidOperationError():
            return 400

        case AuthenticationError():
            return 401

        case AuthorizationError():
            return 403

        case _:
            return 500


# ....................... #


async def _forze_exception_handler(_: Request, exc: CoreError) -> JSONResponse:
    """FastAPI exception handler that converts :class:`CoreError` to a JSON response."""

    status_code = _status_code_mapper(exc)
    content: dict[str, Any] = {"detail": exc.message}

    if exc.details and status_code != 500:
        content["context"] = sanitize(exc.details, context="egress")

    return JSONResponse(
        status_code=status_code,
        content=content,
        headers={ERROR_CODE_HEADER: exc.code},
    )


# ....................... #


def register_exception_handlers(app: FastAPI) -> None:
    """Register exception handlers on *app*."""

    app.exception_handler(CoreError)(_forze_exception_handler)
