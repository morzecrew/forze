from forze.base.primitives import JsonDict
from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Final

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from forze.base.exceptions import CoreException, ExceptionKind, exception_egress_policy
from forze.base.scrubbing import sanitize

# ----------------------- #

ERROR_CODE_HEADER: Final[str] = "X-Error-Code"
"""Key of the header used for error code."""

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


async def _forze_exception_handler(_: Request, exc: CoreException) -> JSONResponse:
    """FastAPI exception handler that converts :class:`exc.internal` to a JSON response."""

    policy = exception_egress_policy(exc.kind)
    status_code = _status_code_mapper(exc.kind)
    content: JsonDict = {"detail": exc.summary}

    if exc.details and policy.expose_details:
        content["context"] = sanitize(exc.details, context="egress")

    return JSONResponse(
        status_code=status_code,
        content=content,
        headers={ERROR_CODE_HEADER: exc.code},
    )


# ....................... #


def register_exception_handlers(app: FastAPI) -> None:
    """Register exception handlers on *app*."""

    app.exception_handler(CoreException)(_forze_exception_handler)
