from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

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


async def forze_exception_handler(request: Request, exc: CoreError) -> JSONResponse:
    """FastAPI exception handler that converts :class:`CoreError` to a JSON response."""

    logger.exception("Exception occurred: %s (%s)", exc.message, exc.code)

    return JSONResponse(
        status_code=_status_code_mapper(exc),
        content={"detail": exc.message},
        headers={ERROR_CODE_HEADER: exc.code},
    )


# ....................... #


def register_exception_handlers(app: FastAPI) -> None:
    """Register the :func:`forze_exception_handler` for :class:`CoreError` on *app*."""

    app.exception_handler(CoreError)(forze_exception_handler)
