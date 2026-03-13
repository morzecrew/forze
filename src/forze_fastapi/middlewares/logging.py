import time
from typing import Awaitable, Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from forze.base.logging import getLogger

# ----------------------- #

logger = getLogger(__name__).bind(scope="api")

# ....................... #


class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        start = time.perf_counter()

        response = await call_next(request)

        duration = int((time.perf_counter() - start) * 1000)
        status = response.status_code

        if status < 300:
            status_code = f"<green>{status}</green>"

        elif status < 400:
            status_code = f"<yellow>{status}</yellow>"

        else:
            status_code = f"<red>{status}</red>"

        logger.info(
            "%s %s %s %dms",
            request.method,
            request.url.path,
            status_code,
            duration,
        )

        return response
