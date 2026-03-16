import time
from typing import Awaitable, Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from forze.base.logging_v2 import get_config, getLogger

# ----------------------- #


def format_status_for_log(status_code: int) -> str:
    """Format HTTP status code for log output.

    When :attr:`LoggingConfig.colorize` is True, returns ANSI-colored string.
    Otherwise returns plain string. Use in access logs (e.g. LoggingMiddleware).
    """
    s = str(status_code)
    if not get_config().colorize:
        return s
    if status_code < 300:
        return f"\033[32m{s}\033[0m"
    if status_code < 400:
        return f"\033[33m{s}\033[0m"
    return f"\033[31m{s}\033[0m"


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
        status_code = format_status_for_log(response.status_code)

        logger.info(
            "%s %s %s (%dms)",
            status_code,
            request.method,
            request.url.path,
            duration,
        )

        return response
