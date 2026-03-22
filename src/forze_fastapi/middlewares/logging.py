from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import time
import urllib.parse
from typing import Any, Awaitable, Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

from forze.base.logging import Logger

# ----------------------- #


class LoggingMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: ASGIApp,
        *,
        process_time_header: str = "X-Process-Time",
        logger_name: str = "api.access",
    ) -> None:
        super().__init__(app)

        self.process_time_header = process_time_header
        self.logger = Logger(logger_name)

    # ....................... #

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        start = time.perf_counter()
        response = await call_next(request)

        process_time_ms = int((time.perf_counter() - start) * 1000)
        status_code = response.status_code
        url = _get_path_with_query_string(request.scope)  # type: ignore

        if request.client is not None:
            client_host = request.client.host
            client_port = str(request.client.port)

        else:
            client_host = "unknown"
            client_port = "unknown"

        http_method = request.method
        http_version = request.scope.get("http_version", "unknown")

        response.headers[self.process_time_header] = str(process_time_ms)

        self.logger.info(
            f"""{client_host}:{client_port} - "{http_method} {url} HTTP/{http_version}" {status_code}""",
            http={
                "url": url,
                "status_code": status_code,
                "method": http_method,
                "version": http_version,
            },
            network={"client": {"ip": client_host, "port": client_port}},
            duration=process_time_ms,
        )

        return response


# ....................... #


def _get_path_with_query_string(scope: dict[str, Any]) -> str:
    res = urllib.parse.quote(scope["path"])

    if scope.get("query_string", ""):
        res = f"{res}?{scope['query_string'].decode('ascii')}"

    return res
