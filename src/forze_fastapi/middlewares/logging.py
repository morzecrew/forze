from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import time
import urllib.parse
from typing import Any

import attrs
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from forze.base.errors import CoreError
from forze.base.logging import Logger
from forze_fastapi._logging import ForzeFastAPILogger

# ----------------------- #

logger = Logger(str(ForzeFastAPILogger.ACCESS))
"""The logger for the logging middleware."""

# ....................... #


@attrs.define(slots=True, frozen=True)
class LoggingMiddleware:
    """Middleware that logs the access to the API."""

    app: ASGIApp
    """The next ASGI application."""

    process_time_header: str = attrs.field(kw_only=True, default="X-Process-Time")
    """The header name for the process time."""

    # ....................... #

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start_time = time.perf_counter()
        request = Request(scope, receive)
        status_code: int = 500
        process_time_ms: int = 0
        logged = False

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code, logged, process_time_ms

            if message["type"] == "http.response.start":
                status_code = int(message["status"])

                headers = list(message.get("headers", []))
                process_time_ms = int((time.perf_counter() - start_time) * 1000)
                headers.append(
                    (
                        self.process_time_header.encode("latin-1"),
                        str(process_time_ms).encode("latin-1"),
                    )
                )
                message["headers"] = headers

            elif message["type"] == "http.response.body" and not logged:
                if process_time_ms == 0:
                    process_time_ms = int((time.perf_counter() - start_time) * 1000)

                self._log_access(request, scope, status_code, process_time_ms)
                logged = True

            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)

        except CoreError:
            # Pass through CoreError to be handled by the exception handler
            raise

        except Exception:
            # fallback only for unhandled exceptions
            process_time_ms = int((time.perf_counter() - start_time) * 1000)
            self._log_exception(request, scope, process_time_ms)

            response = JSONResponse(
                status_code=500,
                content={"detail": "Internal server error"},
            )
            await response(scope, receive, send)

    # ....................... #

    def _prepare_log_extra(
        self,
        request: Request,
        scope: Scope,
        status_code: int,
        process_time_ms: int,
    ) -> dict[str, Any]:
        url = _get_path_with_query_string(scope)  # type: ignore

        if request.client is not None:
            client_host = request.client.host
            client_port = str(request.client.port)

        else:
            client_host = "unknown"
            client_port = "unknown"

        http_method = request.method
        http_version = request.scope.get("http_version", "unknown")

        return dict(
            http={
                "url": url,
                "status_code": status_code,
                "method": http_method,
                "version": http_version,
            },
            network={"client": {"ip": client_host, "port": client_port}},
            duration=process_time_ms,
        )

    # ....................... #

    def _log_access(
        self,
        request: Request,
        scope: Scope,
        status_code: int,
        process_time_ms: int,
    ) -> None:
        log_extra = self._prepare_log_extra(
            request, scope, status_code, process_time_ms
        )
        logger.info("Processed request", **log_extra)

    # ....................... #

    def _log_exception(
        self,
        request: Request,
        scope: Scope,
        process_time_ms: int,
    ) -> None:
        log_extra = self._prepare_log_extra(request, scope, 500, process_time_ms)
        logger.critical_exception("Unhandled exception", **log_extra)


# ....................... #


def _get_path_with_query_string(scope: dict[str, Any]) -> str:
    res = urllib.parse.quote(scope["path"])

    query_string: bytes = scope.get("query_string", b"")
    if query_string:
        # The query_string in ASGI scope is already bytes that are usually url-encoded.
        # However, we cannot trust it to be safely encoded for logging.
        # We replace newline and carriage return characters which are the primary
        # vectors for log injection, while keeping the rest of the string as is to
        # avoid double-encoding issues.
        decoded = query_string.decode("ascii", errors="replace")
        safe_query = decoded.replace("\n", "%0A").replace("\r", "%0D")
        res = f"{res}?{safe_query}"

    return res
