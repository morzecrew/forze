"""FastMCP middleware that emits a structured access log per MCP message.

The MCP analogue of ``forze_fastapi``'s request-logging middleware: it wraps the FastMCP
message pipeline (``on_message`` is the outermost hook, fired once per request/notification)
and logs the method, target (tool/resource/prompt name), direction, duration, and outcome via
Forze's structlog-backed :class:`~forze.base.logging.Logger`. Add it to a server you own with
``server.add_middleware(LoggingMiddleware())``; pair it with ``configure_logging`` /
``attach_foreign_loggers`` (for uvicorn) to get a single consistent log format.
"""

from forze_mcp._compat import require_mcp

require_mcp()

# ....................... #

import time
from typing import Any

from fastmcp.server.middleware import (
    CallNext,
    Middleware,
    MiddlewareContext,
)

from forze.base.exceptions import CoreException
from forze.base.logging import AccessLogSampler, Logger
from forze_mcp._logging import ForzeMCPLogger

# ----------------------- #

logger = Logger(ForzeMCPLogger.ACCESS)
"""The logger for the MCP access middleware."""

# ....................... #


def _target(message: Any) -> str | None:
    """Best-effort human label for the message subject (tool/prompt name or resource URI)."""

    name = getattr(message, "name", None)

    if isinstance(name, str):
        return name

    uri = getattr(message, "uri", None)

    return None if uri is None else str(uri)


# ....................... #


class LoggingMiddleware(Middleware):
    """Log each MCP message: method, target, direction, duration, and outcome.

    Successful requests are sampled (see :class:`~forze.base.logging.AccessLogSampler`);
    errors are always logged. Pass ``access_log=`` to change the volume policy.
    """

    def __init__(self, *, access_log: AccessLogSampler | None = None) -> None:
        super().__init__()
        self.access_log = access_log if access_log is not None else AccessLogSampler()

    # ....................... #

    def _extra(
        self,
        context: MiddlewareContext[Any],
        duration_ms: int,
        outcome: str,
    ) -> dict[str, Any]:
        return {
            "mcp": {
                "method": context.method,
                "type": context.type,
                "source": context.source,
                "target": _target(context.message),
            },
            "duration": duration_ms,
            "outcome": outcome,
        }

    # ....................... #

    async def on_message(
        self,
        context: MiddlewareContext[Any],
        call_next: CallNext[Any, Any],
    ) -> Any:
        start_time = time.perf_counter()

        try:
            result = await call_next(context)

        except CoreException as exc:
            # An expected, classified error — log it without a stack and re-raise so the
            # host server translates it into the MCP error response.
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            logger.warning(
                "MCP request failed",
                error_code=getattr(exc, "code", None),
                **self._extra(context, duration_ms, "error"),
            )
            raise

        except Exception as exc:
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            logger.critical_exception(
                "Unhandled MCP request error",
                exc=exc,
                **self._extra(context, duration_ms, "error"),
            )
            raise

        duration_ms = int((time.perf_counter() - start_time) * 1000)

        if self.access_log.should_log(subject=context.method, is_error=False):
            logger.info("Processed MCP request", **self._extra(context, duration_ms, "ok"))

        return result
