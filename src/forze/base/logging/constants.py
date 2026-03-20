from typing import Final, Literal

# ----------------------- #
# Keys

TRACE_LEVEL_KEY: Final[str] = "_trace_level"
"""Trace level key."""

ERR_TYPE_KEY: Final[str] = "error.type"
"""Error type key."""

ERR_MESSAGE_KEY: Final[str] = "error.message"
"""Error message key."""

ERR_STACK_KEY: Final[str] = "error.stack"
"""Error stack (traceback) key."""

OTEL_SPAN_ID_KEY: Final[str] = "span_id"
"""OpenTelemetry span id key."""

OTEL_TRACE_ID_KEY: Final[str] = "trace_id"
"""OpenTelemetry trace id key."""

# ....................... #
# Literals

LogLevel = Literal["notset", "trace", "debug", "info", "warning", "error", "critical"]
"""Logging level."""

LogLevelToRank: dict[LogLevel, int] = {
    "notset": 0,
    "trace": 5,
    "debug": 10,
    "info": 20,
    "warning": 30,
    "error": 40,
    "critical": 50,
}
"""Logging level rank mapping."""

RenderMode = Literal["console", "json"]
"""Render mode."""
