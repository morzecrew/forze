from typing import Final, Literal

from forze._logging import ForzeLogger

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

RICH_EXC_INFO_KEY: Final[str] = "_rich_exc_info"
"""Rich exception info key (only for console renderer / dev output)."""

OTEL_DEFAULT_SPAN_ID_KEY: Final[str] = "span_id"
"""OpenTelemetry span id key."""

OTEL_DEFAULT_TRACE_ID_KEY: Final[str] = "trace_id"
"""OpenTelemetry trace id key."""

INTEGRATION_LOGGER_PREFIX: Final[str] = str(ForzeLogger.INTEGRATIONS)
"""Default logger-name prefix for shared adapter/port machinery.

Generic code assembled across integrations logs under ``forze.integrations.<domain>``
(e.g. ``forze.integrations.cache``) unless a concrete adapter supplies its own
package-local logger (e.g. ``forze_postgres.adapters``). See
:func:`~forze.base.logging.logger.resolve_logger`. Derived from the single source of
truth, ``ForzeLogger.INTEGRATIONS`` in ``forze._logging``.
"""

SAMPLE_KEY: Final[str] = "_sample"
"""Per-event control key: keep 1-in-N events sharing the same sampling bucket."""

DEDUP_KEY: Final[str] = "_dedup_key"
"""Per-event control key: collapse repeats of this key within the dedup window."""

DEDUP_WINDOW_KEY: Final[str] = "_dedup_window"
"""Per-event control key: dedup window in seconds (overrides the default)."""

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
