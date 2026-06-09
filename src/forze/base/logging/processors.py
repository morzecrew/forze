import sys
import traceback
from typing import Any, cast

import attrs
from opentelemetry import trace as otel_trace
from structlog import DropEvent
from structlog.typing import EventDict, ExcInfo

from .constants import (
    ERR_MESSAGE_KEY,
    ERR_STACK_KEY,
    ERR_TYPE_KEY,
    OTEL_DEFAULT_SPAN_ID_KEY,
    OTEL_DEFAULT_TRACE_ID_KEY,
    RICH_EXC_INFO_KEY,
    TRACE_LEVEL_KEY,
    LogLevel,
    LogLevelToRank,
    RenderMode,
)

# Keys left untouched by :class:`EventDictSanitizer` (message template, metadata, stacks).
_EVENT_DICT_SANITIZE_SKIP: frozenset[str] = frozenset(
    {
        "event",
        "level",
        "timestamp",
        "logger",
        "logger_name",
        TRACE_LEVEL_KEY,
        ERR_TYPE_KEY,
        RICH_EXC_INFO_KEY,
        OTEL_DEFAULT_SPAN_ID_KEY,
        OTEL_DEFAULT_TRACE_ID_KEY,
        "exc_info",
        "stack_info",
    }
)

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class ExceptionInfoFormatter:
    """Processor to format exception info to a dictionary."""

    render_mode: RenderMode
    """Render mode."""

    include_exception_stack: bool = True
    """When false, omit ``error.stack`` from structured output."""

    # ....................... #

    def __call__(self, _: Any, __: str, event_dict: EventDict) -> EventDict:
        """Format exception info to a dictionary."""

        exc_info = event_dict.pop("exc_info", None)

        if not exc_info:
            return event_dict

        if exc_info is True:
            exc_info = sys.exc_info()

        if isinstance(exc_info, tuple):
            exc_type, exc, tb = cast(ExcInfo, exc_info)

        else:
            exc_type = type(cast(Exception, exc_info))
            exc = exc_info
            tb = exc.__traceback__

        event_dict[ERR_TYPE_KEY] = exc_type.__name__
        event_dict[ERR_MESSAGE_KEY] = str(exc)

        if self.include_exception_stack:
            event_dict[ERR_STACK_KEY] = "".join(
                traceback.format_exception(exc_type, exc, tb)
            )

        # only for console renderer / dev output
        if self.render_mode == "console":
            event_dict[RICH_EXC_INFO_KEY] = (exc_type, exc, tb)

        return event_dict


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class ExceptionFieldsSanitizer:
    """Scrub ``error.message`` and ``error.stack`` (always uses log string rules)."""

    # ....................... #

    def __call__(self, _: Any, __: str, event_dict: EventDict) -> EventDict:
        from forze.base.scrubbing.policy import scrub_log_string

        for key in (ERR_MESSAGE_KEY, ERR_STACK_KEY):
            value = event_dict.get(key)

            if isinstance(value, str):
                event_dict[key] = scrub_log_string(value)

        return event_dict


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class OpenTelemetryContextInjector:
    """Processor to inject OpenTelemetry context into the event dict."""

    span_key: str = OTEL_DEFAULT_SPAN_ID_KEY
    """Key to inject the span id into."""

    trace_key: str = OTEL_DEFAULT_TRACE_ID_KEY
    """Key to inject the trace id into."""

    # ....................... #

    def __call__(self, _: Any, __: str, event_dict: EventDict) -> EventDict:
        """Inject OpenTelemetry context into the event dict."""

        span = otel_trace.get_current_span()
        ctx = span.get_span_context()

        if not ctx or not ctx.is_valid:
            return event_dict

        event_dict[self.span_key] = format(ctx.span_id, "016x")
        event_dict[self.trace_key] = format(ctx.trace_id, "032x")

        return event_dict


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class RedundantKeysDropper:
    """Processor to drop redundant keys from the event dict."""

    keys: list[str]
    """Keys to drop from the event dict."""

    # ....................... #

    def __call__(self, _: Any, __: str, event_dict: EventDict) -> EventDict:
        """Drop redundant keys from the event dict."""

        for key in self.keys:
            event_dict.pop(key, None)

        return event_dict


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class TraceLevelResolver:
    """Processor to resolve the trace level."""

    configured_level: LogLevel
    """Minimum level (from configuration)."""

    # ....................... #

    def __call__(self, _: Any, __: str, event_dict: EventDict) -> EventDict:
        """Resolve the trace level or drop the event if 'trace' is below the configured level."""

        override = event_dict.pop(TRACE_LEVEL_KEY, None)

        if override:
            event_dict["level"] = override

        configured_rank = LogLevelToRank.get(self.configured_level, 0)
        event_rank = LogLevelToRank.get(event_dict["level"], 0)

        # Trace is logged via FilteringBoundLogger.debug (structlog has no trace
        # level); rank ordering (trace < debug) drops trace here unless the
        # configured minimum is trace or lower.
        if event_rank < configured_rank:
            raise DropEvent()

        return event_dict


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class EventDictSanitizer:
    """Scrub sensitive values from structlog event fields (not the ``event`` message)."""

    text_scrub: bool = True
    """When true, apply log string scrub rules to string leaves in ``log`` context."""

    # ....................... #

    def __call__(self, _: Any, __: str, event_dict: EventDict) -> EventDict:
        from forze.base.scrubbing import sanitize
        from forze.base.scrubbing.policy import SECRET_PLACEHOLDER, is_sensitive_key

        for key in list(event_dict.keys()):
            if key in _EVENT_DICT_SANITIZE_SKIP:
                continue

            if is_sensitive_key(key):
                event_dict[key] = SECRET_PLACEHOLDER
                continue

            event_dict[key] = sanitize(
                event_dict[key],
                context="log",
                text_scrub=self.text_scrub,
            )
        return event_dict
