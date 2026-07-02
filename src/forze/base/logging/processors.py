import sys
import threading
import traceback
from typing import Any, cast

import attrs
from structlog import DropEvent
from structlog.typing import EventDict, ExcInfo

from .constants import (
    DEDUP_KEY,
    DEDUP_WINDOW_KEY,
    ERR_MESSAGE_KEY,
    ERR_STACK_KEY,
    ERR_TYPE_KEY,
    OTEL_DEFAULT_SPAN_ID_KEY,
    OTEL_DEFAULT_TRACE_ID_KEY,
    RICH_EXC_INFO_KEY,
    SAMPLE_KEY,
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

        # Deferred import: this processor is only added to the pipeline when OTel
        # injection is enabled, so importing this module never pulls ``opentelemetry``.
        from opentelemetry import trace as otel_trace

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
    """Scrub sensitive values from structlog event fields.

    The rendered ``event`` message itself is scrubbed with log string rules when
    ``text_scrub`` is enabled (after positional args are interpolated), so secrets
    embedded in the message text (e.g. ``logger.info("token=%s", token)``) are
    masked alongside the extras.
    """

    text_scrub: bool = True
    """When true, apply log string scrub rules to string leaves in ``log`` context."""

    # ....................... #

    def __call__(self, _: Any, __: str, event_dict: EventDict) -> EventDict:
        from forze.base.scrubbing import sanitize
        from forze.base.scrubbing.policy import (
            SECRET_PLACEHOLDER,
            is_sensitive_key,
            scrub_log_string,
        )

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

        # The message text is client-visible output too: positional args are
        # already interpolated by the time this sanitizer runs, so apply the
        # log string rules to ``event`` itself when text scrubbing is on.
        if self.text_scrub:
            event_value = event_dict.get("event")

            if isinstance(event_value, str):
                event_dict["event"] = scrub_log_string(event_value)

        return event_dict


# ....................... #


@attrs.define(slots=True, eq=False)
class SamplingDeduplicator:
    """Collapse high-volume events via opt-in per-event sampling and time-window dedup.

    A no-op for ordinary events: only events carrying a control key are affected, and an
    event without one passes straight through (two ``pop`` checks). This generalizes the
    hand-rolled "warn once" guards scattered across integrations into one pipeline stage.

    Callers opt in per event via reserved extras (stripped before rendering):

    - ``_sample=N`` — keep 1 in ``N`` events sharing the same ``(logger, event)`` bucket;
      the rest are dropped. Use for uniformly high-volume, low-signal events.
    - ``_dedup_key=key`` — emit at most one event per ``key`` per window; repeats within
      the window are dropped. Use for a flapping condition (a dependency retrying) that
      would otherwise log identically thousands of times.
    - ``_dedup_window=seconds`` — override the default dedup window for this event.

    State is bounded implicitly by the number of distinct buckets/keys in use (each is a
    stable literal in code, not user input), so it does not grow with traffic.
    """

    default_window: float = 60.0
    """Default dedup window in seconds when ``_dedup_window`` is not given."""

    _counts: dict[tuple[str, str], int] = attrs.field(factory=dict, init=False)
    _last_emit: dict[str, float] = attrs.field(factory=dict, init=False)
    _lock: "threading.Lock" = attrs.field(factory=threading.Lock, init=False)

    # ....................... #

    def __call__(self, _: Any, __: str, event_dict: EventDict) -> EventDict:
        sample = event_dict.pop(SAMPLE_KEY, None)
        dedup = event_dict.pop(DEDUP_KEY, None)
        window = event_dict.pop(DEDUP_WINDOW_KEY, None)

        if sample is None and dedup is None:
            return event_dict

        with self._lock:
            if sample is not None and self._sampled_out(event_dict, int(sample)):
                raise DropEvent()

            if dedup is not None and self._deduplicated(str(dedup), window):
                raise DropEvent()

        return event_dict

    # ....................... #

    def _sampled_out(self, event_dict: EventDict, n: int) -> bool:
        """Keep the 1st of every ``n`` events in the bucket; drop the rest."""

        if n <= 1:
            return False

        bucket = (
            str(event_dict.get("logger_name") or event_dict.get("logger") or ""),
            str(event_dict.get("event", "")),
        )
        count = self._counts.get(bucket, 0)
        self._counts[bucket] = count + 1

        return count % n != 0

    # ....................... #

    def _deduplicated(self, key: str, window: Any) -> bool:
        """Drop when *key* was already emitted within its window."""

        # Route through the time seam (not raw ``time.monotonic``) so the dedup window is
        # deterministic under simulation, tracking virtual time when a source is bound.
        from forze.base.primitives import monotonic

        span = float(window) if window is not None else self.default_window
        now = monotonic()
        last = self._last_emit.get(key)

        if last is not None and (now - last) < span:
            return True

        self._last_emit[key] = now

        return False
