from enum import StrEnum
from functools import cache
from typing import Any, Final, Self, cast, final

import attrs
from structlog import get_logger as _structlog_get_logger
from structlog.typing import ExcInfo, FilteringBoundLogger

from .constants import (
    INTEGRATION_LOGGER_PREFIX,
    TRACE_LEVEL_KEY,
    LogLevel,
    LogLevelToRank,
)

# ----------------------- #

_TRACE_RANK: Final[int] = LogLevelToRank["trace"]
_DEBUG_RANK: Final[int] = LogLevelToRank["debug"]
_configured_min_rank: int = LogLevelToRank["info"]
"""Configured minimum level rank; defaults to the INFO rank until configured.

Trace is opt-in: a process that never calls
:func:`~forze.base.logging.configure.configure_logging` drops every
:meth:`Logger.trace` call at the gate (one integer comparison) instead of
building the event dict and running the full structlog pipeline for output
nobody asked for. ``configure_logging(level="trace")`` opens the gate.

:meth:`Logger.debug` has a gate of its own, :data:`_debug_gate_rank`; info and
above always go to the structlog backend, whose own (configured or default)
filtering applies.

:class:`~forze.base.logging.processors.TraceLevelResolver` drops a trace event
(rank 5) whenever ``5 < configured_rank``, so :meth:`Logger.trace` can short-circuit
above that threshold without building the event or touching the backend.
"""

_debug_gate_rank: int = LogLevelToRank["notset"]
"""Configured minimum level rank for :meth:`Logger.debug`; open until configured.

Once :func:`~forze.base.logging.configure.configure_logging` sets a level above
debug, structlog's filtering logger drops debug anyway, so the gate drops it
first without materializing the backend logger, which costs microseconds per call
on per-statement paths. Unlike the trace gate it starts open: a process that never
configures logging keeps structlog's default, which prints debug.
"""


def set_configured_min_rank(level: LogLevel) -> None:
    """Record the configured minimum level for the trace and debug fast-skip gates.

    Called by :func:`~forze.base.logging.configure.configure_logging`; keeps the
    per-call gates of :meth:`Logger.trace` and :meth:`Logger.debug` a single integer
    comparison instead of a structlog pipeline pass. Until this runs, the trace gate
    sits at the INFO rank, so trace is dropped in unconfigured processes, and the
    debug gate is open; any explicitly configured level — including ``"trace"`` — is
    honored as-is. A later ``structlog.configure`` that bypasses
    :func:`~forze.base.logging.configure.configure_logging` does not move the gates.
    """

    global _configured_min_rank, _debug_gate_rank
    _configured_min_rank = _debug_gate_rank = LogLevelToRank.get(level, 0)


# ----------------------- #


@final
@attrs.define(slots=True, frozen=True)
class Logger:
    """Convenience wrapper around structlog's FilteringBoundLogger."""

    name: str | StrEnum = attrs.field(converter=str)
    """Logger name."""

    bound: dict[str, Any] = attrs.field(factory=dict, kw_only=True)
    """Bound context."""

    # ....................... #

    def bind(self, **kwargs: Any) -> Self:
        """Bind additional context to the logger."""

        bound = {**self.bound, **kwargs}
        return attrs.evolve(self, bound=bound)

    # ....................... #

    @property
    def backend(self) -> FilteringBoundLogger:
        log = cast(FilteringBoundLogger, _structlog_get_logger(self.name))

        if self.bound:
            log = log.bind(**self.bound)

        return log

    # ....................... #

    def trace(self, event: str, *sub: Any, **extras: Any) -> None:
        """Log at TRACE level (below DEBUG). Use for noisy per-op details.

        Structlog's bound logger has no trace level, so this calls ``debug`` and
        relies on :class:`~forze.base.logging.processors.TraceLevelResolver` to
        label or drop the event according to the configured minimum level.

        Fast-skips when trace is below the configured level (the common production
        case), so per-item callers on hot paths pay only one integer comparison
        instead of building the event dict and materializing the backend.

        Trace is opt-in: unconfigured processes (no
        :func:`~forze.base.logging.configure.configure_logging` call) gate at the
        INFO rank and drop trace; configure with ``level="trace"`` to emit it.
        """

        if _configured_min_rank > _TRACE_RANK:
            return

        extras = {**extras, TRACE_LEVEL_KEY: "trace"}
        self.backend.debug(event, *sub, **extras)

    # ....................... #

    def debug(
        self,
        event: str,
        *sub: Any,
        exc_info: bool = False,
        **extras: Any,
    ) -> None:
        """Log at DEBUG level.

        Fast-skips once logging is configured above debug, like :meth:`trace`.
        """

        if _debug_gate_rank > _DEBUG_RANK:
            return

        self.backend.debug(event, *sub, exc_info=exc_info, **extras)

    # ....................... #

    def info(self, event: str, *sub: Any, **extras: Any) -> None:
        """Log at INFO level."""

        self.backend.info(event, *sub, **extras)

    # ....................... #

    def warning(self, event: str, *sub: Any, **extras: Any) -> None:
        """Log at WARNING level."""

        self.backend.warning(event, *sub, **extras)

    # ....................... #

    def error(self, event: str, *sub: Any, **extras: Any) -> None:
        """Log at ERROR level (no exception info)."""

        self.backend.error(event, *sub, **extras)

    # ....................... #

    def critical(self, event: str, *sub: Any, **extras: Any) -> None:
        """Log at CRITICAL level (no exception info)."""

        self.backend.critical(event, *sub, **extras)

    # ....................... #

    def exception(self, event: str, *sub: Any, **extras: Any) -> None:
        """Log at ERROR level (with exception info)."""

        self.backend.error(event, *sub, exc_info=True, **extras)

    # ....................... #

    def critical_exception(
        self,
        event: str,
        *sub: Any,
        exc: BaseException | None = None,
        **extras: Any,
    ) -> None:
        """Log at CRITICAL level (with exception info). Mostly for unhandled exceptions."""

        exc_info: bool | ExcInfo = (type(exc), exc, exc.__traceback__) if exc is not None else True

        self.backend.critical(event, *sub, exc_info=exc_info, **extras)

    # ....................... #

    def log(self, level: LogLevel, event: str, *sub: Any, **extras: Any) -> None:
        """Log at the given level."""

        self.backend.log(LogLevelToRank.get(level, 0), event, *sub, **extras)


# ----------------------- #


def get_logger(name: str | StrEnum) -> Logger:
    """Return a :class:`Logger` for *name* — the convenience factory for app code.

    A thin, discoverable front door over ``Logger(name)`` so callers do not have to
    import the class directly. Prefer a namespaced name (``"myapp.orders"``) so the
    logger can be configured and filtered independently.
    """

    return Logger(name)


# ....................... #


@cache
def _integration_logger(domain: str) -> Logger:
    """Memoized default logger for shared adapter/port machinery serving *domain*.

    ``Logger`` is frozen and immutable, so one cached instance per domain is safe to
    share and keeps the hot resolve path allocation-free after warm-up.
    """

    return Logger(f"{INTEGRATION_LOGGER_PREFIX}.{domain}")


# ....................... #


def resolve_logger(override: Logger | None, *, domain: str) -> Logger:
    """Resolve the logger for generic machinery: *override* if given, else the default.

    Shared code assembled across integrations (port proxies, base adapters, resilience)
    has no package-local logger of its own. It logs under ``forze.integrations.<domain>``
    by default so all such output is filterable as a group (``forze.integrations.*``) or
    per domain (``forze.integrations.cache``). A concrete adapter that wants its own
    identity passes *override* (e.g. its ``forze_postgres.adapters`` logger).
    """

    if override is not None:
        return override

    return _integration_logger(domain)
