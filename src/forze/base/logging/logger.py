from typing import Any, Self, cast, final

import attrs
from structlog import get_logger
from structlog.typing import ExcInfo, FilteringBoundLogger

from .constants import TRACE_LEVEL_KEY, LogLevel, LogLevelToRank

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True)
class Logger:
    """Convenience wrapper around structlog's FilteringBoundLogger."""

    name: str
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
        log = cast(FilteringBoundLogger, get_logger(self.name))

        if self.bound:
            log = log.bind(**self.bound)

        return log

    # ....................... #

    def notset(self, event: str, *sub: Any, **extras: Any) -> None:
        """Log at NOTSET level."""

        self.backend.log(LogLevelToRank["notset"], event, *sub, **extras)

    # ....................... #

    def trace(self, event: str, *sub: Any, **extras: Any) -> None:
        """Log at TRACE level (below DEBUG). Use for noisy per-op details.

        Structlog's bound logger has no trace level, so this calls ``debug`` and
        relies on :class:`~forze.base.logging.processors.TraceLevelResolver` to
        label or drop the event according to the configured minimum level.
        """

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
        """Log at DEBUG level."""

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

        exc_info: bool | ExcInfo = (
            (type(exc), exc, exc.__traceback__) if exc is not None else True
        )

        self.backend.critical(event, *sub, exc_info=exc_info, **extras)

    # ....................... #

    def log(self, level: LogLevel, event: str, *sub: Any, **extras: Any) -> None:
        """Log at the given level."""

        self.backend.log(LogLevelToRank.get(level, 0), event, *sub, **extras)
