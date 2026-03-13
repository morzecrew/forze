"""Stdlib-like Logger wrapper around loguru.

Provides a familiar API (debug, info, warning, etc.) with %-style
message formatting, while delegating to loguru for actual emission.
"""

from contextlib import contextmanager
from typing import Any, Iterator, Self

import attrs
from loguru._logger import Logger as _LoguruLogger

from .context import log_section
from .formatting import effective_level_for_name
from .helpers import escape_loguru_braces, level_no, normalize_level, render_message
from .types import LogLevel, LogLevelName

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class Logger:
    """Stdlib-like logger bound to a name.

    Wraps a loguru logger with a fixed ``logger_name`` in extra.
    All methods use %-style formatting and escape braces for loguru.
    """

    name: str
    """Logger name used for level resolution and display."""

    logger: _LoguruLogger
    """The underlying loguru logger with bound context."""

    # ....................... #

    def isEnabledFor(self, level: LogLevel) -> bool:
        """Return whether a message at *level* would be emitted.

        Compares the requested level against the effective configured
        level for this logger's name.
        """
        want = level_no(normalize_level(level))
        have = level_no(effective_level_for_name(self.name))
        return want >= have

    # ....................... #

    def bind(self, **kwargs: Any) -> Self:
        """Return a logger with additional bound context.

        Forwards to loguru's bind. Useful for request IDs, etc.
        """
        bound = self.logger.bind(**kwargs)  # type: ignore[no-untyped-call]
        return attrs.evolve(self, logger=bound)  # type: ignore[no-untyped-call]

    # ....................... #

    @contextmanager
    def contextualize(self, **kwargs: Any) -> Iterator[None]:
        """Return a logger with additional bound context.

        Forwards to loguru's bind. Useful for request IDs, etc.
        """

        with self.logger.contextualize(  # pyright: ignore[reportUnknownMemberType]
            **kwargs
        ):
            yield

    # ....................... #

    @contextmanager
    def section(self) -> Iterator[None]:
        """Return a logger with a section bound context.

        Forwards to loguru's with_section. Useful for nested log blocks.
        """

        with log_section():
            yield

    # ....................... #

    def trace(self, message: Any, *args: Any) -> None:
        """Log at TRACE level."""

        self._log("TRACE", message, *args)

    # ....................... #

    def debug(self, message: Any, *args: Any) -> None:
        """Log at DEBUG level."""

        self._log("DEBUG", message, *args)

    # ....................... #

    def info(self, message: Any, *args: Any) -> None:
        """Log at INFO level."""

        self._log("INFO", message, *args)

    # ....................... #

    def success(self, message: Any, *args: Any) -> None:
        """Log at SUCCESS level."""

        self._log("SUCCESS", message, *args)

    # ....................... #

    def warning(self, message: Any, *args: Any) -> None:
        """Log at WARNING level."""

        self._log("WARNING", message, *args)

    warn = warning

    # ....................... #

    def error(self, message: Any, *args: Any) -> None:
        """Log at ERROR level."""

        self._log("ERROR", message, *args)

    # ....................... #

    def critical(self, message: Any, *args: Any) -> None:
        """Log at CRITICAL level."""

        self._log("CRITICAL", message, *args)

    # ....................... #

    def exception(self, message: Any, *args: Any) -> None:
        """Log at ERROR level with exception traceback.

        Call from an except block to include the current exception.
        """
        rendered = render_message(message, args)
        escaped = escape_loguru_braces(rendered)
        self.logger.opt(exception=True).error(escaped)  # type: ignore[no-untyped-call]

    # ....................... #

    def log(self, level: LogLevel, message: Any, *args: Any) -> None:
        """Log at an arbitrary level."""

        self._log(normalize_level(level), message, *args)

    # ....................... #

    def _log(self, level: LogLevelName, message: Any, *args: Any) -> None:
        """Internal call: render, escape, and emit."""

        rendered = render_message(message, args)
        escaped = escape_loguru_braces(rendered)
        self.logger.log(level, escaped)  # type: ignore[no-untyped-call]
