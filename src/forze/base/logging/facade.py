"""Public facade: configure, getLogger, reset."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Callable, Mapping, Optional

import structlog

from .config import (
    DEFAULT_EVENT_WIDTH,
    DEFAULT_LEVEL,
    DEFAULT_PREFIX_WIDTH,
    DEFAULT_STEP,
    LoggingConfig,
    LogLevel,
    LogLevelName,
    normalize_level,
    set_config,
)
from .handlers import configure_logging

if TYPE_CHECKING:
    from .logger import Logger


def getLogger(
    name: str | None = None,
    *,
    scope: str | None = None,
    source: str | None = None,
) -> "Logger":
    from .logger import Logger

    logger_name = name or "root"
    initial: dict[str, object] = {"logger": logger_name}
    if scope is not None:
        initial["scope"] = scope
    if source is not None:
        initial["source"] = source
    bound = structlog.get_logger(logger_name).bind(**initial)
    return Logger(name=logger_name, backend=bound)


def configure(
    *,
    level: LogLevel = DEFAULT_LEVEL,
    levels: Optional[Mapping[str, LogLevel]] = None,
    step: str = DEFAULT_STEP,
    event_width: Optional[int] = None,
    extra_indent: int = 1,
    prefix_width: Optional[int] = None,
    extra_dim: Optional[str] = None,
    extra_key_sort: Optional[Callable[[str], int]] = None,
    colorize: bool = False,
    render_json: bool = False,
    dual_output: bool = False,
) -> None:
    """Configure logging. Level patterns use fnmatch (e.g. ``forze.application.*``)."""
    normalized_levels: dict[str, LogLevelName] | None = None
    if levels is not None:
        normalized_levels = {
            pattern: normalize_level(lv) for pattern, lv in levels.items()
        }

    config = LoggingConfig(
        level=normalize_level(level),
        levels=normalized_levels,
        step=step,
        event_width=event_width if event_width is not None else DEFAULT_EVENT_WIDTH,
        extra_indent=extra_indent,
        prefix_width=prefix_width if prefix_width is not None else DEFAULT_PREFIX_WIDTH,
        extra_dim=extra_dim,
        extra_key_sort=extra_key_sort,
        colorize=colorize,
        render_json=render_json,
        dual_output=dual_output,
    )
    set_config(config)
    configure_logging(config)


def reset() -> None:
    structlog.reset_defaults()


def register_unhandled_exception_handler(
    loop: Optional[asyncio.AbstractEventLoop] = None,
) -> None:
    """Install sys.excepthook and optionally asyncio exception handler.

    Call after :func:`configure`. Replaces the default traceback with our log output.

    :param loop: If provided (e.g. from ``asyncio.get_running_loop()`` in lifespan),
        also sets the asyncio exception handler for unhandled task exceptions.
    """
    import sys

    def _forze_excepthook(
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_tb: object,
    ) -> None:
        log = getLogger("forze.unhandled")
        log.critical_exception(
            "Unhandled exception: {exc_type}: {message}",
            sub={"exc_type": exc_type.__name__, "message": str(exc_value)},
            exc=exc_value,
        )

    sys.excepthook = _forze_excepthook

    if loop is not None:

        def _asyncio_handler(
            loop: asyncio.AbstractEventLoop,
            context: dict[str, object],
        ) -> None:
            exc = context.get("exception")
            if isinstance(exc, BaseException):
                log = getLogger("forze.unhandled")
                log.critical_exception(
                    "Unhandled asyncio task exception: {exc_type}: {message}",
                    sub={
                        "exc_type": type(exc).__name__,
                        "message": str(exc),
                    },
                    exc=exc,
                )
            else:
                # No exception in context (e.g. BaseExceptionGroup)
                msg = context.get("message", "Unknown asyncio error")
                log = getLogger("forze.unhandled")
                log.critical(
                    "Unhandled asyncio error: {message}",
                    sub={"message": str(msg)},
                )

        loop.set_exception_handler(_asyncio_handler)
