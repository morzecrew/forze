"""Public facade: getLogger, configure, reset.

This is the main entry point for application code. No automatic
configuration on import; call :func:`configure` explicitly (or
:func:`~.setup.setup_default` for sensible defaults).
"""

import sys
from typing import Mapping, Optional, cast

from loguru import logger as _base_logger
from loguru._logger import Logger as _LoguruLogger

from .config import LoggingConfig, set_config
from .constants import DEFAULT_LEVEL, DEFAULT_PREFIXES
from .formatting import record_filter, record_format
from .helpers import normalize_level
from .logger import Logger
from .types import LogLevel, LogLevelName

# ----------------------- #

# Active sink ids so reconfiguration cleanly replaces previous setup.
_sink_ids: list[int] = []

# ....................... #


def getLogger(name: str | None = None) -> Logger:
    """Return a stdlib-like logger bound to *name*.

    :param name: Logger name (e.g. ``__name__``). Defaults to ``"root"``.
    :returns: A :class:`~.logger.Logger` instance.
    """
    logger_name = name or "root"
    bound = _base_logger.bind(logger_name=logger_name)  # type: ignore[arg-type]
    return Logger(logger=cast(_LoguruLogger, bound), name=logger_name)


# ....................... #


def configure(
    *,
    level: LogLevel = DEFAULT_LEVEL,
    levels: Optional[Mapping[str, LogLevel]] = None,
    prefixes: tuple[str, ...] = DEFAULT_PREFIXES,
    step: str = "  ",
    keep_sections: Optional[Mapping[str, int]] = None,
    default_keep_sections: Optional[int] = None,
    root_aliases: Optional[Mapping[str, str]] = None,
    width: int = 36,
    colorize: bool = False,
) -> None:
    """Configure global logging.

    Replaces the current sink with a new one. Removes any previously
    added sinks. Call this at application startup.

    :param level: Default fallback level.
    :param levels: Optional per-prefix levels, e.g.
        ``{"forze.application": "DEBUG", "forze.base": "TRACE"}``.
    :param prefixes: Namespaces that receive indentation in output.
    :param step: Indentation unit for nested sections.
    :param keep_sections: Per-namespace truncation of logger name segments.
    :param default_keep_sections: Default segment count when no prefix matches.
    :param root_aliases: Root alias replacements for rendered logger names.
    :param width: Width of the logger-name column.
    :param colorize: Whether to emit ANSI color codes.
    """
    global _sink_ids

    normalized_levels: dict[str, LogLevelName] | None = None

    if levels is not None:
        normalized_levels = {
            prefix: normalize_level(prefix_level)
            for prefix, prefix_level in levels.items()
        }

    config = LoggingConfig(
        level=normalize_level(level),
        levels=normalized_levels,
        prefixes=prefixes,
        step=step,
        default_keep_sections=default_keep_sections,
        keep_sections=keep_sections,
        root_aliases=root_aliases,
        width=width,
        colorize=colorize,
    )
    set_config(config)

    _base_logger.remove()

    _sink_ids = [  # pyright: ignore[reportUnknownVariableType]
        _base_logger.add(
            sys.stderr,
            level="TRACE",
            format=record_format,  # type: ignore[arg-type]
            filter=record_filter,  # type: ignore[arg-type]
            colorize=colorize,
            backtrace=False,
            diagnose=False,
            enqueue=False,
        )
    ]


# ....................... #


def reset() -> None:
    """Reset logging sinks configured through this facade.

    Removes sinks added by :func:`configure`. Does not affect other
    loguru sinks that may have been added elsewhere.
    """
    global _sink_ids

    for sink_id in _sink_ids:
        try:
            _base_logger.remove(sink_id)
        except ValueError:
            pass

    _sink_ids = []
