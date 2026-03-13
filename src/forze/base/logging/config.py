"""Logging configuration and mutable state.

Holds the active configuration used by format/filter callbacks and by
:func:`~.facade.configure`. Configuration is global and mutable; call
:func:`~.facade.configure` to update it.
"""

from dataclasses import dataclass
from typing import Optional

from .constants import (
    DEFAULT_LEVEL,
    DEFAULT_PREFIXES,
    DEFAULT_STEP,
    DEFAULT_WIDTH,
)
from .types import KeepSectionsMap, LevelsMap, LogLevelName, RootAliasesMap

# ----------------------- #


@dataclass(slots=True, frozen=True)
class LoggingConfig:
    """Immutable snapshot of logging configuration.

    Used by format and filter callbacks. Do not construct directly;
    use :func:`~.facade.configure` instead.
    """

    level: LogLevelName = DEFAULT_LEVEL
    """Default log level when no per-namespace override applies."""

    levels: LevelsMap = None
    """Per-namespace level overrides. Longest matching prefix wins."""

    prefixes: tuple[str, ...] = DEFAULT_PREFIXES
    """Namespaces that receive indentation in formatted output."""

    step: str = DEFAULT_STEP
    """Indentation unit for nested log sections."""

    default_keep_sections: Optional[int] = None
    """Default number of logger-name segments to keep when truncating."""

    width: int = DEFAULT_WIDTH
    """Width of the logger-name column in formatted output."""

    colorize: bool = True
    """Whether to emit ANSI color codes."""

    root_aliases: RootAliasesMap = None
    """Root alias replacements for rendered logger names."""

    keep_sections: KeepSectionsMap = None
    """Per-namespace truncation of logger name segments."""


# ....................... #

# Mutable global config. Updated by configure().
_config: LoggingConfig = LoggingConfig()

# ....................... #


def get_config() -> LoggingConfig:
    """Return the current logging configuration.

    :returns: The active :class:`LoggingConfig` snapshot.
    """

    return _config


# ....................... #


def set_config(config: LoggingConfig) -> None:
    """Replace the global configuration.

    Used internally by :func:`~.facade.configure`. Prefer calling
    :func:`~.facade.configure` instead.
    """

    global _config
    _config = config
