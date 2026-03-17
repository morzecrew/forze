"""Logging configuration and level resolution.

Level patterns use fnmatch: ``*`` matches any sequence, ``?`` matches one character.
Longest matching pattern wins (most specific).
"""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Mapping, Optional, cast

if TYPE_CHECKING:
    pass

# ----------------------- #
# Types

LogLevelName = Literal["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
LogLevel = LogLevelName | int
LevelsMap = Optional[Mapping[str, LogLevelName]]

TRACE = 5
DEBUG = 10
INFO = 20
WARNING = 30
ERROR = 40
CRITICAL = 50

LEVEL_TO_NO: dict[str, int] = {
    "TRACE": TRACE,
    "DEBUG": DEBUG,
    "INFO": INFO,
    "WARNING": WARNING,
    "ERROR": ERROR,
    "CRITICAL": CRITICAL,
}
NO_TO_LEVEL: dict[int, str] = {v: k for k, v in LEVEL_TO_NO.items()}

DEFAULT_LEVEL: LogLevelName = "INFO"
DEFAULT_STEP = "  "
DEFAULT_WIDTH = 36

# ----------------------- #
# Config


@dataclass(slots=True, frozen=True)
class LoggingConfig:
    """Immutable logging configuration."""

    level: LogLevelName = DEFAULT_LEVEL
    levels: LevelsMap = None
    step: str = DEFAULT_STEP
    width: int = DEFAULT_WIDTH
    colorize: bool = False
    render_json: bool = False
    dual_output: bool = False


_config: Optional[LoggingConfig] = None


def get_config() -> LoggingConfig:
    return _config or LoggingConfig()


def set_config(config: LoggingConfig) -> None:
    global _config
    _config = config


# ----------------------- #
# Level helpers


def normalize_level(level: str | int) -> LogLevelName:
    if isinstance(level, int):
        if level not in NO_TO_LEVEL:
            raise ValueError(f"Unknown log level number: {level}")
        return cast(LogLevelName, NO_TO_LEVEL[level])
    upper = str(level).upper()
    if upper not in LEVEL_TO_NO:
        raise ValueError(f"Unknown log level name: {level}")
    return cast(LogLevelName, upper)


def level_no(level: str | int) -> int:
    if isinstance(level, int):
        return level
    return LEVEL_TO_NO.get(str(level).upper(), 20)


def _pattern_matches(pattern: str, name: str) -> bool:
    """Match name against pattern. If pattern has no wildcards, treat as prefix."""
    if "*" in pattern or "?" in pattern:
        return fnmatch.fnmatch(name, pattern)
    # No wildcards: prefix match (backward compat)
    return (
        name == pattern
        or name.startswith(f"{pattern}.")
        or name.startswith(f"{pattern}_")
    )


def effective_level_for_name(name: str) -> LogLevelName:
    """Resolve effective level for logger name using fnmatch patterns.

    Patterns use ``*`` (any sequence) and ``?`` (one char). Longest match wins.
    Patterns without wildcards match as prefix (e.g. ``forze.application`` matches
    ``forze.application.execution``).
    """
    config = get_config()
    if not config.levels:
        return config.level

    matched_pattern: str | None = None
    matched_level: LogLevelName | None = None

    for pattern, level in config.levels.items():
        if _pattern_matches(pattern, name):
            if matched_pattern is None or len(pattern) > len(matched_pattern):
                matched_pattern = pattern
                matched_level = level

    return matched_level or config.level
